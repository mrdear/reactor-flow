package fun.libx.flow;

import fun.libx.flow.common.CustomThreadFactory;
import fun.libx.flow.exception.NodeException;
import fun.libx.flow.model.FlowDag;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.task.FlowTaskInstance;
import fun.libx.flow.task.TaskOutputResult;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 使用CompletableFuture实现的DAG执行图
 * 功能与FlowExecuteGraph相同，但使用CompletableFuture代替Mono/Flux
 *
 * @author quding
 * @since 2025/4/27
 */
public class FlowFutureExecuteGraph {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlowFutureExecuteGraph.class);

    // 用于检测超时线程池
    static final ScheduledThreadPoolExecutor delayer;

    static {
        delayer = new ScheduledThreadPoolExecutor(1, new CustomThreadFactory("flow-delayer"));
        delayer.setRemoveOnCancelPolicy(true);
    }

    /**
     * 绑定的DAG图
     */
    private FlowDag dag;

    /**
     * 执行上下文
     */
    private FlowContext context;

    /**
     * 线程池，用于执行任务
     */
    private final ExecutorService executorService;

    /**
     * 任务分发器
     */
    private final FlowTaskEngineRouter flowTaskEngineRouter;

    /**
     * 存储已完成的节点 (仅用于记录状态和日志，不参与核心流转控制)
     */
    private final Set<String> completedNodes = ConcurrentHashMap.newKeySet();

    /**
     * 核心状态控制：存储节点当前的就绪前驱数量
     * Key: NodeId, Value: 已完成的前驱数量
     */
    private final ConcurrentHashMap<String, AtomicInteger> joinState = new ConcurrentHashMap<>();

    /**
     * 构造函数
     *
     * @param dag 要执行的DAG图
     */
    public FlowFutureExecuteGraph(FlowDag dag, FlowContext context, ExecutorService executorService, FlowTaskEngineRouter flowTaskEngineRouter) {
        this.dag = dag;
        this.context = context;
        this.executorService = executorService;
        this.flowTaskEngineRouter = flowTaskEngineRouter;
    }

    /**
     * 使用BFS方式执行DAG图
     *
     * @return 执行完成的Future
     */
    public CompletableFuture<Void> bfsExecute() {
        // 获取起始节点（没有前驱的节点）
        TaskNode startNode = dag.getStartingNode();

        // TODO 调度开始事件以及调度结束事件

        // 使用递归方式处理队列
        return executeNode(startNode);
    }

    /**
     * 处理BFS队列，支持节点的并发执行
     *
     * @return CompletableFuture<Void>
     */
    private CompletableFuture<Void> executeNode(TaskNode taskNode) {

        // 检查是否取消调度执行
        if (this.context.isCancellationTriggered()) {
            return CompletableFuture.completedFuture(null);
        }

        // 2. 准备执行
        long startTime = System.currentTimeMillis();
        FlowTaskInstance instance = flowTaskEngineRouter.router(taskNode, context);

        LOGGER.info("并发执行节点: {} 线程: {}", taskNode.getId(), Thread.currentThread().getName());

        // 3. 执行业务逻辑
        CompletableFuture<TaskOutputResult> executeFuture = executeNodeWithRetry(taskNode, instance);

        // 5. 链式处理结果与后续流转
        return executeFuture
                .handle((result, throwable) -> {
                    // 异常处理逻辑
                    if (null == throwable) {
                        return result;
                    }
                    Throwable actualThrowable = unwrapThrowable(throwable);
                    if (FlowDataKeys.NODE_IGNORE_EXCEPTION.hasTureData(taskNode)) {
                        LOGGER.warn("节点 {} 发生异常但被忽略: {}", taskNode.getId(), actualThrowable.getMessage());
                        return result;
                    }
                    // 触发取消并抛出异常
                    context.triggerCancellation();
                    throw new NodeException("handle node failed", actualThrowable);
                }).whenComplete((r, v) -> {
                    long endTime = System.currentTimeMillis();
                    LOGGER.info("节点 {} 执行完成, 耗时: {}ms", taskNode.getId(), (endTime - startTime));
                }).thenComposeAsync(v -> {
                    // 如果已取消，不再触发后续
                    if (context.isCancellationTriggered()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    // 记录完成状态
                    completedNodes.add(taskNode.getId());

                    // 6. 核心逻辑：触发后继节点
                    List<CompletableFuture<Void>> successorFutures = new ArrayList<>();

                    for (TaskNode successor : taskNode.getSuccessors()) {
                        // 获取后继节点需要的总前驱数
                        int totalPredecessors = successor.getPredecessors().size();

                        // 原子操作：增加就绪计数器
                        int readyCount = joinState.computeIfAbsent(successor.getId(), k -> new AtomicInteger(0))
                                .incrementAndGet();

                        // 只有当"就绪数 == 总数"的那个线程，才有资格触发执行
                        if (readyCount == totalPredecessors) {
                            LOGGER.info("节点 {} 前驱全部就绪 ({}/{})，触发执行", successor.getId(), readyCount, totalPredecessors);
                            successorFutures.add(executeNode(successor));
                        } else {
                            // 其他前驱还没完成，或者已经由其他线程处理，当前线程无需操作
                            LOGGER.debug("节点 {} 等待其他前驱 ({}/{})", successor.getId(), readyCount, totalPredecessors);
                        }
                    }

                    // 如果当前节点是末端节点（无后继），或者后继节点都还没这就绪
                    if (successorFutures.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    // 等待所有被触发的后续分支完成
                    return CompletableFuture.allOf(successorFutures.toArray(new CompletableFuture[0]));
                }, executorService);
    }

    /**
     * 带重试执行节点
     */
    private CompletableFuture<TaskOutputResult> executeNodeWithRetry(TaskNode taskNode, FlowTaskInstance instance) {
        int maxAttempts = FlowDataKeys.NODE_RETRY_MAX_ATTEMPTS.getDataOr(taskNode, 1);
        long waitMillis = FlowDataKeys.NODE_RETRY_WAIT_MILLIS.getDataOr(taskNode, 0L);
        int normalizedAttempts = Math.max(1, maxAttempts);
        long normalizedWaitMillis = Math.max(0L, waitMillis);

        if (normalizedAttempts <= 1) {
            return executeSingleAttempt(taskNode, instance);
        }

        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(normalizedAttempts)
                .waitDuration(Duration.ofMillis(normalizedWaitMillis))
                .retryOnException(throwable -> shouldRetry(taskNode, throwable))
                .build();
        Retry retry = Retry.of("flow-node-" + taskNode.getId(), retryConfig);
        retry.getEventPublisher().onRetry(event ->
                LOGGER.warn("节点 {} 执行失败，准备进行第 {} 次尝试", taskNode.getId(), event.getNumberOfRetryAttempts() + 1));

        Supplier<CompletionStage<TaskOutputResult>> retrySupplier = Retry.decorateCompletionStage(
                retry,
                delayer,
                () -> {
                    if (context.isCancellationTriggered()) {
                        return CompletableFuture.failedFuture(new CancellationException("flow cancellation triggered"));
                    }
                    return executeSingleAttempt(taskNode, instance);
                }
        );
        return retrySupplier.get().toCompletableFuture();
    }

    /**
     * 执行单次尝试,每次尝试都绑定独立超时
     */
    private CompletableFuture<TaskOutputResult> executeSingleAttempt(TaskNode taskNode, FlowTaskInstance instance) {
        CompletableFuture<TaskOutputResult> executeFuture = instance.execute(taskNode, context);
        ScheduledFuture<?> timeoutFuture = timeoutSchedule(taskNode, executeFuture);
        return executeFuture.whenComplete((r, e) -> timeoutFuture.cancel(false));
    }

    /**
     * 是否应该重试
     */
    private static boolean shouldRetry(TaskNode taskNode, Throwable throwable) {
        Throwable actualThrowable = unwrapThrowable(throwable);
        if (actualThrowable instanceof CancellationException) {
            return false;
        }
        if ((actualThrowable instanceof TimeoutException) && !FlowDataKeys.NODE_RETRY_ON_TIMEOUT.getDataOr(taskNode, true)) {
            return false;
        }
        return true;
    }

    /**
     * 拆包CompletableFuture链路中的包装异常
     */
    private static Throwable unwrapThrowable(Throwable throwable) {
        Throwable current = throwable;
        while ((current instanceof CompletionException) && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    /**
     * 节点超时逻辑控制
     */
    private static ScheduledFuture<?> timeoutSchedule(TaskNode node, CompletableFuture<TaskOutputResult> executeFuture) {
        Long timeout = FlowDataKeys.NODE_TIMEOUT_SECOND.getDataOr(node, 20L);

        return delayer.schedule(() -> {
            if (executeFuture.isDone()) {
                return;
            }
            executeFuture.completeExceptionally(new TimeoutException("task timeout"));
        }, timeout, TimeUnit.SECONDS);
    }

}
