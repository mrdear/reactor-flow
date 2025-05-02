package fun.libx.flow;

import fun.libx.flow.exception.NodeException;
import fun.libx.flow.model.FlowDag;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.task.FlowTaskInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * 使用CompletableFuture实现的DAG执行图
 * 功能与FlowExecuteGraph相同，但使用CompletableFuture代替Mono/Flux
 *
 * @author quding
 * @since 2025/4/27
 */
public class CompletableFutureFlowExecuteGraph {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompletableFutureFlowExecuteGraph.class);

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
    private ExecutorService executorService;

    /**
     * 任务分发器
     */
    private FlowTaskEngineRouter flowTaskEngineRouter;

    /**
     * 存储已完成的节点
     */
    private Set<String> completedNodes = ConcurrentHashMap.newKeySet();

    /**
     * 存储已添加到队列的节点ID，防止重复添加
     */
    private Set<String> queuedNodes = ConcurrentHashMap.newKeySet();

    /**
     * 使用队列进行BFS遍历
     * 使用ConcurrentLinkedQueue确保线程安全
     */
    private Queue<TaskNode> runningQueue = new ConcurrentLinkedQueue<>();

    /**
     * 构造函数
     *
     * @param dag 要执行的DAG图
     */
    public CompletableFutureFlowExecuteGraph(FlowDag dag, FlowContext context, ExecutorService executorService, FlowTaskEngineRouter flowTaskEngineRouter) {
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
        // 添加起始节点
        runningQueue.offer(startNode);
        queuedNodes.add(startNode.getId());

        // TODO 调度开始事件以及调度结束事件

        // 使用递归方式处理队列
        return processQueue();
    }

    /**
     * 处理BFS队列，支持节点的并发执行
     *
     * @return CompletableFuture<Void>
     */
    private CompletableFuture<Void> processQueue() {

        // 检查是否取消调度执行
        if (this.context.isCancellationTriggered()) {
            return CompletableFuture.completedFuture(null);
        }

        // 递归结束标识
        if (runningQueue.isEmpty()) {
            LOGGER.info("队列为空,忽略本次执行");
            return CompletableFuture.completedFuture(null);
        }

        // 收集当前队列中所有可以执行的节点
        List<TaskNode> readyNodes = new ArrayList<>();

        // 遍历队列中的所有节点，找出所有可以执行的节点
        // 使用临时列表收集当前队列中的所有节点，避免在遍历过程中队列大小变化导致的问题
        List<TaskNode> currentNodes = new ArrayList<>();
        TaskNode currentNode;
        while ((currentNode = runningQueue.poll()) != null) {
            currentNodes.add(currentNode);
            // 从queuedNodes中移除，因为节点已经从队列中取出
            queuedNodes.remove(currentNode.getId());
        }

        LOGGER.info("当前队列中的节点数量: {}", currentNodes.size());

        // 遍历收集到的节点，检查哪些节点可以执行
        for (TaskNode node : currentNodes) {
            // 检查当前节点的所有前驱是否已完成
            Set<TaskNode> predecessors = node.getPredecessors();
            boolean allPredecessorsCompleted = predecessors.isEmpty() ||
                    predecessors.stream().allMatch(pred -> completedNodes.contains(pred.getId()));

            if (allPredecessorsCompleted) {
                LOGGER.info("节点准备好并发执行: {}", node.getId());
                readyNodes.add(node);
            }
        }

        // 无法执行,结束当前的future
        if (readyNodes.isEmpty()) {
            LOGGER.info("不满足执行条件,忽略本次执行");
            return CompletableFuture.completedFuture(null);
        }

        LOGGER.info("准备并发执行 {} 个节点", readyNodes.size());

        // 并发执行所有准备好的节点
        List<CompletableFuture<Void>> futures = readyNodes.stream()
                .map(node -> executeNode(node)
                        .thenComposeAsync(v -> {
                            // 检查是否已经取消
                            if (context.isCancellationTriggered()) {
                                return CompletableFuture.completedFuture(null);
                            }

                            LOGGER.info("节点 {} 处理完成，准备添加后继节点", node.getId());
                            // 标记当前节点为已完成
                            completedNodes.add(node.getId());

                            // 将后继节点加入队列
                            for (TaskNode successor : node.getSuccessors()) {
                                String successorId = successor.getId();
                                // 使用原子操作检查并添加节点，防止并发问题
                                if (!completedNodes.contains(successorId) && queuedNodes.add(successorId)) {
                                    LOGGER.info("添加后继节点到队列: {}", successorId);
                                    runningQueue.offer(successor);
                                } else {
                                    LOGGER.info("跳过已在队列或已完成的节点: {}", successorId);
                                }
                            }
                            return this.processQueue();
                        }, executorService))
                .toList();

        // 等待所有节点执行完成
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * 执行单个节点的任务
     *
     * @param node 要执行的节点
     * @return CompletableFuture<Void>
     */
    private CompletableFuture<?> executeNode(TaskNode node) {
        // 检查是否已经取消
        if (context.isCancellationTriggered()) {
            return CompletableFuture.completedFuture(null);
        }

        // 记录开始时间
        long startTime = System.currentTimeMillis();

        // 寻找实例
        FlowTaskInstance instance = flowTaskEngineRouter.router(node, context);

        // 直接调用task instance的execute方法
        LOGGER.info("并发执行节点: {} 线程: {}", node.getId(), Thread.currentThread().getName());
        return instance.execute(node, context)
                .handle((result, throwable) -> {
                    // 正常情况
                    if (null == throwable) {
                        return result;
                    }

                    // 异常: 检查节点是否允许忽略异常
                    if (FlowDataKeys.NODE_IGNORE_EXCEPTION.hasTureData(node)) {
                        return result;
                    }

                    // 未配置,则取消调度,继续传递异常
                    context.triggerCancellation();
                    throw new NodeException("handle node failed", throwable);
                })
                .whenComplete((r, v) -> {
                    // 记录执行完成时间和耗时
                    long endTime = System.currentTimeMillis();
                    LOGGER.info("节点 {} 执行完成,结果: {} 线程: {} 时间: {} 耗时: {}ms",
                            node.getId(), null == v, Thread.currentThread().getName(), endTime, (endTime - startTime));
                });
    }

}
