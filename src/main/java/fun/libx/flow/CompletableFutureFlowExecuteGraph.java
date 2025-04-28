package fun.libx.flow;

import fun.libx.flow.model.FlowDag;
import fun.libx.flow.model.TaskNode;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 使用CompletableFuture实现的DAG执行图
 * 功能与FlowExecuteGraph相同，但使用CompletableFuture代替Mono/Flux
 * 
 * @author quding
 * @since 2025/4/27
 */
public class CompletableFutureFlowExecuteGraph {
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
     * 存储已完成的节点
     */
    private Set<String> completedNodes = Collections.synchronizedSet(new HashSet<>());

    /**
     * 使用队列进行BFS遍历
     * 使用ConcurrentLinkedQueue确保线程安全
     */
    private Queue<TaskNode> runningQueue = new ConcurrentLinkedQueue<>();

    /**
     * 构造函数
     * @param dag 要执行的DAG图
     */
    public CompletableFutureFlowExecuteGraph(FlowDag dag) {
        this.dag = dag;
        this.executorService = Executors.newCachedThreadPool();
    }

    /**
     * 使用BFS方式执行DAG图
     * @param context 执行上下文
     * @return 执行完成的Future
     */
    public CompletableFuture<Void> bfsExecute(FlowContext context) {
        this.context = context;

        // 获取起始节点（没有前驱的节点）
        TaskNode startNode = dag.getStartingNode();
        // 添加起始节点
        runningQueue.offer(startNode);

        // 使用递归方式处理队列
        return processQueue();
    }

    /**
     * 处理BFS队列，支持节点的并发执行
     * @return CompletableFuture<Void>
     */
    private CompletableFuture<Void> processQueue() {
        // 递归结束标识
        if (runningQueue.isEmpty()) {
            System.out.println("[DEBUG_LOG] 队列为空,忽略本次执行 ");
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
        }

        System.out.println("[DEBUG_LOG] 当前队列中的节点数量: " + currentNodes.size());

        // 遍历收集到的节点，检查哪些节点可以执行
        for (TaskNode node : currentNodes) {
            // 检查当前节点的所有前驱是否已完成
            Set<TaskNode> predecessors = node.getPredecessors();
            boolean allPredecessorsCompleted = predecessors.isEmpty() ||
                    predecessors.stream().allMatch(pred -> completedNodes.contains(pred.getId()));

            if (allPredecessorsCompleted) {
                System.out.println("[DEBUG_LOG] 节点准备好并发执行: " + node.getId());
                readyNodes.add(node);
            } else {
                // 还无法执行，重新放回队列尾部，确保下次能再次判断
                runningQueue.offer(node);
            }
        }

        // 无法执行,结束当前的future
        if (readyNodes.isEmpty()) {
            System.out.println("[DEBUG_LOG] 不满足执行条件,忽略本次执行 ");
            return CompletableFuture.completedFuture(null);
        }

        System.out.println("[DEBUG_LOG] 准备并发执行 " + readyNodes.size() + " 个节点");

        // 并发执行所有准备好的节点
        List<CompletableFuture<Void>> futures = readyNodes.stream()
                .map(node -> executeNode(node)
                        .thenCompose(v -> {
                            System.out.println("节点 " + node.getId() + " 处理完成，准备添加后继节点");
                            // 标记当前节点为已完成
                            completedNodes.add(node.getId());

                            // 将后继节点加入队列
                            for (TaskNode successor : node.getSuccessors()) {
                                System.out.println("添加后继节点到队列: " + successor.getId());
                                runningQueue.offer(successor);
                            }
                            return this.processQueue();
                        }))
                .collect(Collectors.toList());

        // 等待所有节点执行完成
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * 执行单个节点的任务
     * @param node 要执行的节点
     * @return CompletableFuture<Void>
     */
    private CompletableFuture<Void> executeNode(TaskNode node) {
        // 这里是节点执行的逻辑，可以根据实际需求实现
        // 例如，可以根据节点类型调用不同的处理逻辑
        System.out.println("[DEBUG_LOG] 并发执行节点: " + node.getId() + " 线程: " + Thread.currentThread().getName());

        // 记录开始时间
        long startTime = System.currentTimeMillis();

        // 模拟异步执行任务
        return CompletableFuture.runAsync(() -> {
            // 使用System.out确保在测试输出中可见
            System.out.println("[DEBUG_LOG] 节点 " + node.getId() + " 开始执行 线程: " + Thread.currentThread().getName() + " 时间: " + System.currentTimeMillis());

            // 模拟任务执行时间
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(500, 1000) + 500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("任务执行被中断", e);
            }

            // 使用System.out确保在测试输出中可见
            long endTime = System.currentTimeMillis();
            System.out.println("[DEBUG_LOG] 节点 " + node.getId() + " 执行完成 线程: " + Thread.currentThread().getName() + 
                    " 时间: " + endTime + " 耗时: " + (endTime - startTime) + "ms");
        }, executorService)
        // 添加错误处理
        .exceptionally(e -> {
            System.out.println("[DEBUG_LOG] 节点 " + node.getId() + " 执行失败: " + e.getMessage());
            // 记录错误但继续执行
            return null;
        });
    }

    /**
     * 关闭线程池
     */
    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }
}
