package fun.libx.flow.model;

import fun.libx.flow.CompletableFutureFlowExecuteGraph;
import fun.libx.flow.FlowContext;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 示例代码：演示如何使用CompletableFutureFlowExecuteGraph进行BFS执行DAG
 * 特别展示同一层级节点的并发执行
 * 
 * @author quding
 * @since 2025/4/27
 */
public class CompletableFutureBfsExecutionExample {

    @Test
    public void test() throws InterruptedException, ExecutionException {
        // 创建一个DAG
        FlowDag flowDag = new FlowDag();

        // 创建任务节点
        TaskNode startNode = createTaskNode("start");

        // 创建多个同级处理节点，这些节点将会并发执行
        TaskNode processNode1 = createTaskNode("process1");
        TaskNode processNode2 = createTaskNode("process2");
        TaskNode processNode3 = createTaskNode("process3");
        TaskNode processNode4 = createTaskNode("process4");
        TaskNode processNode5 = createTaskNode("process5");

        // 创建第二层同级节点
        TaskNode middleNode1 = createTaskNode("middle1");
        TaskNode middleNode2 = createTaskNode("middle2");
        TaskNode middleNode3 = createTaskNode("middle3");

        TaskNode joinNode = createTaskNode("join");
        TaskNode endNode = createTaskNode("end");

        // 添加节点到DAG
        flowDag.addTaskNode(startNode);
        flowDag.addTaskNode(processNode1);
        flowDag.addTaskNode(processNode2);
        flowDag.addTaskNode(processNode3);
        flowDag.addTaskNode(processNode4);
        flowDag.addTaskNode(processNode5);
        flowDag.addTaskNode(middleNode1);
        flowDag.addTaskNode(middleNode2);
        flowDag.addTaskNode(middleNode3);
        flowDag.addTaskNode(joinNode);
        flowDag.addTaskNode(endNode);

        // 构建DAG的边
        // 第一层：start -> process1-5 (这些节点将并发执行)
        flowDag.addEdge("start", "process1");
        flowDag.addEdge("start", "process2");
        flowDag.addEdge("start", "process3");
        flowDag.addEdge("start", "process4");
        flowDag.addEdge("start", "process5");

        // 第二层：process节点 -> middle节点 (形成第二层并发)
        flowDag.addEdge("process1", "middle1");
        flowDag.addEdge("process2", "middle1");
        flowDag.addEdge("process3", "middle2");
        flowDag.addEdge("process4", "middle2");
        flowDag.addEdge("process5", "middle3");

        // 第三层：middle节点 -> join
        flowDag.addEdge("middle1", "join");
        flowDag.addEdge("middle2", "join");
        flowDag.addEdge("middle3", "join");

        // 最后：join -> end
        flowDag.addEdge("join", "end");

        // 验证DAG是否无环
        System.out.println("DAG是否无环: " + flowDag.isAcyclic());

        // 获取拓扑排序结果
        System.out.println("拓扑排序结果:");
        for (TaskNode node : flowDag.topologicalSort()) {
            System.out.println("- " + node.getId());
        }

        // 创建执行图并执行
        System.out.println("\n开始BFS执行DAG (使用CompletableFuture，观察同级节点的并发执行):");
        // 使用带外部完成节点集合的构造函数
        CompletableFutureFlowExecuteGraph executeGraph = new CompletableFutureFlowExecuteGraph(flowDag);
        FlowContext context = new FlowContext();

        // 添加更详细的日志
        System.out.println("[DEBUG_LOG] 开始执行DAG图");
        System.out.println("执行开始时间: " + System.currentTimeMillis());

        CompletableFuture<Void> future = executeGraph.bfsExecute(context);

        // 等待执行完成
        future.get();

        // 等待一段时间确保所有日志都被打印
        Thread.sleep(1000);

        System.out.println("执行结束时间: " + System.currentTimeMillis());
        System.out.println("\nDAG执行完成! 注意观察日志中同级节点的并发执行情况和不同的线程ID");

        // 关闭线程池
        executeGraph.shutdown();
    }

    private static TaskNode createTaskNode(String id) {
        TaskNode node = new TaskNode();
        node.setId(id);
        return node;
    }
}