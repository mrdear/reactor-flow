package fun.libx.flow.test;

import com.alibaba.fastjson2.JSONObject;
import fun.libx.flow.FlowContext;
import fun.libx.flow.FlowDataKeys;
import fun.libx.flow.FlowFutureExecuteGraph;
import fun.libx.flow.FlowTaskEngineRouter;
import fun.libx.flow.TaskType;
import fun.libx.flow.TaskTypeEnum;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.FlowDag;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.task.EndTaskInstance;
import fun.libx.flow.task.ExceptionTaskInstance;
import fun.libx.flow.task.StartTaskInstance;
import fun.libx.flow.task.TimeoutTaskInstance;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author quding
 * @since 2025/5/2
 */
public class TimeoutDagTest {

    private static FlowTaskEngineRouter.DefaultEnumRouter router = new FlowTaskEngineRouter.DefaultEnumRouter();

    static {
        // 构造节点
        FlowEventBus eventBus = new FlowEventBus();
        router.register(TaskTypeEnum.START, new StartTaskInstance(eventBus));
        router.register(TaskTypeEnum.END, new EndTaskInstance(eventBus));
        router.register(TaskTypeEnum.EXCEPTION, new ExceptionTaskInstance(eventBus));
        router.register(TaskTypeEnum.TIMEOUT, new TimeoutTaskInstance(eventBus));
    }

    @Test
    public void test_timeoutDag() {
        FlowDag dag = new FlowDag();
        // 创建任务节点
        TaskNode startNode = createTaskNode("start", TaskTypeEnum.START);

        JSONObject nodeConfig = new JSONObject();
        FlowDataKeys.NODE_TIMEOUT_SECOND.putData(nodeConfig, 2L);
        TaskNode timeoutNode1 = createTaskNode("process1", TaskTypeEnum.TIMEOUT, nodeConfig);
        TaskNode endNode = createTaskNode("end", TaskTypeEnum.END);

        // 添加节点到DAG
        dag.addTaskNode(startNode);
        dag.addTaskNode(timeoutNode1);
        dag.addTaskNode(endNode);

        dag.addEdge("start", "process1");
        dag.addEdge("process1", "end");

        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, new FlowContext(), Executors.newFixedThreadPool(1), router);
        CompletableFuture<Void> bfsExecute = graph.bfsExecute();

        AtomicReference<Throwable> throwable = new AtomicReference<>();

        try {
            bfsExecute.whenComplete((r, e) -> {
                throwable.set(e);
            }).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            // ignore
        }

        Assert.assertNotNull(throwable.get());
    }

    @Test
    public void test_exceptionDag_ignore() {
        FlowDag dag = new FlowDag();
        // 创建任务节点
        TaskNode startNode = createTaskNode("start", TaskTypeEnum.START);

        JSONObject nodeConfig = new JSONObject();
        FlowDataKeys.NODE_IGNORE_EXCEPTION.putData(nodeConfig, true);
        FlowDataKeys.NODE_TIMEOUT_SECOND.putData(nodeConfig, 2L);
        TaskNode timeNode1 = createTaskNode("process1", TaskTypeEnum.TIMEOUT, nodeConfig);
        TaskNode endNode = createTaskNode("end", TaskTypeEnum.END);

        // 添加节点到DAG
        dag.addTaskNode(startNode);
        dag.addTaskNode(timeNode1);
        dag.addTaskNode(endNode);

        dag.addEdge("start", "process1");
        dag.addEdge("process1", "end");

        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, new FlowContext(), Executors.newFixedThreadPool(1), router);
        CompletableFuture<Void> bfsExecute = graph.bfsExecute();

        AtomicReference<Throwable> throwable = new AtomicReference<>();

        try {
            bfsExecute.whenComplete((r, e) -> {
                throwable.set(e);
            }).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            // ignore
        }

        Assert.assertNull(throwable.get());
    }


    private static TaskNode createTaskNode(String id, TaskType type) {
        TaskNode node = new TaskNode();
        node.setId(id);
        node.setType(type);
        node.setDefineConfig(new JSONObject());
        return node;
    }

    private static TaskNode createTaskNode(String id, TaskType type, JSONObject nodeConfig) {
        TaskNode node = new TaskNode();
        node.setId(id);
        node.setType(type);
        node.setDefineConfig(nodeConfig);
        return node;
    }
}
