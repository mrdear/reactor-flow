package fun.libx.flow.test;

import com.alibaba.fastjson2.JSONObject;
import fun.libx.flow.FlowContext;
import fun.libx.flow.FlowFutureExecuteGraph;
import fun.libx.flow.FlowTaskEngineRouter;
import fun.libx.flow.TaskType;
import fun.libx.flow.TaskTypeEnum;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.FlowDag;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.task.AbstractTaskInstance;
import fun.libx.flow.task.EndTaskInstance;
import fun.libx.flow.task.InterruptTrackingTaskInstance;
import fun.libx.flow.task.StartTaskInstance;
import fun.libx.flow.task.TaskOutputResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author quding
 * @since 2026/2/15
 */
public class CancellationDagTest {

    @Test
    public void test_exception_branch_should_cancel_running_sync_node() {
        FlowEventBus eventBus = new FlowEventBus();
        StartTaskInstance start = new StartTaskInstance(eventBus);
        EndTaskInstance end = new EndTaskInstance(eventBus);
        InterruptTrackingTaskInstance slowNode = new InterruptTrackingTaskInstance(eventBus);
        WaitAndFailTask failNode = new WaitAndFailTask(eventBus, slowNode);

        FlowTaskEngineRouter router = node -> switch (node.getId()) {
            case "start" -> start;
            case "slow" -> slowNode;
            case "fail" -> failNode;
            case "end" -> end;
            default -> throw new IllegalArgumentException("unknown node id: " + node.getId());
        };

        FlowDag dag = new FlowDag();
        TaskNode startNode = createTaskNode("start", TaskTypeEnum.START);
        TaskNode slowTaskNode = createTaskNode("slow", TaskTypeEnum.TIMEOUT);
        TaskNode failTaskNode = createTaskNode("fail", TaskTypeEnum.EXCEPTION);
        TaskNode endNode = createTaskNode("end", TaskTypeEnum.END);
        dag.addTaskNode(startNode);
        dag.addTaskNode(slowTaskNode);
        dag.addTaskNode(failTaskNode);
        dag.addTaskNode(endNode);
        dag.addEdge("start", "slow");
        dag.addEdge("start", "fail");
        dag.addEdge("slow", "end");
        dag.addEdge("fail", "end");

        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, new FlowContext(), Executors.newFixedThreadPool(4), router);

        long startMillis = System.currentTimeMillis();
        Throwable throwable = executeAndGetThrowable(graph, 6);
        long cost = System.currentTimeMillis() - startMillis;

        Assert.assertNotNull(throwable);
        Assert.assertTrue("flow should stop quickly after cancellation, cost=" + cost, cost < 3000L);
        Assert.assertTrue("running sync task should be interrupted after cancellation", slowNode.wasInterrupted());
    }

    private static Throwable executeAndGetThrowable(FlowFutureExecuteGraph graph, int waitSeconds) {
        AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        try {
            graph.bfsExecute().whenComplete((r, e) -> throwableRef.set(e)).get(waitSeconds, TimeUnit.SECONDS);
        } catch (Exception ignore) {
            // ignore
        }
        return throwableRef.get();
    }

    private static TaskNode createTaskNode(String id, TaskType type) {
        TaskNode node = new TaskNode();
        node.setId(id);
        node.setType(type);
        node.setDefineConfig(new JSONObject());
        return node;
    }

    private static class WaitAndFailTask extends AbstractTaskInstance {
        private final InterruptTrackingTaskInstance slowNode;

        private WaitAndFailTask(FlowEventBus eventBus, InterruptTrackingTaskInstance slowNode) {
            super(eventBus);
            this.slowNode = slowNode;
        }

        @Override
        protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    slowNode.awaitStarted(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                throw new RuntimeException("fail branch");
            });
        }
    }
}
