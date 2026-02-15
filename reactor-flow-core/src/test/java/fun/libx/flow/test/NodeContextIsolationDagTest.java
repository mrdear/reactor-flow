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
import fun.libx.flow.task.StartTaskInstance;
import fun.libx.flow.task.TaskOutputResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author quding
 * @since 2026/2/15
 */
public class NodeContextIsolationDagTest {

    @Test
    public void test_node_context_isolation_and_merge() {
        FlowEventBus eventBus = new FlowEventBus();
        CountDownLatch writerUpdatedContext = new CountDownLatch(1);
        CountDownLatch writerCanFinish = new CountDownLatch(1);
        AtomicReference<String> readerObservedValue = new AtomicReference<>();

        StartTaskInstance start = new StartTaskInstance(eventBus);
        EndTaskInstance end = new EndTaskInstance(eventBus);
        IsolatedWriteTask writer = new IsolatedWriteTask(eventBus, writerUpdatedContext, writerCanFinish);
        IsolatedReadTask reader = new IsolatedReadTask(eventBus, writerUpdatedContext, writerCanFinish, readerObservedValue);

        FlowTaskEngineRouter router = node -> switch (node.getId()) {
            case "start" -> start;
            case "writer" -> writer;
            case "reader" -> reader;
            case "end" -> end;
            default -> throw new IllegalArgumentException("unknown node id: " + node.getId());
        };

        FlowDag dag = new FlowDag();
        TaskNode startNode = createTaskNode("start", TaskTypeEnum.START);
        TaskNode writerNode = createTaskNode("writer", TaskTypeEnum.EXCEPTION);
        TaskNode readerNode = createTaskNode("reader", TaskTypeEnum.TIMEOUT);
        TaskNode endNode = createTaskNode("end", TaskTypeEnum.END);
        dag.addTaskNode(startNode);
        dag.addTaskNode(writerNode);
        dag.addTaskNode(readerNode);
        dag.addTaskNode(endNode);
        dag.addEdge("start", "writer");
        dag.addEdge("start", "reader");
        dag.addEdge("writer", "end");
        dag.addEdge("reader", "end");

        FlowContext context = new FlowContext();
        context.putState("sharedValue", "root");

        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, context, Executors.newFixedThreadPool(4), router);
        Throwable throwable = executeAndGetThrowable(graph, 10);

        Assert.assertNull(throwable);
        Assert.assertEquals("root", readerObservedValue.get());
        Assert.assertEquals("writer", context.getState("sharedValue", String.class));
    }

    @Test
    public void test_mutable_state_should_not_be_shared_between_node_contexts() {
        FlowEventBus eventBus = new FlowEventBus();
        CountDownLatch writerMutatedList = new CountDownLatch(1);
        CountDownLatch writerCanFinish = new CountDownLatch(1);
        AtomicReference<Integer> readerObservedSize = new AtomicReference<>(-1);

        StartTaskInstance start = new StartTaskInstance(eventBus);
        EndTaskInstance end = new EndTaskInstance(eventBus);
        MutableListWriteTask writer = new MutableListWriteTask(eventBus, writerMutatedList, writerCanFinish);
        MutableListReadTask reader = new MutableListReadTask(eventBus, writerMutatedList, writerCanFinish, readerObservedSize);

        FlowTaskEngineRouter router = node -> switch (node.getId()) {
            case "start" -> start;
            case "writer" -> writer;
            case "reader" -> reader;
            case "end" -> end;
            default -> throw new IllegalArgumentException("unknown node id: " + node.getId());
        };

        FlowDag dag = new FlowDag();
        TaskNode startNode = createTaskNode("start", TaskTypeEnum.START);
        TaskNode writerNode = createTaskNode("writer", TaskTypeEnum.EXCEPTION);
        TaskNode readerNode = createTaskNode("reader", TaskTypeEnum.TIMEOUT);
        TaskNode endNode = createTaskNode("end", TaskTypeEnum.END);
        dag.addTaskNode(startNode);
        dag.addTaskNode(writerNode);
        dag.addTaskNode(readerNode);
        dag.addTaskNode(endNode);
        dag.addEdge("start", "writer");
        dag.addEdge("start", "reader");
        dag.addEdge("writer", "end");
        dag.addEdge("reader", "end");

        FlowContext context = new FlowContext();
        context.putState("sharedList", new ArrayList<>(List.of("root")));

        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, context, Executors.newFixedThreadPool(4), router);
        Throwable throwable = executeAndGetThrowable(graph, 10);

        Assert.assertNull(throwable);
        Assert.assertEquals(Integer.valueOf(1), readerObservedSize.get());

        @SuppressWarnings("unchecked")
        List<String> rootList = (List<String>) context.getState("sharedList");
        Assert.assertEquals(1, rootList.size());
        Assert.assertEquals("root", rootList.get(0));
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

    private static class IsolatedWriteTask extends AbstractTaskInstance {
        private final CountDownLatch writerUpdatedContext;
        private final CountDownLatch writerCanFinish;

        private IsolatedWriteTask(FlowEventBus eventBus, CountDownLatch writerUpdatedContext, CountDownLatch writerCanFinish) {
            super(eventBus);
            this.writerUpdatedContext = writerUpdatedContext;
            this.writerCanFinish = writerCanFinish;
        }

        @Override
        protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
            return CompletableFuture.supplyAsync(() -> {
                context.putState("sharedValue", "writer");
                writerUpdatedContext.countDown();
                try {
                    writerCanFinish.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return result;
            });
        }
    }

    private static class IsolatedReadTask extends AbstractTaskInstance {
        private final CountDownLatch writerUpdatedContext;
        private final CountDownLatch writerCanFinish;
        private final AtomicReference<String> readerObservedValue;

        private IsolatedReadTask(FlowEventBus eventBus,
                                 CountDownLatch writerUpdatedContext,
                                 CountDownLatch writerCanFinish,
                                 AtomicReference<String> readerObservedValue) {
            super(eventBus);
            this.writerUpdatedContext = writerUpdatedContext;
            this.writerCanFinish = writerCanFinish;
            this.readerObservedValue = readerObservedValue;
        }

        @Override
        protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    writerUpdatedContext.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                readerObservedValue.set(context.getState("sharedValue", String.class));
                writerCanFinish.countDown();
                return result;
            });
        }
    }

    private static class MutableListWriteTask extends AbstractTaskInstance {
        private final CountDownLatch writerMutatedList;
        private final CountDownLatch writerCanFinish;

        private MutableListWriteTask(FlowEventBus eventBus, CountDownLatch writerMutatedList, CountDownLatch writerCanFinish) {
            super(eventBus);
            this.writerMutatedList = writerMutatedList;
            this.writerCanFinish = writerCanFinish;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
            return CompletableFuture.supplyAsync(() -> {
                List<String> listValue = (List<String>) context.getState("sharedList");
                listValue.add("writer");
                writerMutatedList.countDown();
                try {
                    writerCanFinish.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return result;
            });
        }
    }

    private static class MutableListReadTask extends AbstractTaskInstance {
        private final CountDownLatch writerMutatedList;
        private final CountDownLatch writerCanFinish;
        private final AtomicReference<Integer> readerObservedSize;

        private MutableListReadTask(FlowEventBus eventBus,
                                    CountDownLatch writerMutatedList,
                                    CountDownLatch writerCanFinish,
                                    AtomicReference<Integer> readerObservedSize) {
            super(eventBus);
            this.writerMutatedList = writerMutatedList;
            this.writerCanFinish = writerCanFinish;
            this.readerObservedSize = readerObservedSize;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    writerMutatedList.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                List<String> listValue = (List<String>) context.getState("sharedList");
                readerObservedSize.set(listValue.size());
                writerCanFinish.countDown();
                return result;
            });
        }
    }
}
