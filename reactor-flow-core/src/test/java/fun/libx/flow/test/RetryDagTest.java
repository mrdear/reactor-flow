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
import fun.libx.flow.task.AbstractTaskInstance;
import fun.libx.flow.task.StartTaskInstance;
import fun.libx.flow.task.TaskOutputResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author quding
 * @since 2026/2/15
 */
public class RetryDagTest {

    @Test
    public void test_retry_success_after_failures() {
        FlowEventBus eventBus = new FlowEventBus();
        FlakyExceptionTaskInstance process = new FlakyExceptionTaskInstance(eventBus, 2);
        CountingEndTaskInstance end = new CountingEndTaskInstance(eventBus);

        FlowTaskEngineRouter.DefaultEnumRouter router = new FlowTaskEngineRouter.DefaultEnumRouter();
        router.register(TaskTypeEnum.START, new StartTaskInstance(eventBus));
        router.register(TaskTypeEnum.EXCEPTION, process);
        router.register(TaskTypeEnum.END, end);

        JSONObject processConfig = new JSONObject();
        FlowDataKeys.NODE_RETRY_MAX_ATTEMPTS.putData(processConfig, 3);
        FlowDataKeys.NODE_RETRY_WAIT_MILLIS.putData(processConfig, 10L);

        FlowDag dag = buildSimpleDag(processConfig);
        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, new FlowContext(), Executors.newFixedThreadPool(2), router);

        Throwable throwable = executeAndGetThrowable(graph, 10);
        Assert.assertNull(throwable);
        Assert.assertEquals(3, process.getAttempts());
        Assert.assertEquals(1, end.getExecuteCount());
    }

    @Test
    public void test_retry_exhausted_should_fail() {
        FlowEventBus eventBus = new FlowEventBus();
        FlakyExceptionTaskInstance process = new FlakyExceptionTaskInstance(eventBus, 3);
        CountingEndTaskInstance end = new CountingEndTaskInstance(eventBus);

        FlowTaskEngineRouter.DefaultEnumRouter router = new FlowTaskEngineRouter.DefaultEnumRouter();
        router.register(TaskTypeEnum.START, new StartTaskInstance(eventBus));
        router.register(TaskTypeEnum.EXCEPTION, process);
        router.register(TaskTypeEnum.END, end);

        JSONObject processConfig = new JSONObject();
        FlowDataKeys.NODE_RETRY_MAX_ATTEMPTS.putData(processConfig, 2);
        FlowDataKeys.NODE_RETRY_WAIT_MILLIS.putData(processConfig, 10L);

        FlowDag dag = buildSimpleDag(processConfig);
        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, new FlowContext(), Executors.newFixedThreadPool(2), router);

        Throwable throwable = executeAndGetThrowable(graph, 10);
        Assert.assertNotNull(throwable);
        Assert.assertEquals(2, process.getAttempts());
        Assert.assertEquals(0, end.getExecuteCount());
    }

    @Test
    public void test_retry_timeout_then_success() {
        FlowEventBus eventBus = new FlowEventBus();
        TimeoutThenSuccessTaskInstance process = new TimeoutThenSuccessTaskInstance(eventBus);
        CountingEndTaskInstance end = new CountingEndTaskInstance(eventBus);

        FlowTaskEngineRouter.DefaultEnumRouter router = new FlowTaskEngineRouter.DefaultEnumRouter();
        router.register(TaskTypeEnum.START, new StartTaskInstance(eventBus));
        router.register(TaskTypeEnum.EXCEPTION, process);
        router.register(TaskTypeEnum.END, end);

        JSONObject processConfig = new JSONObject();
        FlowDataKeys.NODE_TIMEOUT_SECOND.putData(processConfig, 1L);
        FlowDataKeys.NODE_RETRY_MAX_ATTEMPTS.putData(processConfig, 2);
        FlowDataKeys.NODE_RETRY_WAIT_MILLIS.putData(processConfig, 10L);

        FlowDag dag = buildSimpleDag(processConfig);
        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, new FlowContext(), Executors.newFixedThreadPool(2), router);

        Throwable throwable = executeAndGetThrowable(graph, 10);
        Assert.assertNull(throwable);
        Assert.assertEquals(2, process.getAttempts());
        Assert.assertEquals(1, end.getExecuteCount());
    }

    @Test
    public void test_retry_exhausted_but_ignore_exception() {
        FlowEventBus eventBus = new FlowEventBus();
        FlakyExceptionTaskInstance process = new FlakyExceptionTaskInstance(eventBus, 5);
        CountingEndTaskInstance end = new CountingEndTaskInstance(eventBus);

        FlowTaskEngineRouter.DefaultEnumRouter router = new FlowTaskEngineRouter.DefaultEnumRouter();
        router.register(TaskTypeEnum.START, new StartTaskInstance(eventBus));
        router.register(TaskTypeEnum.EXCEPTION, process);
        router.register(TaskTypeEnum.END, end);

        JSONObject processConfig = new JSONObject();
        FlowDataKeys.NODE_RETRY_MAX_ATTEMPTS.putData(processConfig, 2);
        FlowDataKeys.NODE_RETRY_WAIT_MILLIS.putData(processConfig, 10L);
        FlowDataKeys.NODE_IGNORE_EXCEPTION.putData(processConfig, true);

        FlowDag dag = buildSimpleDag(processConfig);
        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, new FlowContext(), Executors.newFixedThreadPool(2), router);

        Throwable throwable = executeAndGetThrowable(graph, 10);
        Assert.assertNull(throwable);
        Assert.assertEquals(2, process.getAttempts());
        Assert.assertEquals(1, end.getExecuteCount());
    }

    @Test
    public void test_retry_should_not_merge_failed_attempt_context_state() {
        FlowEventBus eventBus = new FlowEventBus();
        WriteOnFailureThenSuccessTaskInstance process = new WriteOnFailureThenSuccessTaskInstance(eventBus);
        CountingEndTaskInstance end = new CountingEndTaskInstance(eventBus);

        FlowTaskEngineRouter.DefaultEnumRouter router = new FlowTaskEngineRouter.DefaultEnumRouter();
        router.register(TaskTypeEnum.START, new StartTaskInstance(eventBus));
        router.register(TaskTypeEnum.EXCEPTION, process);
        router.register(TaskTypeEnum.END, end);

        JSONObject processConfig = new JSONObject();
        FlowDataKeys.NODE_RETRY_MAX_ATTEMPTS.putData(processConfig, 2);
        FlowDataKeys.NODE_RETRY_WAIT_MILLIS.putData(processConfig, 10L);

        FlowDag dag = buildSimpleDag(processConfig);
        FlowContext context = new FlowContext();
        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, context, Executors.newFixedThreadPool(2), router);

        Throwable throwable = executeAndGetThrowable(graph, 10);
        Assert.assertNull(throwable);
        Assert.assertEquals(2, process.getAttempts());
        Assert.assertEquals(1, end.getExecuteCount());
        Assert.assertNull(context.getState("failedOnlyState"));
    }

    private static FlowDag buildSimpleDag(JSONObject processConfig) {
        FlowDag dag = new FlowDag();
        TaskNode startNode = createTaskNode("start", TaskTypeEnum.START);
        TaskNode processNode = createTaskNode("process", TaskTypeEnum.EXCEPTION, processConfig);
        TaskNode endNode = createTaskNode("end", TaskTypeEnum.END);

        dag.addTaskNode(startNode);
        dag.addTaskNode(processNode);
        dag.addTaskNode(endNode);

        dag.addEdge("start", "process");
        dag.addEdge("process", "end");
        return dag;
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

    private static TaskNode createTaskNode(String id, TaskType type, JSONObject nodeConfig) {
        TaskNode node = new TaskNode();
        node.setId(id);
        node.setType(type);
        node.setDefineConfig(nodeConfig);
        return node;
    }

    private static class FlakyExceptionTaskInstance extends AbstractTaskInstance {
        private final AtomicInteger attempts = new AtomicInteger(0);
        private final int failTimes;

        private FlakyExceptionTaskInstance(FlowEventBus eventBus, int failTimes) {
            super(eventBus);
            this.failTimes = failTimes;
        }

        @Override
        protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
            int attempt = attempts.incrementAndGet();
            if (attempt <= failTimes) {
                throw new RuntimeException("flaky exception");
            }
            result.setResult("ok");
            return CompletableFuture.completedFuture(result);
        }

        int getAttempts() {
            return attempts.get();
        }
    }

    private static class TimeoutThenSuccessTaskInstance extends AbstractTaskInstance {
        private final AtomicInteger attempts = new AtomicInteger(0);

        private TimeoutThenSuccessTaskInstance(FlowEventBus eventBus) {
            super(eventBus);
        }

        @Override
        protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
            int attempt = attempts.incrementAndGet();
            if (attempt == 1) {
                return new CompletableFuture<>();
            }
            result.setResult("ok");
            return CompletableFuture.completedFuture(result);
        }

        int getAttempts() {
            return attempts.get();
        }
    }

    private static class CountingEndTaskInstance extends AbstractTaskInstance {
        private final AtomicInteger executeCount = new AtomicInteger(0);

        private CountingEndTaskInstance(FlowEventBus eventBus) {
            super(eventBus);
        }

        @Override
        protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
            executeCount.incrementAndGet();
            return CompletableFuture.completedFuture(result);
        }

        int getExecuteCount() {
            return executeCount.get();
        }
    }

    private static class WriteOnFailureThenSuccessTaskInstance extends AbstractTaskInstance {
        private final AtomicInteger attempts = new AtomicInteger(0);

        private WriteOnFailureThenSuccessTaskInstance(FlowEventBus eventBus) {
            super(eventBus);
        }

        @Override
        protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
            int attempt = attempts.incrementAndGet();
            if (attempt == 1) {
                context.putState("failedOnlyState", "from-first-failed-attempt");
                throw new RuntimeException("first attempt failed");
            }
            result.setResult("ok");
            return CompletableFuture.completedFuture(result);
        }

        int getAttempts() {
            return attempts.get();
        }
    }
}
