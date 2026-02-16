package fun.libx.flow.test;

import com.alibaba.fastjson2.JSONObject;
import fun.libx.flow.FlowContext;
import fun.libx.flow.FlowDataKeys;
import fun.libx.flow.FlowFutureExecuteGraph;
import fun.libx.flow.FlowTaskEngineRouter;
import fun.libx.flow.TaskType;
import fun.libx.flow.TaskTypeEnum;
import fun.libx.flow.agent.AgentAsyncCall;
import fun.libx.flow.agent.AgentMessage;
import fun.libx.flow.agent.AgentModelClient;
import fun.libx.flow.agent.AgentStopReason;
import fun.libx.flow.agent.AgentTaskInstance;
import fun.libx.flow.agent.AgentTool;
import fun.libx.flow.agent.AgentToolCall;
import fun.libx.flow.agent.AgentToolResult;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * @author quding
 * @since 2026/2/16
 */
public class AgentModeDagTest {

    @Test
    public void test_agent_node_should_execute_tools_and_write_state() {
        FlowEventBus eventBus = new FlowEventBus();
        StartTaskInstance start = new StartTaskInstance(eventBus);
        EndTaskInstance end = new EndTaskInstance(eventBus);

        AtomicInteger modelCallCount = new AtomicInteger(0);
        AtomicInteger toolCallCount = new AtomicInteger(0);

        AgentModelClient modelClient = (request, context) -> {
            int call = modelCallCount.incrementAndGet();
            if (call == 1) {
                AgentMessage firstAssistant = AgentMessage.assistant(
                        "",
                        List.of(new AgentToolCall("tool-1", "echo", Map.of("value", "hello"))),
                        AgentStopReason.TOOL_USE,
                        null
                );
                return AgentAsyncCall.fromFuture(CompletableFuture.completedFuture(firstAssistant));
            }

            AgentMessage secondAssistant = AgentMessage.assistant("done");
            return AgentAsyncCall.fromFuture(CompletableFuture.completedFuture(secondAssistant));
        };

        AgentTool echoTool = new AgentTool() {
            @Override
            public String name() {
                return "echo";
            }

            @Override
            public AgentAsyncCall<AgentToolResult> execute(String toolCallId, Map<String, Object> arguments, fun.libx.flow.NodeContext context) {
                toolCallCount.incrementAndGet();
                String value = String.valueOf(arguments.get("value"));
                return AgentAsyncCall.fromFuture(CompletableFuture.completedFuture(AgentToolResult.ofText("echo:" + value)));
            }
        };

        AgentTaskInstance agentTask = new AgentTaskInstance(eventBus, modelClient, List.of(echoTool));

        FlowTaskEngineRouter router = node -> switch (node.getId()) {
            case "start" -> start;
            case "agent" -> agentTask;
            case "end" -> end;
            default -> throw new IllegalArgumentException("unknown node id: " + node.getId());
        };

        JSONObject agentConfig = new JSONObject();
        FlowDataKeys.NODE_AGENT_PROMPT.putData(agentConfig, "run agent");
        FlowDataKeys.NODE_AGENT_HISTORY_STATE_KEY.putData(agentConfig, "agent.history");
        FlowDataKeys.NODE_AGENT_RESULT_STATE_KEY.putData(agentConfig, "agent.result");
        FlowDataKeys.NODE_AGENT_MAX_TURNS.putData(agentConfig, 8);

        FlowDag dag = new FlowDag();
        dag.addTaskNode(createTaskNode("start", TaskTypeEnum.START));
        dag.addTaskNode(createTaskNode("agent", TaskTypeEnum.AGENT, agentConfig));
        dag.addTaskNode(createTaskNode("end", TaskTypeEnum.END));
        dag.addEdge("start", "agent");
        dag.addEdge("agent", "end");

        FlowContext context = new FlowContext();
        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, context, Executors.newFixedThreadPool(2), router);
        Throwable throwable = executeAndGetThrowable(graph, 10);

        Assert.assertNull(throwable);
        Assert.assertEquals(2, modelCallCount.get());
        Assert.assertEquals(1, toolCallCount.get());
        Assert.assertEquals("done", context.getState("agent.result", String.class));

        @SuppressWarnings("unchecked")
        List<AgentMessage> history = (List<AgentMessage>) context.getState("agent.history");
        Assert.assertNotNull(history);
        Assert.assertEquals(4, history.size());
        Assert.assertEquals(AgentMessage.Role.USER, history.get(0).getRole());
        Assert.assertEquals(AgentMessage.Role.ASSISTANT, history.get(1).getRole());
        Assert.assertEquals(AgentMessage.Role.TOOL_RESULT, history.get(2).getRole());
        Assert.assertEquals(AgentMessage.Role.ASSISTANT, history.get(3).getRole());
    }

    @Test
    public void test_agent_node_should_support_steering_and_skip_remaining_tool_calls() {
        FlowEventBus eventBus = new FlowEventBus();
        StartTaskInstance start = new StartTaskInstance(eventBus);
        EndTaskInstance end = new EndTaskInstance(eventBus);

        AtomicInteger modelCallCount = new AtomicInteger(0);
        AtomicBoolean sawInterruptMessage = new AtomicBoolean(false);
        List<String> executedToolCallIds = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean steeringDelivered = new AtomicBoolean(false);

        AgentModelClient modelClient = (request, context) -> {
            int call = modelCallCount.incrementAndGet();
            if (call == 1) {
                AgentMessage firstAssistant = AgentMessage.assistant(
                        "",
                        List.of(
                                new AgentToolCall("tool-1", "echo", Map.of("value", "first")),
                                new AgentToolCall("tool-2", "echo", Map.of("value", "second"))
                        ),
                        AgentStopReason.TOOL_USE,
                        null
                );
                return AgentAsyncCall.fromFuture(CompletableFuture.completedFuture(firstAssistant));
            }

            boolean hasInterrupt = request.getMessages().stream()
                    .anyMatch(m -> m.getRole() == AgentMessage.Role.USER && "interrupt".equals(m.getText()));
            sawInterruptMessage.set(hasInterrupt);

            AgentMessage secondAssistant = AgentMessage.assistant("handled interrupt");
            return AgentAsyncCall.fromFuture(CompletableFuture.completedFuture(secondAssistant));
        };

        AgentTool echoTool = new AgentTool() {
            @Override
            public String name() {
                return "echo";
            }

            @Override
            public AgentAsyncCall<AgentToolResult> execute(String toolCallId, Map<String, Object> arguments, fun.libx.flow.NodeContext context) {
                executedToolCallIds.add(toolCallId);
                return AgentAsyncCall.fromFuture(CompletableFuture.completedFuture(AgentToolResult.ofText("ok")));
            }
        };

        Supplier<List<AgentMessage>> steeringSupplier = () -> {
            if (executedToolCallIds.size() == 1 && steeringDelivered.compareAndSet(false, true)) {
                return List.of(AgentMessage.user("interrupt"));
            }
            return Collections.emptyList();
        };

        AgentTaskInstance agentTask = new AgentTaskInstance(
                eventBus,
                modelClient,
                List.of(echoTool),
                steeringSupplier,
                null,
                null
        );

        FlowTaskEngineRouter router = node -> switch (node.getId()) {
            case "start" -> start;
            case "agent" -> agentTask;
            case "end" -> end;
            default -> throw new IllegalArgumentException("unknown node id: " + node.getId());
        };

        JSONObject agentConfig = new JSONObject();
        FlowDataKeys.NODE_AGENT_PROMPT.putData(agentConfig, "run agent");
        FlowDataKeys.NODE_AGENT_HISTORY_STATE_KEY.putData(agentConfig, "agent.history.steering");
        FlowDataKeys.NODE_AGENT_RESULT_STATE_KEY.putData(agentConfig, "agent.result.steering");
        FlowDataKeys.NODE_AGENT_MAX_TURNS.putData(agentConfig, 8);

        FlowDag dag = new FlowDag();
        dag.addTaskNode(createTaskNode("start", TaskTypeEnum.START));
        dag.addTaskNode(createTaskNode("agent", TaskTypeEnum.AGENT, agentConfig));
        dag.addTaskNode(createTaskNode("end", TaskTypeEnum.END));
        dag.addEdge("start", "agent");
        dag.addEdge("agent", "end");

        FlowContext context = new FlowContext();
        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, context, Executors.newFixedThreadPool(2), router);
        Throwable throwable = executeAndGetThrowable(graph, 10);

        Assert.assertNull(throwable);
        Assert.assertEquals(2, modelCallCount.get());
        Assert.assertEquals(1, executedToolCallIds.size());
        Assert.assertEquals("tool-1", executedToolCallIds.get(0));
        Assert.assertTrue(sawInterruptMessage.get());

        @SuppressWarnings("unchecked")
        List<AgentMessage> history = (List<AgentMessage>) context.getState("agent.history.steering");
        Assert.assertNotNull(history);

        AgentMessage skippedToolResult = history.stream()
                .filter(m -> m.getRole() == AgentMessage.Role.TOOL_RESULT && "tool-2".equals(m.getToolCallId()))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(skippedToolResult);
        Assert.assertTrue(skippedToolResult.isToolError());
        Assert.assertTrue(skippedToolResult.getText().contains("Skipped due to queued user message"));
    }

    @Test
    public void test_agent_node_should_cancel_pending_model_call_when_flow_is_cancelled() {
        FlowEventBus eventBus = new FlowEventBus();
        StartTaskInstance start = new StartTaskInstance(eventBus);
        EndTaskInstance end = new EndTaskInstance(eventBus);

        CountDownLatch modelStarted = new CountDownLatch(1);
        AtomicBoolean modelCancelled = new AtomicBoolean(false);

        AgentModelClient modelClient = (request, context) -> {
            modelStarted.countDown();
            CompletableFuture<AgentMessage> future = new CompletableFuture<>();
            Runnable cancelAction = () -> {
                modelCancelled.set(true);
                future.completeExceptionally(new CancellationException("cancelled by flow"));
            };
            return AgentAsyncCall.fromFuture(future, cancelAction);
        };

        AgentTaskInstance agentTask = new AgentTaskInstance(eventBus, modelClient);
        WaitAndFailTask failTask = new WaitAndFailTask(eventBus, modelStarted);

        FlowTaskEngineRouter router = node -> switch (node.getId()) {
            case "start" -> start;
            case "agent" -> agentTask;
            case "fail" -> failTask;
            case "end" -> end;
            default -> throw new IllegalArgumentException("unknown node id: " + node.getId());
        };

        JSONObject agentConfig = new JSONObject();
        FlowDataKeys.NODE_AGENT_PROMPT.putData(agentConfig, "blocking prompt");

        FlowDag dag = new FlowDag();
        dag.addTaskNode(createTaskNode("start", TaskTypeEnum.START));
        dag.addTaskNode(createTaskNode("agent", TaskTypeEnum.AGENT, agentConfig));
        dag.addTaskNode(createTaskNode("fail", TaskTypeEnum.EXCEPTION));
        dag.addTaskNode(createTaskNode("end", TaskTypeEnum.END));
        dag.addEdge("start", "agent");
        dag.addEdge("start", "fail");
        dag.addEdge("agent", "end");
        dag.addEdge("fail", "end");

        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, new FlowContext(), Executors.newFixedThreadPool(4), router);
        long startMillis = System.currentTimeMillis();
        Throwable throwable = executeAndGetThrowable(graph, 6);
        long cost = System.currentTimeMillis() - startMillis;

        Assert.assertNotNull(throwable);
        Assert.assertTrue(modelCancelled.get());
        Assert.assertTrue("flow should stop quickly after cancellation, cost=" + cost, cost < 3000L);
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

    private static Throwable executeAndGetThrowable(FlowFutureExecuteGraph graph, int waitSeconds) {
        AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        try {
            graph.bfsExecute().whenComplete((r, e) -> throwableRef.set(e)).get(waitSeconds, TimeUnit.SECONDS);
        } catch (Exception ignore) {
            // ignore
        }
        return throwableRef.get();
    }

    private static class WaitAndFailTask extends AbstractTaskInstance {
        private final CountDownLatch modelStarted;

        private WaitAndFailTask(FlowEventBus eventBus, CountDownLatch modelStarted) {
            super(eventBus);
            this.modelStarted = modelStarted;
        }

        @Override
        protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, fun.libx.flow.NodeContext context, TaskOutputResult result) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    modelStarted.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                throw new RuntimeException("fail branch");
            });
        }
    }
}
