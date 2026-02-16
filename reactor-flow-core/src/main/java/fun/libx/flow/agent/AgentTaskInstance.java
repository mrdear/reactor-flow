package fun.libx.flow.agent;

import fun.libx.flow.FlowDataKeys;
import fun.libx.flow.NodeContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.task.AbstractTaskInstance;
import fun.libx.flow.task.TaskOutputResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.concurrent.CompletableFuture;

/**
 * 内置Agent节点执行实例。
 */
public class AgentTaskInstance extends AbstractTaskInstance {

    private static final int DEFAULT_MAX_TURNS = 64;
    private static final String HISTORY_KEY_PREFIX = "agent.history.";
    private static final String RESULT_KEY_PREFIX = "agent.result.";

    private final AgentModelClient modelClient;
    private final List<AgentTool> tools;
    private final Supplier<List<AgentMessage>> steeringMessagesSupplier;
    private final Supplier<List<AgentMessage>> followUpMessagesSupplier;
    private final AgentEventListener eventListener;
    private final AgentLoopRunner loopRunner;

    public AgentTaskInstance(FlowEventBus eventBus, AgentModelClient modelClient) {
        this(eventBus, modelClient, Collections.emptyList());
    }

    public AgentTaskInstance(FlowEventBus eventBus, AgentModelClient modelClient, List<AgentTool> tools) {
        this(eventBus, modelClient, tools, null, null, null);
    }

    public AgentTaskInstance(FlowEventBus eventBus,
                             AgentModelClient modelClient,
                             List<AgentTool> tools,
                             Supplier<List<AgentMessage>> steeringMessagesSupplier,
                             Supplier<List<AgentMessage>> followUpMessagesSupplier,
                             AgentEventListener eventListener) {
        this(eventBus, modelClient, tools, steeringMessagesSupplier, followUpMessagesSupplier, eventListener, new AgentLoopRunner());
    }

    AgentTaskInstance(FlowEventBus eventBus,
                      AgentModelClient modelClient,
                      List<AgentTool> tools,
                      Supplier<List<AgentMessage>> steeringMessagesSupplier,
                      Supplier<List<AgentMessage>> followUpMessagesSupplier,
                      AgentEventListener eventListener,
                      AgentLoopRunner loopRunner) {
        super(eventBus);
        this.modelClient = Objects.requireNonNull(modelClient, "modelClient cannot be null");
        this.tools = tools == null ? Collections.emptyList() : List.copyOf(tools);
        this.steeringMessagesSupplier = steeringMessagesSupplier;
        this.followUpMessagesSupplier = followUpMessagesSupplier;
        this.eventListener = eventListener;
        this.loopRunner = Objects.requireNonNull(loopRunner, "loopRunner cannot be null");
    }

    @Override
    protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, NodeContext context, TaskOutputResult result) {
        try {
            List<AgentMessage> history = readHistory(taskNode, context);
            String prompt = resolvePrompt(taskNode, context);

            List<AgentMessage> prompts = new ArrayList<>();
            if (prompt != null && !prompt.trim().isEmpty()) {
                prompts.add(AgentMessage.user(prompt));
            }

            if (prompts.isEmpty() && history.isEmpty()) {
                throw new IllegalArgumentException("agent prompt and history cannot both be empty");
            }

            int maxTurns = FlowDataKeys.NODE_AGENT_MAX_TURNS.getDataOr(taskNode, DEFAULT_MAX_TURNS);
            AgentLoopConfig loopConfig = new AgentLoopConfig()
                    .setSystemPrompt(FlowDataKeys.NODE_AGENT_SYSTEM_PROMPT.getDataOr(taskNode, ""))
                    .setModelClient(modelClient)
                    .setTools(tools)
                    .setMaxTurns(Math.max(1, maxTurns))
                    .setSteeringMessagesSupplier(steeringMessagesSupplier)
                    .setFollowUpMessagesSupplier(followUpMessagesSupplier)
                    .setEventListener(eventListener);

            AgentLoopResult loopResult = loopRunner.run(prompts, history, loopConfig, context);

            String historyStateKey = resolveHistoryStateKey(taskNode);
            String resultStateKey = resolveResultStateKey(taskNode);
            context.putState(historyStateKey, loopResult.getAllMessages());
            context.putState(resultStateKey, loopResult.getLastAssistantText());

            result.setResult(loopResult.getLastAssistantText());
            return CompletableFuture.completedFuture(result);
        } catch (Throwable throwable) {
            CompletableFuture<TaskOutputResult> future = new CompletableFuture<>();
            future.completeExceptionally(throwable);
            return future;
        }
    }

    @SuppressWarnings("unchecked")
    private static List<AgentMessage> readHistory(TaskNode taskNode, NodeContext context) {
        String historyStateKey = resolveHistoryStateKey(taskNode);
        Object historyValue = context.getState(historyStateKey);
        if (!(historyValue instanceof List<?> listValue) || listValue.isEmpty()) {
            return Collections.emptyList();
        }
        List<AgentMessage> history = new ArrayList<>();
        for (Object item : listValue) {
            if (item instanceof AgentMessage agentMessage) {
                history.add(agentMessage);
            }
        }
        return history;
    }

    private static String resolvePrompt(TaskNode taskNode, NodeContext context) {
        String promptStateKey = FlowDataKeys.NODE_AGENT_PROMPT_STATE_KEY.getData(taskNode);
        if (promptStateKey != null && !promptStateKey.trim().isEmpty()) {
            Object promptStateValue = context.getState(promptStateKey);
            if (promptStateValue instanceof String promptText) {
                return promptText;
            }
            if (promptStateValue != null) {
                return promptStateValue.toString();
            }
        }
        return FlowDataKeys.NODE_AGENT_PROMPT.getDataOr(taskNode, "");
    }

    private static String resolveHistoryStateKey(TaskNode taskNode) {
        return FlowDataKeys.NODE_AGENT_HISTORY_STATE_KEY.getDataOr(taskNode, HISTORY_KEY_PREFIX + taskNode.getId());
    }

    private static String resolveResultStateKey(TaskNode taskNode) {
        return FlowDataKeys.NODE_AGENT_RESULT_STATE_KEY.getDataOr(taskNode, RESULT_KEY_PREFIX + taskNode.getId());
    }
}
