package fun.libx.flow.agent;

import fun.libx.flow.FlowContext;
import fun.libx.flow.NodeContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * agent执行循环。
 *
 * 设计目标:
 * 1. 与DAG节点执行解耦，单独负责消息循环、工具调用与中断语义
 * 2. 将flow取消信号透传到模型/工具异步调用
 */
public class AgentLoopRunner {

    private static final String SKIPPED_DUE_TO_STEERING = "Skipped due to queued user message.";

    public AgentLoopResult run(List<AgentMessage> prompts,
                               List<AgentMessage> history,
                               AgentLoopConfig config,
                               NodeContext nodeContext) {
        Objects.requireNonNull(config, "config cannot be null");
        Objects.requireNonNull(nodeContext, "nodeContext cannot be null");

        AgentModelClient modelClient = Objects.requireNonNull(config.getModelClient(), "modelClient cannot be null");
        Map<String, AgentTool> toolsByName = buildToolMap(config.getTools());
        Supplier<List<AgentMessage>> steeringSupplier = config.getSteeringMessagesSupplier();
        Supplier<List<AgentMessage>> followUpSupplier = config.getFollowUpMessagesSupplier();
        AgentEventListener eventListener = config.getEventListener();

        List<AgentMessage> currentMessages = new ArrayList<>();
        if (history != null && !history.isEmpty()) {
            currentMessages.addAll(history);
        }
        List<AgentMessage> newMessages = new ArrayList<>();

        emit(eventListener, AgentLoopEvent.agentStart());
        emit(eventListener, AgentLoopEvent.turnStart());

        for (AgentMessage prompt : normalizeMessages(prompts)) {
            appendMessage(prompt, currentMessages, newMessages, eventListener);
        }

        boolean firstTurn = true;
        List<AgentMessage> pendingMessages = pollMessages(steeringSupplier);
        int turnCount = 0;

        // 外层循环：无tool-call且无steering后，检查follow-up决定是否继续
        while (true) {
            boolean hasMoreToolCalls = true;

            // 内层循环：处理assistant响应/tool调用/steering注入
            while (hasMoreToolCalls || !pendingMessages.isEmpty()) {
                List<AgentMessage> steeringAfterTools = Collections.emptyList();

                if (!firstTurn) {
                    emit(eventListener, AgentLoopEvent.turnStart());
                } else {
                    firstTurn = false;
                }

                if (!pendingMessages.isEmpty()) {
                    for (AgentMessage pending : pendingMessages) {
                        appendMessage(pending, currentMessages, newMessages, eventListener);
                    }
                    pendingMessages = Collections.emptyList();
                }

                turnCount++;
                if (turnCount > config.getMaxTurns()) {
                    throw new IllegalStateException("agent max turns exceeded: " + config.getMaxTurns());
                }

                AgentMessage assistantMessage = invokeModel(modelClient, config.getSystemPrompt(), currentMessages, toolsByName, nodeContext);
                appendMessage(assistantMessage, currentMessages, newMessages, eventListener);

                if (assistantMessage.isAssistantTerminalError()) {
                    emit(eventListener, AgentLoopEvent.turnEnd(assistantMessage));
                    emit(eventListener, AgentLoopEvent.agentEnd(newMessages));
                    return new AgentLoopResult(currentMessages, newMessages);
                }

                List<AgentToolCall> toolCalls = assistantMessage.getToolCalls();
                hasMoreToolCalls = !toolCalls.isEmpty();

                if (hasMoreToolCalls) {
                    ToolExecutionBatch batch = executeToolCalls(toolCalls, toolsByName, steeringSupplier, nodeContext, currentMessages, newMessages, eventListener);
                    steeringAfterTools = batch.steeringMessages;
                }

                emit(eventListener, AgentLoopEvent.turnEnd(assistantMessage));

                if (!steeringAfterTools.isEmpty()) {
                    pendingMessages = steeringAfterTools;
                } else {
                    pendingMessages = pollMessages(steeringSupplier);
                }
            }

            List<AgentMessage> followUps = pollMessages(followUpSupplier);
            if (!followUps.isEmpty()) {
                pendingMessages = followUps;
                continue;
            }
            break;
        }

        emit(eventListener, AgentLoopEvent.agentEnd(newMessages));
        return new AgentLoopResult(currentMessages, newMessages);
    }

    private static AgentMessage invokeModel(AgentModelClient modelClient,
                                            String systemPrompt,
                                            List<AgentMessage> currentMessages,
                                            Map<String, AgentTool> toolsByName,
                                            NodeContext nodeContext) {
        AgentModelRequest request = new AgentModelRequest(systemPrompt, currentMessages, new ArrayList<>(toolsByName.keySet()));
        AgentAsyncCall<AgentMessage> asyncCall = modelClient.generate(request, nodeContext);
        AgentMessage assistantMessage = awaitAsyncCall(asyncCall, nodeContext);

        if (assistantMessage == null) {
            throw new IllegalStateException("model returned null assistant message");
        }
        if (assistantMessage.getRole() != AgentMessage.Role.ASSISTANT) {
            throw new IllegalStateException("model must return ASSISTANT message");
        }
        return assistantMessage;
    }

    private static ToolExecutionBatch executeToolCalls(List<AgentToolCall> toolCalls,
                                                       Map<String, AgentTool> toolsByName,
                                                       Supplier<List<AgentMessage>> steeringSupplier,
                                                       NodeContext nodeContext,
                                                       List<AgentMessage> currentMessages,
                                                       List<AgentMessage> newMessages,
                                                       AgentEventListener eventListener) {
        List<AgentMessage> toolResultMessages = new ArrayList<>();
        List<AgentMessage> steeringMessages = Collections.emptyList();

        for (int index = 0; index < toolCalls.size(); index++) {
            AgentToolCall toolCall = toolCalls.get(index);

            emit(eventListener, AgentLoopEvent.toolExecutionStart(toolCall.getId(), toolCall.getName(), toolCall.getArguments()));

            AgentToolResult toolResult;
            boolean isError = false;

            try {
                AgentTool tool = toolsByName.get(toolCall.getName());
                if (tool == null) {
                    throw new IllegalStateException("tool not found: " + toolCall.getName());
                }
                AgentAsyncCall<AgentToolResult> toolAsyncCall = tool.execute(toolCall.getId(), toolCall.getArguments(), nodeContext);
                toolResult = awaitAsyncCall(toolAsyncCall, nodeContext);
                if (toolResult == null) {
                    toolResult = AgentToolResult.ofText("");
                }
            } catch (Throwable throwable) {
                toolResult = AgentToolResult.ofText(resolveThrowableMessage(throwable));
                isError = true;
            }

            emit(eventListener, AgentLoopEvent.toolExecutionEnd(toolCall.getId(), toolCall.getName(), toolResult, isError));

            AgentMessage toolResultMessage = AgentMessage.toolResult(toolCall.getId(), toolCall.getName(), toolResult.getText(), isError);
            appendMessage(toolResultMessage, currentMessages, newMessages, eventListener);
            toolResultMessages.add(toolResultMessage);

            if (steeringSupplier != null) {
                List<AgentMessage> steering = pollMessages(steeringSupplier);
                if (!steering.isEmpty()) {
                    steeringMessages = steering;
                    for (int remainingIndex = index + 1; remainingIndex < toolCalls.size(); remainingIndex++) {
                        AgentToolCall skipped = toolCalls.get(remainingIndex);
                        AgentMessage skippedMessage = skipToolCall(skipped, currentMessages, newMessages, eventListener);
                        toolResultMessages.add(skippedMessage);
                    }
                    break;
                }
            }
        }

        return new ToolExecutionBatch(toolResultMessages, steeringMessages);
    }

    private static AgentMessage skipToolCall(AgentToolCall toolCall,
                                             List<AgentMessage> currentMessages,
                                             List<AgentMessage> newMessages,
                                             AgentEventListener eventListener) {
        AgentToolResult skippedResult = AgentToolResult.ofText(SKIPPED_DUE_TO_STEERING);
        emit(eventListener, AgentLoopEvent.toolExecutionStart(toolCall.getId(), toolCall.getName(), toolCall.getArguments()));
        emit(eventListener, AgentLoopEvent.toolExecutionEnd(toolCall.getId(), toolCall.getName(), skippedResult, true));
        AgentMessage skippedMessage = AgentMessage.toolResult(toolCall.getId(), toolCall.getName(), skippedResult.getText(), true);
        appendMessage(skippedMessage, currentMessages, newMessages, eventListener);
        return skippedMessage;
    }

    private static <T> T awaitAsyncCall(AgentAsyncCall<T> asyncCall, NodeContext nodeContext) {
        if (nodeContext.isCancellationTriggered()) {
            if (asyncCall != null) {
                asyncCall.cancel();
            }
            throw new CancellationException("flow cancellation triggered");
        }
        if (asyncCall == null) {
            throw new IllegalStateException("asyncCall cannot be null");
        }

        CompletableFuture<T> future = asyncCall.future();
        if (future == null) {
            throw new IllegalStateException("asyncCall.future cannot be null");
        }

        FlowContext.CancellationRegistration registration = nodeContext.registerCancellationAction(asyncCall::cancel);
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            asyncCall.cancel();
            throw new CancellationException("agent async call interrupted");
        } catch (CancellationException e) {
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof CancellationException cancellationException) {
                throw cancellationException;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new RuntimeException(cause == null ? e : cause);
        } finally {
            registration.unregister();
        }
    }

    private static void appendMessage(AgentMessage message,
                                      List<AgentMessage> currentMessages,
                                      List<AgentMessage> newMessages,
                                      AgentEventListener eventListener) {
        emit(eventListener, AgentLoopEvent.messageStart(message));
        currentMessages.add(message);
        newMessages.add(message);
        emit(eventListener, AgentLoopEvent.messageEnd(message));
    }

    private static List<AgentMessage> pollMessages(Supplier<List<AgentMessage>> supplier) {
        if (supplier == null) {
            return Collections.emptyList();
        }
        List<AgentMessage> messages = supplier.get();
        return normalizeMessages(messages);
    }

    private static List<AgentMessage> normalizeMessages(List<AgentMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return Collections.emptyList();
        }
        List<AgentMessage> normalized = new ArrayList<>(messages.size());
        for (AgentMessage message : messages) {
            if (message != null) {
                normalized.add(message);
            }
        }
        if (normalized.isEmpty()) {
            return Collections.emptyList();
        }
        return normalized;
    }

    private static Map<String, AgentTool> buildToolMap(List<AgentTool> tools) {
        if (tools == null || tools.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, AgentTool> toolMap = new LinkedHashMap<>();
        for (AgentTool tool : tools) {
            if (tool == null || tool.name() == null || tool.name().trim().isEmpty()) {
                continue;
            }
            toolMap.put(tool.name(), tool);
        }
        return toolMap;
    }

    private static String resolveThrowableMessage(Throwable throwable) {
        if (throwable == null) {
            return "unknown tool error";
        }
        String message = throwable.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return throwable.getClass().getSimpleName();
        }
        return message;
    }

    private static void emit(AgentEventListener listener, AgentLoopEvent event) {
        if (listener == null || event == null) {
            return;
        }
        try {
            listener.onEvent(event);
        } catch (RuntimeException ignore) {
            // ignore listener errors to avoid breaking the loop
        }
    }

    private static final class ToolExecutionBatch {
        private final List<AgentMessage> toolResultMessages;
        private final List<AgentMessage> steeringMessages;

        private ToolExecutionBatch(List<AgentMessage> toolResultMessages, List<AgentMessage> steeringMessages) {
            this.toolResultMessages = toolResultMessages;
            this.steeringMessages = steeringMessages;
        }
    }
}
