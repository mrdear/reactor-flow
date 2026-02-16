package fun.libx.flow.agent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Agent运行时消息。
 */
public class AgentMessage {

    public enum Role {
        USER,
        ASSISTANT,
        TOOL_RESULT
    }

    private final Role role;
    private final String text;
    private final long timestamp;
    private final List<AgentToolCall> toolCalls;
    private final AgentStopReason stopReason;
    private final String errorMessage;
    private final String toolCallId;
    private final String toolName;
    private final boolean toolError;

    private AgentMessage(Role role,
                         String text,
                         long timestamp,
                         List<AgentToolCall> toolCalls,
                         AgentStopReason stopReason,
                         String errorMessage,
                         String toolCallId,
                         String toolName,
                         boolean toolError) {
        this.role = Objects.requireNonNull(role, "role cannot be null");
        this.text = text == null ? "" : text;
        this.timestamp = timestamp;
        this.toolCalls = copyToolCalls(toolCalls);
        this.stopReason = stopReason;
        this.errorMessage = errorMessage;
        this.toolCallId = toolCallId;
        this.toolName = toolName;
        this.toolError = toolError;
    }

    public static AgentMessage user(String text) {
        return user(text, System.currentTimeMillis());
    }

    public static AgentMessage user(String text, long timestamp) {
        return new AgentMessage(Role.USER, text, timestamp, Collections.emptyList(), null, null, null, null, false);
    }

    public static AgentMessage assistant(String text) {
        return assistant(text, Collections.emptyList(), AgentStopReason.STOP, null, System.currentTimeMillis());
    }

    public static AgentMessage assistant(String text,
                                         List<AgentToolCall> toolCalls,
                                         AgentStopReason stopReason,
                                         String errorMessage) {
        return assistant(text, toolCalls, stopReason, errorMessage, System.currentTimeMillis());
    }

    public static AgentMessage assistant(String text,
                                         List<AgentToolCall> toolCalls,
                                         AgentStopReason stopReason,
                                         String errorMessage,
                                         long timestamp) {
        return new AgentMessage(
                Role.ASSISTANT,
                text,
                timestamp,
                toolCalls,
                stopReason == null ? AgentStopReason.STOP : stopReason,
                errorMessage,
                null,
                null,
                false);
    }

    public static AgentMessage toolResult(String toolCallId, String toolName, String text, boolean isError) {
        return toolResult(toolCallId, toolName, text, isError, System.currentTimeMillis());
    }

    public static AgentMessage toolResult(String toolCallId, String toolName, String text, boolean isError, long timestamp) {
        String normalizedToolCallId = toolCallId == null ? "" : toolCallId;
        String normalizedToolName = toolName == null ? "" : toolName;
        return new AgentMessage(
                Role.TOOL_RESULT,
                text,
                timestamp,
                Collections.emptyList(),
                null,
                null,
                normalizedToolCallId,
                normalizedToolName,
                isError);
    }

    public Role getRole() {
        return role;
    }

    public String getText() {
        return text;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public List<AgentToolCall> getToolCalls() {
        return toolCalls;
    }

    public boolean hasToolCalls() {
        return !toolCalls.isEmpty();
    }

    public AgentStopReason getStopReason() {
        return stopReason;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getToolCallId() {
        return toolCallId;
    }

    public String getToolName() {
        return toolName;
    }

    public boolean isToolError() {
        return toolError;
    }

    public boolean isAssistantTerminalError() {
        if (role != Role.ASSISTANT) {
            return false;
        }
        return stopReason == AgentStopReason.ERROR || stopReason == AgentStopReason.ABORTED;
    }

    private static List<AgentToolCall> copyToolCalls(List<AgentToolCall> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyList();
        }
        List<AgentToolCall> copied = new ArrayList<>(source.size());
        for (AgentToolCall toolCall : source) {
            if (toolCall != null) {
                copied.add(toolCall);
            }
        }
        return Collections.unmodifiableList(copied);
    }
}
