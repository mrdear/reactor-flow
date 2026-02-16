package fun.libx.flow.agent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Agent loop运行事件。
 */
public class AgentLoopEvent {

    public enum Type {
        AGENT_START,
        AGENT_END,
        TURN_START,
        TURN_END,
        MESSAGE_START,
        MESSAGE_END,
        TOOL_EXECUTION_START,
        TOOL_EXECUTION_END
    }

    private final Type type;
    private final AgentMessage message;
    private final String toolCallId;
    private final String toolName;
    private final Map<String, Object> toolArguments;
    private final AgentToolResult toolResult;
    private final boolean toolError;
    private final List<AgentMessage> messages;

    private AgentLoopEvent(Type type,
                           AgentMessage message,
                           String toolCallId,
                           String toolName,
                           Map<String, Object> toolArguments,
                           AgentToolResult toolResult,
                           boolean toolError,
                           List<AgentMessage> messages) {
        this.type = type;
        this.message = message;
        this.toolCallId = toolCallId;
        this.toolName = toolName;
        this.toolArguments = copyMap(toolArguments);
        this.toolResult = toolResult;
        this.toolError = toolError;
        this.messages = copyMessages(messages);
    }

    public static AgentLoopEvent agentStart() {
        return new AgentLoopEvent(Type.AGENT_START, null, null, null, null, null, false, null);
    }

    public static AgentLoopEvent agentEnd(List<AgentMessage> messages) {
        return new AgentLoopEvent(Type.AGENT_END, null, null, null, null, null, false, messages);
    }

    public static AgentLoopEvent turnStart() {
        return new AgentLoopEvent(Type.TURN_START, null, null, null, null, null, false, null);
    }

    public static AgentLoopEvent turnEnd(AgentMessage assistantMessage) {
        return new AgentLoopEvent(Type.TURN_END, assistantMessage, null, null, null, null, false, null);
    }

    public static AgentLoopEvent messageStart(AgentMessage message) {
        return new AgentLoopEvent(Type.MESSAGE_START, message, null, null, null, null, false, null);
    }

    public static AgentLoopEvent messageEnd(AgentMessage message) {
        return new AgentLoopEvent(Type.MESSAGE_END, message, null, null, null, null, false, null);
    }

    public static AgentLoopEvent toolExecutionStart(String toolCallId, String toolName, Map<String, Object> args) {
        return new AgentLoopEvent(Type.TOOL_EXECUTION_START, null, toolCallId, toolName, args, null, false, null);
    }

    public static AgentLoopEvent toolExecutionEnd(String toolCallId,
                                                  String toolName,
                                                  AgentToolResult toolResult,
                                                  boolean isError) {
        return new AgentLoopEvent(Type.TOOL_EXECUTION_END, null, toolCallId, toolName, null, toolResult, isError, null);
    }

    public Type getType() {
        return type;
    }

    public AgentMessage getMessage() {
        return message;
    }

    public String getToolCallId() {
        return toolCallId;
    }

    public String getToolName() {
        return toolName;
    }

    public Map<String, Object> getToolArguments() {
        return toolArguments;
    }

    public AgentToolResult getToolResult() {
        return toolResult;
    }

    public boolean isToolError() {
        return toolError;
    }

    public List<AgentMessage> getMessages() {
        return messages;
    }

    private static Map<String, Object> copyMap(Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    private static List<AgentMessage> copyMessages(List<AgentMessage> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(source));
    }
}
