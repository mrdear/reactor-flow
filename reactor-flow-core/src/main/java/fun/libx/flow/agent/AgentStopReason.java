package fun.libx.flow.agent;

/**
 * Agent assistant消息的停止原因。
 */
public enum AgentStopReason {
    STOP,
    LENGTH,
    TOOL_USE,
    ABORTED,
    ERROR
}
