package fun.libx.flow.agent;

import fun.libx.flow.NodeContext;

import java.util.Map;

/**
 * agent工具定义。
 */
public interface AgentTool {

    String name();

    default String description() {
        return "";
    }

    AgentAsyncCall<AgentToolResult> execute(String toolCallId, Map<String, Object> arguments, NodeContext context);
}
