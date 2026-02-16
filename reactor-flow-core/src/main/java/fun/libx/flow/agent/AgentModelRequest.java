package fun.libx.flow.agent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 发送给模型侧的请求。
 */
public class AgentModelRequest {

    private final String systemPrompt;
    private final List<AgentMessage> messages;
    private final List<String> toolNames;

    public AgentModelRequest(String systemPrompt, List<AgentMessage> messages, List<String> toolNames) {
        this.systemPrompt = systemPrompt == null ? "" : systemPrompt;
        this.messages = copyMessages(messages);
        this.toolNames = copyToolNames(toolNames);
    }

    public String getSystemPrompt() {
        return systemPrompt;
    }

    public List<AgentMessage> getMessages() {
        return messages;
    }

    public List<String> getToolNames() {
        return toolNames;
    }

    private static List<AgentMessage> copyMessages(List<AgentMessage> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(source));
    }

    private static List<String> copyToolNames(List<String> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> copied = new ArrayList<>(source.size());
        for (String toolName : source) {
            if (toolName != null && !toolName.trim().isEmpty()) {
                copied.add(toolName);
            }
        }
        return Collections.unmodifiableList(copied);
    }
}
