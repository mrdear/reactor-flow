package fun.libx.flow.agent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Agent loop执行结果。
 */
public class AgentLoopResult {

    private final List<AgentMessage> allMessages;
    private final List<AgentMessage> newMessages;

    public AgentLoopResult(List<AgentMessage> allMessages, List<AgentMessage> newMessages) {
        this.allMessages = copyMessages(allMessages);
        this.newMessages = copyMessages(newMessages);
    }

    public List<AgentMessage> getAllMessages() {
        return allMessages;
    }

    public List<AgentMessage> getNewMessages() {
        return newMessages;
    }

    public AgentMessage getLastAssistantMessage() {
        for (int index = allMessages.size() - 1; index >= 0; index--) {
            AgentMessage message = allMessages.get(index);
            if (message.getRole() == AgentMessage.Role.ASSISTANT) {
                return message;
            }
        }
        return null;
    }

    public String getLastAssistantText() {
        AgentMessage assistant = getLastAssistantMessage();
        return assistant == null ? null : assistant.getText();
    }

    private static List<AgentMessage> copyMessages(List<AgentMessage> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(source));
    }
}
