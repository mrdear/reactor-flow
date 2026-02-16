package fun.libx.flow.agent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Agent loop配置。
 */
public class AgentLoopConfig {

    private String systemPrompt = "";
    private AgentModelClient modelClient;
    private List<AgentTool> tools = Collections.emptyList();
    private Supplier<List<AgentMessage>> steeringMessagesSupplier;
    private Supplier<List<AgentMessage>> followUpMessagesSupplier;
    private AgentEventListener eventListener;
    private int maxTurns = 64;

    public String getSystemPrompt() {
        return systemPrompt;
    }

    public AgentLoopConfig setSystemPrompt(String systemPrompt) {
        this.systemPrompt = systemPrompt == null ? "" : systemPrompt;
        return this;
    }

    public AgentModelClient getModelClient() {
        return modelClient;
    }

    public AgentLoopConfig setModelClient(AgentModelClient modelClient) {
        this.modelClient = modelClient;
        return this;
    }

    public List<AgentTool> getTools() {
        return tools;
    }

    public AgentLoopConfig setTools(List<AgentTool> tools) {
        this.tools = copyTools(tools);
        return this;
    }

    public Supplier<List<AgentMessage>> getSteeringMessagesSupplier() {
        return steeringMessagesSupplier;
    }

    public AgentLoopConfig setSteeringMessagesSupplier(Supplier<List<AgentMessage>> steeringMessagesSupplier) {
        this.steeringMessagesSupplier = steeringMessagesSupplier;
        return this;
    }

    public Supplier<List<AgentMessage>> getFollowUpMessagesSupplier() {
        return followUpMessagesSupplier;
    }

    public AgentLoopConfig setFollowUpMessagesSupplier(Supplier<List<AgentMessage>> followUpMessagesSupplier) {
        this.followUpMessagesSupplier = followUpMessagesSupplier;
        return this;
    }

    public AgentEventListener getEventListener() {
        return eventListener;
    }

    public AgentLoopConfig setEventListener(AgentEventListener eventListener) {
        this.eventListener = eventListener;
        return this;
    }

    public int getMaxTurns() {
        return maxTurns;
    }

    public AgentLoopConfig setMaxTurns(int maxTurns) {
        if (maxTurns <= 0) {
            throw new IllegalArgumentException("maxTurns must be greater than 0");
        }
        this.maxTurns = maxTurns;
        return this;
    }

    private static List<AgentTool> copyTools(List<AgentTool> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyList();
        }
        List<AgentTool> copied = new ArrayList<>(source.size());
        for (AgentTool tool : source) {
            if (tool != null) {
                copied.add(tool);
            }
        }
        return Collections.unmodifiableList(copied);
    }
}
