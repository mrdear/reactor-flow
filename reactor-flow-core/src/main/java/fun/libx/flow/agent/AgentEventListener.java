package fun.libx.flow.agent;

/**
 * agent loop事件监听器。
 */
public interface AgentEventListener {

    void onEvent(AgentLoopEvent event);
}
