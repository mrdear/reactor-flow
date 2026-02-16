package fun.libx.flow.agent;

import fun.libx.flow.NodeContext;

/**
 * 模型调用抽象，负责生成assistant消息。
 */
public interface AgentModelClient {

    AgentAsyncCall<AgentMessage> generate(AgentModelRequest request, NodeContext context);
}
