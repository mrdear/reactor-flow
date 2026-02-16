package fun.libx.flow.mvc.task;

import fun.libx.flow.agent.AgentAsyncCall;
import fun.libx.flow.agent.AgentMessage;
import fun.libx.flow.agent.AgentModelClient;
import fun.libx.flow.agent.AgentStopReason;
import fun.libx.flow.agent.AgentTaskInstance;
import fun.libx.flow.agent.AgentTool;
import fun.libx.flow.agent.AgentToolCall;
import fun.libx.flow.agent.AgentToolResult;
import fun.libx.flow.event.FlowEventBus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * SpringMVC示例中的Agent节点实现。
 */
@Component
public class DemoAgentTaskInstance extends AgentTaskInstance {

    @Autowired
    public DemoAgentTaskInstance(FlowEventBus eventBus) {
        super(eventBus, buildModelClient(), List.of(buildEchoTool()));
    }

    private static AgentModelClient buildModelClient() {
        return (request, context) -> {
            boolean hasToolResult = request.getMessages().stream()
                    .anyMatch(message -> message.getRole() == AgentMessage.Role.TOOL_RESULT);

            AgentMessage assistantMessage;
            if (!hasToolResult) {
                assistantMessage = AgentMessage.assistant(
                        "",
                        List.of(new AgentToolCall("demo-tool-1", "echo_prompt", Map.of("prompt", findLatestUserText(request.getMessages())))),
                        AgentStopReason.TOOL_USE,
                        null
                );
            } else {
                String latestToolResultText = findLatestToolResultText(request.getMessages());
                assistantMessage = AgentMessage.assistant("demo-agent final: " + latestToolResultText);
            }

            return AgentAsyncCall.fromFuture(CompletableFuture.completedFuture(assistantMessage));
        };
    }

    private static AgentTool buildEchoTool() {
        return new AgentTool() {
            @Override
            public String name() {
                return "echo_prompt";
            }

            @Override
            public String description() {
                return "Echo and normalize prompt text";
            }

            @Override
            public AgentAsyncCall<AgentToolResult> execute(String toolCallId, Map<String, Object> arguments, fun.libx.flow.NodeContext context) {
                Object promptObject = arguments.get("prompt");
                String promptText = promptObject == null ? "" : promptObject.toString();
                String normalized = promptText.trim();
                if (normalized.isEmpty()) {
                    normalized = "empty prompt";
                }
                AgentToolResult result = AgentToolResult.ofText("echo_prompt => " + normalized.toUpperCase(Locale.ROOT));
                return AgentAsyncCall.fromFuture(CompletableFuture.completedFuture(result));
            }
        };
    }

    private static String findLatestUserText(List<AgentMessage> messages) {
        for (int index = messages.size() - 1; index >= 0; index--) {
            AgentMessage message = messages.get(index);
            if (message.getRole() == AgentMessage.Role.USER) {
                return message.getText();
            }
        }
        return "";
    }

    private static String findLatestToolResultText(List<AgentMessage> messages) {
        for (int index = messages.size() - 1; index >= 0; index--) {
            AgentMessage message = messages.get(index);
            if (message.getRole() == AgentMessage.Role.TOOL_RESULT) {
                return message.getText();
            }
        }
        return "";
    }
}
