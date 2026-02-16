package fun.libx.flow.mvc.controller;

import com.alibaba.fastjson2.JSONObject;
import fun.libx.flow.FlowDataKeys;
import fun.libx.flow.FlowFutureExecuteGraph;
import fun.libx.flow.FlowTaskEngineRouter;
import fun.libx.flow.TaskType;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.FlowDag;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.mvc.model.ExtendedFlowContext;
import fun.libx.flow.mvc.task.TaskTypeEnum;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * REST controller for the flow engine.
 * Provides endpoints to create and execute flows.
 * 
 * @author quding
 * @since 2025/5/1
 */
@RestController
@RequestMapping("/api/flow")
public class FlowController {

    private static final String AGENT_PROMPT_STATE_KEY = "agent.demo.prompt";
    private static final String AGENT_RESULT_STATE_KEY = "agent.demo.result";
    private static final String AGENT_HISTORY_STATE_KEY = "agent.demo.history";

    @Resource
    private FlowTaskEngineRouter router;
    @Resource
    private FlowEventBus eventBus;
    @Resource
    private ExecutorService executorService;

    private static final FlowDag DAG;
    private static final FlowDag AGENT_DAG;
    
    static {
        DAG = new FlowDag();
        // 创建任务节点
        TaskNode startNode = createTaskNode("start", TaskTypeEnum.START);

        // 创建多个同级处理节点，这些节点将会并发执行
        TaskNode processNode1 = createTaskNode("process1", TaskTypeEnum.FAST);
        TaskNode processNode2 = createTaskNode("process2", TaskTypeEnum.DELAY);

        // 创建第二层同级节点
        TaskNode middleNode1 = createTaskNode("middle1", TaskTypeEnum.DELAY);
        TaskNode middleNode2 = createTaskNode("middle2", TaskTypeEnum.FAST);

        TaskNode joinNode = createTaskNode("join", TaskTypeEnum.FAST);
        TaskNode endNode = createTaskNode("end", TaskTypeEnum.END);

        // 添加节点到DAG
        DAG.addTaskNode(startNode);
        DAG.addTaskNode(processNode1);
        DAG.addTaskNode(processNode2);
        DAG.addTaskNode(middleNode1);
        DAG.addTaskNode(middleNode2);
        DAG.addTaskNode(joinNode);
        DAG.addTaskNode(endNode);

        // 构建DAG的边
        // 第一层：start -> process1-2 (这些节点将并发执行)
        DAG.addEdge("start", "process1");
        DAG.addEdge("start", "process2");

        // 第二层：process节点 -> middle节点 (形成第二层并发)
        DAG.addEdge("process1", "middle1");
        DAG.addEdge("process2", "middle2");

        // 第三层：middle节点 -> join
        DAG.addEdge("middle1", "join");
        DAG.addEdge("middle2", "join");

        // 最后：join -> end
        DAG.addEdge("join", "end");

        AGENT_DAG = new FlowDag();
        TaskNode agentStart = createTaskNode("agent-start", TaskTypeEnum.START);

        JSONObject agentNodeConfig = new JSONObject();
        FlowDataKeys.NODE_AGENT_PROMPT_STATE_KEY.putData(agentNodeConfig, AGENT_PROMPT_STATE_KEY);
        FlowDataKeys.NODE_AGENT_RESULT_STATE_KEY.putData(agentNodeConfig, AGENT_RESULT_STATE_KEY);
        FlowDataKeys.NODE_AGENT_HISTORY_STATE_KEY.putData(agentNodeConfig, AGENT_HISTORY_STATE_KEY);
        FlowDataKeys.NODE_AGENT_MAX_TURNS.putData(agentNodeConfig, 8);
        TaskNode agentNode = createTaskNode("agent-node", TaskTypeEnum.AGENT, agentNodeConfig);
        TaskNode agentEnd = createTaskNode("agent-end", TaskTypeEnum.END);

        AGENT_DAG.addTaskNode(agentStart);
        AGENT_DAG.addTaskNode(agentNode);
        AGENT_DAG.addTaskNode(agentEnd);
        AGENT_DAG.addEdge("agent-start", "agent-node");
        AGENT_DAG.addEdge("agent-node", "agent-end");
    }
    
    /**
     * Creates and executes a simple flow.
     * 
     * @return the result of the flow execution
     */
    @GetMapping("/execute-simple")
    public CompletableFuture<JSONObject> executeSimpleFlow() {
        // Create a flow context
        ExtendedFlowContext context = new ExtendedFlowContext();
        context.setFlowId(UUID.randomUUID().toString());

        // Create and execute the flow
        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(
                DAG, context, executorService, router);

        // Return the future directly
        return graph.bfsExecute()
                .thenCompose(r -> {
                    JSONObject response = new JSONObject();
                    response.put("flowId", context.getFlowId());
                    response.put("status", "completed");
                    response.put("message", "Flow execution completed successfully");
                    return CompletableFuture.completedFuture(response);
                });
    }

    /**
     * Runs a simple agent node in the DAG pipeline.
     */
    @GetMapping("/execute-agent")
    public CompletableFuture<JSONObject> executeAgentFlow(@RequestParam(value = "prompt", required = false) String prompt) {
        ExtendedFlowContext context = new ExtendedFlowContext();
        context.setFlowId(UUID.randomUUID().toString());

        String normalizedPrompt = (prompt == null || prompt.trim().isEmpty()) ? "summarize current flow status" : prompt;
        context.putState(AGENT_PROMPT_STATE_KEY, normalizedPrompt);

        FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(
                AGENT_DAG, context, executorService, router);

        return graph.bfsExecute().thenApply(r -> {
            JSONObject response = new JSONObject();
            response.put("flowId", context.getFlowId());
            response.put("status", "completed");
            response.put("prompt", normalizedPrompt);
            response.put("agentResult", context.getState(AGENT_RESULT_STATE_KEY, String.class));

            Object historyValue = context.getState(AGENT_HISTORY_STATE_KEY);
            int historySize = 0;
            if (historyValue instanceof java.util.List<?> listValue) {
                historySize = listValue.size();
            }
            response.put("agentHistorySize", historySize);
            return response;
        });
    }

    private static TaskNode createTaskNode(String id, TaskType type) {
        return createTaskNode(id, type, new JSONObject());
    }

    private static TaskNode createTaskNode(String id, TaskType type, JSONObject nodeConfig) {
        TaskNode node = new TaskNode();
        node.setId(id);
        node.setType(type);
        node.setDefineConfig(nodeConfig);
        return node;
    }
}
