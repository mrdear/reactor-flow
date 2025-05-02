package fun.libx.flow.mvc.controller;

import com.alibaba.fastjson2.JSONObject;
import fun.libx.flow.CompletableFutureFlowExecuteGraph;
import fun.libx.flow.FlowTaskEngineRouter;
import fun.libx.flow.TaskType;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.FlowDag;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.mvc.model.ExtendedFlowContext;
import fun.libx.flow.mvc.task.TaskTypeEnum;
import jakarta.annotation.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
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

    @Resource
    private FlowTaskEngineRouter router;
    @Resource
    private FlowEventBus eventBus;
    @Resource
    private ExecutorService executorService;

    private static final FlowDag DAG;
    
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
        // 第一层：start -> process1-5 (这些节点将并发执行)
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
        CompletableFutureFlowExecuteGraph graph = new CompletableFutureFlowExecuteGraph(
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

    private static TaskNode createTaskNode(String id, TaskType type) {
        TaskNode node = new TaskNode();
        node.setId(id);
        node.setType(type);
        return node;
    }
}
