package fun.libx.flow.task;

import fun.libx.flow.NodeContext;
import fun.libx.flow.model.TaskNode;

import java.util.concurrent.CompletableFuture;

/**
 * task的接口
 * @author quding
 * @since 2025/5/1
 */
public interface FlowTaskInstance {

    /**
     * 执行当前的任务
     * @param taskNode 对应的节点
     * @param context 节点执行上下文
     * @return 执行结果
     */
    CompletableFuture<TaskOutputResult> execute(TaskNode taskNode, NodeContext context);

}
