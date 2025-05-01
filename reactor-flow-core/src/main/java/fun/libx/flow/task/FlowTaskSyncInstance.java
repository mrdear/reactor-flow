package fun.libx.flow.task;

import fun.libx.flow.FlowContext;
import fun.libx.flow.model.TaskNode;

import java.util.concurrent.CompletableFuture;

/**
 * 针对一些不方便包装为异步任务的链使用
 * @author quding
 * @since 2025/5/1
 */
public interface FlowTaskSyncInstance extends FlowTaskInstance {

    /**
     * 执行当前的任务
     * @param taskNode 对应的节点
     * @param context 调度上下文
     * @return 执行结果
     */
    default CompletableFuture<TaskOutputResult> execute(TaskNode taskNode, FlowContext context) {
        return CompletableFuture.completedFuture(executeSync(taskNode, context));
    }

    /**
     * 同步执行的逻辑
     * @param taskNode 对应的节点
     * @param context 对应的上下文
     * @return 执行结果
     */
    TaskOutputResult executeSync(TaskNode taskNode, FlowContext context);

}
