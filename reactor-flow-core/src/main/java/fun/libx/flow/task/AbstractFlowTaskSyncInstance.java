package fun.libx.flow.task;

import fun.libx.flow.FlowContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;

import java.util.concurrent.CompletableFuture;

/**
 * 针对一些不方便包装为异步任务的链使用
 * @author quding
 * @since 2025/5/1
 */
public abstract class AbstractFlowTaskSyncInstance extends AbstractTaskInstance {

    public AbstractFlowTaskSyncInstance(FlowEventBus eventBus) {
        super(eventBus);
    }

    @Override
    protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
        executeSync(taskNode, context, result);
        return CompletableFuture.completedFuture(result);
    }

    /**
     * 同步执行的逻辑
     * @param taskNode 对应的节点
     * @param context 对应的上下文
     */
    abstract void executeSync(TaskNode taskNode, FlowContext context, TaskOutputResult result);

}
