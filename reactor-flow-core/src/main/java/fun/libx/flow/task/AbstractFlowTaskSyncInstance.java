package fun.libx.flow.task;

import fun.libx.flow.FlowContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 针对一些不方便包装为异步任务的链使用
 * @author quding
 * @since 2025/5/1
 */
public abstract class AbstractFlowTaskSyncInstance extends AbstractTaskInstance {

    protected ExecutorService executorService;

    public AbstractFlowTaskSyncInstance(FlowEventBus eventBus, ExecutorService executorService) {
        super(eventBus);
        this.executorService = executorService;
    }

    @Override
    protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
        // 需要异步化,杜绝同步等待
        return CompletableFuture.supplyAsync(() -> {
            executeSync(taskNode, context, result);
            return result;
        }, executorService);
    }

    /**
     * 同步执行的逻辑
     * @param taskNode 对应的节点
     * @param context 对应的上下文
     */
    abstract void executeSync(TaskNode taskNode, FlowContext context, TaskOutputResult result);

}
