package fun.libx.flow.task;

import fun.libx.flow.FlowContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

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
        CompletableFuture<TaskOutputResult> future = new CompletableFuture<>();
        AtomicBoolean completedByWorker = new AtomicBoolean(false);
        Future<?> runningTask = executorService.submit(() -> {
            try {
                if (context.isCancellationTriggered()) {
                    throw new CancellationException("flow cancellation triggered");
                }
                executeSync(taskNode, context, result);
                completedByWorker.set(true);
                future.complete(result);
            } catch (Throwable throwable) {
                completedByWorker.set(true);
                future.completeExceptionally(throwable);
            }
        });

        FlowContext.CancellationRegistration cancellationRegistration = context.registerCancellationAction(() -> {
            runningTask.cancel(true);
            future.completeExceptionally(new CancellationException("flow cancellation triggered"));
        });

        future.whenComplete((r, e) -> {
            cancellationRegistration.unregister();
            if (!completedByWorker.get() && !runningTask.isDone()) {
                runningTask.cancel(true);
            }
        });
        return future;
    }

    /**
     * 同步执行的逻辑
     * @param taskNode 对应的节点
     * @param context 对应的上下文
     */
    abstract void executeSync(TaskNode taskNode, FlowContext context, TaskOutputResult result);

}
