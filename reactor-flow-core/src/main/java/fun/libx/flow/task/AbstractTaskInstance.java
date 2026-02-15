package fun.libx.flow.task;

import fun.libx.flow.FlowContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.event.model.NodeFinishEvent;
import fun.libx.flow.event.model.NodeStartEvent;
import fun.libx.flow.model.TaskNode;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

/**
 * @author quding
 * @since 2025/5/1
 */
public abstract class AbstractTaskInstance implements FlowTaskInstance {

    private FlowEventBus eventBus;

    public AbstractTaskInstance(FlowEventBus eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    public CompletableFuture<TaskOutputResult> execute(TaskNode taskNode, FlowContext context) {
        // 发出开始执行事件
        NodeStartEvent event = new NodeStartEvent();
        event.setTaskNode(taskNode);
        event.setEventDate(new Date());
        eventBus.sendEvent(event);

        TaskOutputResult taskOutputResult = new TaskOutputResult();
        taskOutputResult.setTaskNode(taskNode);

        try {
            // 节点执行逻辑
            CompletableFuture<TaskOutputResult> result = internalExecute(taskNode, context, taskOutputResult);

            // 执行后事件
            return result.whenComplete((r, e) -> {
                TaskOutputResult finalResult = (r == null) ? taskOutputResult : r;
                if (e != null && finalResult.getException() == null) {
                    finalResult.setException(e);
                }

                NodeFinishEvent finishEvent = new NodeFinishEvent();
                finishEvent.setTaskNode(taskNode);
                finishEvent.setResult(finalResult);
                finishEvent.setEventDate(new Date());
                eventBus.sendEvent(finishEvent);
            });
        } catch (Exception e) {

            // 非链式结构中的错误
            NodeFinishEvent finishEvent = new NodeFinishEvent();
            finishEvent.setTaskNode(taskNode);
            taskOutputResult.setException(e);
            finishEvent.setResult(taskOutputResult);

            finishEvent.setEventDate(new Date());
            eventBus.sendEvent(finishEvent);

            // 构造异常返回
            CompletableFuture<TaskOutputResult> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    /**
     * 内部执行逻辑
     * @param taskNode 节点类
     * @param context 调度上下文
     * @return 调度future
     */
    abstract protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result);

}
