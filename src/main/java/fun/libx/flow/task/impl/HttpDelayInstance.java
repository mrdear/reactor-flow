package fun.libx.flow.task.impl;

import fun.libx.flow.FlowContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.task.AbstractTaskInstance;
import fun.libx.flow.task.TaskOutputResult;

import java.util.concurrent.CompletableFuture;

/**
 * 延迟节点
 * @author quding
 * @since 2025/5/1
 */
public class HttpDelayInstance extends AbstractTaskInstance {


    public HttpDelayInstance(FlowEventBus eventBus) {
        super(eventBus);
    }

    @Override
    protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
        return null;
    }

}
