package fun.libx.flow.task;

import fun.libx.flow.NodeContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;

import java.util.concurrent.CompletableFuture;

/**
 * @author quding
 * @since 2025/5/1
 */
public class StartTaskInstance extends AbstractTaskInstance {


    public StartTaskInstance(FlowEventBus eventBus) {
        super(eventBus);
    }

    @Override
    protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, NodeContext context, TaskOutputResult result) {
        return CompletableFuture.completedFuture(result);
    }

}
