package fun.libx.flow.task;

import fun.libx.flow.NodeContext;
import fun.libx.flow.FlowTaskEngineRouter;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;

import java.util.concurrent.CompletableFuture;

/**
 * @author quding
 * @since 2025/5/1
 */
public class EndTaskInstance extends AbstractTaskInstance {


    public EndTaskInstance(FlowEventBus eventBus) {
        super(eventBus);
    }

    @Override
    protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, NodeContext context, TaskOutputResult result) {
        return CompletableFuture.completedFuture(result);
    }

}
