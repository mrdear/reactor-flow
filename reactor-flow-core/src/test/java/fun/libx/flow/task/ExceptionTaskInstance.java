package fun.libx.flow.task;

import fun.libx.flow.NodeContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;

import java.util.concurrent.CompletableFuture;

/**
 * @author quding
 * @since 2025/5/1
 */
public class ExceptionTaskInstance extends AbstractTaskInstance {


    public ExceptionTaskInstance(FlowEventBus eventBus) {
        super(eventBus);
    }

    @Override
    protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, NodeContext context, TaskOutputResult result) {
        throw new RuntimeException("exception node");
    }

}
