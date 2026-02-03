package fun.libx.flow.mvc.task;

import fun.libx.flow.FlowContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.task.AbstractTaskInstance;
import fun.libx.flow.task.TaskOutputResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * @author quding
 * @since 2025/5/1
 */
@Component
public class StartTaskInstance extends AbstractTaskInstance {


    @Autowired
    public StartTaskInstance(FlowEventBus eventBus) {
        super(eventBus);
    }

    @Override
    protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
        // 处理逻辑

        return CompletableFuture.completedFuture(result);
    }

}
