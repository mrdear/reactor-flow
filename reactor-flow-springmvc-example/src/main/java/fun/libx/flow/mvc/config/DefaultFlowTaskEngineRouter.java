package fun.libx.flow.mvc.config;

import fun.libx.flow.FlowContext;
import fun.libx.flow.FlowTaskEngineRouter;
import fun.libx.flow.TaskType;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.mvc.task.EndTaskInstance;
import fun.libx.flow.mvc.task.FastTaskInstance;
import fun.libx.flow.mvc.task.HttpDelayInstance;
import fun.libx.flow.mvc.task.StartTaskInstance;
import fun.libx.flow.mvc.task.TaskTypeEnum;
import fun.libx.flow.task.FlowTaskInstance;
import jakarta.annotation.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static fun.libx.flow.mvc.task.TaskTypeEnum.DELAY;
import static fun.libx.flow.mvc.task.TaskTypeEnum.END;
import static fun.libx.flow.mvc.task.TaskTypeEnum.FAST;
import static fun.libx.flow.mvc.task.TaskTypeEnum.START;

/**
 * Default implementation of FlowTaskEngineRouter.
 * Routes tasks to their appropriate implementations based on task type.
 *
 * @author quding
 * @since 2025/5/1
 */
@Component
public class DefaultFlowTaskEngineRouter implements FlowTaskEngineRouter {

    @Resource
    private FlowEventBus eventBus;

    @Resource
    private StartTaskInstance startTaskInstance;

    @Resource
    private EndTaskInstance endTaskInstance;

    @Resource
    private FastTaskInstance fastTaskInstance;

    @Resource
    private HttpDelayInstance httpDelayInstance;


    @Override
    public FlowTaskInstance router(TaskNode node) {
        TaskType type = node.getType();

        if (type == null) {
            throw new IllegalArgumentException("Task type cannot be null");
        }

        return switch (TaskTypeEnum.valueOf(type.name())) {
            case START -> startTaskInstance;
            case END -> endTaskInstance;
            case FAST -> fastTaskInstance;
            case DELAY -> httpDelayInstance;
            default -> throw new IllegalArgumentException("Unsupported task type: " + type);
        };
    }

    @Override
    public FlowTaskInstance router(TaskNode node, FlowContext context) {
        // Use the default router implementation for now
        // This could be extended to use context information for more sophisticated routing
        return router(node);
    }
}
