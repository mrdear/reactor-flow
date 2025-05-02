package fun.libx.flow.task;

import fun.libx.flow.FlowContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;

/**
 * @author quding
 * @since 2025/5/1
 */
public class TimeoutTaskInstance extends AbstractFlowTaskSyncInstance {


    public TimeoutTaskInstance(FlowEventBus eventBus) {
        super(eventBus);
    }

    @Override
    void executeSync(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
