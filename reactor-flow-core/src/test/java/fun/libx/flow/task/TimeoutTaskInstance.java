package fun.libx.flow.task;

import fun.libx.flow.NodeContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author quding
 * @since 2025/5/1
 */
public class TimeoutTaskInstance extends AbstractFlowTaskSyncInstance {


    public TimeoutTaskInstance(FlowEventBus eventBus) {
        super(eventBus, Executors.newFixedThreadPool(1));
    }

    @Override
    void executeSync(TaskNode taskNode, NodeContext context, TaskOutputResult result) {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
