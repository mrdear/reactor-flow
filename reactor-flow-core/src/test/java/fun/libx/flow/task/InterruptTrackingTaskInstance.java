package fun.libx.flow.task;

import fun.libx.flow.FlowContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author quding
 * @since 2026/2/15
 */
public class InterruptTrackingTaskInstance extends AbstractFlowTaskSyncInstance {

    private final CountDownLatch started = new CountDownLatch(1);
    private final AtomicBoolean interrupted = new AtomicBoolean(false);

    public InterruptTrackingTaskInstance(FlowEventBus eventBus) {
        super(eventBus, Executors.newFixedThreadPool(1));
    }

    @Override
    void executeSync(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
        started.countDown();
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            interrupted.set(true);
            Thread.currentThread().interrupt();
        }
    }

    public boolean awaitStarted(long timeout, TimeUnit unit) throws InterruptedException {
        return started.await(timeout, unit);
    }

    public boolean wasInterrupted() {
        return interrupted.get();
    }
}
