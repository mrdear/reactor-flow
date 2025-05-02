package fun.libx.flow;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author quding
 * @since 2025/4/27
 */
public class FlowContext {
    /**
     * 代表取消状态
     */
    private final AtomicBoolean cancellationTriggered = new AtomicBoolean(false);

    /**
     * 获取取消状态
     */
    public boolean isCancellationTriggered() {
        return cancellationTriggered.get();
    }

    /**
     * 触发取消
     */
    public boolean triggerCancellation() {
        // 尝试触发取消，如果已触发则返回 false
        if (cancellationTriggered.compareAndSet(false, true)) {
            return true; // 首次触发
        }
        return true; // 已经被其他节点触发了
    }


}
