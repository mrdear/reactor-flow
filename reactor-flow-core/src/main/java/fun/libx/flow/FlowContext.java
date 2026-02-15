package fun.libx.flow;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author quding
 * @since 2025/4/27
 */
public class FlowContext {

    private static final String ROOT_SCOPE = "__root__";

    /**
     * flow级别共享取消状态与取消动作注册表
     */
    private final CancellationRegistry cancellationRegistry;

    /**
     * 当前context归属范围(root/nodeId)
     */
    private final String scope;

    /**
     * context状态数据
     */
    private final ConcurrentHashMap<String, Object> state;

    /**
     * 当前context改动过的key，用于merge增量合并
     */
    private final Set<String> dirtyStateKeys = ConcurrentHashMap.newKeySet();

    /**
     * 创建root context
     */
    public FlowContext() {
        this(new CancellationRegistry(), ROOT_SCOPE, new ConcurrentHashMap<>());
    }

    /**
     * 创建子context
     */
    protected FlowContext(FlowContext parent, String scope) {
        this(
                parent.cancellationRegistry,
                normalizeScope(scope),
                new ConcurrentHashMap<>(parent.state)
        );
        parent.copyCustomStateTo(this);
    }

    private FlowContext(CancellationRegistry cancellationRegistry, String scope, ConcurrentHashMap<String, Object> state) {
        this.cancellationRegistry = cancellationRegistry;
        this.scope = scope;
        this.state = state;
    }

    private static String normalizeScope(String scope) {
        if (scope == null || scope.trim().isEmpty()) {
            return ROOT_SCOPE;
        }
        return scope;
    }

    /**
     * 为节点创建隔离context
     */
    public FlowContext forkForNode(String nodeId) {
        return new FlowContext(this, nodeId);
    }

    /**
     * 子context执行完成后合并回主context
     */
    public synchronized void mergeFrom(FlowContext nodeContext) {
        if (nodeContext == null || nodeContext == this) {
            return;
        }

        for (String key : nodeContext.dirtyStateKeys) {
            if (nodeContext.state.containsKey(key)) {
                this.state.put(key, nodeContext.state.get(key));
            } else {
                this.state.remove(key);
            }
        }
        mergeCustomStateFrom(nodeContext);
    }

    /**
     * 可覆盖: 将子类扩展字段从当前context复制到子context
     */
    protected void copyCustomStateTo(FlowContext childContext) {
        // no-op
    }

    /**
     * 可覆盖: 合并子类扩展字段
     */
    protected void mergeCustomStateFrom(FlowContext childContext) {
        // no-op
    }

    public String getScope() {
        return scope;
    }

    /**
     * 写入状态
     */
    public void putState(String key, Object value) {
        String normalizedKey = Objects.requireNonNull(key, "state key cannot be null");
        state.put(normalizedKey, value);
        dirtyStateKeys.add(normalizedKey);
    }

    /**
     * 读取状态
     */
    public Object getState(String key) {
        if (key == null) {
            return null;
        }
        return state.get(key);
    }

    /**
     * 按类型读取状态
     */
    public <T> T getState(String key, Class<T> type) {
        Object value = getState(key);
        if (value == null) {
            return null;
        }
        if (!type.isInstance(value)) {
            throw new ClassCastException(
                    "state key '" + key + "' expected " + type.getName() + " but got " + value.getClass().getName());
        }
        return type.cast(value);
    }

    /**
     * 读取状态，不存在则返回默认值
     */
    public <T> T getStateOrDefault(String key, Class<T> type, T defaultValue) {
        T value = getState(key, type);
        return value == null ? defaultValue : value;
    }

    /**
     * 删除状态
     */
    public Object removeState(String key) {
        if (key == null) {
            return null;
        }
        dirtyStateKeys.add(key);
        return state.remove(key);
    }

    /**
     * 获取状态快照
     */
    public Map<String, Object> snapshotState() {
        return Collections.unmodifiableMap(new ConcurrentHashMap<>(state));
    }

    /**
     * 获取取消状态
     */
    public boolean isCancellationTriggered() {
        return cancellationRegistry.isCancellationTriggered();
    }

    /**
     * 触发取消
     */
    public boolean triggerCancellation() {
        return cancellationRegistry.triggerCancellation();
    }

    /**
     * 注册取消回调。触发取消时会执行回调（用于中断线程/取消IO）
     */
    public CancellationRegistration registerCancellationAction(Runnable cancellationAction) {
        return cancellationRegistry.registerCancellationAction(cancellationAction);
    }

    public interface CancellationRegistration {
        void unregister();
    }

    private static final class CancellationRegistry {
        private final AtomicBoolean cancellationTriggered = new AtomicBoolean(false);
        private final ConcurrentHashMap<String, Runnable> cancellationActions = new ConcurrentHashMap<>();

        private boolean isCancellationTriggered() {
            return cancellationTriggered.get();
        }

        private CancellationRegistration registerCancellationAction(Runnable cancellationAction) {
            if (cancellationAction == null) {
                return NoopCancellationRegistration.INSTANCE;
            }
            if (cancellationTriggered.get()) {
                runCancellationAction(cancellationAction);
                return NoopCancellationRegistration.INSTANCE;
            }

            String actionId = UUID.randomUUID().toString();
            cancellationActions.put(actionId, cancellationAction);

            // 防止在put之后，触发取消之前的竞态
            if (cancellationTriggered.get()) {
                Runnable action = cancellationActions.remove(actionId);
                if (action != null) {
                    runCancellationAction(action);
                }
                return NoopCancellationRegistration.INSTANCE;
            }

            return new DefaultCancellationRegistration(this, actionId);
        }

        private void unregisterAction(String actionId) {
            if (actionId == null) {
                return;
            }
            cancellationActions.remove(actionId);
        }

        private boolean triggerCancellation() {
            if (cancellationTriggered.compareAndSet(false, true)) {
                runAllCancellationActions();
                return true;
            }
            runAllCancellationActions();
            return true;
        }

        private void runAllCancellationActions() {
            cancellationActions.forEach((actionId, action) -> {
                if (cancellationActions.remove(actionId, action)) {
                    runCancellationAction(action);
                }
            });
        }

        private static void runCancellationAction(Runnable cancellationAction) {
            try {
                cancellationAction.run();
            } catch (Throwable ignore) {
                // ignore cancellation callback exceptions
            }
        }
    }

    private static final class DefaultCancellationRegistration implements CancellationRegistration {
        private final CancellationRegistry cancellationRegistry;
        private final String actionId;
        private final AtomicBoolean unregistered = new AtomicBoolean(false);

        private DefaultCancellationRegistration(CancellationRegistry cancellationRegistry, String actionId) {
            this.cancellationRegistry = cancellationRegistry;
            this.actionId = actionId;
        }

        @Override
        public void unregister() {
            if (unregistered.compareAndSet(false, true)) {
                cancellationRegistry.unregisterAction(actionId);
            }
        }
    }

    private enum NoopCancellationRegistration implements CancellationRegistration {
        INSTANCE;

        @Override
        public void unregister() {
            // no-op
        }
    }

}
