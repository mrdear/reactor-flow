package fun.libx.flow;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
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
                copyStateMap(parent.state)
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
                this.state.put(key, cloneStateValue(nodeContext.state.get(key)));
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

    /**
     * 可覆盖: 重试前将子类扩展字段重置到主context最新值
     */
    protected void resetCustomStateFrom(FlowContext sourceContext) {
        // no-op
    }

    /**
     * 在重试前重置节点context，避免失败尝试的脏状态影响成功尝试
     */
    synchronized void resetFrom(FlowContext sourceContext) {
        if (sourceContext == null || sourceContext == this) {
            return;
        }
        this.state.clear();
        this.state.putAll(copyStateMap(sourceContext.state));
        this.dirtyStateKeys.clear();
        resetCustomStateFrom(sourceContext);
    }

    public String getScope() {
        return scope;
    }

    /**
     * 写入状态
     */
    public void putState(String key, Object value) {
        String normalizedKey = Objects.requireNonNull(key, "state key cannot be null");
        state.put(normalizedKey, cloneStateValue(value));
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
        return Collections.unmodifiableMap(copyStateMap(state));
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

    private static ConcurrentHashMap<String, Object> copyStateMap(Map<String, Object> sourceState) {
        ConcurrentHashMap<String, Object> copied = new ConcurrentHashMap<>();
        sourceState.forEach((key, value) -> copied.put(key, cloneStateValue(value)));
        return copied;
    }

    @SuppressWarnings("unchecked")
    private static Object cloneStateValue(Object value) {
        if (value == null || isKnownImmutable(value)) {
            return value;
        }

        if (value instanceof Map<?, ?> mapValue) {
            Map<Object, Object> copiedMap = new LinkedHashMap<>();
            mapValue.forEach((key, mapItemValue) -> copiedMap.put(cloneStateValue(key), cloneStateValue(mapItemValue)));
            if (value instanceof ConcurrentHashMap<?, ?>) {
                return new ConcurrentHashMap<>(copiedMap);
            }
            return new HashMap<>(copiedMap);
        }

        if (value instanceof List<?> listValue) {
            List<Object> copiedList = new ArrayList<>(listValue.size());
            for (Object listItem : listValue) {
                copiedList.add(cloneStateValue(listItem));
            }
            return copiedList;
        }

        if (value instanceof Set<?> setValue) {
            Set<Object> copiedSet = new LinkedHashSet<>();
            for (Object setItem : setValue) {
                copiedSet.add(cloneStateValue(setItem));
            }
            return copiedSet;
        }

        if (value instanceof Collection<?> collectionValue) {
            List<Object> copiedCollection = new ArrayList<>(collectionValue.size());
            for (Object collectionItem : collectionValue) {
                copiedCollection.add(cloneStateValue(collectionItem));
            }
            return copiedCollection;
        }

        Class<?> valueClass = value.getClass();
        if (valueClass.isArray()) {
            int length = Array.getLength(value);
            Class<?> componentType = valueClass.getComponentType();
            Object copiedArray = Array.newInstance(componentType, length);
            if (componentType.isPrimitive()) {
                System.arraycopy(value, 0, copiedArray, 0, length);
                return copiedArray;
            }
            for (int index = 0; index < length; index++) {
                Array.set(copiedArray, index, cloneStateValue(Array.get(value, index)));
            }
            return copiedArray;
        }

        if (value instanceof Date dateValue) {
            return new Date(dateValue.getTime());
        }

        if (value instanceof Cloneable) {
            try {
                Method cloneMethod = valueClass.getDeclaredMethod("clone");
                cloneMethod.setAccessible(true);
                return cloneMethod.invoke(value);
            } catch (Exception ignore) {
                // fallback to shared reference
            }
        }

        return value;
    }

    private static boolean isKnownImmutable(Object value) {
        if (value instanceof String
                || value instanceof Number
                || value instanceof Boolean
                || value instanceof Character
                || value instanceof Enum<?>) {
            return true;
        }

        return Modifier.isFinal(value.getClass().getModifiers())
                && value.getClass().getName().startsWith("java.time.");
    }

}
