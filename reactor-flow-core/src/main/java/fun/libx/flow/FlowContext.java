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
 * Flow级别调度上下文（全局共享）
 *
 * @author quding
 * @since 2025/4/27
 */
public class FlowContext {

    /**
     * flow级别共享取消状态与取消动作注册表
     */
    private final CancellationRegistry cancellationRegistry;

    /**
     * flow全局状态（节点执行前会拷贝到NodeContext）
     */
    private final ConcurrentHashMap<String, Object> state;

    /**
     * 创建root flow context
     */
    public FlowContext() {
        this(new CancellationRegistry(), new ConcurrentHashMap<>());
    }

    private FlowContext(CancellationRegistry cancellationRegistry, ConcurrentHashMap<String, Object> state) {
        this.cancellationRegistry = cancellationRegistry;
        this.state = state;
    }

    /**
     * 创建节点上下文（用于单个节点执行）
     */
    public NodeContext createNodeContext(String nodeId) {
        return new NodeContext(this, nodeId, copyStateForNode());
    }

    /**
     * 将节点执行结果合并到FlowContext
     */
    public synchronized void mergeFrom(NodeContext nodeContext) {
        if (nodeContext == null) {
            return;
        }

        for (String key : nodeContext.dirtyStateKeysSnapshot()) {
            if (nodeContext.containsStateKey(key)) {
                this.state.put(key, cloneStateValue(nodeContext.stateValue(key)));
            } else {
                this.state.remove(key);
            }
        }
    }

    ConcurrentHashMap<String, Object> copyStateForNode() {
        return copyStateMap(state);
    }

    /**
     * 写入flow状态
     */
    public void putState(String key, Object value) {
        String normalizedKey = Objects.requireNonNull(key, "state key cannot be null");
        state.put(normalizedKey, cloneStateValue(value));
    }

    /**
     * 读取flow状态
     */
    public Object getState(String key) {
        if (key == null) {
            return null;
        }
        return state.get(key);
    }

    /**
     * 按类型读取flow状态
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
     * 读取flow状态，不存在则返回默认值
     */
    public <T> T getStateOrDefault(String key, Class<T> type, T defaultValue) {
        T value = getState(key, type);
        return value == null ? defaultValue : value;
    }

    /**
     * 删除flow状态
     */
    public Object removeState(String key) {
        if (key == null) {
            return null;
        }
        return state.remove(key);
    }

    /**
     * 获取flow状态快照
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

    static ConcurrentHashMap<String, Object> copyStateMap(Map<String, Object> sourceState) {
        ConcurrentHashMap<String, Object> copied = new ConcurrentHashMap<>();
        sourceState.forEach((key, value) -> copied.put(key, cloneStateValue(value)));
        return copied;
    }

    @SuppressWarnings("unchecked")
    static Object cloneStateValue(Object value) {
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
