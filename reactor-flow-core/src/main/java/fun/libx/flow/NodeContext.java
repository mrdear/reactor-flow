package fun.libx.flow;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 节点级执行上下文（每个节点独立）
 *
 * @author quding
 * @since 2026/2/16
 */
public class NodeContext {

    private final FlowContext flowContext;

    private final String nodeId;

    private final ConcurrentHashMap<String, Object> state;

    private final Set<String> dirtyStateKeys = ConcurrentHashMap.newKeySet();

    NodeContext(FlowContext flowContext, String nodeId, ConcurrentHashMap<String, Object> state) {
        this.flowContext = Objects.requireNonNull(flowContext, "flowContext cannot be null");
        this.nodeId = normalizeNodeId(nodeId);
        this.state = state == null ? new ConcurrentHashMap<>() : state;
    }

    private static String normalizeNodeId(String nodeId) {
        if (nodeId == null || nodeId.trim().isEmpty()) {
            return "__unknown__";
        }
        return nodeId;
    }

    public String getNodeId() {
        return nodeId;
    }

    /**
     * 写入节点状态
     */
    public void putState(String key, Object value) {
        String normalizedKey = Objects.requireNonNull(key, "state key cannot be null");
        state.put(normalizedKey, FlowContext.cloneStateValue(value));
        dirtyStateKeys.add(normalizedKey);
    }

    /**
     * 读取节点状态
     */
    public Object getState(String key) {
        if (key == null) {
            return null;
        }
        return state.get(key);
    }

    /**
     * 按类型读取节点状态
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
     * 读取节点状态，不存在则返回默认值
     */
    public <T> T getStateOrDefault(String key, Class<T> type, T defaultValue) {
        T value = getState(key, type);
        return value == null ? defaultValue : value;
    }

    /**
     * 删除节点状态
     */
    public Object removeState(String key) {
        if (key == null) {
            return null;
        }
        dirtyStateKeys.add(key);
        return state.remove(key);
    }

    /**
     * 获取节点状态快照
     */
    public Map<String, Object> snapshotState() {
        return Collections.unmodifiableMap(FlowContext.copyStateMap(state));
    }

    /**
     * 感知flow取消状态
     */
    public boolean isCancellationTriggered() {
        return flowContext.isCancellationTriggered();
    }

    /**
     * 主动触发flow取消
     */
    public boolean triggerCancellation() {
        return flowContext.triggerCancellation();
    }

    /**
     * 注册取消回调（节点执行时可注册IO/线程中断动作）
     */
    public FlowContext.CancellationRegistration registerCancellationAction(Runnable cancellationAction) {
        return flowContext.registerCancellationAction(cancellationAction);
    }

    /**
     * 重试前重置为flow最新状态
     */
    void resetFromFlowContext() {
        this.state.clear();
        this.state.putAll(flowContext.copyStateForNode());
        this.dirtyStateKeys.clear();
    }

    Set<String> dirtyStateKeysSnapshot() {
        return new LinkedHashSet<>(dirtyStateKeys);
    }

    boolean containsStateKey(String key) {
        return state.containsKey(key);
    }

    Object stateValue(String key) {
        return state.get(key);
    }
}
