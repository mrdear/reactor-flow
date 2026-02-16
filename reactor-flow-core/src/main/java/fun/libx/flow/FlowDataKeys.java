package fun.libx.flow;

import fun.libx.flow.common.DataKey;

/**
 * @author quding
 * @since 2025/5/2
 */
public final class FlowDataKeys {

    /**
     * 节点是否忽略异常
     */
    public static final DataKey<Boolean> NODE_IGNORE_EXCEPTION = DataKey.of("NODE_IGNORE_EXCEPTION", Boolean.class);
    /**
     * 节点超时时间
     */
    public static final DataKey<Long> NODE_TIMEOUT_SECOND = DataKey.of("NODE_TIMEOUT_SECOND", Long.class);

    /**
     * 节点最大尝试次数(包含首次执行),默认1(不重试)
     */
    public static final DataKey<Integer> NODE_RETRY_MAX_ATTEMPTS = DataKey.of("NODE_RETRY_MAX_ATTEMPTS", Integer.class);

    /**
     * 节点重试等待间隔(毫秒),默认0
     */
    public static final DataKey<Long> NODE_RETRY_WAIT_MILLIS = DataKey.of("NODE_RETRY_WAIT_MILLIS", Long.class);

    /**
     * 节点超时是否参与重试,默认true
     */
    public static final DataKey<Boolean> NODE_RETRY_ON_TIMEOUT = DataKey.of("NODE_RETRY_ON_TIMEOUT", Boolean.class);

    /**
     * agent节点system prompt
     */
    public static final DataKey<String> NODE_AGENT_SYSTEM_PROMPT = DataKey.of("NODE_AGENT_SYSTEM_PROMPT", String.class);

    /**
     * agent节点输入prompt
     */
    public static final DataKey<String> NODE_AGENT_PROMPT = DataKey.of("NODE_AGENT_PROMPT", String.class);

    /**
     * agent节点输入prompt在state中的key
     */
    public static final DataKey<String> NODE_AGENT_PROMPT_STATE_KEY = DataKey.of("NODE_AGENT_PROMPT_STATE_KEY", String.class);

    /**
     * agent节点输出结果写入state的key
     */
    public static final DataKey<String> NODE_AGENT_RESULT_STATE_KEY = DataKey.of("NODE_AGENT_RESULT_STATE_KEY", String.class);

    /**
     * agent节点消息历史写入state的key
     */
    public static final DataKey<String> NODE_AGENT_HISTORY_STATE_KEY = DataKey.of("NODE_AGENT_HISTORY_STATE_KEY", String.class);

    /**
     * agent节点最大轮次
     */
    public static final DataKey<Integer> NODE_AGENT_MAX_TURNS = DataKey.of("NODE_AGENT_MAX_TURNS", Integer.class);

}
