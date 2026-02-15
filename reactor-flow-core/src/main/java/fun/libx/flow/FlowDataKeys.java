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

}
