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

}
