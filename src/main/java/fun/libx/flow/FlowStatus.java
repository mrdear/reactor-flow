package fun.libx.flow;

/**
 * flow的执行状态
 * @author quding
 * @since 2025/5/1
 */
public enum FlowStatus {
    /**
     * 准备执行
     */
    PENDING,
    /**
     * 执行中
     */
    RUNNING,
    /**
     * 执行完毕
     */
    SUCCEEDED,
    /**
     * 执行失败
     */
    FAILED,
    /**
     * 主动取消
     */
    CANCELED,;

}
