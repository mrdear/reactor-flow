package fun.libx.flow.event;

/**
 * @author quding
 * @since 2025/5/1
 */
public interface EventHandler<T> {
    /**
     * 事件类型
     */
    Event support();

    /**
     * 响应事件
     * @param data
     */
    void onEvent(T data);

}
