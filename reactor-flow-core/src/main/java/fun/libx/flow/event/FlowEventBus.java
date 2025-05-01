package fun.libx.flow.event;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author quding
 * @since 2025/5/1
 */
public class FlowEventBus {

    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(FlowEventBus.class);

    /**
     * 事件监听器
     */
    private Map<Class<?>, List<EventHandler<Event>>> EVENT_HANDLER_MAP = new ConcurrentHashMap<>();


    /**
     * 发送相关事件
     * @param data 数据
     * @param <T> 事件值
     */
    public <T extends Event> void sendEvent(T data) {
        Class<? extends Event> aClass = data.getClass();

        List<EventHandler<Event>> handlers = EVENT_HANDLER_MAP.get(aClass);
        if (null == handlers || handlers.isEmpty()) {
            return;
        }

        for (EventHandler<Event> handler : handlers) {
            try {
                handler.onEvent(data);
            } catch (Exception e) {
                LOGGER.error("event hande error: {},{}", aClass.getSimpleName(), handler.getClass().getSimpleName(), e);
            }
        }
    }


}
