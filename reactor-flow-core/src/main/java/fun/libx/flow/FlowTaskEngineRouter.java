package fun.libx.flow;

import fun.libx.flow.model.TaskNode;
import fun.libx.flow.task.FlowTaskInstance;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 调度task路由
 * @author quding
 * @since 2025/5/1
 */
public interface FlowTaskEngineRouter {

    /**
     * 任务节点
     * @param node 节点自身
     * @return 调度类
     */
    FlowTaskInstance router(TaskNode node);

    /**
     * 同上,增加context参数便于更强控制
     * @param node 当前节点
     * @param context 上下文信息
     * @return 路由结果
     */
    default FlowTaskInstance router(TaskNode node, FlowContext context) {
        return router(node);
    }


    class DefaultEnumRouter implements FlowTaskEngineRouter {
        private final Map<TaskType, FlowTaskInstance> routerMap = new ConcurrentHashMap<>();

        @Override
        public FlowTaskInstance router(TaskNode node) {
            return routerMap.get(node.getType());
        }

        public void register(TaskType type, FlowTaskInstance router){
            routerMap.put(type, router);
        }
    }

}
