package fun.libx.flow.task;

import fun.libx.flow.model.TaskNode;
import lombok.Data;

/**
 * 节点的输出结果,以及一些中间信息记录
 * @author quding
 * @since 2025/5/1
 */
@Data
public class TaskOutputResult {
    /**
     * 节点信息
     */
    private TaskNode taskNode;
    /**
     * 节点执行结果
     */
    private Object result;
    /**
     * 节点异常信息
     */
    private Throwable exception;
}
