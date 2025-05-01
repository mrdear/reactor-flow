package fun.libx.flow.event.model;

import fun.libx.flow.event.Event;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.task.TaskOutputResult;
import lombok.Data;

import java.util.Date;

/**
 * @author quding
 * @since 2025/5/1
 */
@Data
public class NodeFinishEvent implements Event {

    /**
     * 节点
     */
    private TaskNode taskNode;

    /**
     * 节点结果
     */
    private TaskOutputResult result;
    /**
     * 节点时间
     */
    private Date eventDate;

}
