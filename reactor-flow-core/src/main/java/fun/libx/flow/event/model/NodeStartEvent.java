package fun.libx.flow.event.model;

import fun.libx.flow.event.Event;
import fun.libx.flow.model.TaskNode;

import java.util.Date;

/**
 * 节点开始执行事件
 * @author quding
 * @since 2025/5/1
 */
public class NodeStartEvent implements Event {
    /**
     * 开始的节点信息
     */
    private TaskNode taskNode;

    /**
     * 节点开始事件
     */
    private Date eventDate;

    public TaskNode getTaskNode() {
        return taskNode;
    }

    public void setTaskNode(TaskNode taskNode) {
        this.taskNode = taskNode;
    }

    public Date getEventDate() {
        return eventDate;
    }

    public void setEventDate(Date eventDate) {
        this.eventDate = eventDate;
    }

}
