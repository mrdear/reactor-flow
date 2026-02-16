package fun.libx.flow.task;

import fun.libx.flow.model.TaskNode;

/**
 * 节点的输出结果,以及一些中间信息记录
 * @author quding
 * @since 2025/5/1
 */
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

    public TaskNode getTaskNode() {
        return taskNode;
    }

    public void setTaskNode(TaskNode taskNode) {
        this.taskNode = taskNode;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.exception = exception;
    }
}
