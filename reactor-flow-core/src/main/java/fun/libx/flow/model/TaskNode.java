package fun.libx.flow.model;

import com.alibaba.fastjson2.JSONObject;
import fun.libx.flow.TaskType;
import fun.libx.flow.common.DataKey;
import fun.libx.flow.common.DataProvider;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author quding
 * @since 2025/4/27
 */
public class TaskNode implements DataProvider {
    /**
     * 节点id
     */
    private String id;

    /**
     * 设置节点id
     * @param id 节点id
     */
    public void setId(String id) {
        this.id = id;
    }
    /**
     * 任务名称
     */
    private String taskName;
    /**
     * 任务类型
     */
    private TaskType type;
    /**
     * 任务自身参数
     */
    private JSONObject defineConfig;
    /**
     * 任务输入引用参数
     */
    private List<TaskInputParam> inputConfig;
    /**
     * 任务输出引用参数
     */
    private List<TaskOutputParam> outputConfig;

    /**
     * 前驱节点列表
     */
    private Set<TaskNode> predecessors = new HashSet<>();

    /**
     * 后继节点列表
     */
    private Set<TaskNode> successors = new HashSet<>();

    /**
     * 添加前驱节点
     * @param predecessor 前驱节点
     */
    public void addPredecessor(TaskNode predecessor) {
        predecessors.add(predecessor);
    }

    /**
     * 添加后继节点
     * @param successor 后继节点
     */
    public void addSuccessor(TaskNode successor) {
        successors.add(successor);
    }

    public String getId() {
        return id;
    }

    public String getTaskName() {
        return taskName;
    }

    public TaskType getType() {
        return type;
    }

    public void setType(TaskType type) {
        this.type = type;
    }

    public JSONObject getDefineConfig() {
        return defineConfig;
    }

    public void setDefineConfig(JSONObject defineConfig) {
        this.defineConfig = defineConfig;
    }

    public List<TaskInputParam> getInputConfig() {
        return inputConfig;
    }

    public void setInputConfig(List<TaskInputParam> inputConfig) {
        this.inputConfig = inputConfig;
    }

    public List<TaskOutputParam> getOutputConfig() {
        return outputConfig;
    }

    public void setOutputConfig(List<TaskOutputParam> outputConfig) {
        this.outputConfig = outputConfig;
    }

    public Set<TaskNode> getPredecessors() {
        return predecessors;
    }

    public Set<TaskNode> getSuccessors() {
        return successors;
    }

    @Override
    public <T> T getData(DataKey<T> dataKey) {
        return defineConfig.getObject(dataKey.dataId, dataKey.clazz);
    }

    @Override
    public <T> void setData(DataKey<T> key, T data) {
        this.defineConfig.put(key.dataId, data);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof TaskNode)) {
            return false;
        }

        return this.id.equals(((TaskNode) obj).id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

}
