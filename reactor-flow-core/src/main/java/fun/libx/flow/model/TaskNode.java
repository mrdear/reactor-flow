package fun.libx.flow.model;

import com.alibaba.fastjson2.JSONObject;
import fun.libx.flow.TaskType;
import fun.libx.flow.common.DataKey;
import fun.libx.flow.common.DataProvider;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

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
    @Getter
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
    @Getter
    private String taskName;
    /**
     * 任务类型
     */
    @Getter
    @Setter
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
    @Getter
    private Set<TaskNode> predecessors = new HashSet<>();

    /**
     * 后继节点列表
     */
    @Getter
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

    @Override
    public <T> T getData(DataKey<T> dataKey) {
        return defineConfig.getObject(dataKey.dataId, dataKey.clazz);
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
