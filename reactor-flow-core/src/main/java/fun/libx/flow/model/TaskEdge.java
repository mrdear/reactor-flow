package fun.libx.flow.model;

import com.alibaba.fastjson2.JSONObject;

/**
 * @author quding
 * @since 2025/4/27
 */
public class TaskEdge {

    /**
     * 源节点
     */
    private String sourceId;

    /**
     * 目标节点
     */
    private String targetId;

    /**
     * 边配置
     */
    private JSONObject edgeConfig;

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    public JSONObject getEdgeConfig() {
        return edgeConfig;
    }

    public void setEdgeConfig(JSONObject edgeConfig) {
        this.edgeConfig = edgeConfig;
    }

}
