package fun.libx.flow.model;

import com.alibaba.fastjson2.JSONObject;
import lombok.Data;

import java.util.List;

/**
 * @author quding
 * @since 2025/4/27
 */
@Data
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

}
