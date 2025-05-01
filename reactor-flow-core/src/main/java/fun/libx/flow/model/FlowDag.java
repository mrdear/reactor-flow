package fun.libx.flow.model;

import com.alibaba.fastjson2.JSONObject;

import java.util.*;

/**
 * @author quding
 * @since 2025/4/27
 */
public class FlowDag {

    private Map<String, TaskNode> taskNodeMap = new HashMap<>();

    private Map<String, TaskEdge> edgeMap = new HashMap<>();

    /**
     * 添加任务节点
     * @param taskNode 任务节点
     * @return 如果节点ID已存在则返回false，否则返回true
     */
    public boolean addTaskNode(TaskNode taskNode) {
        if (taskNode == null || taskNode.getId() == null) {
            return false;
        }

        if (taskNodeMap.containsKey(taskNode.getId())) {
            return false;
        }

        taskNodeMap.put(taskNode.getId(), taskNode);
        return true;
    }

    /**
     * 添加边（从源节点到目标节点的有向边）
     * @param sourceId 源节点ID
     * @param targetId 目标节点ID
     * @return 如果添加成功返回true，否则返回false
     */
    public boolean addEdge(String sourceId, String targetId) {
        return addEdge(sourceId, targetId, null);
    }

    /**
     * 添加边（从源节点到目标节点的有向边）
     * @param sourceId 源节点ID
     * @param targetId 目标节点ID
     * @param edgeConfig 边的配置信息
     * @return 如果添加成功返回true，否则返回false
     */
    public boolean addEdge(String sourceId, String targetId, JSONObject edgeConfig) {
        TaskNode sourceNode = taskNodeMap.get(sourceId);
        TaskNode targetNode = taskNodeMap.get(targetId);

        if (sourceNode == null || targetNode == null) {
            return false;
        }

        // 检查添加这条边是否会导致环
        if (hasPath(targetId, sourceId)) {
            return false; // 避免创建环
        }

        // 创建边对象
        TaskEdge edge = new TaskEdge();
        edge.setSourceId(sourceId);
        edge.setTargetId(targetId);
        edge.setEdgeConfig(edgeConfig);

        // 存储边
        String edgeKey = sourceId + "->" + targetId;
        edgeMap.put(edgeKey, edge);

        sourceNode.addSuccessor(targetNode);
        targetNode.addPredecessor(sourceNode);
        return true;
    }

    /**
     * 添加边（使用TaskEdge对象）
     * @param edge 边对象
     * @return 如果添加成功返回true，否则返回false
     */
    public boolean addEdge(TaskEdge edge) {
        if (edge == null || edge.getSourceId() == null || edge.getTargetId() == null) {
            return false;
        }
        return addEdge(edge.getSourceId(), edge.getTargetId(), edge.getEdgeConfig());
    }

    /**
     * 根据ID获取任务节点
     * @param id 节点ID
     * @return 任务节点，如果不存在则返回null
     */
    public TaskNode getTaskNode(String id) {
        return taskNodeMap.get(id);
    }

    /**
     * 获取所有任务节点
     * @return 所有任务节点的集合
     */
    public Collection<TaskNode> getAllTaskNodes() {
        return taskNodeMap.values();
    }

    /**
     * 获取节点的所有前驱节点
     * @param id 节点ID
     * @return 前驱节点列表，如果节点不存在则返回空列表
     */
    public Set<TaskNode> getPredecessors(String id) {
        TaskNode node = taskNodeMap.get(id);
        if (node == null) {
            return Collections.emptySet();
        }
        return node.getPredecessors();
    }

    /**
     * 获取节点的所有后继节点
     * @param id 节点ID
     * @return 后继节点列表，如果节点不存在则返回空列表
     */
    public Set<TaskNode> getSuccessors(String id) {
        TaskNode node = taskNodeMap.get(id);
        if (node == null) {
            return Collections.emptySet();
        }
        return node.getSuccessors();
    }

    /**
     * 检查从起始节点到目标节点是否存在路径
     * @param startId 起始节点ID
     * @param targetId 目标节点ID
     * @return 如果存在路径则返回true，否则返回false
     */
    public boolean hasPath(String startId, String targetId) {
        TaskNode startNode = taskNodeMap.get(startId);
        if (startNode == null) {
            return false;
        }

        Set<String> visited = new HashSet<>();
        Queue<TaskNode> queue = new LinkedList<>();
        queue.offer(startNode);
        visited.add(startId);

        while (!queue.isEmpty()) {
            TaskNode current = queue.poll();

            if (current.getId().equals(targetId)) {
                return true;
            }

            for (TaskNode successor : current.getSuccessors()) {
                if (!visited.contains(successor.getId())) {
                    visited.add(successor.getId());
                    queue.offer(successor);
                }
            }
        }

        return false;
    }

    /**
     * 检查图是否是无环的（DAG）
     * @return 如果图是无环的返回true，否则返回false
     */
    public boolean isAcyclic() {
        // 使用拓扑排序检查是否有环
        List<TaskNode> result = topologicalSort();
        return result.size() == taskNodeMap.size();
    }

    /**
     * 执行拓扑排序
     * @return 拓扑排序的结果，如果图中有环则返回的列表可能不完整
     */
    public List<TaskNode> topologicalSort() {
        List<TaskNode> result = new ArrayList<>();
        Map<String, Integer> inDegree = new HashMap<>();

        // 计算每个节点的入度
        for (TaskNode node : taskNodeMap.values()) {
            inDegree.put(node.getId(), 0);
        }

        for (TaskNode node : taskNodeMap.values()) {
            for (TaskNode successor : node.getSuccessors()) {
                inDegree.put(successor.getId(), inDegree.get(successor.getId()) + 1);
            }
        }

        // 将所有入度为0的节点加入队列
        Queue<TaskNode> queue = new LinkedList<>();
        for (TaskNode node : taskNodeMap.values()) {
            if (inDegree.get(node.getId()) == 0) {
                queue.offer(node);
            }
        }

        // 执行拓扑排序
        while (!queue.isEmpty()) {
            TaskNode node = queue.poll();
            result.add(node);

            for (TaskNode successor : node.getSuccessors()) {
                int newInDegree = inDegree.get(successor.getId()) - 1;
                inDegree.put(successor.getId(), newInDegree);

                if (newInDegree == 0) {
                    queue.offer(successor);
                }
            }
        }

        return result;
    }

    /**
     * 获取所有起始节点（没有前驱节点的节点）
     * @return 起始节点列表
     */
    public TaskNode getStartingNode() {
        for (TaskNode node : taskNodeMap.values()) {
            if (node.getPredecessors().isEmpty()) {
                return node;
            }
        }
        return null;
    }

    /**
     * 根据源节点ID和目标节点ID获取边
     * @param sourceId 源节点ID
     * @param targetId 目标节点ID
     * @return 边对象，如果不存在则返回null
     */
    public TaskEdge getEdge(String sourceId, String targetId) {
        String edgeKey = sourceId + "->" + targetId;
        return edgeMap.get(edgeKey);
    }

    /**
     * 获取所有边
     * @return 所有边的集合
     */
    public Collection<TaskEdge> getAllEdges() {
        return edgeMap.values();
    }
}
