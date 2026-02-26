# Reactor Flow 仓库说明与多模块改造参考

本文档用于说明当前仓库结构、核心实现、已识别的问题，以及后续可落地的 Maven 多模块拆分方案。目标是为后续持续演进提供统一参考。

## 1. 仓库概览

当前仓库是一个编排系统，支持两类能力：

1. 基于 Workflow DAG 的节点编排执行。
2. 基于 Agent 的自主循环执行（当前以 DAG 节点形态接入）。

当前模块：

1. `reactor-flow-core`
2. `reactor-flow-springmvc-example`

根工程使用 Maven 多模块聚合（`packaging=pom`）。

## 2. 当前代码结构与职责

### 2.1 core 模块（`reactor-flow-core`）

主要职责：

1. DAG 模型定义（`FlowDag`、`TaskNode`、`TaskEdge`）。
2. DAG 执行引擎（`FlowFutureExecuteGraph`）。
3. 任务路由与任务抽象（`FlowTaskEngineRouter`、`FlowTaskInstance`、`AbstractTaskInstance`）。
4. 运行上下文（`FlowContext`、`NodeContext`）。
5. Agent 能力（`fun.libx.flow.agent.*`，如 `AgentTaskInstance`、`AgentLoopRunner`）。

关键现状：

1. Agent 执行目前通过 `TaskType.AGENT` 作为 DAG 节点接入，不是独立编排引擎。
2. `FlowDataKeys` 同时存放 DAG 与 Agent 的 key，语义混用。

### 2.2 示例模块（`reactor-flow-springmvc-example`）

主要职责：

1. 提供 SpringMVC 示例接口。
2. 演示普通 DAG 和 Agent-DAG 两条调用路径。
3. 提供 `DefaultFlowTaskEngineRouter` 将不同 `TaskType` 路由到具体任务实例。

## 3. 当前执行链路（简化）

1. Controller 构建 `FlowDag`。
2. Controller 创建 `FlowFutureExecuteGraph` 并执行 `bfsExecute()`。
3. 图执行期间通过 `FlowTaskEngineRouter` 找到节点对应 `FlowTaskInstance`。
4. 对于 `AGENT` 节点，进入 `AgentTaskInstance`，内部使用 `AgentLoopRunner` 做模型调用与工具循环。

## 4. 当前主要问题

1. DAG 与 Agent 在 `core` 内强耦合，职责边界不清晰。
2. “统一构建接口”尚未形成，调用方需要自己区分不同编排产物。
3. 扩展成本高：新增一种编排模式时，需要修改核心模块内多个位置。
4. 可发布形态不够灵活：无法按需仅引入 DAG 引擎或仅引入 Agent 引擎。

## 5. 目标设计原则

1. 统一入口：调用方只面向一套构建与执行 API。
2. 引擎解耦：DAG 和 Agent 在不同模块演进，互不依赖。
3. 渐进迁移：保留兼容层，避免一次性大迁移风险。
4. 可插拔：未来支持第三种编排模式时，尽量无需修改既有引擎模块。

## 6. 推荐 Maven 模块拆分

建议最终拆分为如下模块：

1. `reactor-flow-bom`（可选，统一版本管理）
2. `reactor-flow-api`（统一接口与编排定义）
3. `reactor-flow-runtime`（统一执行入口与注册机制）
4. `reactor-flow-engine-dag`（DAG 编译与执行）
5. `reactor-flow-engine-agent`（Agent 编译与执行）
6. `reactor-flow-spring-boot-starter`（自动装配，按需装配引擎）
7. `reactor-flow-springmvc-example`（示例）

依赖方向建议：

1. `api <- runtime`
2. `api + runtime <- engine-dag`
3. `api + runtime <- engine-agent`
4. `runtime + engine-* <- starter`
5. `starter <- example`

要求：

1. `engine-dag` 与 `engine-agent` 不互相依赖。
2. `api` 不依赖具体引擎实现。

## 7. 各模块职责建议

### 7.1 `reactor-flow-api`

提供稳定契约：

1. `OrchestrationMode`（`DAG`、`AGENT`）
2. `OrchestrationDefinition`
3. `OrchestrationBuilder`
4. `ExecutionResult`
5. 公共 SPI（如 `Capability`、`Handler`）

### 7.2 `reactor-flow-runtime`

提供统一运行编排入口：

1. `Orchestrator`（统一 `run(...)`）
2. `PlanCompiler` 注册与分发
3. `PlanExecutor` 注册与分发
4. 公共上下文与取消机制（可承接现有 `FlowContext`/`NodeContext`）

### 7.3 `reactor-flow-engine-dag`

1. `DagCompiler`：把统一定义编译成 DAG 计划。
2. `DagExecutor`：执行 DAG 计划。
3. 迁入现有 DAG 执行代码（如 `FlowFutureExecuteGraph` 及 DAG model）。

### 7.4 `reactor-flow-engine-agent`

1. `AgentCompiler`：把统一定义编译成 Agent 计划。
2. `AgentExecutor`：执行 Agent 计划。
3. 迁入现有 `fun.libx.flow.agent.*`。

## 8. 统一接口形态（示意）

```java
OrchestrationDefinition definition = OrchestrationBuilder.newBuilder()
        .id("order-flow")
        .mode(OrchestrationMode.DAG) // 或 AGENT
        .build();

CompletableFuture<ExecutionResult> future = orchestrator.run(definition, context);
```

说明：

1. 调用方只构建一类 `OrchestrationDefinition`。
2. Runtime 根据 `mode` 分发到不同 compiler/executor。

## 9. 渐进迁移路线

### 阶段 1：抽接口

1. 创建 `api` 与 `runtime`。
2. 先不移动旧代码，在 runtime 做适配层。

### 阶段 2：拆 DAG 引擎

1. 把 DAG 相关模型与执行器迁到 `engine-dag`。
2. 保留兼容 API（旧类保留薄包装，内部委托新实现）。

### 阶段 3：拆 Agent 引擎

1. 把 Agent 相关代码迁到 `engine-agent`。
2. 将“Agent 作为 DAG 节点”的模式逐步替换为“Agent 独立计划执行”。

### 阶段 4：清理与稳定

1. 将 `FlowDataKeys` 按领域拆分（如 DAG keys、Agent keys）。
2. 抽离示例中的硬编码 DAG 构建逻辑，统一走 builder。
3. 增加模块边界测试（防止反向依赖）。

## 10. 本仓库后续维护建议

1. 所有新增能力优先加在 `api/runtime/engine-*` 新结构，避免继续加重 `core` 耦合。
2. 对外公开接口放 `api`，实现细节放 `runtime` 或 `engine-*`。
3. 版本发布时优先发布 `bom`，减少使用方版本错配成本。
4. 在 CI 增加 `mvn -pl <module> -am test` 的分模块流水线。

## 11. 当前状态结论

当前仓库可继续运行，但已经进入“能力增长快于结构”的阶段。建议尽快进行多模块化分层，以避免后续 Agent 与 DAG 特性互相牵制、难以维护的问题。

