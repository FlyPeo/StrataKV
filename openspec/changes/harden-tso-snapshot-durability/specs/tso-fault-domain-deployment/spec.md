## Purpose

定义 TSO 三成员在本地开发模式和跨物理故障域模式下的拓扑、身份、数据目录与健康检查要求，使三副本能够从进程冗余提升为可验证的主机级故障容忍。

## ADDED Requirements

### Requirement: Local and distributed deployment modes are explicit
部署工具 SHALL 明确区分本地开发模式与跨故障域模式。本地模式可以使用同机 loopback 端点；跨故障域模式 MUST 要求三个可路由成员端点和三个不同的故障域标识。

#### Scenario: Existing local development command is used
- **WHEN** 用户未选择跨故障域模式并使用现有默认参数启动集群
- **THEN** 系统 SHALL 继续启动 `127.0.0.1:26300..26302` 三个本地 TSO 进程
- **THEN** 状态输出 MUST 将其标识为本地进程冗余，而不是主机级高可用

#### Scenario: Distributed mode uses three domains
- **WHEN** 用户为三个成员提供唯一端点、稳定 node ID、独立数据目录和三个不同的故障域标识
- **THEN** 配置 SHALL 通过验证并生成每个成员一致有序的 peer 列表

### Requirement: Invalid fault-domain topologies are rejected before startup
跨故障域模式 MUST 在启动任何成员之前拒绝重复端点、重复 node ID、缺失成员、重复故障域、不可解析地址和数据目录复用。

#### Scenario: Two members share one fault domain
- **WHEN** 跨故障域配置中的两个成员声明相同故障域
- **THEN** 启动 SHALL 失败并指出冲突成员

#### Scenario: Members reuse a persistent directory
- **WHEN** 两个成员指向同一个 Raft 或 TSO 水位目录
- **THEN** 启动 SHALL 在创建进程前失败

### Requirement: Member identity and peer ordering are stable
三个成员 MUST 在所有进程上使用相同顺序的 peer 列表，并在重启后保持 node ID、endpoint、故障域和持久化目录的绑定关系。

#### Scenario: One TSO member restarts
- **WHEN** 成员在相同 node ID、endpoint 和数据目录下重启
- **THEN** 它 SHALL 以原身份重新加入并从持久化状态追赶

#### Scenario: Endpoint order differs between members
- **WHEN** 不同成员配置中的 peer 顺序无法映射到相同 node ID
- **THEN** 配置验证 MUST 拒绝启动

### Requirement: Quorum behavior reflects independent failures
三成员 TSO SHALL 在任意一个故障域不可用时继续通过剩余多数派发号，并在两个故障域不可用时停止发号。

#### Scenario: One physical domain is unavailable
- **WHEN** 一个 TSO 故障域及其持久化设备不可访问
- **THEN** 剩余两个成员 SHALL 能选出唯一 Leader 并分配严格递增时间戳

#### Scenario: Two physical domains are unavailable
- **WHEN** 仅剩一个 TSO 成员可用
- **THEN** 所有 `Next` 和 `Observe` 请求 MUST 失败
- **THEN** 单成员 MUST NOT 发布新的全局时间戳

### Requirement: Health checks verify readiness rather than process count
集群健康判定 MUST 验证恰好一个 TSO Leader、可用多数派、Leader 状态机追平以及三个成员身份与配置匹配，不能只检查进程或端口存在。

#### Scenario: Three processes run but state machine is behind
- **WHEN** 三个 TSO 进程均存活但 Leader 状态机尚未追平 committed state
- **THEN** 集群健康检查 MUST 报告未就绪

#### Scenario: Healthy distributed topology
- **WHEN** 三个成员身份匹配、一个 Leader 已追平且多数派可通信
- **THEN** 健康检查 SHALL 报告 TSO control plane ready，并显示每个成员的故障域
