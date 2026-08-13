## Purpose

定义 SDK 与 Gateway 访问 TSO 集群时的 Leader 缓存、端点轮转、并发安全、重试和端到端超时语义，使正常选主能够透明恢复而完全不可用不会无限占用事务执行线程。

## ADDED Requirements

### Requirement: Every TSO operation has one end-to-end deadline
`Next`、`Peek` 和 `Observe` MUST 使用覆盖连接、发送、接收、端点轮转、退避和全部重试的单一端到端 deadline。兼容构造路径的默认 deadline SHALL 为 5 秒，并允许调用方显式配置。

#### Scenario: All endpoints are blackholed
- **WHEN** 三个配置端点都不接受连接或不返回响应
- **THEN** 操作 SHALL 在配置 deadline 加测试容差内返回 unavailable
- **THEN** 系统 MUST NOT 为每个端点或每轮重试重新开始完整 deadline

#### Scenario: Remaining budget is shorter than one RPC timeout
- **WHEN** 当前操作的剩余预算小于默认单次连接或 I/O 超时
- **THEN** 新 RPC 尝试 MUST 使用不超过剩余预算的超时，或立即结束操作

### Requirement: Leader failover remains transparent within the deadline
客户端 SHALL 优先尝试最后成功的 Leader，并在 transport failure、RPC timeout 或明确的 not-leader 响应后轮转其他端点，直到成功或总 deadline 耗尽。

#### Scenario: Cached Leader is killed
- **WHEN** 缓存的 TSO Leader 在下一次请求前被终止且剩余两成员形成多数派
- **THEN** 客户端 SHALL 在同一个操作 deadline 内发现新 Leader 并完成请求

#### Scenario: Endpoint reports not-leader
- **WHEN** 一个可达成员明确返回 not-leader
- **THEN** 客户端 SHALL 在剩余预算内尝试其他端点，而不是把该响应作为永久失败

### Requirement: Ambiguous allocation results never cause timestamp reuse
当 `Next` 已可能提交但响应丢失时，客户端 MUST 将结果视为未知并通过 TSO 集群重新申请；客户端 MUST NOT 本地重建、猜测或复用可能已经分配的时间戳。

#### Scenario: Response is lost after Raft commit
- **WHEN** Leader 已提交并应用时间戳但客户端在收到响应前连接断开
- **THEN** 后续成功重试 SHALL 返回一个全局唯一且不小于已提交水位的新时间戳
- **THEN** 允许产生未返回给调用方的时间戳空洞

#### Scenario: Observe response is lost
- **WHEN** `Observe(T)` 已提交但响应丢失
- **THEN** 重试 `Observe(T)` SHALL 幂等成功且不能降低水位

### Requirement: Concurrent callers share the client safely
一个 TSO 客户端实例 MUST 支持 Gateway pthread 并发调用，端点选择、连接复用和 Leader 缓存不得造成响应串线、重复时间戳或数据竞争。

#### Scenario: Concurrent Next calls during stable leadership
- **WHEN** 多个 pthread 通过同一客户端实例并发调用 `Next`
- **THEN** 每个成功调用 SHALL 收到不同的非零时间戳

#### Scenario: Concurrent calls during Leader change
- **WHEN** 多个 `Next`、`Peek` 或 `Observe` 请求与 Leader 切换重叠
- **THEN** 每个请求 SHALL 在自身 deadline 内独立成功或失败
- **THEN** 一个请求的连接故障 MUST NOT 破坏其他连接上的完整响应

### Requirement: Timeout failures have explicit transaction semantics
TSO deadline 耗尽 MUST 返回可识别的 unavailable/timeout 结果。事务在获得 `startTs` 之前不得开始；Prewrite 完成但尚未获得 `commitTs` 时，协调器 MUST 走既有回滚路径且不得提交 Primary。

#### Scenario: Begin cannot obtain startTs
- **WHEN** `Next` 在事务开始阶段耗尽 deadline
- **THEN** Begin SHALL 失败且不得向任何 Region 写入 Prewrite 或锁

#### Scenario: Commit cannot obtain commitTs
- **WHEN** 所有 Prewrite 已成功但获取 `commitTs` 的 `Next` 耗尽 deadline
- **THEN** 协调器 SHALL 回滚已知 Prewrite 和相关悲观锁
- **THEN** Primary MUST NOT 被提交
