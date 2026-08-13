## Why

StrataKV 的三成员 TSO 已能抵抗单进程崩溃和 Leader 切换，但共享 Raft 快照交接、Raft 文件持久化、物理故障域部署和客户端总超时仍未形成可靠性闭环。当前缺口可能导致落后 TSO 成员通过快照追赶后状态机水位仍旧滞后、掉电后 Raft 稳定状态损坏、三个逻辑副本被单机故障同时带走，或 TSO 全部不可达时长期占用 Gateway 请求线程。

## What Changes

- 统一共享 Raft 层的快照所有权和安装顺序，确保 Raft 元数据与 TSO/Region 状态机在快照追赶后处于同一 applied index，并阻止状态机未追平的 TSO 成员对外发号。
- 将通用 `Persister` 改为可检测损坏的崩溃一致持久化：临时文件写入、文件同步、原子替换、目录同步，并保证 Raft state 与 snapshot 的成对恢复语义。
- 为 TSO 增加显式的跨故障域拓扑配置和校验；保留当前三个 loopback 进程的本地开发模式，同时支持三个成员部署在三个独立物理节点。
- 为 `RemoteTimestampOracle` 增加覆盖全部端点与重试轮次的端到端 deadline；保留 Leader 缓存和故障转移，但确保 `Next`、`Peek`、`Observe` 在有界时间内成功或失败。
- 增加确定性的可靠性测试，覆盖落后节点快照追赶后当选 Leader、持久化写入阶段故障/损坏检测、独立故障域拓扑验证和黑洞端点超时上界。
- 记录兼容性、监控指标、性能前后对比和可回退步骤。

### Non-goals

- 不实现动态 Raft 成员变更、跨数据中心 TSO、批量/区间时间戳租约或多 TSO Group。
- 不在本次变更中实现 RPC 身份认证、TLS、TSO `Observe` 权限控制、GC SafePoint 上移或 TSO Client/Raft RPC 线程池拆分。
- 不改变 Gateway Fiber、阻塞 SDK/2PC pthread、NodeServer、RegionPeer 和 Region Raft Group 的既有职责边界。

## Capabilities

### New Capabilities

- `raft-snapshot-recovery`: 规定共享 Raft 快照从接收、持久化到状态机安装和重新参与领导权的完整正确性要求。
- `raft-durable-persistence`: 规定 Raft state/snapshot 的原子持久化、掉电恢复、损坏检测和失败关闭行为。
- `tso-fault-domain-deployment`: 规定本地开发与跨物理故障域 TSO 拓扑的配置、校验、启动和健康判定。
- `tso-client-failover`: 规定 TSO Leader 缓存、端点轮转、重试、端到端 deadline 以及不确定结果语义。

### Modified Capabilities

当前 `openspec/specs/` 中没有既有能力规格，本变更不修改现有 capability。

## Impact

- **Affected code:** `src/raft/raft.cpp`、`src/raft/persister.cpp`、TSO consensus/service/client、RPC 超时控制、部署脚本和 TSO/Region 快照测试。
- **Correctness:** 修复可能导致时间戳回退的快照状态机缺口；持久化错误必须失败关闭，不能以空状态继续选举或发号。
- **Compatibility:** 现有 `Client::Connect`、Gateway 参数和默认 `127.0.0.1:26300..26302` 本地拓扑保持可用。新增 deadline 和分布式拓扑参数使用兼容默认值；超时后可能比旧实现更早返回 unavailable，这是有意的可用性边界收紧。
- **Performance:** 原子写入与 `fsync` 可能增加 Raft 持久化延迟；实现必须避免不必要的重复同步，并用同一负载记录 TSO 分配吞吐和 p50/p95/p99 延迟变化。端到端 deadline 应减少故障期间 Gateway pthread 的最长占用时间。
- **Rollout:** 先引入可同时读取旧格式并写入新格式的 Persister，再启用新快照交接和部署配置；升级期间保持静态三成员 topology，不进行成员替换。
- **Rollback:** 在确认新格式兼容读取和旧数据备份后，可回退二进制并恢复升级前的 Raft state/snapshot 备份。若新实现已经写出旧版本无法解析的格式，禁止直接降级，必须先停机并执行显式格式回退工具或恢复备份。
