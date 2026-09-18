# proposal: add-online-replica-migration

## Why

在完成前两步变更后，StrataKV 已经具备动态元数据控制面（第 1 变更）与崩溃安全的在线 Region 分裂能力（第 2 变更）。通过分裂，集群已能将持续写入的大 Region 拆分为更小粒度的调度单元。

然而，目前集群的 Region 副本仍被静态绑定在初始节点上。当面临以下场景时系统无法自愈或扩缩容：
1. **热点倾斜**：分裂后的多个高负载 Region 聚集在同一个物理节点上，单机 CPU、内存或磁盘 I/O 达到瓶颈，无法分散到空闲节点；
2. **节点退役与维护**：存储节点硬件故障、系统升级或机器下线时，缺乏将数据与 Raft 副本在线迁移到健康节点的能力；
3. **容量扩容**：集群加入新物理节点后，无法将现有分片的副本迁移至新节点以均衡数据。

本变更实现**手动触发的、崩溃安全的在线副本迁移与 Raft 成员变更（Online Replica Migration / ConfChange）**，包括单步成员变更、副本在线追赶（Learner 状态同步与快照传输）、元数据拓扑协调与旧副本安全下线，为第 4 步自动化调度器（Auto-Balancer）提供核心的数据搬迁执行引擎。

## What Changes

- **Raft 成员变更机制（Single-step ConfChange）**：
  - 在 Raft 协议与状态机中新增 `ConfChange` 管理命令，支持单步添加节点（`AddPeer`）与移除节点（`RemovePeer`）；
  - 单步成员变更约束：同一 Raft Group 任意时刻至多允许一个未提交的 `ConfChange` 在途，杜绝双 Quorum 分裂脑风险；
  - 每次成员变更生效时单调递增 `RegionEpoch.conf_version`，与第 1 变更确立的 Epoch 验证器无缝协同。
- **副本在线追赶与状态传输（Learner Catch-up & Snapshot Transfer）**：
  - 新增非投票成员（Learner）机制：新副本在迁入目标节点时先作为 Learner 加入，不计入选举与日志提交 Quorum；
  - 数据同步：当落后日志超出 GC 阈值时，Leader 触发 RocksDB Checkpoint/Snapshot 通过流式 RPC 发送到目标节点解包加载；
  - 追赶判定与提拔（Promote）：当新副本日志水位与 Leader 差距落入预设阈值内时，通过 `ConfChange` 正式提升为投票成员（Voter）。
- **两阶段元数据拓扑协同与 ID 分配**：
  - MetaServer 提供迁移调度协同：分配全局唯一的全新 `peerId`，并在元数据状态机中记录迁移 In-Flight 状态；
  - 在成员变更阶段提交新拓扑（更新 `peers` 列表，推进 `conf_version + 1`），保证客户端能拉取到权威拓扑。
- **存储节点生命周期与注销回收**：
  - 目标节点 `NodeServer` 接收调度指令，动态创建本地 `RegionPeer` 并初始化存储目录，安全纳入 `RegionRegistry`；
  - 源节点在 `RemovePeer` 生效后，将本地副本置为 `Retiring`，等待排空在途请求后由 `RegionRegistry` 原子卸载并异步清理本地 RocksDB 数据目录。
- **客户端透明路由与事务兼容**：
  - 迁移期间客户端请求若打到已移除或尚未就绪的副本，收到 `EPOCH_NOT_MATCH` 或 `NOT_LEADER`，触发 SDK 缓存刷新并重新路由；
  - 跨分片事务与 MVCC 记录在迁移过程中保持原子性，不产生数据丢失或孤儿锁。
- **管理入口与状态观测**：
  - 命令行工具新增操作：`bin/stratakv-admin move-peer <region_id> <from_store_id> <to_store_id>`；
  - 支持查询迁移进度（Phase、复制水位、追赶时延、已传输字节数）。

### Goals

- 迁移全流程读写与分布式事务不中断，业务无感知或仅遭遇轻微瞬态重试。
- 保证严格共识安全性：单步变更防止双主脑裂，任何时间点集群只存在单一合法 Quorum。
- 任意崩溃点安全恢复：Leader 崩溃、源节点崩溃、目标节点崩溃或网络分区，系统均能通过 Raft 与元数据持久化状态前滚恢复或安全超时重试，不损坏数据。
- 迁移完成后资源彻底回收：源节点旧数据物理删除，不再驻留无效 Peer 实例。

### Non-goals

- 基于存储大小或负载的自动化调度触发（由第 4 变更调度器实现）。
- 连续多副本联合共识（Joint Consensus）：本项目采用工程成熟且更轻量的单步成员变更（One-by-one ConfChange）。
- Region 合并（Merge）。
- 静态模式下的副本迁移（迁移强依赖 MetaServer 拓扑同步与动态注册表，仅在 `dynamic` 模式可用）。

### Correctness and performance expectations

1. **正确性风险与防范**：
   - *双 Quorum 风险*：严格执行 Raft 单步变更协议，Leader 在上一笔 `ConfChange` 未 Apply 之前拒绝提交新的成员变更。
   - *Epoch 错配与脏读*：成员变更每次递增 `conf_version`，过期副本与旧客户端通过结构化 `RegionResponseHeader` 拦截并强制重定向。
   - *追赶期间影响写入性能*：快照传输采用速率受限的流式分块 RPC，避免打爆网络带宽或阻塞心跳与正常数据复制。
2. **性能预期**：
   - 稳态读写几乎零开销（仅在提拔与移除的 Apply 瞬间微量增加一次元数据交互与路由重试）；
   - 快照生成复用 RocksDB Checkpoint 硬链接，磁盘开销小、耗时低。

### Rollback strategy

- **提拔前失败**：若新节点未追赶成功或发生物理故障，只需向 Raft 提交 `RemoveLearner` 并上报元数据取消，原 3 副本 Quorum 始终保持完整，回滚代价为 0。
- **提拔后回滚**：新副本已成为 Voter 但旧副本未移除时，若需放弃迁移，可通过发起反向 `RemovePeer(新节点)` 恢复原拓扑。
- **运行时崩溃**：节点重启后根据持久化 Raft 日志与元数据状态续做前滚或超时下线临时副本。

## Capabilities

### New Capabilities

- `online-replica-migration`: 在线副本迁移完整闭环——包含 Raft 单步 ConfChange（Add/Remove）、Learner 异步追赶与快照传输、MetaServer 协同、NodeServer 动态 Peer 拉起与安全下线、SDK 透明路由更新与管理运维 CLI。

### Modified Capabilities

- 无。第 1 变更和第 2 变更建立的 `dynamic-region-routing`、`dynamic-region-registry`、`region-metadata-control-plane` 已原生支持 `conf_version` 单调递增校验与 Registry Replace/Remove 语义，本次仅作为下层能力被调用。

## Impact

- **协议与核心**：`raft_rpc.proto` / `region.proto` 新增 `ConfChange` 消息；`Raft` 状态机实现成员动态增删与 Learner 状态管理；`Persister` 持久化成员配置。
- **存储与节点**：`RegionPeer` 接入成员变更回调；`NodeServer` 增加跨节点快照收发服务与动态 Peer 销毁回收。
- **元数据控制面**：`MetadataStateMachine` 支持 `ConfChange` 事务，持久化新成员并推进 `conf_version`。
- **管理 CLI**：新增 `move-peer`、`add-peer`、`remove-peer` 等运维子命令。
- **部署模式**：仅在 `dynamic` 拓扑模式下启用，保持静态模式完全隔离。

