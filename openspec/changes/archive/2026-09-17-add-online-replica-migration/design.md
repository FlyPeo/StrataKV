# design: add-online-replica-migration

## Context

在第 1 变更（动态路由与元数据控制面）和第 2 变更（崩溃安全 Region 在线分裂）完成后，StrataKV 已经拥有了按 key 拆分热点 Region 的能力。但当前副本在存储节点之间的分布依然是固定的，缺乏在线动态迁移副本的能力。

本设计实现 **在线副本迁移与 Raft 单步成员变更（Online Replica Migration / ConfChange）**，使得集群能够在不停机、不中断读写的前提下，将指定 Region 的副本平滑搬移至新存储节点。

## Goals / Non-Goals

**Goals**：
- 支持 `stratakv-admin move-peer <region_id> <from_store_id> <to_store_id>` 端到端执行；
- Raft 单步成员变更保证无双主风险（单步增删，在途限流）；
- 新副本通过 Learner 角色追赶数据（支持增量 AppendEntries 与全量 RocksDB Checkpoint 快照流式传输）；
- 迁移全程 2PC 分布式事务与单分片读写不中断；
- 迁出节点完成任务后安全卸载、排空在途请求并物理回收 RocksDB 资源；
- 任意崩溃点（Leader 崩溃、目标节点挂掉、协调器退出）均可通过幂等前滚或取消恢复。

**Non-Goals**：
- 自动化指标感知与调度（留给第 4 变更调度器）；
- Joint Consensus 连续多副本变更（单步变更足够满足工程需求且更简单稳健）；
- Region 合并（Merge）；
- 静态模式支持（仅在 `dynamic` 拓扑模式下可用）。

## Decisions

### 1. Raft 单步成员变更（One-by-one ConfChange）

选用单步成员变更协议（每次仅变更单个节点的 Voter/Learner 身份）：
- **Raft 日志管理项**：在 Raft 日志中引入特殊条目 `ConfChangeCommand`（类型包含 `AddLearner`、`PromoteLearner`、`RemovePeer`）；
- **在途防并发（In-flight Invariant）**：Leader 在提议 `ConfChange` 前检查是否有尚未 Apply 的 `ConfChange`。若存在，直接拒绝新的提议。这在理论上严格杜绝了新旧配置形成互斥法定人数（Quorum）的可能性；
- **配置生效点**：当 `ConfChange` 被多数派提交并 Apply 时，所有副本原子更新内存中的 `m_peers`，并将 `RegionEpoch.conf_version` 单调自增 1；
- **被移除节点自绝**：当本地节点 Apply 的 `RemovePeer` 目标是自身时，节点立即停止参与 Raft 选举、停止响应心跳，转入停机流程。

### 2. Learner 机制与快照流式传输（State Transfer）

为了不破坏既有 3 副本的高可用法定人数，新副本必须**先作为非投票成员（Learner）接入**：
1. **加入阶段**：Leader 提议 `AddLearner(new_peer_id, target_store)`，新节点以 Learner 身份加入，接收 AppendEntries 但不计算在投票法定人数内；
2. **状态追赶（Catch-up）**：
   - **增量追赶**：若新节点的起始位点日志尚未被 Leader GC，直接通过常规 `AppendEntries` 快速追赶；
   - **快照传输（Snapshot Stream）**：若 Leader 已截断落后日志，Leader 对本地 RocksDB 执行 Checkpoint 快照，并通过流式 RPC（`InstallSnapshot` 分块分批发送）推送到目标节点，目标节点加载快照并拉起本地 RocksDB 实例；
3. **提拔为正式成员（Promote）**：
   - Leader 周期性检查 Learner 的 `match_index`；
   - 当 `match_index >= commit_index - K`（例如差距在 100 条日志以内）时，判定追赶完毕；
   - Leader 提议 `PromoteLearner(new_peer_id)`，多数派提交并 Apply 后，新节点正式成为具有投票权的 Voter（此时形成瞬态 4 副本 Voter）。

### 3. 源节点下线与物理资源回收（Decommission & Cleanup）

在新节点正式成为 Voter 并稳定服务后，触发旧节点下线：
1. Leader 提议 `RemovePeer(old_peer_id)`；
2. 多数派提交并 Apply（集群恢复为 3 副本 Voter）；
3. 源节点的 `NodeServer` 监听到本地副本被移除后：
   - 将该 `RegionPeer` 置为 `Retiring` 状态；
   - 等待本地正在执行的读写请求与 Apply 任务完成（设置有界超时排空）；
   - 从本地 `RegionRegistry` 原子注销（`UnregisterRegion`）；
   - 彻底 `Stop()` 该 Peer，关闭 RocksDB 句柄；
   - 异步安全删除该副本的磁盘持久化目录（`region-{id}_node-{n}_peer-{i}`）。

### 4. 元数据控制面协同与全局 ID 分配

整个迁移过程由 MetaServer 保持权威视图：
- **两阶段元数据交互**：
  - 开始迁移前，向 MetaServer 提交 `PrepareMovePeer(region_id, from_store, to_store)`，MetaServer 分配全局单调递增的新 `peer_id`，并记录 `migration_in_progress`；
  - 提拔新节点并移除旧节点后，向 MetaServer 提交 `CommitMovePeer`，MetaServer 更新 Region 的成员列表，推进 `conf_version + 1`，持久化新修订版（Revision）；
- **幂等重试**：所有元数据变更均携带独一无二的 `mutation_id`，防止网络超时引发重复分配。

### 5. 客户端路由自愈与事务连续性

- 客户端（SDK）在迁移期间可能将请求发往源节点或尚未选为主的节点；
- 收到 `NOT_LEADER` 提示后，客户端依既有逻辑重定向到新 Leader；
- 收到 `EPOCH_NOT_MATCH`（由 `conf_version` 变化触发）后，客户端刷新 `RegionCache` 并用最新 Peer 列表重试；
- 2PC 分布式事务在迁移期间提交，因 Raft 多数派一致性与 MVCC 多版本保护，事务执行结果与锁清理完全不受物理搬迁影响。

## Request and State Flows

### 在线迁移全流程（Happy Path）

```
Admin                  MetaServer              Leader(Store A)       Learner(Store B)       OldPeer(Store C)
  │                         │                         │                      │                     │
  ├── PrepareMovePeer ─────▶│ (分配 new_peer_id)       │                      │                     │
  │                         │                         │                      │                     │
  ├── InitTargetPeer ───────────────────────────────────────────────────────▶│ (初始化本地目录)     │
  │                                                   │                      │                     │
  ├── Propose AddLearner ────────────────────────────▶│                      │                     │
  │                                                   ├── AppendEntries/ ───▶│ (追赶日志/快照)     │
  │                                                   │   Snapshot           │                     │
  │                                                   │                      │                     │
  │   (轮询追赶进度: match_index >= commit_index - K) │                      │                     │
  ├── Propose PromoteLearner ────────────────────────▶│                      │                     │
  │                                                   │ (成为 4-Voter)       │                     │
  │                                                   │                      │                     │
  ├── Propose RemovePeer ────────────────────────────▶│                      │                     │
  │                                                   │                      │                     ├── Apply RemovePeer
  │                                                   │ (恢复 3-Voter)       │                     ├── Retiring & 排空
  │                                                   │                      │                     └── 删除本地 RocksDB
  ├── CommitMovePeer ──────▶│ (发布新拓扑 Revision)   │                      │                     │
  │                         │                         │                      │                     │
  ▼ Complete                ▼                         ▼                      ▼                     ▼
```

### 崩溃与故障恢复矩阵

| 故障场景 | 系统恢复行为 |
|---|---|
| **Learner 追赶期间目标节点 Store B 崩溃** | Leader 停止向 Store B 发送日志；集群仍保持原 3 副本 Quorum，读写完全不受影响；Admin 超时后可取消迁移，调用 `RemovePeer(Learner)` 清理 |
| **Learner 追赶期间 Leader 崩溃** | 原 3 副本触发重新选举；新 Leader 接管后继续向 Learner 复制日志；无脑裂风险 |
| **PromoteLearner 提议期间网络分区** | 单步变更保障任意时刻只存在单一 Quorum；分区侧无法提交，连通后自动恢复 |
| **RemovePeer 提交后 Store C 崩溃** | Store C 重启时回放 Raft 日志立即识别到 `RemovePeer`，直接执行退役与本地文件清理 |
| **Admin 协调器在任意步骤意外退出** | 各阶段状态通过元数据与 Raft 日志持久化；重新执行 `move-peer` 命中幂等状态机，直接续做后半流程 |

## Safety Invariants

1. **单步配置不变量**：同一 Raft Group 任意时刻至多允许一个未 Apply 的 `ConfChange`。
2. **Quorum 单一性**：从 3 副本增加 Learner 不改变 Quorum（仍为 2/3）；提拔为 4 副本 Voter 时 Quorum 为 3/4（原 3 副本的任意多数派 2 节点必然与 3/4 有交集）；移除旧副本后 Quorum 为 2/3。全程任何时刻绝不存在两个互斥的多数派。
3. **Epoch 单调性**：每次 Voter 集合变化均严格单调递增 `epoch.conf_version`。
4. **资源安全回收**：被移除副本的物理数据删除仅在 `RemovePeer` 经多数派确认且本地所有在途 RPC 引用归零后执行。

## Risks / Trade-offs

- **快照传输对带宽的占用**：大 Region 触发快照传输时，流式 RPC 需设置吞吐上限（Rate Limiter），防止抢占业务数据复制带宽。
- **瞬态 4 副本窗口的 Quorum 抬升**：在 PromoteLearner 成功到 RemovePeer 成功的短暂窗口内，法定人数由 2 抬升至 3（4 节点中需 3 节点响应）。若在此极端窗口内恰好有节点宕机，写入会阻塞直到完成 RemovePeer。因此实现中 Promote 和 Remove 应紧密流水化执行，将该窗口缩至毫秒级。

