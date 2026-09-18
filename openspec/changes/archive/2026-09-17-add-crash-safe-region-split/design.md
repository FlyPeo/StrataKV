# design: add-crash-safe-region-split

## Context

第 1 变更已交付本设计依赖的全部基础：三副本元数据控制面（MetadataStateMachine 支持区间替换 mutation、durable ID 高水位、mutation 去重）、RegionPeer 生命周期（Initializing/Serving/Retiring/Stopped + 有界 drain）、RegionRegistry 原子替换（`Replace` 已支持一次发布多删多增）、NodeTxnScheduler 动态注册、RegionCache 区间替换刷新、RegionRequestSender 的 `RegroupRequired` 重分组、typed Region header/错误。每 Region 独立 RocksDB（`region-{id}_node-{n}_peer-{i}` 目录）的存储形态保留，分裂采用 TiKV rocksdb-per-region RFC 同款 checkpoint 克隆路线，而非共享实例改造。

执行模型边界不变：Gateway fiber 只管 HTTP socket；分裂的协调、checkpoint、Raft、MVCC 全部在既有有界 pthread/apply 路径上。

## Goals / Non-Goals

**Goals**：手动 `split-region` 命令端到端可用；任意崩溃点前滚恢复；分裂期间读写与跨 Region 事务不中断；拓扑不变量（无缝无叠、key 单一归属、ID 不复用）在任何时刻成立。

**Non-Goals**：自动触发、合并、成员变更、副本迁移、限速调优、静态模式分裂。

## Decisions

### 1. 两阶段元数据 + Raft AdminSplit 的职责划分

MetaServer 负责权威拓扑与 ID 分配（阶段一 `PrepareSplit`：校验 + 分配子 ID + 记录 in-flight 关联，含 dedup；阶段二 `CommitSplit`：发布父收缩 + 子描述符，原子替换）。父 Region Raft 日志负责**数据面分裂点**：Leader 在阶段二提交后提出 `AdminSplit{split_key, metadata_revision, child_descriptors}`，apply 时本地落地。两个提交点各自原子、顺序固定（先元数据后数据面），中间态（元数据已变、副本未 apply）由 epoch/key 校验兜底：右半 key 打到父副本时返回 `EPOCH_NOT_MATCH` + 新描述符，SDK 走既有刷新重试。

理由：让 Raft 只做"数据何时分裂"的确定性排序，元数据做"拓扑与 ID"的权威源，两者都天然获得崩溃恢复（Raft 重放 / 元数据快照），不需要新的分布式协议。备选"仅 Raft 决定再上报元数据"被否：ID 分配与拓扑发布将出现第二个无共识排序的写点。

### 2. 本地落地用 RocksDB Checkpoint，不做在线删除再拷贝

apply AdminSplit 时：父 RocksDB 打 `Checkpoint` 到子目录 `child-{regionId}-gen{N}.pending`（硬链接级原子），成功后 fsync 目录、写入 `RegionLocalState{phase=child-ready, parent_desc, child_desc, generation=N}` 到父副本的本地状态文件（Persister 扩展），再 rename 去 `.pending`。子 Region 首次以空 Raft 日志启动，全部数据来自 checkpoint；**子副本日志起点约定**：子 RegionPeer 以 `initial_log_index = 父 AdminSplit 的 log index + 1` 初始化（占位_term），保证崩溃重放不会越过分裂点重放父日志。父 Region 收缩不移动数据：直接在父 RocksDB 上以有界速率 `DeleteRange[start_child, end)` + 手动 compact 清理，期间 `OwnsKey` 已按新范围拒绝右半写入。

备选"父子共享 RocksDB 实例 + 列主隔离"被否：破坏每 Region 独立实例的既有边界，快照/压缩/锁竞争互相污染，改造量远超本变更。

### 3. 副本本地状态机（RegionLocalState 五阶段）

```
NotStarted → Checkpointing → ChildReady → ParentShrunk → Complete
```

- 持久化于父副本 Persister（`split_state_region_{id}.json`，原子 rename 写）。
- 崩溃恢复规则：重启后若日志含未 apply 的 AdminSplit → 重放 apply；若已 apply 且 state 落后 → 按阶段前滚（Checkpointing：作废 `.pending` 重做；ChildReady：跳过 checkpoint 直接收缩；ParentShrunk：只做清理收尾）。
- apply 幂等：重放同一 AdminSplit 时若 state 已 ≥ 对应阶段则为 no-op。

### 4. 子 Region 实例化与注册：复用 RegionRegistry.Replace

NodeServer 的 apply 回调检测到本地 AdminSplit 完成后，在节点层（非 apply 线程）执行：创建子 RegionPeer（descriptor 来自 AdminSplit，含确定性 peerId；RocksDB 指向 generation 目录）→ 子 peer `Start()` → `registry_.Replace({父}, {父'（收缩描述符需重建 peer？否——父 peer 原地更新描述符，见决策 5）, 子}, revision)`。

关键点：**父 RegionPeer 原地收缩**（更新 in-memory descriptor + epoch，接受旧 epoch 拒绝新逻辑），不重建对象——避免迁移 16 个 mutation lane 与在途请求；Registry 的 Replace 语义用于"父描述符变更 + 子新增"的原子可见性（Replace 接口已支持删除集/新增集，父以"同 ID 新 epoch 替换"表达，冲突检查按 epoch 单调性放行）。子 RegionPeer 创建失败（磁盘满等）→ 节点不发布、保留父服务、指数退避重试；超过有界次数上报告警，父保持完整旧范围服务（拓扑元数据已收缩——此时以元数据为准：父 must 收缩，故失败重试是唯一路径，崩溃恢复同路径）。

### 5. 描述符与 epoch 的传播点

- AdminSplit 携带阶段二提交的完整父子描述符（含父新 epoch `{version+1, conf_version}`、子 `{1,1}` 与新 peerIds）——apply 不再访问元数据网络。
- RegionPeer 新增 `UpdateDescriptorShrunk(new_desc)`：CAS 更新 `m_descriptor`（epoch 单调校验），此后旧 epoch 请求由既有 validator 拒绝。
- RegionCache/sender/RegroupRequired 零改动：新描述符经 `EPOCH_NOT_MATCH` 的 `current_regions` 与元数据扫描两条既有路径扩散。

### 6. 事务与 MVCC 语义

- MVCC 记录按 user key 聚簇（既有存储布局），checkpoint 克隆天然把一个 key 的 data/lock/write/rollback 带到唯一归属侧；无跨侧拆分问题。
- 分裂点在 apply 排队中天然屏障：AdminSplit 前提交的事务其 commit 已 apply；prewrite 未 commit 的事务，其锁记录随 key 归属，后续 CheckTxnStatus/ResolveLock 按新归属路由（LockResolver 经 router 按 key 路由，自动指向新 Region）。
- 重分组：BatchPrewrite 收到 `EPOCH_NOT_MATCH` 时 sender 返回 RegroupRequired（已实现），协调器以原 startTs/primary/commitTs 重分批。

### 7. 管理命令与进度观测

`stratakv-admin split-region <region_id> <key>`（动态模式强制）：经 MetadataClient 提交两阶段 mutation，轮询 `NodeServer` 的 split 状态（新增 Status RPC 字段或复用日志+metrics）直到 `Complete`，输出各阶段耗时。metrics：split_phase、checkpoint 时长/字节数、清理剩余 key 数、子 Region 上线时延。日志只含 RegionId/epoch/revision/phase。

## Request and State Flows

### 分裂主流程（happy path）

```
admin ──PrepareSplit──▶ Meta(分配ID, 记in-flight) ──CommitSplit──▶ Meta(发布父'+子)
父Leader(看到新revision/被通知) ──propose AdminSplit──▶ 父Raft多数派
每副本apply: 校验 → checkpoint子目录 → 写RegionLocalState → 父收缩+DeleteRange右半
            → 通知NodeServer
NodeServer: 创建子RegionPeer(Raft起点=split index) → Start → Registry.Replace原子发布
           → 上报Meta(幂等) → 子Region Serving
client: 旧epoch→EPOCH_NOT_MATCH→刷新→按新界重试/重分组
后台: 父删右半数据(有界)，子无需清理(克隆即整段)；父完成即 Complete
```

### 崩溃恢复表

| 崩溃点 | 重启后行为 |
|---|---|
| 阶段一提交、AdminSplit 未提出 | 元数据 in-flight 存在；admin 重试幂等续做；父服务不变 |
| AdminSplit 已提交、checkpoint 中 | 日志重放触发重做：作废 `.pending`、重新 checkpoint |
| ChildReady、未收缩 | 跳过 checkpoint，直接收缩+清理 |
| 已发布、上报丢失 | 上报幂等重试；元数据阶段二已含全部信息，不依赖上报 |
| 子 Raft 组已启动、节点崩溃 | 子 peer 以持久化 generation 目录重启，日志起点约定防重放穿越 |

## Safety Invariants

1. 任意时刻每个 key 至多属于一个 Serving Region（元数据 phase 二原子性 + 本地 epoch 校验双保险）。
2. AdminSplit 只在元数据阶段二提交后提出；副本 apply 前置校验失败即 replicated no-op。
3. 子目录以 generation 标记，`.pending` 永不服务；RegionLocalState 先于内存注册持久化。
4. 子 Region ID/PeerID 只从阶段一分配的高水位之后取，去重表拦截重放。
5. 已确认写入在分裂与崩溃恢复后可读：数据面分裂点在 Raft 日志中排序，checkpoint 是崩溃一致快照。
6. Registry 发布原子；Retiring/drain 语义沿用第 1 变更。

## Risks / Trade-offs

- [Checkpoint 毛刺] 硬链接为主、dir fsync 一次；大 Region 首次分裂可能百毫秒级写延迟抖动 → 观测 split_checkpoint_duration，限速调优留给后续。
- [父 DeleteRange 有界清理期间空间双份] 清理完成前磁盘占用约 1.5×；速率可配置，默认保守。
- [子 Raft 空日志 + checkpoint 的 durability 论证] 依赖"checkpoint 生成于 AdminSplit apply 之后"的顺序保证（LocalState 阶段门），重启重放不会在 checkpoint 前写入子数据。
- [admin 误操作] 预检（epoch/key/范围/in-flight）+ 明确拒绝语义；无 dry-run（留给第 4 变更调度器的预演机制）。

## Migration Plan

1. 全量二进制升级（第 1 变更后状态即可热升级）。
2. 测试集群动态模式下演练 `split-region`，跑正确性套件。
3. 生产：确认静态回滚窗口已关闭（或接受），升级后按需手动分裂热 Region；每次分裂前后跑 verify。

## Rollback Design

AdminSplit 提交前：无副作用，放弃即可。提交后：拓扑不可逆（静态回滚规则已由第 1 变更声明），故障路径一律前滚（本地状态机续做）；极端情况下子 Region 数据与父数据同源，人工导出可兜底（不在自动化范围）。
