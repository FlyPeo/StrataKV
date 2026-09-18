# proposal: add-crash-safe-region-split

## Why

StrataKV 的 Region 边界在启动后固定（第 1 变更只建立了"认识拓扑变化"的能力：元数据控制面、typed epoch、动态路由与注册表）。一个持续写入的热 Region 无法被拆分，单 Region 的 Raft/Apply 串行点成为吞吐上限；同时大 Region 拉长快照与恢复时间。本变更实现**手动触发的、崩溃安全的 Region 分裂**，为第 3 步（在线副本迁移）提供可搬动的更小调度单元。自动触发（按大小/负载）不在本变更内，属第 4 步调度器。

## What Changes

- 新增 `AdminSplit` Raft 管理命令：由管理员通过 CLI 发起，携带 `region_id + expected_epoch + split_key`；分裂决定在父 Region 的 Raft 日志中提交，所有副本在同一日志位置确定性生效。
- 元数据控制面新增分裂变更命令：MetaServer 在分裂前原子分配子 Region/Peer ID 并提交新拓扑（父区间收缩、子区间新建、epoch.version 单调递增），复用第 1 变更已实现的区间替换校验（无缝隙/无重叠/父区间被完整覆盖）。
- 副本本地分裂落地：apply AdminSplit 时以 RocksDB Checkpoint 将父 Region 数据克隆出子 Region 数据目录（每 Region 一 RocksDB 的既有形态保留），子 Region 后台删除范围外数据；父 Region 应用后收缩自身范围并清理右半数据；全程持久化 `RegionLocalState`（含 split 阶段与 tablet generation），崩溃后按阶段恢复：未提交的不落地、已提交的续做。
- 节点侧动态实例化：NodeServer 检测到本地副本 apply 了 AdminSplit 后，创建子 RegionPeer（新 Raft 组）、经 RegionRegistry 以一次原子替换发布（父收缩 + 子新增，复用 Replace 语义），完成后向 MetaServer 上报。
- 事务与路由兼容：分裂后旧 epoch 请求收到 `EPOCH_NOT_MATCH`（携带新描述符），SDK/Gateway 经既有 RegionCache 刷新与 RegroupRequired 重分组路径恢复，保持 start_ts/primary key/commit_ts/mutation identity；MVCC data/lock/write/rollback 记录按用户 key 随所在子 Region 移动（同一 key 的全部记录永远在同一子 Region）。
- 管理入口：`stratakv-admin split-region <region_id> <split_key>`（需动态模式），含预检（key 存在性、区间合法性、epoch 匹配）与进度输出。

### Goals

- 任意阶段崩溃（AdminSplit 提交前/后、checkpoint 完成前/后、子 Region 注册前/后、MetaServer 上报前/后）都不丢已确认写入、不产生重叠或空洞拓扑、不需要人工修复即可继续。
- 分裂期间服务不中断：父 Region 持续接受读写直到本副本 apply 完成；跨 Region 事务在边界变化后按既有重分组语义继续。
- 每个用户 key 在任意时刻至多属于一个可写 Region；拓扑快照任意时刻无缝无叠。

### Non-goals

- 自动分裂触发（大小/QPS 采样）——第 4 变更调度器。
- Region 合并、Raft 成员变更、副本迁移、Leader 均衡——第 3/4 变更。
- 静态模式下的分裂：分裂依赖元数据控制面，仅动态模式可用（静态回滚规则不变：发生过分裂后不可直接回退 static）。
- 分裂性能优化（checkpoint 速率、删区间数据限速）只做安全边界内的基础实现。

### Correctness and performance expectations

正确性风险集中在三处：(1) AdminSplit 与并发事务写的交错——apply 阶段二次校验 epoch/split_key，事务记录按 key 原子归属；(2) checkpoint 与崩溃交错——RocksDB Checkpoint 是硬链接级原子快照，子目录以 generation 号+阶段标记持久化，重启可续做或作废重做；(3) 双主窗口——子 Region 在元数据发布并本地注册为 Serving 前不接流量，父 Region 收缩后旧右半请求被 epoch/key 校验拒绝。性能预期：分裂瞬间父 Region 写延迟出现一次 checkpoint 触发的毛刺（硬链接为主，预期 < 数百毫秒级）；分裂后两个 Region 各自独立 Raft/Apply，热写吞吐上限随之翻倍；后台删除范围外数据以有界速率进行，不设硬性性能承诺（基准留待分裂稳定后的第 3 变更前置基线）。

### Rollback strategy

分裂提交前：直接放弃（AdminSplit 不产生任何本地副作用）。分裂提交后：拓扑已变，需按第 1 变更文档的规则执行——静态回滚不再适用；回退路径为"反向分裂"（合并），不在本变更范围。因此发布顺序要求：先在测试集群演练分裂 → 再在生产开启动态。运行时回退：分裂进行中的节点崩溃重启后按持久化阶段继续完成分裂（前滚而非回滚）。

## Capabilities

### New Capabilities

- `crash-safe-region-split`: 手动 Region 分裂的端到端行为——管理员命令、元数据两阶段（分配+发布）、AdminSplit Raft 命令、副本本地 checkpoint 落地、崩溃恢复各阶段语义、子 Region 服务上线、旧路由失效与事务重分组、后台范围外数据清理。

### Modified Capabilities

（无——第 1 变更的 dynamic-region-routing / dynamic-region-registry / region-metadata-control-plane 规格已覆盖分裂所依赖的路由刷新、原子替换发布与元数据变更语义；本变更只新增能力，不改变既有 Requirement。）

## Impact

- 新增：AdminSplit 协议消息（region.proto/metadata_rpc.proto）、RegionLocalState 持久化、副本分裂 apply 流程、NodeServer 子 Region 实例化、admin split-region 子命令。
- 修改：MetadataStateMachine（分裂 mutation：一父拆二子的区间替换 + ID 分配）、RegionPeer（收缩自身范围与数据、apply 钩子）、RegionRegistry（Replace 已支持，需接分裂调用点）、NodeTxnScheduler/TxnRecoveryManager（动态注册已支持）、RegionCache 刷新（已支持区间替换）。
- 不修改：TSO、Raft 核心算法、静态模式行为。
- 部署：仅动态模式；分裂需 NodeServer 与 meta 均为本变更后二进制。
