# 构建与验证标准（以下所有任务适用）

- 配置：`cmake -S . -B build/stratakv/release -DCMAKE_BUILD_TYPE=Release`；构建：`cmake --build build/stratakv/release -j`
- 每个任务只跑其声明的聚焦测试；全量 `ctest --test-dir build/stratakv/release --output-on-failure` 仅在 5.1 里程碑执行。
- 每个测试必须自终止（≤60s）；挂住的测试按缺陷处理，先修测试再继续。
- 保留工作树中与本变更无关的用户改动；`docs/` 为独立 Git 仓库。
- 副本迁移只允许在动态模式与动态集群集成测试中验证；静态模式行为必须保持逐字节一致。

## 1. Raft 单步成员变更核心协议

- [x] 1.1 实现 Raft 单步成员变更核心：扩展 `raft_rpc.proto` 与 `region.proto` 引入 `ConfChange` 命令（含 `AddLearner`、`PromoteLearner`、`RemovePeer`），在 Raft 状态机中实现单步成员变更逻辑，严格执行在途 ConfChange 互斥检查（前一笔未 Apply 时直接拒绝新提议），成员变更提交生效时单调递增 `epoch.conf_version`，且被移除副本立即停止参与选举与心跳。验证：新增聚焦测试 `stratakv-test-raft-confchange`，覆盖单步增删节点、并发在途互斥拦截、移除节点主动退役以及 `conf_version` 单调自增。

## 2. Learner 追赶与快照传输

- [x] 2.1 实现 Learner 副本在线追赶与流式快照传输：在 Raft 复制中支持非投票的 Learner 角色（接收 AppendEntries 但不计入提交 Quorum）；当新节点落后日志超出 GC 截断点时，Leader 基于 RocksDB Checkpoint 生成原子快照并通过分块流式 RPC（`InstallSnapshot`）推送至目标节点解包加载；实现追赶水位探测（`match_index >= commit_index - K`）并在追赶完成时触发提拔。验证：新增聚焦测试 `stratakv-test-replica-catchup`，覆盖增量日志追赶、跨 GC 快照传输以及提拔为 Voter 的无缝切换。

## 3. 动态 Peer 生命周期与元数据协同

- [x] 3.1 交付节点级动态 Peer 挂载与旧副本资源回收，并打通元数据控制面：目标节点 `NodeServer` 接收迁移指令动态实例化新 `RegionPeer` 并纳入 `RegionRegistry`；源节点在 Apply `RemovePeer` 后将本地副本置为 `Retiring`，排空在途请求后从 `RegionRegistry` 原子注销，关闭 RocksDB 并异步删除本地磁盘数据；MetaServer 扩展成员变更 Mutation，从高水位分配全新 `peer_id` 并原子发布新拓扑。验证：新增聚焦测试 `stratakv-test-node-migration`，覆盖动态挂载、安全下线排空与磁盘物理回收。

## 4. 管理命令与端到端迁移集成

- [x] 4.1 交付管理命令行入口与客户端透明路由集成：实现 `bin/stratakv-admin move-peer <region_id> <from_store_id> <to_store_id>`（带预检、迁移阶段输出与超时保护）；确保迁移过程中客户端遭遇 `EPOCH_NOT_MATCH` 或 `NOT_LEADER` 时自动刷新 `RegionCache` 重新路由；保证跨 Region 2PC 分布式事务在成员迁移期间并发执行不产生孤儿锁或数据丢失。验证：新增集成测试 `stratakv-test-online-migration`，端到端演练跨节点副本迁移，并在迁移全程施加并发读写与事务负载验证零丢单。

## 5. 正确性验证、全量回归与文档就绪

- [x] 5.1 执行迁移容错验证、全仓回归与文档沉淀：在副本迁移各个阶段注入故障（Leader 宕机选举、目标节点宕机超时取消、源节点宕机恢复），验证集群自愈无脑裂；运行全仓测试 `ctest --test-dir build/stratakv/release --output-on-failure`（要求 100% PASS）；在 `README.md` 与 `Docs/9-优化记录/` 中更新操作指南与架构文档。
