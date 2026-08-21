## Why

StrataKV 的 Snapshot Isolation 只检测写集合冲突，因此不同 Key 上的并发写可以产生 write skew；已有单 Key 悲观锁内部链路既未开放，也缺少 current read、多 Key 清理和可靠恢复。现有纯逻辑 TSO、客户端侧锁扫描与 GC 还会把网络未知结果、锁过期和长事务快照处理成不安全状态，不能直接承载公开悲观事务。

## What Changes

- 为公共 SDK 增加精确 Key 的 `GetForUpdate`、`BatchGetForUpdate` 和 `LockKeys`，并在 Gateway HTTP 与 CLI 提供对应入口。
- 在事务和持久化 MVCC Lock 中记录 `forUpdateTs`，使加锁读返回该时间戳下的最新已提交值，同时保留普通 `Get` 的固定 `startTs` 快照语义。
- 扩展悲观锁 RPC 和 Region 命令语义：锁请求只在 Raft 提交并 Apply 到 MVCC/RocksDB 后返回成功，重复请求和响应丢失后的重试保持幂等。
- 让 Prewrite 验证并升级同一事务的悲观锁，并保证 `commitTs > max(startTs, forUpdateTs)`。
- 多 Key 加锁使用确定顺序和 fail-fast 策略；任一 Key 失败时清理已获得的锁，将事务置为 abort-only，并要求调用方从 `Begin` 整体重试。
- 将共享 TSO 升级为 physical+logical HLC，并定义旧高水位迁移、时钟回拨和 Leader 切换下的全局单调语义；事务时限、锁 TTL 与 RPC budget 使用各自正确的时间域。
- 增加权威 `TxnCheckStatus`、`TxnResolveLock` 与明确的 `ResultUnknown`/`CleanupPending` 状态，禁止 primary 结果未知时盲目回滚 secondary。
- 将过期锁恢复和 MVCC GC 从 SDK/Gateway 生命周期迁移到 Node 常驻服务；第一阶段强制 60 秒事务上限、120 秒锁 TTL 和至少 5 分钟的版本保留窗口。
- 增加可确定复现的 write-skew 对照测试，以及 TSO 重启/回拨、重复请求、RPC 超时、Leader 切换、跨 Region 部分加锁失败、Commit、Rollback、GC 和重启恢复测试。
- 增加悲观锁请求、冲突、延迟、部分失败清理和 abort-only 计数的可观测性，并用同一工作负载对比乐观与悲观路径。
- 本变更不实现锁等待队列、wait-for graph、死锁检测、锁心跳、范围/gap/谓词锁、SSI 或 Serializable；也不让 `NodeLatchManager` 承担事务锁等待。超过硬事务上限的调用必须明确失败，而不是依赖后台误杀。

## Capabilities

### New Capabilities

- `pessimistic-transactions`: 定义精确 Key 悲观加锁读、`forUpdateTs`、多 Key fail-fast/清理、悲观锁到 Prewrite 的升级、公共 API、故障语义以及 write-skew 对照验证。

### Modified Capabilities

无。当前 `openspec/specs/` 没有已建立的悲观事务能力规格。

## Impact

- **Affected code:** `src/tso`、`src/sdk`、`src/gateway`、`src/cli`、`src/transaction`、TSO/Region protobuf、`src/server`、`src/raft/region_peer.*`、部署配置、事务/可靠性测试和性能证据。
- **Correctness:** 主要风险是 HLC 倒退、加锁后仍返回旧快照、过早 GC、部分加锁遗留锁、将网络未知误判为不存在，或 primary 已提交后错误回滚 secondary。规格和故障测试必须分别锁定这些不变量。
- **Compatibility:** 新 SDK 方法、HTTP 路由和 protobuf 字段为增量接口；旧客户端仍使用乐观 SI 路径。HLC 状态必须从旧 TSO 高水位单调迁移，MVCC Lock 缺少新字段时按兼容默认值解码。新悲观能力只在所有 TSO 与存储节点升级完成后开放，旧二进制不得重写新锁。
- **Dependencies:** 复用现有 TSO Raft group、NodeTxnScheduler、Region Raft、MVCC/RocksDB 和同步 MPRPC；不引入新的第三方依赖。Node 增加 TSO endpoint 配置，阻塞式 SDK/2PC/恢复工作继续运行在有界 pthread 工作线程而非 Gateway Fiber。
- **Performance:** 无冲突悲观路径会多一次 TSO 取号和至少一轮 Raft/RPC；高冲突负载可以更早拒绝冲突事务，减少无效 Prewrite/2PC。不对性能收益作预设，必须以同负载前后数据证明。
- **Rollback:** 该能力为显式 opt-in。回退时先停用新入口、等待或解析全部新格式锁并验证无未决事务，再回退存储节点；HLC TSO 一经迁移不得回退为可能发出更小时间戳的旧 allocator，除非保留已迁移高水位并由旧实现显式 Observe。
