## Purpose

为 StrataKV 定义第一阶段可用的精确 Key 悲观事务契约，使应用能够先锁定共同读集或约定的 guard key，再基于当前已提交状态做写入决策，从而缓解 Snapshot Isolation 下的 write skew。

## ADDED Requirements

### Requirement: Public exact-key pessimistic transaction APIs
公共事务 SDK MUST 提供 `GetForUpdate`、`BatchGetForUpdate` 和 `LockKeys`。Gateway HTTP API 与 CLI MUST 暴露等价入口，并使用与 SDK 一致的成功结果、`LockConflict`、`WriteConflict`、`AbortOnly`、`CleanupPending`、超时和不可用错误语义。

`GetForUpdate` MUST 锁定一个精确 Key 并返回值与存在性；`BatchGetForUpdate` MUST 对一组精确 Key 执行一个逻辑批次并按输入 Key 返回值与存在性；`LockKeys` MUST 只锁定精确 Key 而不把它们加入写集合。普通 `Get` MUST 继续读取事务 `startTs` 快照，不能因为新增接口而变成当前读。

#### Scenario: Single-key locking read through SDK
- **WHEN** 活跃事务对 Key K 调用 `GetForUpdate`
- **THEN** 调用 MUST 锁定 K 并返回该次加锁读定义的当前已提交值或不存在结果

#### Scenario: Batch locking read through Gateway and CLI
- **WHEN** 调用方通过 Gateway 或 CLI 对事务执行 `BatchGetForUpdate([A, B])`
- **THEN** 入口 MUST 使用与公共 SDK 相同的批次原子失败、返回值和错误分类

#### Scenario: Ordinary get remains a snapshot read
- **WHEN** 事务在开始后通过普通 `Get` 读取一个未写入的 Key
- **THEN** 读取 MUST 仍以固定 `startTs` 判定可见性且 MUST NOT 隐式获取悲观锁

#### Scenario: Locking an absent exact key
- **WHEN** 活跃事务对当前不存在的精确 Key K 调用 `GetForUpdate`
- **THEN** 调用 MUST 在 K 上建立悲观锁并返回不存在结果，使同一事务可以安全创建 K 且其他事务不能并发创建同一个 K

#### Scenario: Feature remains gated during rolling upgrade
- **WHEN** TSO 或任一存储节点尚未声明支持 HLC 与新版悲观锁协议
- **THEN** SDK、Gateway 和 CLI MUST 拒绝开启新悲观事务能力，旧乐观 SI 路径 MUST 继续可用

### Requirement: forUpdateTs defines the locking-read view
每次单 Key 加锁调用和每个多 Key 加锁批次 MUST 在写入任何锁之前从共享 TSO 获得一个非零 `forUpdateTs`。同一批次的全部 Key MUST 使用同一个 `forUpdateTs`，事务 MUST 记录成功或结果未知的最大 `forUpdateTs`。

成功的 `GetForUpdate` 或 `BatchGetForUpdate` MUST 返回 `commitTs <= forUpdateTs` 的最新已提交版本。若加锁判定时已经存在 `commitTs > forUpdateTs` 的版本，请求 MUST 以 `WriteConflict` 失败而不能返回更旧值。事务提交时的 `commitTs` MUST 大于该事务的 `startTs` 和最大 `forUpdateTs`。

#### Scenario: Commit occurred after transaction start but before locking read
- **WHEN** 版本 V 在事务 `startTs` 之后、该次 `forUpdateTs` 之前提交，且 Key 上没有其他事务的锁
- **THEN** `GetForUpdate` MUST 允许加锁并返回 V，而不是返回 `startTs` 下的旧版本

#### Scenario: Newer version wins the lock race
- **WHEN** Region 在处理加锁请求时发现最新版本的 `commitTs` 大于请求的 `forUpdateTs`
- **THEN** 加锁 MUST 以 `WriteConflict` 失败且事务 MUST 进入 abort-only

#### Scenario: Commit timestamp follows locking reads
- **WHEN** 一个事务完成多个加锁读并准备提交
- **THEN** 成功提交使用的 `commitTs` MUST 大于事务观察到的最大 `forUpdateTs`

#### Scenario: TSO unavailable before a batch starts
- **WHEN** 多 Key 调用无法在统一 deadline 内取得 `forUpdateTs`
- **THEN** 调用 MUST 返回可识别的 unavailable 或 timeout，且 MUST NOT 向任何 Region 写入该批次的锁

### Requirement: Shared TSO provides monotonic hybrid logical timestamps
共享 TSO MUST 以一个可排序的 `uint64` 同时编码物理毫秒和同毫秒内的逻辑序号。每次发号 MUST 大于所有已发出、已持久化和通过 `Observe` 获知的时间戳；本地物理时钟回拨、TSO 进程重启和 Raft Leader 切换均不能使时间戳倒退或重复。

从旧纯逻辑 allocator 升级时，新格式首个时间戳 MUST 大于旧持久化高水位。持久化格式 MUST 可识别版本，未知格式 MUST 明确拒绝启动而不能从较小值重新发号。

#### Scenario: Multiple allocations in one millisecond
- **WHEN** TSO 在同一个物理毫秒内处理多个分配请求
- **THEN** 每个结果 MUST 具有相同或更大的物理部分和严格递增的逻辑部分

#### Scenario: Wall clock moves backwards
- **WHEN** TSO 观察到的系统时钟小于最后发号的物理部分
- **THEN** TSO MUST 保持最后物理部分并推进逻辑部分，结果 MUST 严格大于此前时间戳

#### Scenario: Legacy high-water migration
- **WHEN** 新 TSO 首次打开旧纯逻辑高水位状态
- **THEN** 它 MUST 原子迁移为带版本的 HLC 状态且首个新时间戳 MUST 大于旧高水位

#### Scenario: Leader change preserves HLC order
- **WHEN** TSO Raft Leader 在已分配时间戳后切换
- **THEN** 新 Leader MUST 在继承已提交高水位后才发号，且新时间戳 MUST 大于切换前所有已确认时间戳

### Requirement: Acknowledged locks are Raft-applied durable state
悲观锁成功响应 MUST 只在所属 Region 的锁命令已经由 Raft 提交并 Apply 到持久化 MVCC 状态之后发出。仅写入 Leader 内存、进入 Raft 日志、取得多数派确认或占有进程内调度 latch 均不足以返回成功。

#### Scenario: Leader fails before Apply
- **WHEN** Leader 在锁命令 Apply 到 MVCC 状态之前失效
- **THEN** 客户端 MUST NOT 收到该锁的成功响应，重试 MUST 由新 Leader 按持久化状态重新裁决

#### Scenario: Leader fails after Apply before response
- **WHEN** 锁已经 Apply 但响应在到达客户端前丢失
- **THEN** 对同一事务和 Key 的重试 MUST 幂等成功或返回可解析的既有事务结果，且 MUST NOT 创建第二把锁

#### Scenario: Restart restores an acknowledged lock
- **WHEN** Region 在确认悲观锁后重启并从日志或快照恢复
- **THEN** 恢复完成后其他事务仍 MUST 观察到该锁冲突，直到原事务 Commit、Rollback 或按既有清理规则被解析

### Requirement: Pessimistic lock records carry compatible update and expiry state
持久化悲观锁 MUST 保存其事务 `startTs`、非零 `forUpdateTs`、固定 Primary 和基于 HLC 物理部分的过期时间。锁编码 MUST 以尾部追加字段的兼容方式扩展；读取缺少 `forUpdateTs` 的旧锁时 MUST 将其解释为 `forUpdateTs = startTs`。旧锁缺少 HLC 过期字段时 MUST 进入受控的 legacy 解析路径，不能把它当作损坏数据、未加锁状态或立即过期锁。

#### Scenario: New binary reads a legacy pessimistic lock
- **WHEN** Region 恢复一个不含 `forUpdateTs` 的旧格式悲观锁
- **THEN** 该锁 MUST 保持有效且其 `forUpdateTs` MUST 按 `startTs` 处理

#### Scenario: New lock survives restart
- **WHEN** 含 `forUpdateTs` 的悲观锁被持久化后 Region 重启
- **THEN** 恢复出的 `startTs`、`forUpdateTs`、Primary、HLC 过期时间和悲观锁标记 MUST 与写入前一致

#### Scenario: Old binary cannot rewrite a new lock
- **WHEN** 集群存在包含新版尾部字段的未决悲观锁
- **THEN** 降级流程 MUST 阻止旧二进制接管或重写该锁，直到新格式锁已全部解析并验证清空

### Requirement: Exact-key conflicts fail fast
不同事务不能同时持有同一个精确 Key 的悲观锁。请求遇到其他事务的悲观锁或 Prewrite Lock 时 MUST 立即返回 `LockConflict`，不得进入锁等待队列。第一阶段中任何 `LockConflict` 或 `WriteConflict` MUST 使当前事务进入 abort-only，调用方 MUST 通过新的 `Begin` 整体重试。

#### Scenario: Competing transaction owns the key
- **WHEN** T1 已持有 K 的悲观锁且 T2 请求锁定 K
- **THEN** T2 MUST 立即收到 `LockConflict`，MUST NOT 等待 T1，且 T2 MUST 进入 abort-only

#### Scenario: Abort-only transaction receives another operation
- **WHEN** 事务因加锁冲突进入 abort-only 后调用 `Get`、`Put`、`Delete`、任一加锁 API 或 `Commit`
- **THEN** 调用 MUST 返回 `AbortOnly` 且 MUST NOT 产生新的读写或锁副作用

#### Scenario: Caller restarts after conflict
- **WHEN** 调用方在 `LockConflict` 后执行 `Rollback` 并重新 `Begin`
- **THEN** 新事务 MUST 使用新的 `startTs`，并可重新执行完整业务逻辑

### Requirement: Multi-key locking is deterministic and cleans partial acquisition
`BatchGetForUpdate` 和 `LockKeys` MUST 在发出 Region 请求前按 Key 原始字节的升序排序并去重，使所有调用方采用相同确定顺序。任一 Key 冲突、超时、不可用或返回未知结果时，整个批次 MUST 失败，事务 MUST 进入 abort-only，并 MUST 对本批次所有已确认和可能已 Apply 的 Key 发起幂等 Rollback。

当相关 Region 可达时，批次失败 MUST 在返回已清理结果前确认所有已获得锁已释放。若统一清理 deadline 内无法确认，调用 MUST 返回 `CleanupPending`；事务仍为 abort-only，后续显式 `Rollback` 或事务恢复 MUST 继续清理，不能把未知锁遗漏为普通失败。

#### Scenario: Duplicate and unsorted keys
- **WHEN** 调用方请求 `[B, A, B]`
- **THEN** 系统 MUST 只对 A、B 各锁一次并按 A、B 的确定顺序获取，同时按 API 契约返回 A、B 对应结果

#### Scenario: Later key conflicts after earlier key succeeds
- **WHEN** 批次已经成功锁定 A，随后锁定 B 时收到 `LockConflict`
- **THEN** 批次 MUST 失败、事务 MUST 进入 abort-only，并 MUST 幂等释放 A 以及清理 B 的任何不确定结果

#### Scenario: Response timeout makes ownership uncertain
- **WHEN** 某个锁请求可能已经 Apply 但客户端在响应前超时
- **THEN** 清理集合 MUST 包含该 Key；未确认释放前 MUST 返回 `CleanupPending` 而不能允许事务 Commit

#### Scenario: Cross-Region partial failure
- **WHEN** 排序后的 Key 分布在多个 Region 且后续 Region 在获取过程中不可用
- **THEN** 已可达 Region 上的已获锁 MUST 被释放，事务 MUST 保持 abort-only，并由 Rollback 或恢复继续处理不可达 Region

### Requirement: Transaction status and lock resolution are authoritative
系统 MUST 提供按 `startTs` 与 Primary 查询的权威事务状态，明确返回 `Locked`、`Committed(commitTs)`、`RolledBack` 或 `NotFound`，并 MUST 区分传输失败与真实 `NotFound`。状态裁决以及必要的过期回滚 MUST 在当前 Region Leader 上通过 Raft 顺序化，不能由客户端拼接多个非原子本地读结果。

`TxnResolveLock` MUST 只根据权威 Primary 状态操作 secondary：Primary 已提交时 roll-forward，已回滚或原子判定为过期且未提交时 rollback，状态未知或服务不可用时不得猜测。重复检查与解析 MUST 幂等。

#### Scenario: Rollback record is not a commit
- **WHEN** Primary 存在与目标 `startTs` 对应的 Rollback write record
- **THEN** `TxnCheckStatus` MUST 返回 `RolledBack`，MUST NOT 把该 write record 的时间戳解释为 commitTs

#### Scenario: Primary commit response is lost
- **WHEN** Primary Commit 已 Apply 但响应丢失
- **THEN** 协调器 MUST 查询权威状态并继续提交 secondary；在限定查询预算内仍无法确定时 MUST 返回 `ResultUnknown(startTs, primaryKey)` 且 MUST NOT 回滚 secondary

#### Scenario: Query failure is not missing state
- **WHEN** `TxnCheckStatus` 因 Leader 不可用、超时或传输错误无法完成
- **THEN** 调用 MUST 返回相应不可用或超时错误，MUST NOT 返回 `NotFound` 或触发事务回滚

#### Scenario: Recovery rolls forward committed primary
- **WHEN** 恢复器发现过期 secondary 且其 Primary 权威状态为 `Committed(commitTs)`
- **THEN** 恢复器 MUST 以相同 commitTs 幂等提交 secondary，直到事务全部收敛

#### Scenario: Recovery rolls back an expired uncommitted transaction
- **WHEN** 权威状态原子确认 Primary 未提交、已超过锁 TTL 且事务不能再合法提交
- **THEN** 恢复器 MUST 持久化回滚裁决并幂等清理 Primary 与 secondary

### Requirement: Repeated lock operations are idempotent
同一事务对同一 Key 的相同请求或传输重试 MUST 保持幂等。已经由该事务持有的悲观锁不能被报告为其他事务冲突；重复请求 MUST 返回与该锁保护下当前已提交版本一致的值或无值结果，并可将记录的 `forUpdateTs` 单调推进但绝不能回退。

#### Scenario: Duplicate request after response loss
- **WHEN** 客户端因响应丢失而以相同事务身份重试一个已 Apply 的 `GetForUpdate`
- **THEN** 重试 MUST 成功返回锁保护下的同一已提交值，且持久化状态中 MUST 只有一把该事务的锁

#### Scenario: Retry reaches a new Leader
- **WHEN** 原 Leader Apply 锁后失效且重试到达新 Leader
- **THEN** 新 Leader MUST 从已恢复状态识别同事务锁并给出幂等结果

#### Scenario: Older repeated forUpdateTs
- **WHEN** 同一事务对已持有的 Key 重放一个较小或相等的 `forUpdateTs`
- **THEN** 锁中记录的 `forUpdateTs` MUST NOT 回退

### Requirement: Prewrite upgrades locks owned by the same transaction
Prewrite 对一个已由同一 `startTs` 事务持有的悲观锁 MUST 原子升级为普通 Prewrite Lock，而不是返回自冲突。升级 MUST 验证锁所有者、Primary 和 `forUpdateTs` 关系，并保留正常 2PC 的重复请求与冲突检查；其他事务的锁绝不能被升级或覆盖。

#### Scenario: Mutation upgrades own pessimistic lock
- **WHEN** 事务先通过 `GetForUpdate(K)` 锁定 K，随后对 K 写入并执行 Prewrite
- **THEN** Apply 后 K 的悲观锁 MUST 被同事务的 Prewrite Lock 替换，且事务可按现有 Primary/Secondary 顺序 Commit

#### Scenario: Prewrite encounters another owner
- **WHEN** Prewrite 的 Key 由不同 `startTs` 的事务持有悲观锁
- **THEN** Prewrite MUST 返回 `LockConflict` 且 MUST NOT 修改或覆盖现有锁

#### Scenario: Duplicate prewrite after upgrade
- **WHEN** 升级已经 Apply 但 Prewrite 响应丢失并被重试
- **THEN** 重试 MUST 幂等识别同事务 Prewrite Lock，不能恢复为悲观锁或生成第二条写入

### Requirement: Commit and Rollback release all pessimistic state
事务第一个成功锁定或写入的 Key MUST 固定为 Primary，事务期间不得因后续 mutation 重新选择。成功 Commit MUST 提交事务写集合，并释放没有被升级为 Prewrite 的纯悲观锁；只持有悲观锁而没有 mutation 的 Commit MUST 对外成功并释放全部锁。显式 Rollback、冲突后的自动清理和事务恢复 MUST 幂等清理普通 Prewrite Lock 与悲观锁；abort-only 事务 MUST NOT Commit。

#### Scenario: Commit with read-only locked keys
- **WHEN** 事务锁定 A、B，仅修改 A 并成功 Commit
- **THEN** A MUST 按 2PC 提交，B 的纯悲观锁 MUST 被释放，随后其他事务可锁定 B

#### Scenario: Explicit rollback
- **WHEN** 持有多个悲观锁的事务调用 `Rollback`
- **THEN** 所有已知或可能获得的悲观锁 MUST 被幂等清理，重复 Rollback MUST 成功或返回等价的已回滚结果

#### Scenario: Abort-only commit attempt
- **WHEN** 事务在部分加锁失败后调用 `Commit`
- **THEN** Commit MUST 返回 `AbortOnly`，MUST NOT Prewrite 或提交任何 mutation，并 MUST 保留或继续执行清理责任

#### Scenario: Lock-only transaction commits successfully
- **WHEN** 事务只持有悲观锁且没有 `Put` 或 `Delete` mutation，然后调用 `Commit`
- **THEN** Commit MUST 返回成功并幂等释放全部锁，不能把正常的只锁事务暴露为回滚失败

#### Scenario: Primary remains stable after a later mutation
- **WHEN** 事务先锁定 A 并随后只修改 B
- **THEN** A MUST 保持该事务的 Primary，B 的 Prewrite Lock MUST 引用 A，协调器不能改选 B

### Requirement: Locking a common exact key can prevent the tested write skew
系统 MUST 提供确定性测试证明：在 Snapshot Isolation 乐观路径中，两个事务读取相同约束状态但写入不同 Key 时可同时提交并产生 write skew；当两个事务在决策前通过 `BatchGetForUpdate` 锁定相同精确读集，或锁定同一约定 guard key 后执行受保护的当前读时，其中一个事务 MUST 冲突并整体重试，使该测试约束始终成立。

该能力仅保证显式列出的精确 Key 的互斥。系统 MUST NOT 宣称自动识别业务约束，也 MUST NOT 将该能力表述为范围锁、gap lock、谓词锁、SSI 或 Serializable。

#### Scenario: Optimistic control demonstrates write skew
- **WHEN** 测试屏障让 T1、T2 在同一 SI 快照读取 A、B 均满足约束，并分别只写 A、B
- **THEN** 未加共同锁的对照执行 MUST 能确定性地让两者提交并展示约束被破坏

#### Scenario: Shared read set serializes the decision
- **WHEN** T1、T2 在计算相同约束前都执行 `BatchGetForUpdate([A, B])`
- **THEN** 至少一个事务 MUST 获得冲突并从新 `Begin` 重试，所有完成执行后的 A、B MUST 始终满足约束

#### Scenario: Guard key requires an application convention
- **WHEN** 所有修改某业务约束的事务都先锁定同一个精确 guard key，并在锁后读取受保护状态
- **THEN** 这些事务的决策 MUST 被该 guard key 串行化；guard key MUST 预先存在并长期保留，未遵守约定的事务不获得此保证

#### Scenario: Disjoint keys do not imply serializability
- **WHEN** 两个事务只锁定互不相交的 Key 且业务约束依赖未锁定的谓词或范围
- **THEN** 系统 MAY 仍出现 write skew，且文档 MUST NOT 声明该事务具有 Serializable 隔离级别

### Requirement: Node-owned recovery and GC preserve bounded transactions
存储 Node MUST 运行可停止、可 join 的常驻事务恢复服务，并通过配置的 TSO endpoints 获取当前 HLC；恢复职责不能依赖 SDK 或 Gateway 进程存活。第一阶段事务最大时长默认 MUST 为 60 秒，悲观锁 TTL 默认 MUST 为 120 秒且始终大于事务最大时长；超过上限的事务 MUST 进入 abort-only 并明确失败，不能继续读写或提交。

MVCC GC MUST 由 Node 侧服务推进，默认版本保留窗口 MUST 至少为 5 分钟且大于事务最大时长与锁 TTL。SDK/Gateway MUST NOT 再独立计算和推进 10 秒 safe point。GC 在删除版本前 MUST 使用 HLC cutoff，并保证所有仍可合法执行的事务所需快照版本尚未被回收。

#### Scenario: No client process remains alive
- **WHEN** 创建锁的 SDK 或 Gateway 崩溃且没有其他客户端进程存活
- **THEN** Node 常驻恢复服务 MUST 仍能在锁到期后查询 Primary 并使事务最终收敛

#### Scenario: Transaction exceeds the hard lifetime
- **WHEN** 当前 HLC 已超过事务 `startTs` 物理时间 60 秒
- **THEN** 下一次事务操作 MUST 返回超时并把事务置为 abort-only，Commit MUST NOT 成功

#### Scenario: GC preserves every legal snapshot
- **WHEN** 一个未超过硬上限的事务读取其 `startTs` 快照且同一 Key 已有多个更新版本
- **THEN** GC MUST 保留该事务所需的可见版本，读取 MUST NOT 因回收而变成错误的值或 `NotFound`

#### Scenario: Client-side GC is disabled
- **WHEN** SDK 或 Gateway coordinator 启动
- **THEN** 它 MUST NOT 创建能够向 Region 推进 safe point 的本地 GC manager

### Requirement: Pessimistic paths use distinct bounded deadlines and are observable
事务年龄与持久锁 TTL MUST 使用 HLC 物理时间判断；单次 RPC、TSO 调用、重试和清理预算 MUST 使用调用进程本地 `steady_clock`，跨 RPC 只能传剩余预算而不能传本地绝对 tick。实现 MUST NOT 创建锁等待线程或让进程内调度 latch 承担事务锁等待。系统 MUST 暴露悲观锁请求、成功、冲突、写冲突、延迟、重复请求、部分失败清理、清理未确认、结果未知、恢复裁决、事务超时和 abort-only 的可区分计数或状态。

#### Scenario: Locked key does not consume a wait queue
- **WHEN** 请求命中其他事务的锁
- **THEN** 请求 MUST 在 RPC 调度容差内返回 `LockConflict`，且 MUST NOT 登记等待者或 wait-for 边

#### Scenario: Cleanup exceeds its deadline
- **WHEN** 部分失败后的 Rollback 无法在端到端 deadline 内确认
- **THEN** 调用 MUST 返回 `CleanupPending`，增加对应可观测计数，并 MUST NOT 将事务恢复为 active

#### Scenario: RPC budget is not persisted as lock time
- **WHEN** 一个锁请求跨进程传递超时预算
- **THEN** 请求 MUST 只携带剩余时长，Region MUST NOT 持久化或比较来源进程的 `steady_clock` 绝对值

#### Scenario: Performance comparison uses equivalent workloads
- **WHEN** 记录乐观路径与悲观路径的性能结果
- **THEN** 两组测试 MUST 使用相同数据、业务约束、并发度和持久化配置，并报告吞吐与 p50/p95/p99 延迟而不能预设悲观路径更快
