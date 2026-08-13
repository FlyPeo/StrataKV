## Purpose

定义共享 Raft 快照在接收、持久化、状态机安装和重新参与选举期间必须满足的单调恢复契约，确保 TSO 水位与 Region 数据都不会因落后副本追赶而回退、跳过或重复应用。

## ADDED Requirements

### Requirement: Snapshot installation has one authoritative completion point
系统 SHALL 以状态机成功安装快照作为快照恢复完成的唯一判定点。在状态机安装成功之前，Raft 层 MUST NOT 将该成员报告为已经完成相应 applied index 的追赶。

#### Scenario: Lagging member receives a snapshot
- **WHEN** 一个成员落后于 Leader 已保留日志的起点并收到 index 为 N 的有效快照
- **THEN** 系统仅在快照内容被状态机成功安装后，才将该成员的状态机 applied index 推进到 N
- **THEN** Raft 与状态机对 N 的完成状态保持一致

#### Scenario: Snapshot persistence succeeds but state-machine installation fails
- **WHEN** 快照文件已经持久化但状态机无法解析或安装快照
- **THEN** 该成员 MUST 失败关闭并且 MUST NOT 报告已追平
- **THEN** 该成员 MUST NOT 对外提供 TSO 发号或 Region 读写服务

### Requirement: Snapshot application is monotonic and idempotent
系统 MUST 忽略不会推进状态机 applied index 的重复或陈旧快照，并且 MUST NOT 使用任何快照降低已经应用的 TSO 水位、MVCC 状态或 applied index。

#### Scenario: Duplicate snapshot delivery
- **WHEN** 相同 snapshot index 和内容被重复投递
- **THEN** 状态机最终状态与只安装一次完全相同
- **THEN** 后续日志 MUST NOT 被重复应用

#### Scenario: Stale snapshot arrives after newer state
- **WHEN** 成员已经应用到 index M 且收到 index N 的快照，其中 N 小于或等于 M
- **THEN** 系统 MUST 保持当前状态不变并安全忽略该快照

### Requirement: Post-snapshot log continuity is preserved
系统 SHALL 从已安装 snapshot index 的下一条日志开始连续应用，不能跳过、重复或乱序执行任何已提交命令。

#### Scenario: Logs follow an installed snapshot
- **WHEN** index N 的快照安装完成并且 N+1 到 N+K 的日志已经提交
- **THEN** 状态机 SHALL 按 index 递增顺序恰好应用 N+1 到 N+K
- **THEN** 最终状态机 applied index SHALL 等于最新已应用日志 index

### Requirement: TSO leadership is gated by recovered state
TSO 成员 MUST 在快照、继承日志和当前任期领导权屏障全部应用完成后才能成功处理 `Next`、`Peek` 或 `Observe`。

#### Scenario: Recovered TSO member wins an election
- **WHEN** 一个通过快照追赶的 TSO 成员当选 Leader
- **THEN** 它返回的第一个时间戳 MUST 大于快照和后续日志中观察到的所有时间戳

#### Scenario: Election occurs before state-machine catch-up
- **WHEN** 成员已获得 Raft Leader 身份但状态机尚未完成快照或日志追赶
- **THEN** `Next`、`Peek` 和 `Observe` MUST 返回可重试的未就绪或非 Leader 结果
- **THEN** 系统 MUST NOT 分配或发布时间戳

### Requirement: Shared Raft snapshot semantics protect every state machine
共享 Raft 快照协议 SHALL 对 TSO 状态机和所有 RegionPeer 状态机使用同一套完成语义，任何调用方都不能通过绕过状态机安装来推进 applied index。

#### Scenario: Region follower catches up through snapshot
- **WHEN** Region follower 只能通过快照恢复 RocksDB/MVCC 状态
- **THEN** 它只有在状态机恢复成功后才被视为已应用该 snapshot index
- **THEN** 后续成为 Leader 时对外提供的状态 MUST 包含该快照中的数据
