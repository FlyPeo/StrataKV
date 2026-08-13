## Purpose

定义 Raft stable state 与 snapshot 在进程崩溃、主机掉电和部分文件损坏情况下的持久化与恢复契约，使选举投票、日志复制和快照恢复不会从半写或静默重置的状态继续运行。

## ADDED Requirements

### Requirement: Stable-state updates are crash atomic
每次 Raft stable state 更新 MUST 在恢复后呈现为完整旧版本或完整新版本，系统 MUST NOT 读取到截断、拼接或部分写入的中间版本。

#### Scenario: Process stops during a state write
- **WHEN** 进程在 stable state 写入、同步、替换或目录同步的任意阶段被终止
- **THEN** 重启后系统 SHALL 恢复最后一个完整且已确认的版本，或者检测错误并失败关闭
- **THEN** 系统 MUST NOT 将半写文件解释为空白 Raft 状态

#### Scenario: Host loses power after a successful persistence call
- **WHEN** 持久化操作已经向 Raft 报告成功后主机立即掉电
- **THEN** 重启后对应 term、vote、log 和 snapshot 元数据 MUST 可恢复

### Requirement: Raft acknowledgements follow durable persistence
成员 MUST 在相关 term、vote、日志或 snapshot stable state 达到持久化完成点之后，才向其他成员确认会影响 Raft 安全性的 RPC 成功。

#### Scenario: AppendEntries updates follower log
- **WHEN** follower 接受会新增或覆盖日志的 AppendEntries
- **THEN** follower MUST 在日志 stable state 持久化成功后才返回成功

#### Scenario: Vote is granted
- **WHEN** 成员决定在新任期中投票给候选者
- **THEN** 新 term 和 voted-for 信息 MUST 在投票成功响应前持久化

### Requirement: State and snapshot generations are recoverable as a pair
系统 SHALL 保存可验证的格式版本、generation 和内容完整性信息，使恢复过程能够选择彼此一致的 stable state 与 snapshot 组合。

#### Scenario: Snapshot replacement is interrupted
- **WHEN** 新 snapshot 已写入但引用它的 stable state 尚未完成，或 stable state 已准备但 snapshot 尚未完成
- **THEN** 恢复过程 SHALL 选择最后一个完整匹配的 generation
- **THEN** 系统 MUST NOT 把新旧 generation 混合恢复

#### Scenario: Snapshot and metadata match
- **WHEN** stable state 与 snapshot 的 generation 和完整性校验均匹配
- **THEN** 系统 SHALL 恢复该 generation 并从其 snapshot index 继续

### Requirement: Corruption is detected and fails closed
系统 MUST 检测格式头、长度、generation 或校验和不一致，并在无法证明状态完整时拒绝该成员参与选举、复制确认或业务服务。

#### Scenario: Stable state is truncated
- **WHEN** stable state 文件只包含预期内容的一部分
- **THEN** 启动 MUST 失败并报告明确的持久化损坏错误
- **THEN** 系统 MUST NOT 以 term 0 和空日志继续运行

#### Scenario: Snapshot content is corrupted
- **WHEN** snapshot 内容与其完整性信息不一致
- **THEN** 状态机 MUST NOT 安装该 snapshot
- **THEN** 成员 MUST 失败关闭或进入显式修复状态

### Requirement: Existing persisted data has a controlled migration path
升级后的系统 MUST 能识别当前旧格式并在不丢失既有 term、vote、log 和 snapshot 的前提下迁移。产生旧版本无法读取的新格式后，系统 MUST 阻止无准备的直接降级。

#### Scenario: First startup with legacy files
- **WHEN** 成员使用当前版本的旧格式 Raft 文件首次启动新实现
- **THEN** 系统 SHALL 完整读取旧状态并在下一次安全持久化时迁移到新格式

#### Scenario: Downgrade is attempted after migration
- **WHEN** 运维尝试使用不支持新格式的旧二进制启动已迁移目录
- **THEN** 启动流程 MUST 提示需要恢复升级前备份或执行显式格式回退

### Requirement: Persistence failures are observable
每次持久化失败 SHALL 提供成员身份、操作阶段、目标 generation 和可操作错误原因，同时不得泄露数据内容。

#### Scenario: Directory synchronization fails
- **WHEN** 文件替换成功但父目录同步失败
- **THEN** 持久化操作 MUST 返回失败并记录失败阶段
- **THEN** 上层 MUST NOT 将对应 Raft 操作视为已持久完成
