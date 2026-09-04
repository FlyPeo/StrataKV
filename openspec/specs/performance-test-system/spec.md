# performance-test-system Specification

## Purpose
为 StrataKV 定义一套可在本地 WSL 完整执行、结果可复现且足以向面试官证明性能、故障恢复和一致性能力的核心测试契约，同时严格区分 YCSB 单记录 OPS 与跨 Region 事务 TPS。

## Requirements

### Requirement: YCSB-compatible workloads preserve the core read/write mixes

系统 MUST 提供单记录 YCSB-compatible workload A、B、C、F：A 为 50% Read/50% Update，B 为 95% Read/5% Update，C 为 100% Read，F 为 50% Read/50% Read-Modify-Write。一次操作 MUST 只选择一个记录 Key 并路由到一个 Region，完整数据集 MUST 覆盖所有 Region。Key 选择 MUST 支持 uniform 和版本化 Zipfian，并把算法版本、参数和 seed 写入 manifest。结果 MUST 标为 `record OPS`，不得称为跨 Region transaction TPS。

本期 MUST 拒绝 YCSB Scan/E，不得用多次 Point Get 冒充范围扫描。

#### Scenario: Workload mixes are exact and deterministic
- **WHEN** A、B、C、F 分别以相同 seed 生成 N 个 operation
- **THEN** 实际 Read、Update、Read-Modify-Write 数量 MUST 符合各 workload 定义，并且重复运行产生相同操作类型和 record id 序列

#### Scenario: One operation touches one Region
- **WHEN** runner 执行一次 Read、Update 或 Read-Modify-Write
- **THEN** 该 operation MUST 只访问一个 record Key，路由统计 MUST 只包含该 Key 所属的一个 Region

#### Scenario: Uniform and Zipfian selection are bounded
- **WHEN** runner 对同一记录范围执行 uniform 或 Zipfian workload
- **THEN** 所有 record id MUST 位于 manifest 范围内，报告 MUST 标明实际分布，且 Zipfian 热点集中度 MUST 通过统计测试

#### Scenario: Unsupported scan is rejected
- **WHEN** 调用方选择 workload E 或 Scan
- **THEN** runner MUST 在施压前返回 unsupported，MUST NOT 产生可与 A/B/C/F 比较的性能结果

### Requirement: Load and run use a reproducible dataset and common adapters

Runner MUST 分离 `load` 与 `run`。Load MUST 用固定 Key codec、record count、value size 和 seed 创建数据并验证每个 Region 的记录数；Run MUST 读取兼容的完整 manifest，只访问已装载记录。相同 load 参数在中断后重跑 MUST 幂等恢复，不能扩大逻辑键空间。

公共 workload core MUST 支持 Direct SDK 和 Gateway adapter。两条路径使用相同 generator、Key、事务语义和计时边界，但结果 MUST 使用独立命名空间。Gateway MUST 记录 connection mode；重试时端到端延迟 MUST 包含失败尝试和退避，并单独统计 retry、timeout 与 result-unknown。

#### Scenario: Duplicate load resumes safely
- **WHEN** Load 在部分完成后中断并以同一 manifest 参数重跑
- **THEN** 它 MUST 写入相同 Key/初始值并最终通过记录数与 value 校验，MUST NOT 创建额外逻辑记录

#### Scenario: Run rejects incompatible data
- **WHEN** manifest 缺失、state 不是 complete、Region 边界不同或 value size 不兼容
- **THEN** runner MUST 在测量前失败，并把该 case 标为 invalid 而不是输出可比较吞吐

#### Scenario: Direct and Gateway are separated
- **WHEN** 同一操作序列分别通过 Direct SDK 和 Gateway 执行
- **THEN** 两组 MUST 分别报告 OPS、P50/P95/P99/Max、错误和客户端资源，MUST NOT 合并延迟样本

#### Scenario: Gateway retry preserves the client boundary
- **WHEN** Gateway operation 首次超时后按故障 profile 重试并最终成功
- **THEN** successful OPS、retry count 和 result classification MUST 分开，客户端延迟 MUST 从首次调用前计到最终结果确定后

### Requirement: A1 reports concurrency curves for all retained YCSB mixes

A1 MUST 对 Gateway 全链路的 A/B/C/F uniform workload 执行 `1/4/8/16/32` worker 并发曲线，每点三次；MUST 对 Gateway Zipfian A/B 在 8 workers 运行代表点；MUST 对 Direct SDK uniform A/C 在 8 workers 运行分层对照。每个 case MUST 从兼容的 post-load checkpoint 开始，并报告最大吞吐、满足 P99 约束的可用吞吐和吞吐不再增长时的并发拐点。

#### Scenario: Gateway A through F concurrency matrix
- **WHEN** 执行 `interview-full` A1
- **THEN** A/B/C/F uniform 的五个并发点、Zipfian A/B 代表点和 Direct A/C 对照点 MUST 各有三份原始结果及中位数

#### Scenario: One repetition is unstable
- **WHEN** 同一 case 任一轮吞吐相对三轮中位数偏差超过 10%
- **THEN** case MUST 标记 unstable、展示三轮原值，并且 MUST NOT 只发布中位数作为稳定结论

### Requirement: A2 and A3 isolate distributed transaction cost

跨 Region workload MUST 用 `transaction TPS` 表示完整逻辑事务。A2 MUST 固定三次 mutation、value size、分布、seed、并发与操作数，只比较跨 Region 事务占比 0/15/100%；本地事务的三个 Key MUST 位于同一 Region，跨 Region 事务 MUST 固定覆盖三个 Region。A3 MUST 固定每事务三次 mutation，只比较参与 Region 数 1/2/3。Key MUST 根据实际 Region 边界生成，不能依赖固定 `a/h/p` 假设。

#### Scenario: A2 ratio is the only changed variable
- **WHEN** A2 比较 0/15/100% 三个点
- **THEN** 三点 MUST 使用兼容 checkpoint 和相同事务参数，并分别报告目标/实际 local、distributed attempted/committed 数量、TPS 和延迟

#### Scenario: A3 keeps transaction size constant
- **WHEN** A3 比较 1/2/3 Region
- **THEN** 每笔事务 MUST 始终包含三次 mutation，报告 MUST 展示实际 Region fanout、committed TPS 和端到端延迟，不得预设成本曲线形状

#### Scenario: Partial Region failure is not committed
- **WHEN** 部分 Region 已接受 prewrite 而另一 Region timeout 或 unavailable
- **THEN** 事务 MUST 分类为 timeout、unavailable、cleanup-pending 或 result-unknown，MUST NOT 计入 committed TPS，恢复检查 MUST 验证没有部分提交

### Requirement: A4 reports useful work under contention

A4 MUST 在目标争用份额 0/5/20% 下，用相同 checkpoint、热点集合、操作序列和有限重试预算比较乐观事务与现有悲观 fast-fail 事务。报告 MUST 同时给出目标份额、实际冲突率、attempted TPS、committed TPS、成功率、平均 attempts/commit 和包含退避的业务完成延迟。只有曲线真实交叉时才可报告策略切换点。

#### Scenario: Target share differs from conflict rate
- **WHEN** 目标争用份额为 5% 但实测冲突率不同
- **THEN** 报告 MUST 同时保留两个值，并以实测冲突率解释有效吞吐

#### Scenario: Fast failures are not successful throughput
- **WHEN** 悲观模式快速返回大量冲突
- **THEN** attempted TPS MAY 很高，但 committed TPS、成功率和业务完成延迟 MUST 独立报告，MUST NOT 把失败尝试称为性能提升

#### Scenario: No crossover is observed
- **WHEN** 同一种策略在所有实测点的 committed TPS 和业务完成延迟均占优
- **THEN** 报告 MUST 写明未观察到切换阈值，MUST NOT 外推不存在的交叉点

### Requirement: Metrics name their subject and timing boundary

每个指标 MUST 声明主体、单位、分母与计时边界。单记录 workload MUST 报告 attempted/successful record OPS 和各操作类型的 P50/P95/P99/Max；跨 Region workload MUST 报告 attempted/committed transaction TPS 与 Begin 前到最终 Commit 结果后的端到端延迟；Gateway HTTP QPS MUST 单独命名。错误 MUST 至少分为 conflict、timeout、unavailable、cleanup-pending 和 result-unknown。

客户端 MUST 采集 CPU/RSS；编排器 MUST 在 case 前后采集服务进程 CPU/IO、Leader 布局以及现有 Raft/MVCC/GC 状态。只有样本数不少于 100,000 时，P99.9 才可标为有效结论；本期不建立 P99.99 门禁。

#### Scenario: Attempted and successful throughput differ
- **WHEN** N 次尝试中只有 S 次成功
- **THEN** attempted throughput MUST 使用 N，successful throughput MUST 使用 S，失败必须按类型计数

#### Scenario: Commit-only latency is not end-to-end latency
- **WHEN** 一个组件结果只测量 Commit 调用
- **THEN** 字段 MUST 命名为 commit latency，MUST NOT 与包含 Begin、读写和网络的端到端延迟直接比较

#### Scenario: Extreme percentile has too few samples
- **WHEN** 一个 case 的成功样本少于 100,000
- **THEN** 报告 MUST 提供 P50/P95/P99/Max，但 P99.9 MUST 标记 exploratory 或省略，MUST NOT 用于门禁

### Requirement: Interview profiles have executable local scales

系统 MUST 提供两个可版本化 profile，所有覆盖参数 MUST 写入 manifest：

- `interview-smoke`：3,000 条 × 256 B；Gateway A/C 各 10,000 operations、1/8 workers；A2 0/100%、A3 1/3 Region、A4 两种模式 × 0/20% 各 1,000 transactions；B1；C1 1,000 transfers；C2 300 operations；一次。
- `interview-full`：100,000 条 × 1 KiB；A1 Gateway uniform A/B/C/F 每点 20,000 operations、1/4/8/16/32 workers、三次，Gateway Zipfian A/B 与 Direct uniform A/C 在 8 workers 各 20,000 operations、三次；A2 0/15/100% 与 A3 1/2/3 Region 各 10,000 transactions、8 workers、三次；A4 两种模式 × 0/5/20% 各 5,000 transactions、16 workers、三次；随后执行 B1、B3、C1 10,000 transfers 和按 epoch 分段的 C2 1,000 operations。

两个 profile 的报告 MUST 标记为 `development-baseline` 和 `invalid-for-production-capacity`。

#### Scenario: Smoke finishes with the required subset
- **WHEN** 用户运行 `interview-smoke` 且不覆盖参数
- **THEN** runner MUST 按定义规模完成全部必选 case、生成 complete 报告并停止专用项目

#### Scenario: Full profile retains every YCSB mix
- **WHEN** 用户运行 `interview-full`
- **THEN** A、B、C、F MUST 全部出现在 Gateway uniform 并发曲线中，A/B MUST 有 Zipfian 代表结果，A/C MUST 有 Direct SDK 对照

### Requirement: Runs are self-describing and scoped

每次执行 MUST 在 `test-results/performance/<run-id>/` 创建独立目录，至少包含 `manifest.json`、`summary.csv`、`summary.json`、`REPORT.md`、`raw/`、`metrics/`、`faults/` 和 `histories/`。Manifest MUST 记录 commit/dirty 状态、构建类型、二进制标识、OS/CPU/内存/文件系统、拓扑和 Region 边界、Leader 布局、数据集、workload、seed、并发、重试与超时。开始时 MUST 标记 incomplete，只有所有必选文件和检查通过后才更新为 complete。

生命周期操作 MUST 只作用于显式测试 project 和已解析 PID；正常、失败或信号退出均 MUST 停止测试集群，但结果默认保留。性能工具 MUST NOT 在仓库根目录写 JSON/JSONL。

#### Scenario: Interrupted run preserves evidence
- **WHEN** runner 在 case 中断或超时
- **THEN** 已有产物 MUST 保留并标记 incomplete，MUST NOT 覆盖完整 run 或进入 D1 基线

#### Scenario: Cleanup is project scoped
- **WHEN** runner 异常退出
- **THEN** 它 MUST 只停止自己创建的 project 和已验证 PID，MUST NOT 使用宽泛 `pkill` 或删除其他项目数据

### Requirement: B1 and B3 prove fault recovery without partial commit

B1 MUST 从权威状态解析当前 Region Leader，在持续跨 Region 写入期间只向该 project 的 Leader PID 发送 `SIGKILL`，记录信号、退出、新 Leader 可服务、旧节点恢复与 workload 结束时间。所有 acknowledged transaction MUST 在恢复和完整重启后全量可见。

B3 MUST 在显式测试构建中提供两个一次性 project-scoped barrier：`after_all_prewrite_before_primary_commit` 和 `after_primary_commit_before_secondaries`。编排器 MUST 等待 marker 后强杀 Coordinator。Primary 未提交时事务 MUST 回滚/清理；Primary 已提交时 secondary MUST 用相同 commitTs 幂等 roll-forward。默认生产构建 MUST 无法触发 failpoint。

#### Scenario: Region Leader is killed during writes
- **WHEN** B1 在持续负载中 SIGKILL 当前 Region Leader
- **THEN** runner MUST 记录 committed、timeout、unavailable、result-unknown 与恢复时间，并最终验证每个 acknowledged transaction 的全部 Key

#### Scenario: Coordinator stops before primary commit
- **WHEN** B3 在全部 prewrite 后、Primary Commit 前命中 marker 并杀死 Coordinator
- **THEN** 恢复后该事务 MUST 回滚或保持未提交，MUST 不存在部分可见 Key或永久锁

#### Scenario: Coordinator stops after primary commit
- **WHEN** B3 在 Primary Commit 已 Apply、secondary 尚未全部提交时杀死 Coordinator
- **THEN** 恢复器 MUST 根据 Primary 权威状态 roll-forward secondary，全部 Key MUST 以同一 commitTs 可见

#### Scenario: Duplicate recovery is idempotent
- **WHEN** B3 恢复与 lock resolution 被重复执行
- **THEN** 最终状态 MUST 与执行一次相同，MUST NOT 产生重复版本、状态反转或残留锁

### Requirement: C1 and C2 make bounded consistency claims

C1 MUST 对固定账户集执行并发双 Key 转账，并在无故障、B1 和 B3 后读取全部账户验证总额、记录编码和 acknowledged 转账。报告只能宣称跨 Key 原子性与业务不变量成立，不能外推为完整 Snapshot Isolation。

C2 MUST 为单 Key register 的每个 Read/Write 记录 invocation、completion、input、output、status 和 fault event，并检查是否存在尊重实时先后关系的合法串行历史。Checker MUST 用 quiescent epoch 控制搜索规模，并输出 pass、fail 或 inconclusive 及可诊断反例。简单值不回退不能代替线性一致性检查。

#### Scenario: Transfer total survives faults
- **WHEN** 多 worker 在基线、B1 或 B3 场景执行转账
- **THEN** 所有 acknowledged transaction 收敛后账户总额 MUST 等于初始值，任何部分转账、错误值或确认金额丢失 MUST 使 C1 失败

#### Scenario: Register history is linearizable
- **WHEN** C2 在无故障和 B1 Leader 切换期间产生完整 history
- **THEN** checker MUST 找到尊重 `complete(A) < invoke(B)` 的合法 register 顺序，否则 MUST 失败并输出反例

#### Scenario: Unknown write remains pending
- **WHEN** 故障造成写入结果未知且权威事务状态仍无法确定
- **THEN** checker MUST 将其作为 pending 或 inconclusive 处理，MUST NOT 猜测成功或失败

### Requirement: D1 compares only compatible evidence

D1 MUST 只比较 manifest 中 commit/build、path、workload、dataset、value size、distribution、topology、profile 和 connection mode 兼容且 state=complete 的 run。三次重复 MUST 以中位数为中心并展示原值；默认门禁为 successful throughput 下降超过 10% 或 P99 上升超过 20%。任一正确性、C2、acknowledged durability 或清理失败 MUST 无条件阻断。

D1 的面试核心集 MUST 至少包含 A1 Gateway uniform A/C 的 8-worker 点、A2 0/100%、A3 3 Region、A4 两种模式的 20%、B1、C1 和 C2。

#### Scenario: Compatible performance regresses
- **WHEN** 当前三轮中位 successful throughput 比兼容基线低 12%
- **THEN** D1 MUST 失败并展示两边绝对值、相对变化和 P99

#### Scenario: WSL is not production capacity
- **WHEN** 当前结果来自客户端与服务共置的 WSL
- **THEN** 报告 MUST 标记 development baseline，MUST NOT 输出 production capacity 结论

#### Scenario: Correctness overrides performance
- **WHEN** 吞吐提升但 B1/B3/C1/C2 任一安全门禁失败
- **THEN** 整体结论 MUST 为 FAIL，MUST NOT 用性能提升抵消正确性失败
