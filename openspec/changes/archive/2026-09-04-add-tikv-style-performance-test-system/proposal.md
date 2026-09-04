## Why

StrataKV 已有若干组件基准和一次性的全链路结果，但缺少一套面试时能够快速复现、清楚解释且经得起追问的证据链：标准 YCSB 读写模型、跨 Region 2PC 成本、冲突策略、Leader/Coordinator 故障恢复以及一致性检查。当前方案的 58 项任务包含完整性能平台、特权网络隔离、多机容量和长期 soak，超出“证明系统能力”的必要范围，需要收缩成能在当前 WSL 落地的核心套件。

## What Changes

- 保留四项性能证据：
  - A1：YCSB-compatible 单记录负载，完整保留 A（50% Read/50% Update）、B（95% Read/5% Update）、C（100% Read）和 F（50% Read/50% Read-Modify-Write），支持 uniform/Zipfian 与并发梯度。
  - A2：固定三 mutation 事务，比较跨 Region 事务占比 0/15/100%。
  - A3：固定三 mutation，比较参与 Region 数 1/2/3。
  - A4：比较目标争用份额 0/5/20% 下乐观事务与现有悲观 fast-fail 事务的有效提交吞吐。
- 保留两项故障证据：B1 在持续写入时精确强杀 Region Leader；B3 在 Primary Commit 前后两个关键阶段强杀 2PC Coordinator，并验证事务最终收敛。
- 保留两项一致性证据：C1 并发转账总额守恒；C2 用带 invocation/completion 时间的单 Key register history 做有界线性一致性检查。
- 保留 D1 回归：只比较兼容环境下三次重复的中位数，同时把正确性失败作为无条件门禁。
- 提供公共 C++ workload core、Direct SDK/Gateway adapter、load/run 分阶段执行、固定 seed、基础 checkpoint、统一指标和结果目录。
- A1 的 Gateway 全链路对 A/B/C/F 执行 `1/4/8/16/32` 并发梯度；Direct SDK 只选择 A/C 的代表并发做分层对照。Uniform 覆盖完整矩阵，Zipfian 只对 A/B 的代表并发执行，避免无价值的笛卡尔积。
- 固化两个本地 profile：`interview-smoke` 用于快速正确性门禁，`interview-full` 用于生成面试报告。结果必须标记为 WSL development baseline，不作为生产容量。
- 统一报告 record OPS、transaction TPS、P50/P95/P99/Max、满足样本门槛时的 P99.9、错误/冲突/重试、客户端 CPU/RSS、进程 IO 和现有 Raft/MVCC/GC 状态。
- 所有产物写入 `test-results/performance/<run-id>/`，包含 manifest、原始结果、故障时间线、history/check 结果、CSV/JSON 汇总和 `REPORT.md`；所有测试程序开头注明目标、策略、数据规模和验证内容。
- 不改变现有事务、Raft、MVCC、Gateway API 或存储格式；新增 failpoint 只存在于显式测试构建。

本期明确不做：B2 network namespace/iptables 网络分区、Region Follower/TSO Leader 故障矩阵、100 万条 nightly、多机 release capacity、6～24 小时 soak、YCSB Scan/E、TPC-C/Sysbench、HTTP keep-alive、完整 Prometheus 监控平台和所有内部阶段的 histogram。

## Capabilities

### New Capabilities

- `performance-test-system`: 定义 A1–A4、B1/B3、C1/C2、D1 的面试证明套件，包括 YCSB A/B/C/F、可执行规模、指标口径、结果目录和正确性门禁。

### Modified Capabilities

无。现有 `pessimistic-transactions` 行为契约保持不变；A4 只测量其既有 fast-fail 语义。

## Impact

- 主要影响 `test/support/performance/`、`test/benchmarks/`、`test/system/`、`test/CMakeLists.txt`、`deploy/` 和测试结果目录。
- Gateway/SDK/事务协调器只增加测试 adapter 或低开销只读统计；Region/Raft/MVCC 优先复用现有状态文件与计数器，不建设新的全阶段监控系统。
- B1 的风险是误杀其他项目，必须只使用显式 project 的已验证 PID；B3 的风险是测试 failpoint 泄漏到生产路径，必须由测试构建和 project token 双重限制。
- 预期运行开销主要来自客户端直方图与低频 `/proc`、状态文件采样；不得在 Raft Apply 或事务热路径逐请求落盘。
- 现有 CLI 和测试目标保持兼容。回滚时删除新增 runner、编排入口和测试 failpoint 即可，不需要迁移数据；专用项目由 scoped cleanup 回收，结果目录默认保留。
