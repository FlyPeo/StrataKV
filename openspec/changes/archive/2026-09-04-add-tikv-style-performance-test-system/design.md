## Context

参见 [proposal.md](proposal.md) 的范围与 [performance-test-system spec](specs/performance-test-system/spec.md) 的行为契约。

StrataKV 已有 TSO、Region 2PC、热点争用和 Gateway 可靠性测试，但它们的生成器、数据规模、延迟边界和结果格式不同，难以形成一组可以复跑、对比并向面试官解释的证据。当前本地标准拓扑是 1 个 Gateway、3 个 TSO 和 3 个 NodeServer；每个 NodeServer 承载 3 个 RegionPeer，因此是 3 个 Raft Group、每组 3 副本。

本变更不建设完整的 TiKV 性能实验室，而是借鉴其“标准负载 → 分布式事务 → 故障恢复 → 一致性校验 → 版本回归”方法，落地一套规模适合 12 vCPU/16 GiB WSL 的面试证据集。结果只能用于开发机趋势和版本前后比较，不能宣称为生产容量或与 TiKV 官方结果横向对比。

## Goals / Non-Goals

**Goals:**

- 保留 YCSB-compatible A/B/C/F 四种核心读写分布，并清楚区分单记录 OPS 与跨 Region transaction TPS。
- 用同一生成器驱动 Direct SDK 与 Gateway，使操作序列、数据集、计时和错误分类可比。
- 用 A1–A4 证明吞吐拐点、跨 Region 比例成本、参与 Region 数成本以及争用策略边界。
- 用 B1、B3、C1、C2 证明 Leader 故障恢复、2PC 决策收敛、转账原子性和有界寄存器线性一致性。
- 用固定初始 checkpoint、三次重复、中位数和兼容签名形成 D1 回归证据。
- 一条命令运行 smoke 或 full，产物自描述、退出后不残留测试集群。
- 每个新增测试文件开头说明测试目标、测试策略、数据规模和验证条件。

**Non-Goals:**

- 不接入 Go 版 go-ycsb，也不声称结果经过官方 YCSB 认证。
- 不实现 Scan/Workload E、TPC-C、Sysbench、长时间 soak 或多机 release 容量测试。
- 不实现 B2 network namespace/iptables 分区，不扩展 Region Follower/TSO Leader 的完整故障矩阵。
- 不为本测试体系新建完整 Prometheus/dashboard 或逐阶段高频直方图平台。
- 不改 HTTP keep-alive、事务协议、Raft 时序、存储格式或线上默认行为。
- 不用转账守恒单独证明完整 Snapshot Isolation，也不用有限历史外推无限执行下的线性一致性。

## Decisions

### 1. 采用一个 C++ workload core 和两个 adapter

测试支持层包含以下职责：

- `WorkloadSpec`：不可变地保存 profile、workload、分布、seed、数据规模、并发和超时。
- `DatasetManifest`：保存 Key 编码、Region 边界、记录数、value 大小、seed 和 load 状态。
- `OperationGenerator`：以全局 operation sequence number 和 seed 决定操作类型、record id 与 Region，不依赖线程调度顺序。
- `DirectSdkAdapter` / `GatewayAdapter`：实现相同的 Read、Update、RMW 与跨 Region transaction 接口。
- `WorkloadRunner`：管理 worker、预热、测量窗口、截止条件和取消。
- `RunAggregator`：合并线程局部计数器与直方图，生成 interval 和 summary。

主进程持有一份 `WorkloadSpec` 和一个原子全局序号。每个 pthread worker 独占 adapter session、计数器和直方图，热路径不争用全局统计锁；聚合器仅周期读取快照。Gateway 的 fiber 仍只处理 HTTP socket，阻塞 SDK/2PC/MPRPC 继续由现有有界 pthread pool 执行。Direct adapter 跳过 HTTP/Gateway，其余事务、Raft 和存储路径相同。

```text
Gateway: runner worker -> HTTP -> Gateway request pool -> Client/2PC
Direct:  runner worker -----------------------------> Client/2PC
                                                     -> NodeServer RPC
                                                     -> RegionPeer/Raft
                                                     -> Apply/MVCC/RocksDB
```

这样可以比较 Gateway 接入层开销，但不能把 Direct 与 Gateway 的吞吐直接混成一个基线；`path` 和 `connection_mode` 必须进入兼容签名。

### 2. YCSB A/B/C/F 保持单记录语义

数据集均匀覆盖三个 Region，但每次 YCSB operation 只访问一个 record，因此只进入一个 Region：

```text
Read:              Begin -> Get(K) -> Rollback
Update:            Begin -> Put(K,Vn) -> Commit
Read-Modify-Write: Begin -> Get(K) -> Put(K,f(V)) -> Commit
```

- A：50% Read / 50% Update，代表读写均衡。
- B：95% Read / 5% Update，代表读多写少。
- C：100% Read，代表只读上限。
- F：50% Read / 50% Read-Modify-Write，代表读后改同一记录。

Uniform 用于稳定基线，Zipfian 用于热点差异。操作比例由 sequence number 与 seed 确定，报告实际操作计数。Scan/E 在启动施压前明确拒绝。这里的 OPS 是客户端完成的单记录操作数；它不是跨分片事务 TPS。

### 3. A1 只保留能回答三个核心问题的矩阵

A1 的完整矩阵为：

- Gateway + Uniform：A/B/C/F，各跑 workers=1/4/8/16/32，回答全链路吞吐峰值、P99 和拐点。
- Gateway + Zipfian：A/B，仅跑 workers=8，回答热点是否显著放大写入与尾延迟。
- Direct SDK + Uniform：A/C，仅跑 workers=8，回答绕过 Gateway 后的代表性读写差异。

每个 full 点执行 20,000 operations、独立恢复 checkpoint、重复 3 次并取中位数。该矩阵保留四种读写分布，同时避免 Direct/Gateway × 所有 workload × 所有并发的笛卡尔积。

### 4. A2/A3/A4 分别只改变一个主变量

- A2 固定 3 mutations、3 Region 跨分布式事务结构和 workers=8，只改变跨 Region transaction 占比 0/15/100%。本地事务在三个 Region 间轮换；跨 Region 事务访问三个 Region。按 local/distributed 分桶输出 attempted/committed TPS 和延迟。
- A3 固定每笔 3 mutations、workers=8，只改变实际参与 Region 数 1/2/3。报告 TPS、P99 和每增加一个参与者的实测成本，不预先假定超线性。
- A4 固定事务与数据，只改变 target contention share 0/5/20%，分别运行乐观和当前悲观 fast-fail，workers=16。必须同时报告实际 LockConflict/WriteConflict、成功率、attempts/commit、committed TPS 和包含退避的端到端延迟；只有曲线真实交叉时才给出策略切换点。

跨 Region 用 transaction TPS，A1 单记录用 record OPS。所有端到端延迟从首次客户端调用前开始，到成功、明确失败或 result-unknown 分类完成为止；启用重试时包含失败尝试和退避。

### 5. 两个固定 profile 控制规模

`interview-smoke` 用于开发期正确性与脚本闭环：

- Load 3,000 records × 256 B。
- A1 只跑 Gateway Uniform A/C，10,000 ops，workers=1/8，各 1 次。
- A2 跑 0/100%，A3 跑 1/3 Region，A4 跑两种模式 × 0/20%，每点 1,000 transactions。
- B1 跑一次；C1 跑 1,000 transfers；C2 跑 300 operations。

`interview-full` 用于最终面试证据：

- Load 100,000 records × 1 KiB。
- A1 按决策 3 的矩阵执行，每点 20,000 ops、3 次重复。
- A2 0/15/100% 与 A3 1/2/3 Region，每点 10,000 transactions、workers=8、3 次重复。
- A4 两种模式 × 0/5/20%，每点 5,000 transactions、workers=16、3 次重复。
- 随后执行 B1、B3、C1 10,000 transfers 和 C2 1,000-operation epochs。

两个 profile 都写入 `environment_class=development-baseline` 和 `capacity_claim=invalid`。若 full 超出当前机器预算，必须保留矩阵和数据规模，只能在 manifest 中明确记录未完成，不得用少跑点伪装完整结果。

### 6. Load checkpoint 隔离顺序污染

编排器完成一次确定性 Load 和 warmup 后，先 scoped down 集群，再用 reflink（不可用则普通复制）建立 post-load checkpoint。每个写入型 case/并发点/重复轮次都从 checkpoint 恢复，启动后做记录数和抽样值校验；只读 C 可共享稳定数据集。

该策略隔离 MVCC 历史版本、Raft log、flush/compaction 和缓存随执行顺序增长造成的偏差。创建或恢复 checkpoint 前必须验证测试 project 的所有进程已经退出；任何 cleanup 只能作用于显式 project 路径。

### 7. 只采集解释结论所需的指标

公共结果至少包含：

- record attempted/successful OPS，transaction attempted/committed TPS；
- P50/P95/P99/Max；样本数至少 100,000 时才展示 P99.9；
- error、timeout、conflict、retry、result-unknown 和 attempts/commit；
- load generator CPU/RSS，各服务进程 CPU、RSS 与读写 IO；
- 现有 status/metrics 能提供的 Leader、Raft/MVCC/GC 前后快照。

不为了本方案修改每个服务热路径来补齐细粒度 stage histogram。采集器读取 `/proc` 和已有状态输出，测试 worker 使用固定内存线程局部直方图。P99.99 不作为本规模的报告或门禁指标，因为样本量不足以稳定支持它。

### 8. 结果目录自描述且原子完成

```text
test-results/performance/<run-id>/
├── manifest.json
├── raw/                 # A1/A2/A3/A4 每轮原始 summary/interval
├── metrics/             # 客户端与服务 before/after 快照
├── faults/              # B1/B3 marker 与时间线
├── histories/           # C1/C2 history 与 checker 结果
├── summary.json
├── summary.csv
└── REPORT.md
```

创建 run 时先写 `state=incomplete`；所有必需 case、checker 和报告完成后，才通过同目录临时文件加 rename 更新为 `complete`。Manifest 保存 git commit、binary hash、build type、硬件/WSL、拓扑、profile、path、connection mode、数据集、seed、重试策略和 checkpoint 方法。

### 9. B1 只验证当前 Region Leader crash

B1 在持续写负载期间，从权威 status 解析目标 Region 当前 Leader 和对应测试 PID，校验 PID 属于显式 project 后发送 `SIGKILL`。记录信号时间、进程退出时间、新 Leader 可服务时间、错误窗口、acknowledged transaction 集和重启追赶时间。

测试通过要求：多数派恢复服务、故障前确认提交的数据零丢失、故障窗口没有部分事务、旧节点重启后追赶完成，并在一次完整集群重启后再次全量验证 acknowledged transaction。Follower、TSO Leader、全进程 restart 不作为独立故障 case，因为它们增加任务数但不会显著增强面试主结论。

### 10. B3 用两个命名 failpoint 覆盖 Primary Commit 边界

事务协调器的测试构建增加一次性、带 project token 的 barrier：

- `after_all_prewrite_before_primary_commit`：全部 prewrite 完成、Primary 尚未 commit；恢复后应回滚或清理。
- `after_primary_commit_before_secondaries`：Primary 已 commit、Secondary 尚未全部 commit；恢复后应按 Primary 决策 roll-forward。

目标事务写 marker 并暂停后，编排器才强杀 coordinator。Failpoint 由测试专用编译开关保护，默认构建不可触发。注册表只在测试进程内存在，用 mutex/condition variable 等待目标事务，不持有事务或 Raft 的业务锁。每个 case 恢复后检查所有 Key、Primary 决策、commitTs 和残留锁；脚本必须可重复执行且不依赖随机 sleep 猜阶段。

### 11. C1/C2 给出边界明确的一致性证据

C1 只做并发双 Key 转账：每笔事务从一个账户减 N、向另一个账户加 N，记录 acknowledged transaction，结束后读取全部账户并验证总额恒定、无负值（若 workload 规定）、无部分转账。它证明该场景下的事务原子性和守恒，不单独声称完整 SI。

C2 对一个 register 记录 invoke/complete、输入/输出、状态和 B1 nemesis 时间。历史按 quiescent boundary 分为最多 1,000 operations 的 epoch，由有界 checker 尊重实时先后关系判断是否存在合法串行化；仍无法解析的写保留为 pending/inconclusive，不能猜为成功。先跑无故障基线，再组合 B1。

### 12. D1 只比较兼容、可重复的核心点

Manifest 的 schema、binary、path、workload、dataset、value size、distribution、topology、build type、profile 和 connection mode 组成 compatibility signature。签名不一致时只并排展示，不计算回归。

每个 full 性能点保留 3 次原值并用中位数比较。默认告警为 successful throughput 下降超过 10% 或 P99 上升超过 20%；任一 correctness、durability 或 checker 失败无条件阻断。报告必须按“环境 → 数字 → 对比 → 指标证据 → 适用边界”组织，不能在缺少指标时把瓶颈猜测写成事实。

## Risks / Trade-offs

- [C++ generator 与 go-ycsb 的具体随机算法不同] → 固定算法版本、seed 和 golden sequence，并统一称为 YCSB-compatible。
- [Gateway 当前短连接成本可能主导结果] → 把 `connection_mode=close` 写入 manifest，Direct/Gateway 分开解释。
- [100,000 × 1 KiB 仍可能部分命中页缓存] → 只作 development baseline 和相对比较，不作生产容量声明。
- [20,000 ops 对 P99.9 样本不足] → 正式比较只使用 P50/P95/P99/Max；满足 100,000 样本才展示 P99.9。
- [checkpoint 复制活动 RocksDB 会损坏] → 创建/恢复前必须 scoped down 并校验进程退出，启动后执行 load verify。
- [性能重试掩盖服务错误] → 性能默认有限且显式记录；所有延迟包含失败尝试与退避，并单列 attempted/success/error。
- [本地噪声导致误判] → 同 checkpoint 三次重复、取中位数、保留原始轮次和环境快照。
- [测试 failpoint 误入生产] → 测试构建加编译保护，运行时再要求 project token，默认二进制无入口。
- [线性化检查状态空间过大] → 以 quiescent epoch 限制每段历史，超出预算报告 inconclusive 而非伪造 pass。

## Migration Plan

1. 新增公共 workload、manifest、histogram、结果 writer 和单元测试，不修改现有服务行为。
2. 实现 Direct/Gateway adapters 和 YCSB A/B/C/F，再落地 A1 最小矩阵。
3. 让现有 Region 2PC/争用 benchmark 复用公共生成与结果层，增加 A2/A3/A4。
4. 新增 scoped 编排、checkpoint、指标快照和 D1 报告。
5. 增加 B1、测试专用 B3 failpoint、C1 和 C2，并补齐恢复校验。
6. 先执行 `interview-smoke` 验证闭环，再执行一次 `interview-full` 产出面试报告。

回滚时删除新增测试 runner、测试编排入口和测试构建 failpoint；旧测试目标、默认服务二进制、事务协议和存储格式不受影响。测试数据和结果仅由显式 project/run-id 管理，只有显式 `clean` 才删除。
