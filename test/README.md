# StrataKV 测试目录

本目录只保存 StrataKV 主项目自己的测试源码。Pulsar 已有的测试和基准直接引用
`src/pulsar` 子模块中的唯一源文件，不在这里维护重复副本。

## 目录结构

| 路径               | 内容                             | 执行方式                 |
| ------------------ | -------------------------------- | ------------------------ |
| `unit/`          | 不依赖外部服务的单元与组件检查   | CTest 自动执行             |
| `integration/`   | 自包含集成检查与脚本契约测试     | CTest 自动执行             |
| `benchmarks/`    | 性能、吞吐和 A/B 基准            | 由 `deploy/` 脚本编排      |
| `system/`        | 需要真实集群的系统与可靠性负载   | 由 `deploy/` 脚本编排      |
| `support/`       | 测试共用 workload、统计和报告器  | 被测试程序和脚本复用       |
| `CMakeLists.txt` | 定义测试程序、依赖和 CTest 注册  | CMake 配置时读取           |

## 自动测试

先构建并运行全部 19 项 CTest：

```bash
cmake --preset release
cmake --build --preset release -j"$(nproc)"
ctest --preset release
```

只运行一个测试：

```bash
ctest --preset release -R '^stratakv-test-txn-scheduler$'
```

### StrataKV 单元与组件检查

| 源文件                                      | CTest 名称                                 | 用途                                                            |
| ------------------------------------------- | ------------------------------------------ | --------------------------------------------------------------- |
| `unit/bounded_thread_pool_check.cpp`      | `stratakv-test-bounded-thread-pool`      | 检查有界线程池的队列背压和任务完成行为                          |
| `unit/raft_log_gc_check.cpp`              | `stratakv-test-raft-log-gc`              | 检查 Raft 日志软/硬 GC 阈值和已 Apply 安全边界                  |
| `unit/performance_support_check.cpp`      | `stratakv-test-performance-support-check`| 检查 YCSB 配比、确定性键分布、分位数和结果发布契约              |
| `unit/workload_runner_check.cpp`          | `stratakv-test-workload-runner-check`    | 检查公共负载执行器的事务边界、重试、并发和统计                  |
| `unit/a1_matrix_check.cpp`                | `stratakv-test-a1-matrix-check`          | 检查 A1 矩阵定义、命令行参数解析及 YCSB 确定性序列              |
| `unit/a2_transaction_check.cpp`           | `stratakv-test-a2-transaction-check`     | 检查 A2 跨 Region 事务混合生成器、路由分桶及本地/跨区统计       |
| `unit/a3_fanout_check.cpp`                | `stratakv-test-a3-fanout-check`          | 检查 A3 1/2/3 Region fanout 事务生成与单事务多 Region 路由      |
| `unit/a4_contention_check.cpp`            | `stratakv-test-a4-contention-check`      | 检查 A4 乐观/悲观争用模拟、重试 vs 快速失败和 crossover 分析    |
| `unit/b3_barrier_check.cpp`               | `stratakv-test-b3-barrier-check`         | 检查测试专用 2PC failpoint barrier、显式 token 与原子 marker    |
| `unit/b3_recovery_check.cpp`              | `stratakv-test-b3-recovery-check`        | 检查 B3 协调器崩溃恢复：Primary 未提交回滚与 Primary 提交 roll-forward |
| `unit/c1_transfer_check.cpp`              | `stratakv-test-c1-transfer-check`        | 检查 C1 并发双 Key 转账、总额守恒、零部分转账与负向篡改检测     |
| `unit/c2_linearizability_check.cpp`       | `stratakv-test-c2-linearizability-check` | 检查 C2 寄存器线性一致性、golden 历史、实时偏序与反例诊断       |
| `unit/txn_scheduler_check.cpp`            | `stratakv-test-txn-scheduler`            | 检查事务调度、Latch、Raft propose/apply、超时、悲观锁及重启恢复 |
| `unit/timestamp_oracle_check.cpp`         | `stratakv-test-timestamp-oracle`         | 检查 HLC 单调性、时钟回拨、逻辑位溢出、持久化迁移和重启         |
| `unit/transaction_coordinator_check.cpp`  | `stratakv-test-transaction-coordinator`  | 检查 2PC 状态、清理、写偏斜防护、超时分类和协调器恢复           |
| `unit/mvcc_batch_check.cpp`               | `stratakv-test-mvcc-batch`               | 检查 Region 内批量 MVCC prepare/apply 的原子性和失败进度        |
| `unit/region_batch_coordinator_check.cpp` | `stratakv-test-region-batch-coordinator` | 检查跨 Region 分组、并行执行、部分失败清理和协议降级            |
| `unit/sdk_contract_check.cpp`             | `stratakv-test-sdk-contract`             | 检查公开 SDK 类型、状态名称和缺失值等接口契约                   |

### 集成测试

| 源文件                                    | CTest 名称                         | 用途                                                                    |
| ----------------------------------------- | ---------------------------------- | ----------------------------------------------------------------------- |
| `integration/tso_integration_check.cpp` | `stratakv-test-tso`                | 启动三成员 TSO，检查并发取号、Leader 故障、少数派 fence、快照和全量重启 |
| `integration/performance_script_check.sh` | `stratakv-test-performance-script` | 静态检查 smoke 编排规模、隔离范围和危险命令                         |
| `integration/performance_report_check.py` | `stratakv-test-performance-report` | 用合成结果检查报告完整性门禁与失败传播                               |
| `integration/d1_compare_check.py`         | `stratakv-test-d1-compare`         | 检查 D1 兼容性签名、三次中位数回归比较、告警阈值及 WS L基准标注      |

### 直接复用的 Pulsar 测试

以下目标由主项目注册到 CTest，但源码位于 `src/pulsar/tests/`，避免两处内容漂移：

| CTest 名称                                | Pulsar 源文件                         | 用途                                       |
| ----------------------------------------- | ------------------------------------- | ------------------------------------------ |
| `stratakv-test-fiber-sync`              | `fiber_sync_check.cpp`              | FiberMutex、条件变量、信号量和超时唤醒     |
| `stratakv-test-fiber-context`           | `fiber_context_check.cpp`           | Fiber 切换、reset、异常和边界行为          |
| `stratakv-test-scheduler-work-stealing` | `scheduler_work_stealing_check.cpp` | Scheduler work stealing 与线程亲和         |
| `stratakv-test-stack-pool`              | `stack_pool_check.cpp`              | Fiber 栈分配、尺寸等级和复用池             |
| `stratakv-test-scheduler-cache`         | `scheduler_cache_check.cpp`         | Scheduler 本地缓存、跨 worker 回收及统计   |
| `stratakv-test-reuse-integration`       | `reuse_integration_check.cpp`       | 栈复用在调度、同步和定时器路径中的集成行为 |

## 手动基准

基准会受机器、编译类型和当前负载影响，因此只生成可执行程序，不注册到 CTest。

| 源文件或来源                                  | 构建目标                               | 用途                                                           |
| --------------------------------------------- | -------------------------------------- | -------------------------------------------------------------- |
| `src/pulsar/benchmarks/fiber_benchmark.cpp` | `stratakv-test-fiber-benchmark`      | Fiber context、生命周期、Scheduler、Timer、Hook 和同步原语基准 |
| `benchmarks/fiber_context_ab_benchmark.cpp` | `stratakv-test-fiber-context-ab`     | Pulsar、Boost.Context 与 Photon 的严格 context-switch A/B      |
| `benchmarks/tso_range_benchmark.cpp`        | `stratakv-test-tso-range-benchmark`  | 自启动三成员 TSO，对比不同 range size 的吞吐和延迟             |
| `benchmarks/txn_contention_benchmark.cpp`   | `stratakv-test-txn-contention`       | 对运行中集群施加热点事务争用负载                               |
| `benchmarks/region_2pc_benchmark.cpp`       | `stratakv-test-region-2pc-benchmark` | 对运行中集群测量跨 Region 2PC 扩展性                           |

运行完整 Pulsar 基准：

```bash
bash deploy/stratakv-fiber-benchmark
```

其他基准可先单独构建，再从 `bin/` 执行并传入所需参数：

```bash
cmake --build --preset release --target stratakv-test-tso-range-benchmark
./bin/stratakv-test-tso-range-benchmark \
  --requests 2048 --clients 8 --warmup 128 \
  --baseline-range 1 --optimized-range 4096
```

## 性能与可靠性测试系统

### 1. 核心概念与口径区分

- **读写分布 (YCSB-compatible Workloads)**：
  - **A** (Update Heavy): 50% Read, 50% Update
  - **B** (Read Predominant): 95% Read, 5% Update
  - **C** (Read Only): 100% Read
  - **F** (Read-Modify-Write): 50% Read, 50% Read-Modify-Write
- **“数据跨 Region 但单操作单 Region” vs “单事务跨 Region”**：
  - **A1 阶段**：整体数据集通过 Key 范围分布在全部 3 个 Region（如 Region 100, 101, 102），但每次单 Key 操作只命中一个特定的 Region，属于单 Region Raft 复制与本地读写，不涉及跨节点分布式 2PC。
  - **A2/A3 阶段**：单笔事务内包含写入多个不同 Region 的 Key（如 1、2、3 个 Region），由事务协调器执行完整的分布式 2PC 协议（Prewrite -> Primary Commit -> Secondary Commit / Roll-Forward）。
- **吞吐与延迟统计口径**：
  - **OPS (Operations Per Second)**：仅用于 A1 单记录读写操作。
  - **TPS (Transactions Per Second)**：用于 A2/A3/A4/C1 完整事务提交，绝不与 OPS 混为一谈。
  - **延迟与分位数**：严格按照 attempted/successful 分离，A1 延迟直方图包含所有逻辑操作（含最终冲突），A4 延迟包含重试与退避；单点样本不足 100,000 时不发布有效 P99.9。Smoke 使用 P50/P95/P99/Max。

### 2. 执行命令与复跑方式

在项目根目录执行 **`deploy/stratakv-performance`**：

```bash
bash deploy/stratakv-performance all --profile interview-smoke
```

该命令自动进行 Release 构建、启动专用集群、Load、逐点 checkpoint 恢复及全量键值校验、
施压、故障/一致性检查、集群停止和报告生成。需要 Bash、CMake/C++ 构建依赖、Python 3、
curl 和 GNU `/usr/bin/time`。本地节点使用 26200–26202 端口，测试前请确保没有另一套集群占用。
运行时间包含构建及多次集群重启，不承诺固定分钟数。

Smoke 的固定规模如下：

| 项目 | 规模 |
|---|---|
| Load | 3,000 条 × 256 B，每个 checkpoint 恢复后逐条核对全部初始值 |
| A1 | Gateway、uniform、A/C × 1/8 workers，每点 10,000 operations |
| A2 | 跨 Region 比例 0/100%，8 workers，每点 1,000 transactions |
| A3 | 1/3 Region，8 workers，每点 1,000 transactions |
| A4 | 乐观/悲观 fast-fail × 0/20% 目标争用，16 workers，每点 1,000 transactions |
| B1 | 持续写入时强杀 Region Leader，检查恢复、追赶及完整重启后的数据 |
| B3（附加） | 内存存储上以异常模拟协调器中断，验证提交前/后的恢复语义 |
| C1 / C2 | 无故障集群上 1,000 次转账 / 300 次寄存器操作 |

默认 run-id 为 `smoke-日期-时间`，project 为 `perf-<run-id>`。也可以显式指定：

```bash
bash deploy/stratakv-performance all --profile interview-smoke --run-id smoke-my-check

# 仅查看矩阵；不启动集群
bash deploy/stratakv-performance plan --profile interview-smoke

# 已自行完成构建时，可在 all 命令末尾追加 --no-build
# 手动停止或删除指定测试项目（结果目录保留）
bash deploy/stratakv-performance down --project perf-smoke-my-check
bash deploy/stratakv-performance clean --project perf-smoke-my-check
```

同一 run-id 不能覆盖已有结果，重新运行请换新 ID。正常结束、失败和中断后保留已有证据，
未完成时保持 `incomplete`。验收检查必选点规模、完整 Load、checkpoint 校验、错误计数、
B1/B3/C1/C2 和清理结果；不能仅凭“有成功请求”判为 PASS。

`interview-full` 的执行矩阵已存在，但完整验收和报告能力仍待补齐。D1 比较器也仍有待修复的
采样/正确性门禁问题，目前不要把它们的 PASS 当作完整变更验收证据。

### 3. 结果目录结构

测试输出严格收敛于 `test-results/performance/<run-id>/`：

```text
test-results/performance/<run-id>/
├── manifest.json       # 环境配置、版本、拓扑及整体 PASS/FAIL
├── REPORT.md           # 人类可读 Markdown 报告，数字完全追溯
├── summary.json        # 机器可读全量聚合指标
├── summary.csv         # 核心点二维表格
├── raw/                # 各 case 原始执行日志与 json 结果
│   ├── load/
│   ├── a1/
│   ├── a2/
│   ├── a3/
│   └── a4/
├── metrics/            # /proc/status, /proc/io, prometheus 快照
├── faults/             # B1/B3 故障时间线、杀进程日志与恢复校验
└── histories/          # C1/C2 事务历史与线性一致性反例诊断
```

### 4. 结果分析与适用边界

运行完成后先核对 `manifest.json` 的 `state=complete`、`overall=PASS`，再阅读
`REPORT.md` 和 `summary.json`。客户端 CPU 时间与峰值 RSS 位于 `metrics/*-client.txt`，
服务 CPU/IO 和状态位于各点 before/after 目录，二进制 SHA-256 位于 manifest。

- A1 的 1/8 worker 点只支持描述这两个点的变化，不能据此宣称达到吞吐平台或定位 Gateway 瓶颈。
- A2/A3 的单位是事务 TPS；其写入值是 benchmark 自己生成的短字符串，不应假设与 A1 的 256 B 相同。
- A4 必须同时看提交吞吐、实际冲突率、成功率和包含退避的延迟，未观察到曲线交叉时不外推切换点。
- B1 时间线是脚本观测时间，包含重启、轮询和校验成本，不等同于纯 Raft 选举耗时。
- B3 当前是内存模拟单元测试，不等于真实集群 Coordinator 进程强杀。
- C1 检查账户总额守恒，C2 检查有限 register history；两者目前没有与 B1/B3 组合运行。
- C1/C2 的总体耗时还包含最终校验或 epoch 管理，不作为与 A1 同口径的性能对照。
- Smoke 每点一次，结论仅是 WSL development baseline，不能作为生产容量、完整事务隔离证明或稳定性能回归结论。

## 维护约定

- 新的无外部依赖检查放入 `unit/`，并在 `CMakeLists.txt` 中注册 `add_test`。
- 会启动本地进程但能自包含运行的测试放入 `integration/`。
- 性能程序放入 `benchmarks/`，默认不要注册到 CTest。
- 依赖已部署集群或会写持久化结果的负载放入 `system/`。
- 多个性能测试共用的生成器、adapter、统计和报告代码放入 `support/performance/`。
- Pulsar 已有测试应直接引用子模块源码，不要复制到本目录。
- 可执行目标继续使用 `stratakv-test-*` 命名，并由根目录 CMake 输出到 `bin/`。
- 每个测试源码开头必须写明测试目标、策略、数据规模和通过条件。
