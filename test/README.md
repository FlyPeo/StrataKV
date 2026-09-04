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
  - **延迟与分位数**：严格按照 attempted/successful 分离，仅当单点有效样本达到 100,000 时才报告可靠的 P99.9；在 smoke 阶段以 P50/P95/P99/Max 为准。

### 2. 执行命令与复跑方式

面试版全链路测试由统一入口执行，完全基于 project 命名空间隔离，无宽泛危险命令：

```bash
# 1. 快速冒烟测试 (约 1~2 分钟)
bash deploy/stratakv-performance all --profile interview-smoke

# 2. 全量复现测试 (包含 100,000 records, 3 次重复, B1/B3, C1/C2)
bash deploy/stratakv-performance all --profile interview-full

# 3. D1 回归比对
bash deploy/stratakv-performance compare test-results/performance/<baseline-id> test-results/performance/<candidate-id>

# 4. 清理指定项目运行时数据 (测试结果保留在 test-results/ 中)
bash deploy/stratakv-performance clean --project perf-<run-id>
```

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

### 4. 面试讲法与适用边界

1. **环境定位**：测试均在 WSL/开发机器上完成，明确声明为 development baseline，关注相对趋势而非硬件极限吞吐。
2. **扩展性证据**：
   - A1 从 1 到 8 workers 展示了并发读写随核数的良好扩展。
   - A2/A3 清晰揭示了跨 Region 2PC 的网络与协商成本（随着参与 Region 增加，协调开销上升）。
3. **争用与自适应**：
   - A4 展示了低争用下乐观锁的高吞吐，以及高争用下乐观锁反复冲突与悲观锁快速排队/快速失败的权衡。
4. **高可用与一致性闭环**：
   - B1 证明多数派下 Region Leader 故障可秒级自愈且旧数据不丢；
   - B3 证明 2PC 协调器在 Primary 提交前后崩溃时，锁解析器能够幂等回滚或 roll-forward，无悬挂锁；
   - C1 证明并发转账总额守恒，零部分转账；
   - C2 证明单寄存器读写在静态 epoch 下严格满足有界线性一致性，未知写绝不猜测为成功。

## 维护约定

- 新的无外部依赖检查放入 `unit/`，并在 `CMakeLists.txt` 中注册 `add_test`。
- 会启动本地进程但能自包含运行的测试放入 `integration/`。
- 性能程序放入 `benchmarks/`，默认不要注册到 CTest。
- 依赖已部署集群或会写持久化结果的负载放入 `system/`。
- 多个性能测试共用的生成器、adapter、统计和报告代码放入 `support/performance/`。
- Pulsar 已有测试应直接引用子模块源码，不要复制到本目录。
- 可执行目标继续使用 `stratakv-test-*` 命名，并由根目录 CMake 输出到 `bin/`。
- 每个测试源码开头必须写明测试目标、策略、数据规模和通过条件。
