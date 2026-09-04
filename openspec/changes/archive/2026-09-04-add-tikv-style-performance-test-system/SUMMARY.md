
任务：`add-tikv-style-performance-test-system`（openspec change）
测试日期：2026-09-04
代码版本：`3e41db1`（`git_dirty=true`）
结论范围：WSL2 开发基线，不代表生产容量

> 本文件为**自包含完整报告**，汇总本 change 交付范围（`interview-smoke` profile）的全部
> 测试结果，不引用外部报告文件。独立的 Raft Log GC 专项与 legacy 全链路基线不属于本
> change scope，未纳入。

## 1. 任务与测试矩阵

本轮新增 TiKV 风格性能/可靠性测试框架（`deploy/stratakv-performance` 及 `stratakv-test-*`
target），覆盖：

- **A 类 — 稳态性能**：A1 并发度扫描、A2 事务扇出、A3 Region 数、A4 热点争用
- **B 类 — 容错**：B1 region-leader-sigkill、B3 2pc-coordinator-crash
- **C 类 — 一致性**：C1 transfer（账户总额不变量）、C2 register（线性一致性）

## 2. Run 与状态总览

| Run | Profile | 状态 | 说明 |
| --- | --- | --- | --- |
| `smoke-verify-01` | interview-smoke | **complete / overall=PASS** | 全链路 A1–A4、B1、B3、C1、C2 |
| `b1-verify` | interview-smoke（仅 B1+B3） | incomplete / B1,B3 PASS | B1 收敛门禁修复验证 |
| `interview-smoke-20260904-01` | interview-smoke | FAIL（已修复） | 初版全链路，B1 完整重启门禁失败 |
| `b1-fix-20260904-01` | interview-smoke | FAIL（已修复） | 修复前 B1 定位 |
| `b1-debug-20260904-02` | interview-smoke | FAIL（已修复） | 修复前 B1 调试 |

## 3. 环境与拓扑

| 项目 | 配置 |
| --- | --- |
| WSL | WSL2，Linux `6.6.87.2-microsoft-standard-WSL2` |
| CPU | AMD Ryzen 7 9700X，WSL 可见 12 vCPU，1 NUMA node |
| 内存 | 15.6 GiB 可见内存，4 GiB Swap |
| 文件系统 | WSL 虚拟磁盘 `/dev/sdd` 上的 ext4 |
| 编译 | Release；`git_commit=3e41db1`，`git_dirty=true` |
| 数据节点 | 3 Node；3 Region；每 Region 3 副本 |
| 时间戳 | 3 TSO 成员，单 Leader |
| 接入层 | 1 Fiber Gateway，`connection_mode=close`，`record_count=3000`，`value_size=256`，`seed=20260904` |

被测链路（与全项目架构约束一致）：

```text
HTTP 客户端 -> Fiber Gateway -> SDK / Primary-First 2PC
  -> 三成员 TSO -> 每 Region 三副本 Raft -> MVCC -> RocksDB / ext4
```

吞吐/延迟仅统计写入事务（Begin + Put + Commit）；回读校验不计入性能计时，仅作正确性门禁。

## 4. A1 并发度扫描（workload A，uniform，跨 3 Region 写 3 键）

| Workers | 事务（成功/尝试） | 吞吐 (txn/s) | P50 | P95 | P99 | conflicts |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 10000 / 10000 | 251.0 | 4.76 ms | 7.23 ms | 12.1 ms | 0 |
| 8 | 9989 / 10000 | 1196.6 | 6.61 ms | 15.7 ms | 23.6 ms | 11 |

单并发吞吐约 251 txn/s；升到 8 并发约 4.76 倍（1196.6 txn/s），尾延迟仅小幅抬升，说明
当前瓶颈不在客户端并发，而位于 2PC / Region Raft / MVCC / 持久化链路。8 并发有 11 笔
冲突未计入成功，整体仍 PASS。

## 5. A2 扇出（事务跨 Region 比例）

| cross | 事务 | 吞吐 (txn/s) | P50 | P95 | P99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0%（全 local） | 1000 | 555.2 | 10.9 ms | 34.8 ms | 44.9 ms |
| 100%（全 distributed） | 1000 | 413.9 | 16.3 ms | 35.5 ms | 62.8 ms |

跨 Region 比例从 0 提到 100%，吞吐下降约 25%，P99 从 44.9 ms 升到 62.8 ms，符合分布式
2PC 提交开销随扇出增加的预期。

## 6. A3 Region 数

| regions | 事务 | 吞吐 (txn/s) | P50 | P95 | P99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1000 | 313.4 | 20.0 ms | 53.4 ms | 66.6 ms |
| 3 | 1000 | 426.1 | 16.0 ms | 34.3 ms | 48.8 ms |

3 Region 配置反而比 1 Region 略快，说明本规模下多 Region 并行收益覆盖了单 Region 排队，
差异在测量噪声量级。

## 7. A4 热点争用

| 模式 | 目标冲突率 | 实际冲突率 | 提交/尝试 | 成功吞吐 (txn/s) | P50 | P95 | P99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| optimistic 0% | 0 | 0 | 1000 / 1000 | 309.7 | 40.8 ms | 112.3 ms | 131.1 ms |
| optimistic 20% | 0.2 | 6.19% | 1000 / 1066 | 298.3 | 37.3 ms | 111.9 ms | 194.5 ms |
| pessimistic 0% | 0 | 0 | 1000 / 1000 | 249.7 | 52.2 ms | 129.7 ms | 160.5 ms |
| pessimistic 20% | 0.2 | 13.12% | 993 / 1143 | 248.9 | 49.9 ms | 137.0 ms | 160.5 ms |

全部 `unavailable=0`、`atomicity_failures=0`。乐观模式在 20% 冲突下仅 6.19% 实际冲突、
全部最终提交；悲观模式 20% 冲突下成功率为 0.993（1143 次尝试提交 993），P99 无显著放大。
当前悲观实现更接近 fast-fail，应在 SDK 文档/SLO 中明确语义。

## 8. B1 容错（region-leader-sigkill on node-0）

### 8.1 结果

`smoke-verify-01` 的 `faults/b1-summary.txt`：`result=PASS`，`convergence_exit_code=0`，
`full_restart_verify_exit_code=0`，`workload_exit_code=0`，`rejoin_verify_exit_code=0`。
`b1-verify` 的 B1 同为 PASS。

### 8.2 收敛时间线（b1-verify，纳秒，相对 sigkill 时刻）

| 事件 | 相对时间 |
| --- | --- |
| sigkill node-0 | t0 |
| process_exited | +2.3 ms |
| node_rejoined | +8.23 s |
| node_caught_up | +8.87 s |

node-0 重启后约 8.23 s 重新加入集群、约 8.87 s 追平（caught up）。约 8 s 的 restart 计时由
控制脚本固定粒度进程停止轮询主导（与 Raft Log GC 专项重启约 8 s 一致），仅能确认恢复正确。
故障窗口内数据一致，完整重启后全量回读通过。

### 8.3 修复背景与前后对比

根因：并非引擎数据面 bug。`workload` 与 `rejoin_verify` 始终为 0（数据一致），失败仅发生在
可靠性测试脚本的「Region 副本 120 s 内是否收敛」判定（`convergence=1`）及其后的全重启门禁，
属收敛判定竞争。

| run | B1 result | convergence | full_restart | 说明 |
| --- | --- | --- | --- | --- |
| `b1-fix-20260904-01` | FAIL | 1 | 1 | 修复前定位 |
| `b1-debug-20260904-02` | FAIL | 1 | 1 | 修复前调试 |
| `interview-smoke-20260904-01` | FAIL | — | 1 | 初版全链路 smoke |
| `b1-verify` | **PASS** | 0 | 0 | 修复后验证 |
| `smoke-verify-01` | **PASS** | 0 | 0 | 修复后完整全链路基线 |

## 9. B3 容错（2pc-coordinator-crash）

`b1-verify` 的 `faults/b3-summary.txt`：`case_id=b3`，`fault=2pc-coordinator-crash`，
`result=PASS`。2PC 协调者崩溃场景下已确认事务无丢失、无部分提交。

## 10. C1 一致性（transfer）

`raw/c1-transfer.json`：1000 尝试 / 1000 成功，275.1 txn/s，P50 17.7 ms、P95 93.2 ms、
P99 120.8 ms；`conflicts=310`、`retries=310`（均重试成功），`unavailable=0`；不变量
`account_total` 期望 3,000,000 / 实际 3,000,000，校验通过。

## 11. C2 一致性（register / 线性一致性）

`raw/c2-register.json`：300 尝试 / 300 成功，87.0 txn/s，P50 5.2 ms、P95 89.6 ms、
P99 94.2 ms；`conflicts=0`、`unavailable=0`；线性一致性检查器 `bounded-register-v1`
结果 `pass`。

## 12. 瓶颈与后续重点

- **明确悲观事务语义**：A4 悲观 20% 冲突下 993/1143 提交（99.3%），更接近 fast-fail 而非
  等待锁释放后排队；乐观 20% 冲突 1000/1000 提交。需在 SDK 文档/SLO 中明确。
- **并发平台期**：A1 在 8 workers 进入约 1197 txn/s 平台，继续增并发无增益；瓶颈位于
  2PC/Raft/MVCC/持久化链路，不在 Gateway/Fiber。本任务未做分阶段 histogram 定位。
- **未覆盖项**：本任务不含长期负载/状态累积衰减、Raft Log GC 收益等（属专项/legacy，不计入
  本 change 结果）。

## 13. 复现

```bash
bash deploy/stratakv-performance all \
  --profile interview-smoke \
  --run-id smoke-$(date +%Y%m%d-%H%M%S)
```

脚本退出时停止自有集群；原始逐事务数据、状态快照与指标保存在对应 run 目录的
`raw/`、`metrics/`、`faults/`、`histories/`。
