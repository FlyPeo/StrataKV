# 实施任务（18 项核心任务）

执行约束：每项开始前检查 `git status`/`git diff`，只修改该任务列出的测试支持、编排和文档文件，不覆盖无关未提交改动。所有新增或实质修改的测试源文件必须在文件开头写明：测试目标、测试策略、数据规模、验证条件。默认服务行为、协议和存储格式不在本变更范围内。

## 1. 公共 workload 与执行核心

- [x] 1.1 在 `test/support/performance/` 实现版本化 `WorkloadSpec`、`DatasetManifest`、线程局部直方图、summary/interval writer 和 `incomplete`→`complete` 原子结果状态；用 schema、分位数、合并、写入中断单元测试验证必填字段、P99.9 样本门槛和既有完整 run 不被覆盖。
- [x] 1.2 实现由全局 sequence number + seed 驱动的 A/B/C/F 操作选择、Uniform/Zipfian record selector 和 Region-aware Key codec；用 golden/statistical/routing 测试验证不同 worker 数下序列可复现、比例正确、索引不越界、数据覆盖三个 Region 且每个 YCSB operation 只访问一个 Region。
- [x] 1.3 实现确定性 `load/run`、`WorkloadRunner`、`DirectSdkAdapter` 和 `GatewayAdapter`，统一 Read/Update/RMW 的事务边界、计时与 retry/result-unknown 分类；用 fake adapter 和小型三 Region 集成测试验证幂等 Load、两路径相同操作序列、重试延迟包含失败尝试，并验证 Scan/E 在施压前失败。

## 2. A1–A4 核心性能矩阵

- [x] 2.1 实现 A1 矩阵与 CLI：Gateway Uniform A/B/C/F × workers=1/4/8/16/32，Gateway Zipfian A/B × workers=8，Direct Uniform A/C × workers=8；用缩小 operation 数的矩阵测试验证所有点恰好执行、A/B/C/F 实际读写计数正确，并分别输出 record attempted/successful OPS 与 P50/P95/P99/Max。
- [x] 2.2 实现 A2 固定 3 mutations 的本地/三 Region transaction 混合生成器，比例只保留 0/15/100%、workers=8；用路由统计和小型集成测试验证实际比例、local/distributed 分桶、transaction attempted/committed TPS 以及除跨 Region 比例外参数完全相同。
- [x] 2.3 实现 A3 固定 3 mutations、1/2/3 Region fanout 矩阵，workers=8；用路由断言和同 checkpoint 测试验证每笔事务的实际参与 Region 数，并输出 TPS、P99 和相对 1 Region 的增量成本。
- [x] 2.4 实现 A4 target contention share=0/5/20% × 乐观/悲观 fast-fail，workers=16；用确定性 hot-slot 与合成曲线测试验证目标份额、实际冲突率、成功率、attempts/commit、含退避延迟和 committed TPS，且只在两条实测曲线交叉时报告切换点。

## 3. 编排、隔离和结果分析

- [x] 3.1 新增 `deploy/stratakv-performance` 的 `prepare/run/fault/check/report/all/down/clean`，固化 `interview-smoke` 与 `interview-full` 参数，实现显式 project 生命周期、异常 trap、post-load checkpoint、恢复后 load verify；用参数测试和故意中断演练验证只停止目标集群、无宽泛 `pkill`、写负载每点从相同状态开始且结果默认保留。
- [x] 3.2 实现 `/proc` 与现有 status/metrics 的 before/after 采集、统一结果目录、compatibility signature、三次中位数 D1 比较器和 Markdown/JSON/CSV 报告；用 golden fixtures 验证 Direct/Gateway 或环境不兼容时拒绝回归比较、吞吐 -10%/P99 +20% 告警、correctness 硬失败，并将 WSL 标记为 development baseline 而非生产容量。

## 4. B1/B3 故障恢复

- [x] 4.1 实现 B1 Region Leader resolver 和 scoped `SIGKILL` controller，在持续写负载中记录故障时间线、可服务恢复时间和 acknowledged set；用多种 Leader 布局 fixture 与专用集群验证不固定选择 node-0、不误杀其他 project、多数派恢复、确认提交零丢失、旧节点追赶完成，并在完整集群重启后再次通过全量校验。
- [x] 4.2 在测试专用构建中实现一次性 `after_all_prewrite_before_primary_commit` 与 `after_primary_commit_before_secondaries` barrier，要求显式 project token 并原子写 marker；用编译/运行单元测试验证默认生产构建无触发入口、错误 token 不生效、命中时不持有事务或 Raft 业务锁。
- [x] 4.3 实现 B3 marker 驱动的 coordinator 强杀与幂等恢复编排；用两个端到端 case 验证 Primary 未提交时事务回滚/锁清理，Primary 已提交时 Secondary 使用同一 commitTs roll-forward，最终没有部分提交、永久锁或无法解释的 result-unknown。

## 5. C1/C2 一致性证据

- [x] 5.1 实现 C1 并发双 Key 转账和全账户校验，在无故障、B1、B3 后运行；用 1,000-transfer 小型测试验证总额恒定、无部分转账、acknowledged 金额不丢失，并在报告中把结论限定为该 workload 的事务原子性/守恒。
- [x] 5.2 实现 C2 register history writer、quiescent epoch 切分和有界 linearizability checker，并组合无故障与 B1；用 legal/illegal/pending golden histories 和 300-operation 集成测试验证 pass/fail/inconclusive、实时先后约束、反例输出及未知写不被猜成成功。

## 6. 独立验证、实测与文档

- [x] 6.1 为生成器、Key 路由、直方图、schema、D1 比较器、failpoint 和 history checker 补齐独立单元测试；运行对应 CTest 验证边界/失败路径，并用检查脚本确认每个测试源文件头部都包含目标、策略、规模、验证条件四项说明。
- [x] 6.2 完成 Release 全量构建，运行全部既有 CTest 和新增 adapter/fault/recovery 集成测试，再一条命令执行 `interview-smoke`；验收标准是所有测试通过、run 状态为 `complete`、结果字段齐全、correctness 通过且没有后台测试进程残留。
- [x] 6.3 在当前 12 vCPU/16 GiB WSL 一条命令执行 `interview-full`：100,000 × 1 KiB 数据，A1 每点 20,000 ops × 3，A2/A3 每点 10,000 tx × 3，A4 每点 5,000 tx × 3，随后 B1/B3、C1 10,000 transfers、C2 1,000-operation epochs；验证全部必需点完成，并依据客户端/服务指标给出吞吐拐点、Gateway 开销、2PC fanout/占比、争用边界和故障恢复的证据化结论。
- [x] 6.4 更新 `test/README.md` 与最终 `test-results/performance/<run-id>/REPORT.md`，说明 A/B/C/F 读写分布、“数据跨 Region但单操作单 Region”与“单事务跨 Region”的区别、OPS/TPS/延迟主体、复跑命令、目录结构、限制和面试讲法；逐条执行文档命令并核对报告中的每个数字可追溯到 manifest/raw/metrics/history，旧结果只标为 legacy 且不并入新基线。
