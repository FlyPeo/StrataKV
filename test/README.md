# StrataKV 测试目录

组织主轴:**顶层目录 = 领域**(与 `src/` 模块一一对应),**测试类型放文件名后缀**:

- `*_unit_check.{cpp,py}` — 组件级、进程内,可进 CI 快速反馈;
- `*_integration_check.{cpp,py,sh}` — 跨组件或子进程(拉起真实成员/集群);
- `*_benchmark.cpp` — 手动基准,受机器与负载影响,不注册 CTest;
- 无类型后缀的 `*_support.*` / 驱动程序 — 套件共用库与施压入口。

CTest target 命名规则:文件名 snake → kebab、去 `_check`;unit 类追加 `-unit`,
integration 类以 `-integration` 结尾,benchmark 类以 `-benchmark` 结尾。
Pulsar 子模块的测试源码仍在 `src/pulsar/tests/`(不在本目录维护副本),
target 名沿用子模块命名。

## 目录总览

| 目录 | 领域 | 内容 |
| --- | --- | --- |
| `raft/` | 共识与复制 | 4 个 unit check |
| `transaction/` | MVCC 与分布式事务 | 5 个 unit check |
| `tso/` | 时间戳权威 | 1 unit + 1 integration + 1 benchmark |
| `region/` | Region 数据面与动态分片 | 13 unit + 2 integration + 1 benchmark |
| `metadata/` | 元数据控制面与自动调度 | 9 unit + 2 integration |
| `performance/` | 性能与可靠性套件 | 10 unit + 3 integration + 2 benchmark + 2 驱动 + `support/` 共用库 |
| `sdk/` | 公开接口契约 | 1 unit check |
| `pulsar/` | 协程运行时(源码在子模块) | 1 benchmark + 2 个 A/B 驱动脚本 |
| `foundation/` | 通用基础设施 | 1 unit check |

构建与运行:

```bash
cmake --preset release
cmake --build --preset release -j"$(nproc)"
ctest --preset release                 # 全部 58 项
ctest --preset release -R 'txn-scheduler-unit'   # 只跑一个
```

## raft/ — 共识与复制

| 文件 | CTest 名称 | 用途 |
| --- | --- | --- |
| `raft_confchange_unit_check.cpp` | `stratakv-test-raft-confchange-unit` | 单步成员变更:learner 角色、在途互斥、自退役 |
| `raft_log_gc_unit_check.cpp` | `stratakv-test-raft-log-gc-unit` | 日志软/硬 GC 阈值与已 Apply 安全边界 |
| `replica_catchup_unit_check.cpp` | `stratakv-test-replica-catchup-unit` | 副本追赶、阈值晋升与流式快照安装 |
| `rocksdb_snapshot_unit_check.cpp` | `stratakv-test-rocksdb-snapshot-unit` | 引擎快照钉住语义与恢复一致性 |

## transaction/ — MVCC 与分布式事务

| 文件 | CTest 名称 | 用途 |
| --- | --- | --- |
| `mvcc_batch_unit_check.cpp` | `stratakv-test-mvcc-batch-unit` | Region 内批量 prepare/apply 原子性 |
| `region_batch_coordinator_unit_check.cpp` | `stratakv-test-region-batch-coordinator-unit` | 跨 Region 分组、并行与部分失败清理 |
| `transaction_coordinator_unit_check.cpp` | `stratakv-test-transaction-coordinator-unit` | 2PC 状态、清理、写偏斜防护与恢复 |
| `txn_recovery_dynamic_unit_check.cpp` | `stratakv-test-txn-recovery-dynamic-unit` | 动态拓扑下的过期锁后台恢复 |
| `txn_scheduler_unit_check.cpp` | `stratakv-test-txn-scheduler-unit` | 事务调度、Latch、悲观锁与重启恢复 |

## tso/ — 时间戳权威

| 文件 | CTest 名称 | 用途 |
| --- | --- | --- |
| `timestamp_oracle_unit_check.cpp` | `stratakv-test-timestamp-oracle-unit` | HLC 单调、时钟回拨、持久化与重启 |
| `tso_integration_check.cpp` | `stratakv-test-tso-integration` | 三成员 TSO:并发取号、故障、fence、快照 |
| `tso_range_benchmark.cpp` | `stratakv-test-tso-range-benchmark` | range size 吞吐/延迟对比 |

## region/ — Region 数据面与动态分片

| 文件 | CTest 名称 | 用途 |
| --- | --- | --- |
| `region_metadata_unit_check.cpp` | `stratakv-test-region-metadata-unit` | 描述符/epoch/范围不变量与目录校验 |
| `region_cache_unit_check.cpp` | `stratakv-test-region-cache-unit` | 路由缓存二分定位与 epoch 前进校验 |
| `region_cache_stress_unit_check.cpp` | `stratakv-test-region-cache-stress-unit` | 路由状态并发压力(TSan/ASan 目标) |
| `region_registry_unit_check.cpp` | `stratakv-test-region-registry-unit` | 注册表原子发布、替换与 in-flight 保护 |
| `region_dispatcher_unit_check.cpp` | `stratakv-test-region-dispatcher-unit` | 节点级请求分发与结构化拒绝 |
| `region_request_sender_unit_check.cpp` | `stratakv-test-region-request-sender-unit` | sender 重试预算、退避与错误分类 |
| `region_data_path_unit_check.cpp` | `stratakv-test-region-data-path-unit` | 数据路径:header、定论错误不重试、regroup |
| `region_envelope_compat_unit_check.cpp` | `stratakv-test-region-envelope-compat-unit` | 新旧请求信封兼容矩阵 |
| `region_split_unit_check.cpp` | `stratakv-test-region-split-unit` | 分裂命令 apply、幂等与恢复态 |
| `node_split_unit_check.cpp` | `stratakv-test-node-split-unit` | 节点侧子 Region 物化与注册 |
| `node_migration_unit_check.cpp` | `stratakv-test-node-migration-unit` | 迁移两阶段协议与 learner 挂载 |
| `node_bootstrap_unit_check.cpp` | `stratakv-test-node-bootstrap-unit` | 动态节点全成全败 bootstrap |
| `topology_config_unit_check.cpp` | `stratakv-test-topology-config-unit` | 启动拓扑配置校验,无隐式回退 |
| `dynamic_cluster_integration_check.cpp` | `stratakv-test-dynamic-cluster-integration` | 拉起动态集群检查注册、路由与生命周期 |
| `online_migration_integration_check.cpp` | `stratakv-test-online-migration-integration` | Learner 追平、Promote/Remove 在线迁移 |
| `dynamic_sharding_benchmark.cpp` | `stratakv-test-dynamic-sharding-benchmark` | 在线分裂韧性压测(由 `deploy/stratakv-split-benchmark` 编排) |

## metadata/ — 元数据控制面与自动调度

| 文件 | CTest 名称 | 用途 |
| --- | --- | --- |
| `metadata_state_machine_unit_check.cpp` | `stratakv-test-metadata-state-machine-unit` | 两阶段分裂/迁移状态机与 revision CAS |
| `metadata_consensus_unit_check.cpp` | `stratakv-test-metadata-consensus-unit` | 三成员共识与幂等 mutation |
| `metadata_consensus_fault_unit_check.cpp` | `stratakv-test-metadata-consensus-fault-unit` | 仲裁丢失、重放与截断快照拒绝 |
| `metadata_property_unit_check.cpp` | `stratakv-test-metadata-property-unit` | 随机分裂后的拓扑结构不变量 |
| `metadata_client_unit_check.cpp` | `stratakv-test-metadata-client-unit` | 元数据 RPC 客户端契约 |
| `auto_balancer_state_unit_check.cpp` | `stratakv-test-auto-balancer-state-unit` | 调度器状态与 operator 持久化 |
| `auto_balancer_planner_unit_check.cpp` | `stratakv-test-auto-balancer-planner-unit` | 评分、证据窗口与操作规划 |
| `auto_balancer_heartbeat_unit_check.cpp` | `stratakv-test-auto-balancer-heartbeat-unit` | 心跳遥测的新鲜度与幂等 |
| `auto_balancer_recovery_unit_check.cpp` | `stratakv-test-auto-balancer-recovery-unit` | leader 重启后 reconcile |
| `metadata_integration_check.cpp` | `stratakv-test-metadata-integration` | 三成员元数据集群端到端 |
| `auto_balancer_integration_check.cpp` | `stratakv-test-auto-balancer-integration` | 均衡 operator 生命周期与 reconcile |

## performance/ — 性能与可靠性套件

对应 `deploy/stratakv-performance` 的 A1–A4 / B1 / B3 / C1 / C2 用例族;`support/` 为套件共用库。

| 文件 | CTest 名称 | 用途 |
| --- | --- | --- |
| `a1_matrix_unit_check.cpp` | `stratakv-test-a1-matrix-unit` | A1 矩阵定义与 YCSB 确定性序列 |
| `a2_transaction_unit_check.cpp` | `stratakv-test-a2-transaction-unit` | A2 跨 Region 事务混合生成与分桶 |
| `a3_fanout_unit_check.cpp` | `stratakv-test-a3-fanout-unit` | A3 1/2/3 Region fanout 生成 |
| `a4_contention_unit_check.cpp` | `stratakv-test-a4-contention-unit` | A4 乐观/悲观争用与 crossover |
| `b3_barrier_unit_check.cpp` | `stratakv-test-b3-barrier-unit` | 2PC failpoint barrier 与原子 marker |
| `b3_recovery_unit_check.cpp` | `stratakv-test-b3-recovery-unit` | 协调器崩溃恢复:回滚与前滚 |
| `c1_transfer_unit_check.cpp` | `stratakv-test-c1-transfer-unit` | 转账守恒与部分转账检测 |
| `c2_linearizability_unit_check.cpp` | `stratakv-test-c2-linearizability-unit` | 寄存器线性一致性与反例诊断 |
| `performance_support_unit_check.cpp` | `stratakv-test-performance-support-unit` | YCSB 配比、分位数与结果发布契约 |
| `workload_runner_unit_check.cpp` | `stratakv-test-workload-runner-unit` | 负载执行器事务边界、重试与统计 |
| `performance_script_integration_check.sh` | `stratakv-test-performance-script-integration` | smoke 编排静态检查 |
| `performance_report_integration_check.py` | `stratakv-test-performance-report-integration` | 报告完整性门禁与失败传播 |
| `d1_compare_integration_check.py` | `stratakv-test-d1-compare-integration` | D1 兼容签名与三次中位数回归比较 |
| `txn_contention_benchmark.cpp` | `stratakv-test-txn-contention-benchmark` | 热点事务争用负载 |
| `region_2pc_benchmark.cpp` | `stratakv-test-region-2pc-benchmark` | 跨 Region 2PC 扩展性 |
| `kv_workload.cpp` | `stratakv-test-kv-workload` | YCSB/转账/寄存器施压与校验驱动 |
| `reliability_check.cpp` | `stratakv-test-reliability` | 故障注入负载与原子性/持久化校验 |
| `support/` | (库) | `performance_support`、`workload_runner`、`linearizability_checker`、`d1_compare.py`、`report.py` |

## sdk/、pulsar/、foundation/

| 文件 | CTest 名称 | 用途 |
| --- | --- | --- |
| `sdk/sdk_contract_unit_check.cpp` | `stratakv-test-sdk-contract-unit` | SDK 类型、状态名称与缺失值契约 |
| `pulsar/fiber_context_ab_benchmark.cpp` | `stratakv-test-fiber-context-ab-benchmark` | 上下文切换严格 A/B |
| `pulsar/run_context_switch_ab.py` | (手动) | Pulsar 栈池 Direct/Pooled vs Photon A/B 驱动 |
| `pulsar/run_fiber_reuse_ab.py` | (手动) | 协程栈复用开启前后 A/B 驱动 |
| `foundation/bounded_thread_pool_unit_check.cpp` | `stratakv-test-bounded-thread-pool-unit` | 有界线程池背压与完成语义 |

Pulsar 协程库自身的 6 项检查(`stratakv-test-fiber-sync`、`-fiber-context`、
`-scheduler-work-stealing`、`-stack-pool`、`-scheduler-cache`、`-reuse-integration`)
与 `stratakv-test-fiber-benchmark` 直接引用 `src/pulsar/` 子模块源文件,命名沿用子模块。

## 约定与自检

- 所有测试源文件头部必须包含 4 项规范说明(目标/策略/规模/验证内容),
  由 `python3 test/verify_test_headers.py`(仓库根目录运行)检查;
- 新增测试:放进对应领域目录,按 `<名称>_unit_check.cpp` 命名并在本 README 登记;
- 结果产物一律写入 `test-results/`(按 run-id 建子目录),不散落 `.log`/`.json`。
