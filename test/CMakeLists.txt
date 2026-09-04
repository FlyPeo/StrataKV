add_executable(stratakv-test-fiber-sync ../src/pulsar/tests/fiber_sync_check.cpp)
target_link_libraries(stratakv-test-fiber-sync PRIVATE Pulsar::pulsar pthread dl)
add_test(NAME stratakv-test-fiber-sync COMMAND stratakv-test-fiber-sync)

add_executable(stratakv-test-fiber-context ../src/pulsar/tests/fiber_context_check.cpp)
target_link_libraries(stratakv-test-fiber-context PRIVATE Pulsar::pulsar pthread dl)
add_test(NAME stratakv-test-fiber-context COMMAND stratakv-test-fiber-context)

add_executable(stratakv-test-scheduler-work-stealing ../src/pulsar/tests/scheduler_work_stealing_check.cpp)
target_link_libraries(stratakv-test-scheduler-work-stealing PRIVATE Pulsar::pulsar pthread dl)
add_test(NAME stratakv-test-scheduler-work-stealing COMMAND stratakv-test-scheduler-work-stealing)

add_executable(stratakv-test-stack-pool ../src/pulsar/tests/stack_pool_check.cpp)
target_link_libraries(stratakv-test-stack-pool PRIVATE Pulsar::pulsar pthread dl)
add_test(NAME stratakv-test-stack-pool COMMAND stratakv-test-stack-pool)

add_executable(stratakv-test-scheduler-cache ../src/pulsar/tests/scheduler_cache_check.cpp)
target_link_libraries(stratakv-test-scheduler-cache PRIVATE Pulsar::pulsar pthread dl)
add_test(NAME stratakv-test-scheduler-cache COMMAND stratakv-test-scheduler-cache)

add_executable(stratakv-test-reuse-integration ../src/pulsar/tests/reuse_integration_check.cpp)
target_link_libraries(stratakv-test-reuse-integration PRIVATE Pulsar::pulsar pthread dl)
add_test(NAME stratakv-test-reuse-integration COMMAND stratakv-test-reuse-integration)

add_executable(stratakv-test-bounded-thread-pool unit/bounded_thread_pool_check.cpp)
target_link_libraries(stratakv-test-bounded-thread-pool PRIVATE pthread)
add_test(NAME stratakv-test-bounded-thread-pool COMMAND stratakv-test-bounded-thread-pool)

add_executable(stratakv-test-raft-log-gc unit/raft_log_gc_check.cpp)
target_link_libraries(stratakv-test-raft-log-gc PRIVATE pthread)
add_test(NAME stratakv-test-raft-log-gc COMMAND stratakv-test-raft-log-gc)

add_library(stratakv-test-performance-support STATIC
    support/performance/performance_support.cpp
    support/performance/workload_runner.cpp
    support/performance/linearizability_checker.cpp)
target_include_directories(stratakv-test-performance-support PUBLIC ${PROJECT_SOURCE_DIR}/test)
target_link_libraries(stratakv-test-performance-support PUBLIC stratakv_sdk pthread)

add_executable(stratakv-test-performance-support-check unit/performance_support_check.cpp)
target_link_libraries(stratakv-test-performance-support-check PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-performance-support-check COMMAND stratakv-test-performance-support-check)

add_executable(stratakv-test-workload-runner-check unit/workload_runner_check.cpp)
target_link_libraries(stratakv-test-workload-runner-check PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-workload-runner-check COMMAND stratakv-test-workload-runner-check)

add_executable(stratakv-test-a1-matrix-check unit/a1_matrix_check.cpp)
target_link_libraries(stratakv-test-a1-matrix-check PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-a1-matrix-check COMMAND stratakv-test-a1-matrix-check)

add_executable(stratakv-test-a2-transaction-check unit/a2_transaction_check.cpp)
target_link_libraries(stratakv-test-a2-transaction-check PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-a2-transaction-check COMMAND stratakv-test-a2-transaction-check)

add_executable(stratakv-test-a3-fanout-check unit/a3_fanout_check.cpp)
target_link_libraries(stratakv-test-a3-fanout-check PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-a3-fanout-check COMMAND stratakv-test-a3-fanout-check)

add_executable(stratakv-test-a4-contention-check unit/a4_contention_check.cpp)
target_link_libraries(stratakv-test-a4-contention-check PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-a4-contention-check COMMAND stratakv-test-a4-contention-check)

add_executable(stratakv-test-b3-barrier-check unit/b3_barrier_check.cpp)
target_link_libraries(stratakv-test-b3-barrier-check PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-b3-barrier-check COMMAND stratakv-test-b3-barrier-check)

add_executable(stratakv-test-b3-recovery-check unit/b3_recovery_check.cpp)
target_link_libraries(stratakv-test-b3-recovery-check PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-b3-recovery-check COMMAND stratakv-test-b3-recovery-check)

add_executable(stratakv-test-c1-transfer-check unit/c1_transfer_check.cpp)
target_link_libraries(stratakv-test-c1-transfer-check PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-c1-transfer-check COMMAND stratakv-test-c1-transfer-check)

add_executable(stratakv-test-c2-linearizability-check unit/c2_linearizability_check.cpp)
target_link_libraries(stratakv-test-c2-linearizability-check PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-c2-linearizability-check COMMAND stratakv-test-c2-linearizability-check)

add_executable(stratakv-test-txn-scheduler unit/txn_scheduler_check.cpp)
target_link_libraries(stratakv-test-txn-scheduler PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-txn-scheduler COMMAND stratakv-test-txn-scheduler)

add_executable(stratakv-test-tso integration/tso_integration_check.cpp)
target_link_libraries(stratakv-test-tso PRIVATE stratakv_core pthread)
add_dependencies(stratakv-test-tso stratakv-tso)
add_test(NAME stratakv-test-tso COMMAND stratakv-test-tso)
set_tests_properties(stratakv-test-tso PROPERTIES TIMEOUT 120)

# Self-contained 3-member TSO A/B benchmark. It is intentionally not a CTest
# because latency/throughput measurements are machine- and build-dependent.
add_executable(stratakv-test-tso-range-benchmark benchmarks/tso_range_benchmark.cpp)
target_link_libraries(stratakv-test-tso-range-benchmark PRIVATE stratakv_core pthread)
add_dependencies(stratakv-test-tso-range-benchmark stratakv-tso)

add_executable(stratakv-test-timestamp-oracle unit/timestamp_oracle_check.cpp)
target_link_libraries(stratakv-test-timestamp-oracle PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-timestamp-oracle COMMAND stratakv-test-timestamp-oracle)

add_executable(stratakv-test-transaction-coordinator unit/transaction_coordinator_check.cpp)
target_link_libraries(stratakv-test-transaction-coordinator PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-transaction-coordinator COMMAND stratakv-test-transaction-coordinator)

add_executable(stratakv-test-mvcc-batch unit/mvcc_batch_check.cpp)
target_link_libraries(stratakv-test-mvcc-batch PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-mvcc-batch COMMAND stratakv-test-mvcc-batch)

add_executable(stratakv-test-region-batch-coordinator unit/region_batch_coordinator_check.cpp)
target_link_libraries(stratakv-test-region-batch-coordinator PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-batch-coordinator COMMAND stratakv-test-region-batch-coordinator)

add_executable(stratakv-test-sdk-contract unit/sdk_contract_check.cpp)
target_link_libraries(stratakv-test-sdk-contract PRIVATE stratakv_sdk pthread)
add_test(NAME stratakv-test-sdk-contract COMMAND stratakv-test-sdk-contract)

add_executable(stratakv-test-txn-contention benchmarks/txn_contention_benchmark.cpp)
target_link_libraries(stratakv-test-txn-contention PRIVATE stratakv_sdk pthread)

add_executable(stratakv-test-region-2pc-benchmark benchmarks/region_2pc_benchmark.cpp)
target_link_libraries(stratakv-test-region-2pc-benchmark PRIVATE stratakv_core pthread)

# Release-oriented coroutine benchmark. It is intentionally not registered in
# CTest because the case sizes and CPU-affinity policy belong to the caller.
add_executable(stratakv-test-fiber-benchmark ../src/pulsar/benchmarks/fiber_benchmark.cpp)
target_link_libraries(stratakv-test-fiber-benchmark PRIVATE Pulsar::pulsar pthread dl)

# Public-API half of the strict Pulsar/Photon context-switch A/B. The same
# source can be compiled against Photon with STRATAKV_AB_PHOTON so both sides
# share the loop, clock, warm-up, stack size and correctness checks.
add_executable(stratakv-test-fiber-context-ab benchmarks/fiber_context_ab_benchmark.cpp)
target_compile_definitions(stratakv-test-fiber-context-ab PRIVATE STRATAKV_AB_PULSAR=1)
target_link_libraries(stratakv-test-fiber-context-ab PRIVATE Pulsar::pulsar pthread dl)

# Cluster reliability workload. This is intentionally not registered in CTest:
# it requires a live multi-node Gateway deployment and performs persistent I/O.
add_executable(stratakv-test-reliability system/reliability_check.cpp)
target_link_libraries(stratakv-test-reliability PRIVATE pthread)

# Unified YCSB-compatible/C1/C2 workload. It requires a live three-Region
# deployment, so orchestration lives in deploy/stratakv-performance.
add_executable(stratakv-test-kv-workload system/kv_workload.cpp)
target_link_libraries(stratakv-test-kv-workload PRIVATE stratakv-test-performance-support pthread)

add_test(NAME stratakv-test-performance-script
    COMMAND bash ${PROJECT_SOURCE_DIR}/test/integration/performance_script_check.sh)
add_test(NAME stratakv-test-performance-report
    COMMAND python3 ${PROJECT_SOURCE_DIR}/test/integration/performance_report_check.py)
add_test(NAME stratakv-test-d1-compare
    COMMAND python3 ${PROJECT_SOURCE_DIR}/test/integration/d1_compare_check.py)
add_test(NAME stratakv-test-gateway-smoke
    COMMAND bash ${PROJECT_SOURCE_DIR}/test/integration/gateway_smoke_check.sh)

