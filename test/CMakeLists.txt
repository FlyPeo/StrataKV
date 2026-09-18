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

add_executable(stratakv-test-rocksdb-snapshot unit/rocksdb_snapshot_check.cpp)
target_link_libraries(stratakv-test-rocksdb-snapshot PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-rocksdb-snapshot COMMAND stratakv-test-rocksdb-snapshot)

add_executable(stratakv-test-raft-confchange unit/raft_confchange_check.cpp)
target_link_libraries(stratakv-test-raft-confchange PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-raft-confchange COMMAND stratakv-test-raft-confchange)

add_executable(stratakv-test-replica-catchup unit/replica_catchup_check.cpp)
target_link_libraries(stratakv-test-replica-catchup PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-replica-catchup COMMAND stratakv-test-replica-catchup)

add_executable(stratakv-test-node-migration unit/node_migration_check.cpp)
target_include_directories(stratakv-test-node-migration PRIVATE ${PROJECT_SOURCE_DIR}/src/server ${PROJECT_SOURCE_DIR}/src/metadata/include)
target_link_libraries(stratakv-test-node-migration PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-node-migration COMMAND stratakv-test-node-migration)

add_executable(stratakv-test-auto-balancer-state unit/auto_balancer_state_check.cpp)
target_include_directories(stratakv-test-auto-balancer-state PRIVATE ${PROJECT_SOURCE_DIR}/src/metadata/include)
target_link_libraries(stratakv-test-auto-balancer-state PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-auto-balancer-state COMMAND stratakv-test-auto-balancer-state)

add_executable(stratakv-test-auto-balancer-planner unit/auto_balancer_planner_check.cpp)
target_include_directories(stratakv-test-auto-balancer-planner PRIVATE ${PROJECT_SOURCE_DIR}/src/metadata/include)
target_link_libraries(stratakv-test-auto-balancer-planner PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-auto-balancer-planner COMMAND stratakv-test-auto-balancer-planner)

add_executable(stratakv-test-auto-balancer-heartbeat unit/auto_balancer_heartbeat_check.cpp)
target_include_directories(stratakv-test-auto-balancer-heartbeat PRIVATE
    ${PROJECT_SOURCE_DIR}/src/server
    ${PROJECT_SOURCE_DIR}/src/metadata/include)
target_link_libraries(stratakv-test-auto-balancer-heartbeat PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-auto-balancer-heartbeat COMMAND stratakv-test-auto-balancer-heartbeat)
set_tests_properties(stratakv-test-auto-balancer-heartbeat PROPERTIES TIMEOUT 60)

add_executable(stratakv-test-auto-balancer-recovery unit/auto_balancer_recovery_check.cpp)
target_include_directories(stratakv-test-auto-balancer-recovery PRIVATE
    ${PROJECT_SOURCE_DIR}/src/server
    ${PROJECT_SOURCE_DIR}/src/metadata/include)
target_link_libraries(stratakv-test-auto-balancer-recovery PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-auto-balancer-recovery COMMAND stratakv-test-auto-balancer-recovery)
set_tests_properties(stratakv-test-auto-balancer-recovery PROPERTIES TIMEOUT 60)

add_executable(stratakv-test-online-migration integration/online_migration_integration_check.cpp)
target_link_libraries(stratakv-test-online-migration PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-online-migration COMMAND stratakv-test-online-migration)
set_tests_properties(stratakv-test-online-migration PROPERTIES TIMEOUT 60)

add_executable(stratakv-test-auto-balancer-integration integration/auto_balancer_integration_check.cpp)
target_include_directories(stratakv-test-auto-balancer-integration PRIVATE
    ${PROJECT_SOURCE_DIR}/src/server
    ${PROJECT_SOURCE_DIR}/src/metadata/include)
target_link_libraries(stratakv-test-auto-balancer-integration PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-auto-balancer-integration COMMAND stratakv-test-auto-balancer-integration)
set_tests_properties(stratakv-test-auto-balancer-integration PROPERTIES TIMEOUT 60)

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

add_executable(stratakv-test-txn-recovery-dynamic unit/txn_recovery_dynamic_check.cpp)
target_link_libraries(stratakv-test-txn-recovery-dynamic PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-txn-recovery-dynamic COMMAND stratakv-test-txn-recovery-dynamic)

add_executable(stratakv-test-dynamic-cluster integration/dynamic_cluster_integration_check.cpp)
target_include_directories(stratakv-test-dynamic-cluster PRIVATE ${PROJECT_SOURCE_DIR}/src/metadata/include ${PROJECT_SOURCE_DIR}/src/sdk/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-dynamic-cluster PRIVATE stratakv_sdk stratakv_core pthread)
add_dependencies(stratakv-test-dynamic-cluster stratakv-tso stratakv-meta stratakv-node)
add_test(NAME stratakv-test-dynamic-cluster COMMAND stratakv-test-dynamic-cluster)
set_tests_properties(stratakv-test-dynamic-cluster PROPERTIES TIMEOUT 420)

add_executable(stratakv-test-tso-integration integration/tso_integration_check.cpp)
target_link_libraries(stratakv-test-tso-integration PRIVATE stratakv_core pthread)
add_dependencies(stratakv-test-tso-integration stratakv-tso)
add_test(NAME stratakv-test-tso-integration COMMAND stratakv-test-tso-integration)
set_tests_properties(stratakv-test-tso-integration PROPERTIES TIMEOUT 120)

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

add_executable(stratakv-test-region-metadata unit/region_metadata_check.cpp)
target_link_libraries(stratakv-test-region-metadata PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-metadata COMMAND stratakv-test-region-metadata)

add_executable(stratakv-test-topology-config unit/topology_config_check.cpp)
target_link_libraries(stratakv-test-topology-config PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-topology-config COMMAND stratakv-test-topology-config)

add_executable(stratakv-test-region-cache unit/region_cache_check.cpp)
target_link_libraries(stratakv-test-region-cache PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-cache COMMAND stratakv-test-region-cache)

add_executable(stratakv-test-region-registry unit/region_registry_check.cpp)
target_include_directories(stratakv-test-region-registry PRIVATE ${PROJECT_SOURCE_DIR}/src/server)
target_link_libraries(stratakv-test-region-registry PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-registry COMMAND stratakv-test-region-registry)

add_executable(stratakv-test-region-dispatcher unit/region_dispatcher_check.cpp)
target_include_directories(stratakv-test-region-dispatcher PRIVATE ${PROJECT_SOURCE_DIR}/src/server)
target_link_libraries(stratakv-test-region-dispatcher PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-dispatcher COMMAND stratakv-test-region-dispatcher)

add_executable(stratakv-test-region-split unit/region_split_check.cpp)
target_include_directories(stratakv-test-region-split PRIVATE ${PROJECT_SOURCE_DIR}/src/server)
target_link_libraries(stratakv-test-region-split PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-split COMMAND stratakv-test-region-split)

add_executable(stratakv-test-node-split unit/node_split_check.cpp)
target_include_directories(stratakv-test-node-split PRIVATE ${PROJECT_SOURCE_DIR}/src/server)
target_link_libraries(stratakv-test-node-split PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-node-split COMMAND stratakv-test-node-split)

add_executable(stratakv-test-region-request-sender unit/region_request_sender_check.cpp)
target_include_directories(stratakv-test-region-request-sender PRIVATE ${PROJECT_SOURCE_DIR}/src/transaction/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-region-request-sender PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-request-sender COMMAND stratakv-test-region-request-sender)

add_executable(stratakv-test-region-cache-stress unit/region_cache_stress_check.cpp)
target_include_directories(stratakv-test-region-cache-stress PRIVATE ${PROJECT_SOURCE_DIR}/src/server ${PROJECT_SOURCE_DIR}/src/raft/include ${PROJECT_SOURCE_DIR}/src/transaction/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-region-cache-stress PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-cache-stress COMMAND stratakv-test-region-cache-stress)

add_executable(stratakv-test-region-envelope-compat unit/region_envelope_compat_check.cpp)
target_include_directories(stratakv-test-region-envelope-compat PRIVATE ${PROJECT_SOURCE_DIR}/src/server ${PROJECT_SOURCE_DIR}/src/transaction/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-region-envelope-compat PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-envelope-compat COMMAND stratakv-test-region-envelope-compat)

add_executable(stratakv-test-metadata-consensus-fault unit/metadata_consensus_fault_check.cpp)
target_include_directories(stratakv-test-metadata-consensus-fault PRIVATE ${PROJECT_SOURCE_DIR}/src/metadata/include ${PROJECT_SOURCE_DIR}/src/raft/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-metadata-consensus-fault PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-metadata-consensus-fault COMMAND stratakv-test-metadata-consensus-fault)

add_executable(stratakv-test-metadata-property unit/metadata_property_check.cpp)
target_include_directories(stratakv-test-metadata-property PRIVATE ${PROJECT_SOURCE_DIR}/src/metadata/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-metadata-property PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-metadata-property COMMAND stratakv-test-metadata-property)

add_executable(stratakv-test-region-data-path unit/region_data_path_check.cpp)
target_include_directories(stratakv-test-region-data-path PRIVATE ${PROJECT_SOURCE_DIR}/src/transaction/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-region-data-path PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-data-path COMMAND stratakv-test-region-data-path)

add_executable(stratakv-test-node-bootstrap unit/node_bootstrap_check.cpp)
target_include_directories(stratakv-test-node-bootstrap PRIVATE ${PROJECT_SOURCE_DIR}/src/server)
target_link_libraries(stratakv-test-node-bootstrap PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-node-bootstrap COMMAND stratakv-test-node-bootstrap)

add_executable(stratakv-test-metadata-state-machine unit/metadata_state_machine_check.cpp)
target_link_libraries(stratakv-test-metadata-state-machine PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-metadata-state-machine COMMAND stratakv-test-metadata-state-machine)

add_executable(stratakv-test-metadata-consensus unit/metadata_consensus_check.cpp)
target_link_libraries(stratakv-test-metadata-consensus PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-metadata-consensus COMMAND stratakv-test-metadata-consensus)

add_executable(stratakv-test-metadata-client unit/metadata_client_check.cpp)
target_link_libraries(stratakv-test-metadata-client PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-metadata-client COMMAND stratakv-test-metadata-client)

add_executable(stratakv-test-metadata-integration integration/metadata_integration_check.cpp)
target_link_libraries(stratakv-test-metadata-integration PRIVATE stratakv_core pthread)
add_dependencies(stratakv-test-metadata-integration stratakv-meta)
add_test(NAME stratakv-test-metadata-integration COMMAND stratakv-test-metadata-integration)
set_tests_properties(stratakv-test-metadata-integration PROPERTIES TIMEOUT 120)

add_executable(stratakv-test-sdk-contract unit/sdk_contract_check.cpp)
target_link_libraries(stratakv-test-sdk-contract PRIVATE stratakv_sdk pthread)
add_test(NAME stratakv-test-sdk-contract COMMAND stratakv-test-sdk-contract)

add_executable(stratakv-test-txn-contention benchmarks/txn_contention_benchmark.cpp)
target_link_libraries(stratakv-test-txn-contention PRIVATE stratakv_sdk pthread)

add_executable(stratakv-test-region-2pc-benchmark benchmarks/region_2pc_benchmark.cpp)
target_link_libraries(stratakv-test-region-2pc-benchmark PRIVATE stratakv_core pthread)

add_executable(stratakv-test-dynamic-sharding-benchmark benchmarks/dynamic_sharding_benchmark.cpp)
target_include_directories(stratakv-test-dynamic-sharding-benchmark PRIVATE
    ${PROJECT_SOURCE_DIR}/src/metadata/include
    ${PROJECT_SOURCE_DIR}/src/sdk/include
    ${PROJECT_SOURCE_DIR}/src/rpc/include
    ${PROJECT_SOURCE_DIR}/src/common/include
    ${PROJECT_SOURCE_DIR}/src/transaction/include
    ${PROJECT_SOURCE_DIR}/src/server)
target_link_libraries(stratakv-test-dynamic-sharding-benchmark PRIVATE
    stratakv_sdk stratakv_core pthread)

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
# it requires a live multi-node deployment and performs persistent I/O via native C++ SDK.
add_executable(stratakv-test-reliability system/reliability_check.cpp)
target_link_libraries(stratakv-test-reliability PRIVATE stratakv_sdk pthread)

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
