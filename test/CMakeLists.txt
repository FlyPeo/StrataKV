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

add_executable(stratakv-test-bounded-thread-pool-unit foundation/bounded_thread_pool_unit_check.cpp)
target_link_libraries(stratakv-test-bounded-thread-pool-unit PRIVATE pthread)
add_test(NAME stratakv-test-bounded-thread-pool-unit COMMAND stratakv-test-bounded-thread-pool-unit)

add_executable(stratakv-test-raft-log-gc-unit raft/raft_log_gc_unit_check.cpp)
target_link_libraries(stratakv-test-raft-log-gc-unit PRIVATE pthread)
add_test(NAME stratakv-test-raft-log-gc-unit COMMAND stratakv-test-raft-log-gc-unit)

add_executable(stratakv-test-rocksdb-snapshot-unit raft/rocksdb_snapshot_unit_check.cpp)
target_link_libraries(stratakv-test-rocksdb-snapshot-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-rocksdb-snapshot-unit COMMAND stratakv-test-rocksdb-snapshot-unit)

add_executable(stratakv-test-raft-confchange-unit raft/raft_confchange_unit_check.cpp)
target_link_libraries(stratakv-test-raft-confchange-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-raft-confchange-unit COMMAND stratakv-test-raft-confchange-unit)

add_executable(stratakv-test-replica-catchup-unit raft/replica_catchup_unit_check.cpp)
target_link_libraries(stratakv-test-replica-catchup-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-replica-catchup-unit COMMAND stratakv-test-replica-catchup-unit)

add_executable(stratakv-test-node-migration-unit region/node_migration_unit_check.cpp)
target_include_directories(stratakv-test-node-migration-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/server ${PROJECT_SOURCE_DIR}/src/metadata/include)
target_link_libraries(stratakv-test-node-migration-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-node-migration-unit COMMAND stratakv-test-node-migration-unit)

add_executable(stratakv-test-auto-balancer-state-unit metadata/auto_balancer_state_unit_check.cpp)
target_include_directories(stratakv-test-auto-balancer-state-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/metadata/include)
target_link_libraries(stratakv-test-auto-balancer-state-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-auto-balancer-state-unit COMMAND stratakv-test-auto-balancer-state-unit)

add_executable(stratakv-test-auto-balancer-planner-unit metadata/auto_balancer_planner_unit_check.cpp)
target_include_directories(stratakv-test-auto-balancer-planner-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/metadata/include)
target_link_libraries(stratakv-test-auto-balancer-planner-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-auto-balancer-planner-unit COMMAND stratakv-test-auto-balancer-planner-unit)

add_executable(stratakv-test-auto-balancer-heartbeat-unit metadata/auto_balancer_heartbeat_unit_check.cpp)
target_include_directories(stratakv-test-auto-balancer-heartbeat-unit PRIVATE
    ${PROJECT_SOURCE_DIR}/src/server
    ${PROJECT_SOURCE_DIR}/src/metadata/include)
target_link_libraries(stratakv-test-auto-balancer-heartbeat-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-auto-balancer-heartbeat-unit COMMAND stratakv-test-auto-balancer-heartbeat-unit)
set_tests_properties(stratakv-test-auto-balancer-heartbeat-unit PROPERTIES TIMEOUT 60)

add_executable(stratakv-test-auto-balancer-recovery-unit metadata/auto_balancer_recovery_unit_check.cpp)
target_include_directories(stratakv-test-auto-balancer-recovery-unit PRIVATE
    ${PROJECT_SOURCE_DIR}/src/server
    ${PROJECT_SOURCE_DIR}/src/metadata/include)
target_link_libraries(stratakv-test-auto-balancer-recovery-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-auto-balancer-recovery-unit COMMAND stratakv-test-auto-balancer-recovery-unit)
set_tests_properties(stratakv-test-auto-balancer-recovery-unit PROPERTIES TIMEOUT 60)

add_executable(stratakv-test-online-migration region/online_migration_integration_check.cpp)
target_link_libraries(stratakv-test-online-migration PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-online-migration COMMAND stratakv-test-online-migration)
set_tests_properties(stratakv-test-online-migration PROPERTIES TIMEOUT 60)

add_executable(stratakv-test-auto-balancer-integration metadata/auto_balancer_integration_check.cpp)
target_include_directories(stratakv-test-auto-balancer-integration PRIVATE
    ${PROJECT_SOURCE_DIR}/src/server
    ${PROJECT_SOURCE_DIR}/src/metadata/include)
target_link_libraries(stratakv-test-auto-balancer-integration PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-auto-balancer-integration COMMAND stratakv-test-auto-balancer-integration)
set_tests_properties(stratakv-test-auto-balancer-integration PROPERTIES TIMEOUT 60)

add_library(stratakv-test-performance-support STATIC
    performance/support/performance_support.cpp
    performance/support/workload_runner.cpp
    performance/support/linearizability_checker.cpp)
target_include_directories(stratakv-test-performance-support PUBLIC ${PROJECT_SOURCE_DIR}/test/performance)
target_link_libraries(stratakv-test-performance-support PUBLIC stratakv_sdk pthread)

add_executable(stratakv-test-performance-support-unit performance/performance_support_unit_check.cpp)
target_link_libraries(stratakv-test-performance-support-unit PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-performance-support-unit COMMAND stratakv-test-performance-support-unit)

add_executable(stratakv-test-workload-runner-unit performance/workload_runner_unit_check.cpp)
target_link_libraries(stratakv-test-workload-runner-unit PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-workload-runner-unit COMMAND stratakv-test-workload-runner-unit)

add_executable(stratakv-test-a1-matrix-unit performance/a1_matrix_unit_check.cpp)
target_link_libraries(stratakv-test-a1-matrix-unit PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-a1-matrix-unit COMMAND stratakv-test-a1-matrix-unit)

add_executable(stratakv-test-a2-transaction-unit performance/a2_transaction_unit_check.cpp)
target_link_libraries(stratakv-test-a2-transaction-unit PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-a2-transaction-unit COMMAND stratakv-test-a2-transaction-unit)

add_executable(stratakv-test-a3-fanout-unit performance/a3_fanout_unit_check.cpp)
target_link_libraries(stratakv-test-a3-fanout-unit PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-a3-fanout-unit COMMAND stratakv-test-a3-fanout-unit)

add_executable(stratakv-test-a4-contention-unit performance/a4_contention_unit_check.cpp)
target_link_libraries(stratakv-test-a4-contention-unit PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-a4-contention-unit COMMAND stratakv-test-a4-contention-unit)

add_executable(stratakv-test-b3-barrier-unit performance/b3_barrier_unit_check.cpp)
target_link_libraries(stratakv-test-b3-barrier-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-b3-barrier-unit COMMAND stratakv-test-b3-barrier-unit)

add_executable(stratakv-test-b3-recovery-unit performance/b3_recovery_unit_check.cpp)
target_link_libraries(stratakv-test-b3-recovery-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-b3-recovery-unit COMMAND stratakv-test-b3-recovery-unit)

add_executable(stratakv-test-c1-transfer-unit performance/c1_transfer_unit_check.cpp)
target_link_libraries(stratakv-test-c1-transfer-unit PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-c1-transfer-unit COMMAND stratakv-test-c1-transfer-unit)

add_executable(stratakv-test-c2-linearizability-unit performance/c2_linearizability_unit_check.cpp)
target_link_libraries(stratakv-test-c2-linearizability-unit PRIVATE stratakv-test-performance-support pthread)
add_test(NAME stratakv-test-c2-linearizability-unit COMMAND stratakv-test-c2-linearizability-unit)

add_executable(stratakv-test-txn-scheduler-unit transaction/txn_scheduler_unit_check.cpp)
target_link_libraries(stratakv-test-txn-scheduler-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-txn-scheduler-unit COMMAND stratakv-test-txn-scheduler-unit)

add_executable(stratakv-test-txn-recovery-dynamic-unit transaction/txn_recovery_dynamic_unit_check.cpp)
target_link_libraries(stratakv-test-txn-recovery-dynamic-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-txn-recovery-dynamic-unit COMMAND stratakv-test-txn-recovery-dynamic-unit)

add_executable(stratakv-test-dynamic-cluster region/dynamic_cluster_integration_check.cpp)
target_include_directories(stratakv-test-dynamic-cluster PRIVATE ${PROJECT_SOURCE_DIR}/src/metadata/include ${PROJECT_SOURCE_DIR}/src/sdk/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-dynamic-cluster PRIVATE stratakv_sdk stratakv_core pthread)
add_dependencies(stratakv-test-dynamic-cluster stratakv-tso stratakv-meta stratakv-node)
add_test(NAME stratakv-test-dynamic-cluster COMMAND stratakv-test-dynamic-cluster)
set_tests_properties(stratakv-test-dynamic-cluster PROPERTIES TIMEOUT 420)

add_executable(stratakv-test-tso-integration tso/tso_integration_check.cpp)
target_link_libraries(stratakv-test-tso-integration PRIVATE stratakv_core pthread)
add_dependencies(stratakv-test-tso-integration stratakv-tso)
add_test(NAME stratakv-test-tso-integration COMMAND stratakv-test-tso-integration)
set_tests_properties(stratakv-test-tso-integration PROPERTIES TIMEOUT 120)

# Self-contained 3-member TSO A/B benchmark. It is intentionally not a CTest
# because latency/throughput measurements are machine- and build-dependent.
add_executable(stratakv-test-tso-range-benchmark tso/tso_range_benchmark.cpp)
target_link_libraries(stratakv-test-tso-range-benchmark PRIVATE stratakv_core pthread)
add_dependencies(stratakv-test-tso-range-benchmark stratakv-tso)

add_executable(stratakv-test-timestamp-oracle-unit tso/timestamp_oracle_unit_check.cpp)
target_link_libraries(stratakv-test-timestamp-oracle-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-timestamp-oracle-unit COMMAND stratakv-test-timestamp-oracle-unit)

add_executable(stratakv-test-transaction-coordinator-unit transaction/transaction_coordinator_unit_check.cpp)
target_link_libraries(stratakv-test-transaction-coordinator-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-transaction-coordinator-unit COMMAND stratakv-test-transaction-coordinator-unit)

add_executable(stratakv-test-mvcc-batch-unit transaction/mvcc_batch_unit_check.cpp)
target_link_libraries(stratakv-test-mvcc-batch-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-mvcc-batch-unit COMMAND stratakv-test-mvcc-batch-unit)

add_executable(stratakv-test-region-batch-coordinator-unit transaction/region_batch_coordinator_unit_check.cpp)
target_link_libraries(stratakv-test-region-batch-coordinator-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-batch-coordinator-unit COMMAND stratakv-test-region-batch-coordinator-unit)

add_executable(stratakv-test-region-metadata-unit region/region_metadata_unit_check.cpp)
target_link_libraries(stratakv-test-region-metadata-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-metadata-unit COMMAND stratakv-test-region-metadata-unit)

add_executable(stratakv-test-topology-config-unit region/topology_config_unit_check.cpp)
target_link_libraries(stratakv-test-topology-config-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-topology-config-unit COMMAND stratakv-test-topology-config-unit)

add_executable(stratakv-test-region-cache-unit region/region_cache_unit_check.cpp)
target_link_libraries(stratakv-test-region-cache-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-cache-unit COMMAND stratakv-test-region-cache-unit)

add_executable(stratakv-test-region-registry-unit region/region_registry_unit_check.cpp)
target_include_directories(stratakv-test-region-registry-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/server)
target_link_libraries(stratakv-test-region-registry-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-registry-unit COMMAND stratakv-test-region-registry-unit)

add_executable(stratakv-test-region-dispatcher-unit region/region_dispatcher_unit_check.cpp)
target_include_directories(stratakv-test-region-dispatcher-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/server)
target_link_libraries(stratakv-test-region-dispatcher-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-dispatcher-unit COMMAND stratakv-test-region-dispatcher-unit)

add_executable(stratakv-test-region-split-unit region/region_split_unit_check.cpp)
target_include_directories(stratakv-test-region-split-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/server)
target_link_libraries(stratakv-test-region-split-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-split-unit COMMAND stratakv-test-region-split-unit)

add_executable(stratakv-test-node-split-unit region/node_split_unit_check.cpp)
target_include_directories(stratakv-test-node-split-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/server)
target_link_libraries(stratakv-test-node-split-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-node-split-unit COMMAND stratakv-test-node-split-unit)

add_executable(stratakv-test-region-request-sender-unit region/region_request_sender_unit_check.cpp)
target_include_directories(stratakv-test-region-request-sender-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/transaction/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-region-request-sender-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-request-sender-unit COMMAND stratakv-test-region-request-sender-unit)

add_executable(stratakv-test-region-cache-unit-stress-unit region/region_cache_stress_unit_check.cpp)
target_include_directories(stratakv-test-region-cache-unit-stress-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/server ${PROJECT_SOURCE_DIR}/src/raft/include ${PROJECT_SOURCE_DIR}/src/transaction/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-region-cache-unit-stress-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-cache-unit-stress-unit COMMAND stratakv-test-region-cache-unit-stress-unit)

add_executable(stratakv-test-region-envelope-compat-unit region/region_envelope_compat_unit_check.cpp)
target_include_directories(stratakv-test-region-envelope-compat-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/server ${PROJECT_SOURCE_DIR}/src/transaction/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-region-envelope-compat-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-envelope-compat-unit COMMAND stratakv-test-region-envelope-compat-unit)

add_executable(stratakv-test-metadata-consensus-unit-fault-unit metadata/metadata_consensus_fault_unit_check.cpp)
target_include_directories(stratakv-test-metadata-consensus-unit-fault-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/metadata/include ${PROJECT_SOURCE_DIR}/src/raft/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-metadata-consensus-unit-fault-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-metadata-consensus-unit-fault-unit COMMAND stratakv-test-metadata-consensus-unit-fault-unit)

add_executable(stratakv-test-metadata-property-unit metadata/metadata_property_unit_check.cpp)
target_include_directories(stratakv-test-metadata-property-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/metadata/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-metadata-property-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-metadata-property-unit COMMAND stratakv-test-metadata-property-unit)

add_executable(stratakv-test-region-data-path-unit region/region_data_path_unit_check.cpp)
target_include_directories(stratakv-test-region-data-path-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/transaction/include ${PROJECT_SOURCE_DIR}/src/rpc/include ${PROJECT_SOURCE_DIR}/src/common/include)
target_link_libraries(stratakv-test-region-data-path-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-data-path-unit COMMAND stratakv-test-region-data-path-unit)

add_executable(stratakv-test-node-bootstrap-unit region/node_bootstrap_unit_check.cpp)
target_include_directories(stratakv-test-node-bootstrap-unit PRIVATE ${PROJECT_SOURCE_DIR}/src/server)
target_link_libraries(stratakv-test-node-bootstrap-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-node-bootstrap-unit COMMAND stratakv-test-node-bootstrap-unit)

add_executable(stratakv-test-metadata-state-machine-unit metadata/metadata_state_machine_unit_check.cpp)
target_link_libraries(stratakv-test-metadata-state-machine-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-metadata-state-machine-unit COMMAND stratakv-test-metadata-state-machine-unit)

add_executable(stratakv-test-metadata-consensus-unit metadata/metadata_consensus_unit_check.cpp)
target_link_libraries(stratakv-test-metadata-consensus-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-metadata-consensus-unit COMMAND stratakv-test-metadata-consensus-unit)

add_executable(stratakv-test-metadata-client-unit metadata/metadata_client_unit_check.cpp)
target_link_libraries(stratakv-test-metadata-client-unit PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-metadata-client-unit COMMAND stratakv-test-metadata-client-unit)

add_executable(stratakv-test-metadata-integration metadata/metadata_integration_check.cpp)
target_link_libraries(stratakv-test-metadata-integration PRIVATE stratakv_core pthread)
add_dependencies(stratakv-test-metadata-integration stratakv-meta)
add_test(NAME stratakv-test-metadata-integration COMMAND stratakv-test-metadata-integration)
set_tests_properties(stratakv-test-metadata-integration PROPERTIES TIMEOUT 120)

add_executable(stratakv-test-sdk-contract-unit sdk/sdk_contract_unit_check.cpp)
target_link_libraries(stratakv-test-sdk-contract-unit PRIVATE stratakv_sdk pthread)
add_test(NAME stratakv-test-sdk-contract-unit COMMAND stratakv-test-sdk-contract-unit)

add_executable(stratakv-test-txn-contention-benchmark performance/txn_contention_benchmark.cpp)
target_link_libraries(stratakv-test-txn-contention-benchmark PRIVATE stratakv_sdk pthread)

add_executable(stratakv-test-region-2pc-benchmark performance/region_2pc_benchmark.cpp)
target_link_libraries(stratakv-test-region-2pc-benchmark PRIVATE stratakv_core pthread)

add_executable(stratakv-test-dynamic-sharding-benchmark region/dynamic_sharding_benchmark.cpp)
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
add_executable(stratakv-test-fiber-context-ab-benchmark pulsar/fiber_context_ab_benchmark.cpp)
target_compile_definitions(stratakv-test-fiber-context-ab-benchmark PRIVATE STRATAKV_AB_PULSAR=1)
target_link_libraries(stratakv-test-fiber-context-ab-benchmark PRIVATE Pulsar::pulsar pthread dl)

# Cluster reliability workload. This is intentionally not registered in CTest:
# it requires a live multi-node deployment and performs persistent I/O via native C++ SDK.
add_executable(stratakv-test-reliability performance/reliability_check.cpp)
target_link_libraries(stratakv-test-reliability PRIVATE stratakv_sdk pthread)

# Unified YCSB-compatible/C1/C2 workload. It requires a live three-Region
# deployment, so orchestration lives in deploy/stratakv-performance.
add_executable(stratakv-test-kv-workload performance/kv_workload.cpp)
target_link_libraries(stratakv-test-kv-workload PRIVATE stratakv-test-performance-support pthread)

add_test(NAME stratakv-test-performance-script-integration
    COMMAND bash ${PROJECT_SOURCE_DIR}/test/performance/performance_script_integration_check.sh)
add_test(NAME stratakv-test-performance-report-integration
    COMMAND python3 ${PROJECT_SOURCE_DIR}/test/performance/performance_report_integration_check.py)
add_test(NAME stratakv-test-d1-compare-integration
    COMMAND python3 ${PROJECT_SOURCE_DIR}/test/performance/d1_compare_integration_check.py)
