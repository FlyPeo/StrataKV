add_executable(stratakv-test-fiber-sync fiber_sync_check.cpp)
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

add_executable(stratakv-test-bounded-thread-pool bounded_thread_pool_check.cpp)
target_link_libraries(stratakv-test-bounded-thread-pool PRIVATE pthread)
add_test(NAME stratakv-test-bounded-thread-pool COMMAND stratakv-test-bounded-thread-pool)

add_executable(stratakv-test-txn-scheduler txn_scheduler_check.cpp)
target_link_libraries(stratakv-test-txn-scheduler PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-txn-scheduler COMMAND stratakv-test-txn-scheduler)

add_executable(stratakv-test-tso tso_integration_check.cpp)
target_link_libraries(stratakv-test-tso PRIVATE stratakv_core pthread)
add_dependencies(stratakv-test-tso stratakv-tso)
add_test(NAME stratakv-test-tso COMMAND stratakv-test-tso)
set_tests_properties(stratakv-test-tso PROPERTIES TIMEOUT 120)

add_executable(stratakv-test-timestamp-oracle timestamp_oracle_check.cpp)
target_link_libraries(stratakv-test-timestamp-oracle PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-timestamp-oracle COMMAND stratakv-test-timestamp-oracle)

add_executable(stratakv-test-transaction-coordinator transaction_coordinator_check.cpp)
target_link_libraries(stratakv-test-transaction-coordinator PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-transaction-coordinator COMMAND stratakv-test-transaction-coordinator)

add_executable(stratakv-test-mvcc-batch mvcc_batch_check.cpp)
target_link_libraries(stratakv-test-mvcc-batch PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-mvcc-batch COMMAND stratakv-test-mvcc-batch)

add_executable(stratakv-test-region-batch-coordinator region_batch_coordinator_check.cpp)
target_link_libraries(stratakv-test-region-batch-coordinator PRIVATE stratakv_core pthread)
add_test(NAME stratakv-test-region-batch-coordinator COMMAND stratakv-test-region-batch-coordinator)

add_executable(stratakv-test-sdk-contract sdk_contract_check.cpp)
target_link_libraries(stratakv-test-sdk-contract PRIVATE stratakv_sdk pthread)
add_test(NAME stratakv-test-sdk-contract COMMAND stratakv-test-sdk-contract)

add_executable(stratakv-test-txn-contention txn_contention_benchmark.cpp)
target_link_libraries(stratakv-test-txn-contention PRIVATE stratakv_sdk pthread)

add_executable(stratakv-test-region-2pc-benchmark region_2pc_benchmark.cpp)
target_link_libraries(stratakv-test-region-2pc-benchmark PRIVATE stratakv_core pthread)

# Release-oriented coroutine benchmark. It is intentionally not registered in
# CTest because the case sizes and CPU-affinity policy belong to the caller.
add_executable(stratakv-test-fiber-benchmark fiber_benchmark.cpp)
target_link_libraries(stratakv-test-fiber-benchmark PRIVATE Pulsar::pulsar pthread dl)

# Public-API half of the strict Pulsar/Photon context-switch A/B. The same
# source can be compiled against Photon with STRATAKV_AB_PHOTON so both sides
# share the loop, clock, warm-up, stack size and correctness checks.
add_executable(stratakv-test-fiber-context-ab fiber_context_ab_benchmark.cpp)
target_compile_definitions(stratakv-test-fiber-context-ab PRIVATE STRATAKV_AB_PULSAR=1)
target_link_libraries(stratakv-test-fiber-context-ab PRIVATE Pulsar::pulsar pthread dl)

# Cluster reliability workload. This is intentionally not registered in CTest:
# it requires a live multi-node Gateway deployment and performs persistent I/O.
add_executable(stratakv-test-reliability reliability_check.cpp)
target_link_libraries(stratakv-test-reliability PRIVATE pthread)
