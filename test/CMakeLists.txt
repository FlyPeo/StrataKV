add_executable(stratakv-test-fiber-sync fiber_sync_check.cpp)
target_link_libraries(stratakv-test-fiber-sync PRIVATE Pulsar::pulsar pthread dl)
add_test(NAME stratakv-test-fiber-sync COMMAND stratakv-test-fiber-sync)

add_executable(stratakv-test-fiber-context ../src/pulsar/tests/fiber_context_check.cpp)
target_link_libraries(stratakv-test-fiber-context PRIVATE Pulsar::pulsar pthread dl)
add_test(NAME stratakv-test-fiber-context COMMAND stratakv-test-fiber-context)

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

add_executable(stratakv-test-sdk-contract sdk_contract_check.cpp)
target_link_libraries(stratakv-test-sdk-contract PRIVATE stratakv_sdk pthread)
add_test(NAME stratakv-test-sdk-contract COMMAND stratakv-test-sdk-contract)

add_executable(stratakv-test-txn-contention txn_contention_benchmark.cpp)
target_link_libraries(stratakv-test-txn-contention PRIVATE stratakv_sdk pthread)

# Release-oriented coroutine benchmark. It is intentionally not registered in
# CTest because the case sizes and CPU-affinity policy belong to the caller.
add_executable(stratakv-test-fiber-benchmark fiber_benchmark.cpp)
target_link_libraries(stratakv-test-fiber-benchmark PRIVATE Pulsar::pulsar pthread dl)

# Cluster reliability workload. This is intentionally not registered in CTest:
# it requires a live multi-node Gateway deployment and performs persistent I/O.
add_executable(stratakv-test-reliability reliability_check.cpp)
target_link_libraries(stratakv-test-reliability PRIVATE pthread)
