add_executable(stratakv-test-fiber-sync fiber_sync_check.cpp)
target_link_libraries(stratakv-test-fiber-sync PRIVATE Pulsar::pulsar pthread dl)
add_test(NAME stratakv-test-fiber-sync COMMAND stratakv-test-fiber-sync)

add_executable(stratakv-test-bounded-thread-pool bounded_thread_pool_check.cpp)
target_link_libraries(stratakv-test-bounded-thread-pool PRIVATE pthread)
add_test(NAME stratakv-test-bounded-thread-pool COMMAND stratakv-test-bounded-thread-pool)

# Release-oriented coroutine benchmark. It is intentionally not registered in
# CTest because the case sizes and CPU-affinity policy belong to the caller.
add_executable(stratakv-test-fiber-benchmark fiber_benchmark.cpp)
target_link_libraries(stratakv-test-fiber-benchmark PRIVATE Pulsar::pulsar pthread dl)

# Cluster reliability workload. This is intentionally not registered in CTest:
# it requires a live multi-node Gateway deployment and performs persistent I/O.
add_executable(stratakv-test-reliability reliability_check.cpp)
target_link_libraries(stratakv-test-reliability PRIVATE pthread)
