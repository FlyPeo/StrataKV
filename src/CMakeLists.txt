

add_subdirectory(kvEngine)
add_subdirectory(txn)

add_subdirectory(common) #需要注意如果rpc中需要依靠common中的代码，则需要在rpc在前，其cmake文件中的set才对rpc文件夹可见
# 使用include_directories()函数应该也是可行的
add_subdirectory(rpc)

set(PULSAR_BUILD_TESTS OFF CACHE BOOL "Build Pulsar correctness tests" FORCE)
set(PULSAR_BUILD_BENCHMARKS OFF CACHE BOOL "Build Pulsar benchmarks" FORCE)
add_subdirectory(pulsar)

add_subdirectory(raftRpcPro)

add_subdirectory(raftCore)
