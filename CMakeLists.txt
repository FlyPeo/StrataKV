cmake_minimum_required(VERSION 3.22)
project(StrataKV VERSION 1.2.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()
enable_testing()

# 消除 GCC 13 的位运算警告
add_compile_options(-Wno-deprecated-enum-enum-conversion)

option(STRATAKV_ENABLE_ASAN "Build StrataKV with AddressSanitizer" OFF)
if(STRATAKV_ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

option(STRATAKV_ENABLE_TEST_FAILPOINTS "Enable test-only failpoint barriers in 2PC coordinator" ON)
if(STRATAKV_ENABLE_TEST_FAILPOINTS)
    add_compile_definitions(STRATAKV_ENABLE_TEST_FAILPOINTS=1)
endif()

# Keep every user-facing executable in one predictable directory for all
# generators and build configurations.
set(STRATAKV_BIN_DIR "${PROJECT_SOURCE_DIR}/bin")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${STRATAKV_BIN_DIR}")
foreach(config DEBUG RELEASE RELWITHDEBINFO MINSIZEREL)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${config} "${STRATAKV_BIN_DIR}")
endforeach()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/lib")

# 发布打包(deploy/stratakv-package 消费此布局):安装产物从发行包自带的
# lib/ 解析第三方共享库($ORIGIN 相对定位)。该变量在 target 创建时初始化
# INSTALL_RPATH 属性,因此必须先于所有 add_subdirectory/add_executable 设置;
# 只作用于 install 出来的副本,不影响 build tree 内的二进制。
# 默认的 DT_RUNPATH 不沿依赖链传递(librocksdb 的 libsnappy 等会绕过包内
# lib/ 落回系统目录),因此统一改用可传递的 DT_RPATH。
# 发行包把 bin 安排到 server/bin 与 client/bin(比 lib 深一级),打包脚本以
# -DCMAKE_INSTALL_RPATH='$ORIGIN/../../lib' 覆盖此默认值。
set(CMAKE_INSTALL_RPATH "$ORIGIN/../lib" CACHE STRING "Install-time RPATH")
add_link_options(-Wl,--disable-new-dtags)

# 1. 查找依赖
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

# 2. 优先包含编译生成的路径 (解决路径污染的关键)
# 这样编译器会先在 build 目录找最新的 .pb.h，而不是去源码目录找旧的
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/src/rpc)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/src/raft)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/src/proto)

# 3. 包含源码头文件路径
include_directories(${PROJECT_SOURCE_DIR}/src/common/include)
include_directories(${PROJECT_SOURCE_DIR}/src/rpc)
include_directories(${PROJECT_SOURCE_DIR}/src/rpc/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raft/include)
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/src/proto)
include_directories(${PROJECT_SOURCE_DIR}/src/storage/include)
include_directories(${PROJECT_SOURCE_DIR}/src/transaction/include)
include_directories(${PROJECT_SOURCE_DIR}/src/sdk/include)
include_directories(${PROJECT_SOURCE_DIR}/src/metadata/include)
include_directories(${PROJECT_SOURCE_DIR}/src/server)
include_directories(${PROJECT_SOURCE_DIR}/test)

# 库文件路径
link_directories(${PROJECT_SOURCE_DIR}/lib)

# 4. 子目录处理
add_subdirectory(src)
add_subdirectory(test)

# 5. 核心库构建
# 确保所有子目录传回来的变量都已经包含了相应的 .pb.cc
add_library(stratakv_core STATIC
    ${src_raft}
    ${src_storage}
    ${src_proto}
    ${src_transaction}
    src/metadata/metadata_state_machine.cpp
    src/metadata/metadata_consensus.cpp
    src/metadata/metadata_client.cpp
    src/metadata/auto_balancer_planner.cpp
    src/metadata/topology_operation_coordinator.cpp
    src/metadata/auto_balancer.cpp
    src/transaction/region_cache.cpp
    src/server/region_registry.cpp
    src/server/region_request_validator.cpp
    src/server/structured_region_log.cpp
    src/server/node_bootstrap.cpp
    src/server/node_server.cpp
    src/server/store_heartbeat_reporter.cpp
)
add_dependencies(stratakv_core stratakv_proto_codegen)

target_link_libraries(stratakv_core
    stratakv_rpc
    Pulsar::pulsar
    muduo_net 
    muduo_base 
    pthread 
    dl 
    rocksdb
    ${Protobuf_LIBRARIES}
)

# 可部署的单节点进程。通过不同的 node-id、端口和集群配置启动三个实例，组成一个 Raft group。
add_executable(stratakv-node src/server/node_main.cpp)
target_include_directories(stratakv-node PRIVATE ${PROJECT_SOURCE_DIR}/src/server)
target_link_libraries(stratakv-node PRIVATE stratakv_core)

# Cluster-wide timestamp authority. It is intentionally independent from the
# storage nodes so every SDK process observes one timestamp sequence.
add_executable(stratakv-tso src/tso/tso_main.cpp src/tso/tso_service.cpp src/tso/tso_consensus.cpp)
target_include_directories(stratakv-tso PRIVATE ${PROJECT_SOURCE_DIR}/src/tso)
target_link_libraries(stratakv-tso PRIVATE stratakv_core)

# Replicated Region/Store metadata authority. It is independent from TSO so
# topology traffic and future scheduling cannot delay timestamp allocation.
add_executable(stratakv-meta src/metadata/meta_main.cpp src/metadata/metadata_service.cpp)
target_include_directories(stratakv-meta PRIVATE ${PROJECT_SOURCE_DIR}/src/metadata/include)
target_link_libraries(stratakv-meta PRIVATE stratakv_core)

# 面向部署用户的最小 KV 客户端。自动在配置的 peer 中寻找当前 Leader。
add_executable(stratakv-admin src/admin/admin_main.cpp)
target_link_libraries(stratakv-admin PRIVATE stratakv_core stratakv_rpc)

# 面向业务的常驻命令行客户端:连接一次常驻复用,命令走事务 SDK。
# 与 stratakv-admin 的边界:client 只做业务读写,admin 做底层运维。
add_executable(stratakv-client src/client/client_main.cpp)
target_link_libraries(stratakv-client PRIVATE stratakv_sdk stratakv_core stratakv_rpc)

# Public developer SDK.  It deliberately exposes transactions, not the internal
# Raft RPC protocol used between StrataKV nodes.
add_library(stratakv_sdk STATIC src/sdk/client.cpp)
target_include_directories(stratakv_sdk PUBLIC ${PROJECT_SOURCE_DIR}/src/sdk/include)
target_link_libraries(stratakv_sdk PUBLIC stratakv_core stratakv_rpc)
# ---- 发布打包(deploy/stratakv-package 消费此布局)----
# stratakv_rpc 与 pulsar 定义在子目录中,受 CMP0062 约束,
# 通过 TARGET_FILE 生成表达式安装。
install(TARGETS
    stratakv-node
    stratakv-tso
    stratakv-meta
    stratakv-admin
    stratakv-client
    RUNTIME DESTINATION bin
)
install(TARGETS stratakv_sdk stratakv_core ARCHIVE DESTINATION lib)
install(FILES
    $<TARGET_FILE:stratakv_rpc>
    $<TARGET_FILE:pulsar>
    DESTINATION lib
)
install(DIRECTORY ${PROJECT_SOURCE_DIR}/src/sdk/include/stratakv DESTINATION include)
install(FILES ${PROJECT_SOURCE_DIR}/cmake/stratakvConfig.cmake DESTINATION lib/cmake/stratakv)
