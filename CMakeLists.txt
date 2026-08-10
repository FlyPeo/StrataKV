cmake_minimum_required(VERSION 3.22)
project(StrataKV)

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

# Keep every user-facing executable in one predictable directory for all
# generators and build configurations.
set(STRATAKV_BIN_DIR "${PROJECT_SOURCE_DIR}/bin")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${STRATAKV_BIN_DIR}")
foreach(config DEBUG RELEASE RELWITHDEBINFO MINSIZEREL)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${config} "${STRATAKV_BIN_DIR}")
endforeach()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/lib")

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
target_link_libraries(stratakv-node PRIVATE stratakv_core)

# 面向部署用户的最小 KV 客户端。自动在配置的 peer 中寻找当前 Leader。
add_executable(stratakv-admin src/admin/admin_main.cpp)
target_link_libraries(stratakv-admin PRIVATE stratakv_core stratakv_rpc)

# Public developer SDK.  It deliberately exposes transactions, not the internal
# Raft RPC protocol used between StrataKV nodes.
add_library(stratakv_sdk STATIC src/sdk/client.cpp)
target_include_directories(stratakv_sdk PUBLIC ${PROJECT_SOURCE_DIR}/src/sdk/include)
target_link_libraries(stratakv_sdk PUBLIC stratakv_core stratakv_rpc)
install(TARGETS stratakv_sdk ARCHIVE DESTINATION lib)
install(DIRECTORY ${PROJECT_SOURCE_DIR}/src/sdk/include/stratakv DESTINATION include)

# Stable HTTP/JSON entrypoint for non-C++ users and operational integrations.
add_executable(stratakv-gateway src/gateway/gateway_main.cpp)
target_link_libraries(stratakv-gateway PRIVATE stratakv_sdk pthread)

# End-user command-line client. It calls the public Gateway rather than the
# internal Raft RPC protocol, matching how non-operator applications connect.
add_executable(stratakv-client src/cli/client_main.cpp)
target_link_libraries(stratakv-client PRIVATE pthread)

install(TARGETS
    stratakv-node
    stratakv-admin
    stratakv-gateway
    stratakv-client
    RUNTIME DESTINATION bin
)
