
protobuf_generate_cpp(RPC_PROTO_SRCS RPC_PROTO_HDRS rpc_header.proto)

add_library(stratakv_rpc
    mprpc_channel.cpp
    mprpc_config.cpp
    mprpc_controller.cpp
    rpc_provider.cpp
    ${RPC_PROTO_SRCS}
    ${src_common}
)
target_include_directories(stratakv_rpc PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(stratakv_rpc PUBLIC boost_serialization)
