
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS raft_rpc.proto kv_server_rpc.proto tso_rpc.proto)
add_custom_target(stratakv_proto_codegen DEPENDS ${PROTO_SRCS} ${PROTO_HDRS})

set(src_proto ${PROTO_SRCS} CACHE INTERNAL "Generated Protobuf source files")
