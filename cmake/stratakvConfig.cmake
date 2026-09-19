# stratakvConfig.cmake — StrataKV 发布包的 CMake SDK 消费入口。
#
# 用法(在发行包之外):
#   cmake -S . -B build -DCMAKE_PREFIX_PATH=<发行包根目录>
#   find_package(stratakv REQUIRED)
#   target_link_libraries(my_app PRIVATE stratakv::sdk)
#
# 链接行镜像 bin/stratakv-node 的既有闭包(core → rpc → boost_serialization
# → pulsar → boost_context → muduo → pthread/dl → rocksdb → protobuf),
# 静态库与第三方库都从包内 lib/ 解析,运行时依赖由包内 lib/ 的 rpath 满足。

get_filename_component(STRATAKV_PACKAGE_ROOT "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)

if(NOT TARGET stratakv::sdk)
  add_library(stratakv::sdk INTERFACE IMPORTED)
  set_target_properties(stratakv::sdk PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${STRATAKV_PACKAGE_ROOT}/include"
    INTERFACE_LINK_DIRECTORIES "${STRATAKV_PACKAGE_ROOT}/lib"
    INTERFACE_LINK_OPTIONS "-Wl,--disable-new-dtags;-Wl,-rpath,${STRATAKV_PACKAGE_ROOT}/lib"
    INTERFACE_LINK_LIBRARIES
      "${STRATAKV_PACKAGE_ROOT}/lib/libstratakv_sdk.a;\
${STRATAKV_PACKAGE_ROOT}/lib/libstratakv_core.a;\
${STRATAKV_PACKAGE_ROOT}/lib/libstratakv_rpc.a;\
-lboost_serialization;\
${STRATAKV_PACKAGE_ROOT}/lib/libpulsar.a;\
-lboost_context;\
-lmuduo_net;\
-lmuduo_base;\
-lpthread;\
-ldl;\
-lrocksdb;\
-lprotobuf"
  )
endif()
