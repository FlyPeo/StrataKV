cmake_minimum_required(VERSION 3.22)
project(stratakv_sdk_example CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 只消费发行包导出的 stratakv::sdk 目标;StrataKV 静态库与第三方链接依赖
# 全部由包配置传递,不在此处枚举。
find_package(stratakv REQUIRED)

add_executable(stratakv_sdk_example sdk_example.cpp)
target_link_libraries(stratakv_sdk_example PRIVATE stratakv::sdk)
