/*
 * 测试目标：验证节点启动拓扑配置（static/dynamic 模式）的校验规则与端点解析。
 * 测试策略：直接构造 TopologyConfig 并调用 Validate，给出合法 static（regions.conf）与合法
 *           dynamic（3 个 metadata endpoint）配置，再逐一构造 4 种非法配置，另验证
 *           ParseTopologyMode/ParseServiceEndpoints 的行为。
 * 测试规模：2 个合法配置 + 4 个非法配置 + 2 个解析函数检查。
 * 验证内容：static 模式必须提供 regions 配置，dynamic 模式必须提供可解析的 metadata 端点（非法
 *           端口被拒绝），模式名 "dynamic" 稳定且不存在隐式回退（"auto" 被拒绝），3 个端点解析出 3 项。
 */
#include <chrono>
#include <iostream>
#include <stdexcept>

#include "topology_config.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

template <typename Function>
void RequireInvalid(Function&& function, const char* message) {
  try {
    function();
  } catch (const std::invalid_argument&) {
    return;
  }
  throw std::runtime_error(message);
}

}  // namespace

int main() {
  try {
    TopologyConfig legacy;
    legacy.regionConfigPath = "regions.conf";
    legacy.Validate(false);

    TopologyConfig dynamic;
    dynamic.mode = ParseTopologyMode("dynamic");
    dynamic.metadataEndpoints = "127.0.0.1:26500,127.0.0.1:26501,127.0.0.1:26502";
    dynamic.metadataTimeout = std::chrono::milliseconds(500);
    dynamic.Validate(false);
    Require(ParseServiceEndpoints(dynamic.metadataEndpoints, "metadata").size() == 3,
            "three metadata endpoints must parse");
    Require(std::string(TopologyModeName(dynamic.mode)) == "dynamic",
            "dynamic mode name must be stable");

    RequireInvalid(
        [] {
          TopologyConfig value;
          value.Validate(false);
        },
        "static mode without regions config must fail");
    RequireInvalid(
        [] {
          TopologyConfig value;
          value.mode = TopologyMode::Dynamic;
          value.Validate(false);
        },
        "dynamic mode without metadata endpoints must fail");
    RequireInvalid(
        [] {
          TopologyConfig value;
          value.mode = TopologyMode::Dynamic;
          value.metadataEndpoints = "127.0.0.1:not-a-port";
          value.Validate(false);
        },
        "invalid metadata port must fail");
    RequireInvalid([] { ParseTopologyMode("auto"); }, "implicit fallback mode must fail");
    std::cout << "Topology configuration checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Topology configuration checks failed: " << error.what() << std::endl;
    return 1;
  }
}
