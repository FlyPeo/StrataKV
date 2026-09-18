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
