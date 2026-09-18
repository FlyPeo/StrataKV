#ifndef STRATAKV_TRANSACTION_TOPOLOGY_CONFIG_H
#define STRATAKV_TRANSACTION_TOPOLOGY_CONFIG_H

#include <chrono>
#include <string>
#include <utility>
#include <vector>

enum class TopologyMode {
  Static,
  Dynamic,
};

using ServiceEndpoint = std::pair<std::string, short>;

TopologyMode ParseTopologyMode(const std::string& value);
const char* TopologyModeName(TopologyMode mode);
std::vector<ServiceEndpoint> ParseServiceEndpoints(const std::string& value,
                                                   const std::string& optionName);

struct TopologyConfig {
  TopologyMode mode = TopologyMode::Static;
  std::string regionConfigPath;
  std::string metadataEndpoints;
  std::chrono::milliseconds metadataTimeout{3000};

  // Storage nodes still require regions.conf in dynamic mode during this
  // migration phase to identify local bootstrap assignments.
  void Validate(bool requireRegionConfig) const;
};

#endif  // STRATAKV_TRANSACTION_TOPOLOGY_CONFIG_H
