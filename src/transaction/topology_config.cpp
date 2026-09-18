#include "topology_config.h"

#include <algorithm>
#include <cctype>
#include <limits>
#include <stdexcept>

TopologyMode ParseTopologyMode(const std::string& value) {
  if (value == "static") return TopologyMode::Static;
  if (value == "dynamic") return TopologyMode::Dynamic;
  throw std::invalid_argument("topology mode must be 'static' or 'dynamic'");
}

const char* TopologyModeName(TopologyMode mode) {
  return mode == TopologyMode::Dynamic ? "dynamic" : "static";
}

std::vector<ServiceEndpoint> ParseServiceEndpoints(const std::string& value,
                                                   const std::string& optionName) {
  if (value.empty()) throw std::invalid_argument(optionName + " must not be empty");
  std::vector<ServiceEndpoint> endpoints;
  size_t begin = 0;
  while (begin <= value.size()) {
    const size_t end = value.find(',', begin);
    const std::string endpoint =
        value.substr(begin, end == std::string::npos ? std::string::npos : end - begin);
    const size_t colon = endpoint.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 == endpoint.size()) {
      throw std::invalid_argument("invalid " + optionName + " endpoint: " + endpoint);
    }
    const std::string portText = endpoint.substr(colon + 1);
    if (portText.empty() ||
        std::any_of(portText.begin(), portText.end(),
                    [](unsigned char value) { return !std::isdigit(value); })) {
      throw std::invalid_argument("invalid " + optionName + " port: " + endpoint);
    }
    size_t consumed = 0;
    const unsigned long port = std::stoul(portText, &consumed);
    if (consumed != portText.size() || port == 0 || port > 65535) {
      throw std::invalid_argument("invalid " + optionName + " port: " + endpoint);
    }
    endpoints.emplace_back(endpoint.substr(0, colon), static_cast<short>(port));
    if (end == std::string::npos) break;
    begin = end + 1;
  }
  if (endpoints.empty()) throw std::invalid_argument(optionName + " has no endpoints");
  return endpoints;
}

void TopologyConfig::Validate(bool requireRegionConfig) const {
  if (metadataTimeout.count() <= 0) {
    throw std::invalid_argument("metadata timeout must be positive");
  }
  if (requireRegionConfig && regionConfigPath.empty()) {
    throw std::invalid_argument("regions configuration is required for node bootstrap");
  }
  if (mode == TopologyMode::Static) {
    if (regionConfigPath.empty()) {
      throw std::invalid_argument("static topology mode requires a regions configuration");
    }
    if (!metadataEndpoints.empty()) {
      throw std::invalid_argument("static topology mode does not accept metadata endpoints");
    }
    return;
  }
  ParseServiceEndpoints(metadataEndpoints, "metadata");
}
