// Transaction subsystem: Region topology metadata.
#include "region_metadata.h"

#include <algorithm>
#include <cstdlib>
#include <stdexcept>

#include "mprpc_config.h"

bool RegionMetadata::Contains(const std::string& key) const {
  return key >= startKey && (endKey.empty() || key < endKey);
}

RegionCatalog::RegionCatalog(std::vector<RegionMetadata> regions) : regions_(std::move(regions)) {
  if (regions_.empty()) {
    throw std::invalid_argument("RegionCatalog needs at least one region");
  }
  std::sort(regions_.begin(), regions_.end(),
            [](const RegionMetadata& lhs, const RegionMetadata& rhs) { return lhs.startKey < rhs.startKey; });

  for (size_t i = 0; i < regions_.size(); ++i) {
    const auto& region = regions_[i];
    if (region.regionId < 0 || region.peers.empty() || (!region.endKey.empty() && region.endKey <= region.startKey)) {
      throw std::invalid_argument("invalid Region metadata");
    }
    if (i > 0 && regions_[i - 1].endKey != region.startKey) {
      throw std::invalid_argument("Region ranges must be contiguous");
    }
    if (i + 1 < regions_.size() && region.endKey.empty()) {
      throw std::invalid_argument("only the final Region may have an empty endKey");
    }
  }
}

RegionCatalog RegionCatalog::LoadFromConfig(const std::string& configPath) {
  MprpcConfig config;
  config.LoadConfigFile(configPath.c_str());
  const std::string countText = config.Load("region.count");
  if (countText.empty()) {
    throw std::invalid_argument("missing region.count in " + configPath);
  }

  const int count = std::stoi(countText);
  if (count <= 0) {
    throw std::invalid_argument("region.count must be positive");
  }

  std::vector<RegionMetadata> regions;
  regions.reserve(count);
  for (int index = 0; index < count; ++index) {
    const std::string prefix = "region." + std::to_string(index) + ".";
    RegionMetadata region;
    const std::string id = config.Load(prefix + "id");
    const std::string peerCount = config.Load(prefix + "peer.count");
    if (id.empty() || peerCount.empty()) {
      throw std::invalid_argument("incomplete metadata for " + prefix);
    }
    region.regionId = std::stoi(id);
    region.startKey = config.Load(prefix + "start_key");
    region.endKey = config.Load(prefix + "end_key");

    const int peers = std::stoi(peerCount);
    if (peers <= 0) {
      throw std::invalid_argument("peer.count must be positive for " + prefix);
    }
    region.peers.reserve(peers);
    for (int peerIndex = 0; peerIndex < peers; ++peerIndex) {
      const std::string peerPrefix = prefix + "peer." + std::to_string(peerIndex) + ".";
      const std::string nodeId = config.Load(peerPrefix + "node_id");
      const std::string host = config.Load(peerPrefix + "host");
      const std::string port = config.Load(peerPrefix + "port");
      if (nodeId.empty() || host.empty() || port.empty()) {
        throw std::invalid_argument("incomplete peer metadata for " + peerPrefix);
      }
      region.peers.push_back({std::stoi(nodeId), host, static_cast<short>(std::stoi(port))});
    }
    regions.push_back(std::move(region));
  }
  return RegionCatalog(std::move(regions));
}

const RegionMetadata& RegionCatalog::FindByKey(const std::string& key) const {
  auto it = std::upper_bound(regions_.begin(), regions_.end(), key,
                             [](const std::string& value, const RegionMetadata& region) {
                               return value < region.startKey;
                             });
  if (it == regions_.begin()) {
    throw std::out_of_range("key is before the first Region");
  }
  --it;
  if (!it->Contains(key)) {
    throw std::out_of_range("key is not covered by Region metadata");
  }
  return *it;
}

const RegionMetadata& RegionCatalog::FindById(int regionId) const {
  auto it = std::find_if(regions_.begin(), regions_.end(),
                         [regionId](const RegionMetadata& region) { return region.regionId == regionId; });
  if (it == regions_.end()) {
    throw std::out_of_range("Region does not exist: " + std::to_string(regionId));
  }
  return *it;
}

const std::vector<RegionMetadata>& RegionCatalog::Regions() const { return regions_; }

std::vector<RegionMetadata> RegionCatalog::RegionsOnNode(int nodeId) const {
  std::vector<RegionMetadata> result;
  for (const auto& region : regions_) {
    if (std::any_of(region.peers.begin(), region.peers.end(),
                    [nodeId](const RegionPeer& peer) { return peer.nodeId == nodeId; })) {
      result.push_back(region);
    }
  }
  return result;
}

std::vector<RegionPeerAssignment> RegionCatalog::PeersOnNode(int nodeId) const {
  std::vector<RegionPeerAssignment> result;
  for (const auto& region : regions_) {
    for (size_t peerIndex = 0; peerIndex < region.peers.size(); ++peerIndex) {
      if (region.peers[peerIndex].nodeId == nodeId) {
        result.push_back({region, peerIndex});
      }
    }
  }
  return result;
}
