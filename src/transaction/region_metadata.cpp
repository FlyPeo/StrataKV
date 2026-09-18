// Transaction subsystem: Region topology metadata.
#include "region_metadata.h"

#include <algorithm>
#include <cstdlib>
#include <iomanip>
#include <limits>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

#include "mprpc_config.h"

bool RegionBytewiseLess(const std::string& lhs, const std::string& rhs) {
  return std::lexicographical_compare(
      lhs.begin(), lhs.end(), rhs.begin(), rhs.end(),
      [](char left, char right) {
        return static_cast<unsigned char>(left) < static_cast<unsigned char>(right);
      });
}

bool RegionMetadata::Contains(const std::string& key) const {
  return !RegionBytewiseLess(key, startKey) &&
         (endKey.empty() || RegionBytewiseLess(key, endKey));
}

const RegionPeerLocation* RegionMetadata::FindPeer(uint64_t wantedPeerId) const {
  const auto found = std::find_if(peers.begin(), peers.end(),
                                  [wantedPeerId](const RegionPeerLocation& peer) {
                                    return peer.peerId == wantedPeerId;
                                  });
  return found == peers.end() ? nullptr : &*found;
}

RegionCatalog::RegionCatalog(std::vector<RegionMetadata> regions) : regions_(std::move(regions)) {
  if (regions_.empty()) {
    throw std::invalid_argument("RegionCatalog needs at least one region");
  }
  std::sort(regions_.begin(), regions_.end(),
            [](const RegionMetadata& lhs, const RegionMetadata& rhs) {
              return RegionBytewiseLess(lhs.startKey, rhs.startKey);
            });

  std::unordered_set<int> regionIds;
  std::unordered_set<uint64_t> peerIds;
  std::unordered_map<uint64_t, std::pair<std::string, short>> stores;
  for (size_t i = 0; i < regions_.size(); ++i) {
    auto& region = regions_[i];
    if (region.regionId < 0 || region.peers.empty() ||
        (!region.endKey.empty() && !RegionBytewiseLess(region.startKey, region.endKey)) ||
        region.epoch.version == 0 || region.epoch.confVersion == 0 ||
        region.metadataRevision == 0) {
      throw std::invalid_argument("invalid Region metadata");
    }
    if (!regionIds.insert(region.regionId).second) {
      throw std::invalid_argument("duplicate Region ID");
    }
    if (i == 0 && !region.startKey.empty()) {
      throw std::invalid_argument("first Region must start at the configured lower bound");
    }
    if (i > 0 && regions_[i - 1].endKey != region.startKey) {
      throw std::invalid_argument("Region ranges must be contiguous");
    }
    if (i + 1 < regions_.size() && region.endKey.empty()) {
      throw std::invalid_argument("only the final Region may have an empty endKey");
    }
    if (i + 1 == regions_.size() && !region.endKey.empty()) {
      throw std::invalid_argument("final Region must end at the configured upper bound");
    }

    for (size_t peerIndex = 0; peerIndex < region.peers.size(); ++peerIndex) {
      auto& peer = region.peers[peerIndex];
      if (peer.nodeId < 0 || peer.host.empty() || peer.port <= 0) {
        throw std::invalid_argument("invalid Region peer");
      }
      if (peer.storeId == 0) peer.storeId = static_cast<uint64_t>(peer.nodeId) + 1;
      if (peer.peerId == 0) {
        peer.peerId = (static_cast<uint64_t>(region.regionId) + 1) * 1000000ULL +
                      static_cast<uint64_t>(peerIndex) + 1;
      }
      if (!peerIds.insert(peer.peerId).second) {
        throw std::invalid_argument("duplicate peer ID");
      }
      const auto inserted = stores.emplace(peer.storeId, std::make_pair(peer.host, peer.port));
      if (!inserted.second && inserted.first->second != std::make_pair(peer.host, peer.port)) {
        throw std::invalid_argument("Store ID has conflicting endpoint");
      }
    }
    if (region.leaderPeerId == 0) region.leaderPeerId = region.peers.front().peerId;
    if (region.FindPeer(region.leaderPeerId) == nullptr) {
      throw std::invalid_argument("Region leader peer is not in the peer set");
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
    const std::string version = config.Load(prefix + "epoch.version");
    const std::string confVersion = config.Load(prefix + "epoch.conf_version");
    const std::string metadataRevision = config.Load(prefix + "metadata_revision");
    const std::string leaderPeerId = config.Load(prefix + "leader_peer_id");
    if (!version.empty()) region.epoch.version = std::stoull(version);
    if (!confVersion.empty()) region.epoch.confVersion = std::stoull(confVersion);
    if (!metadataRevision.empty()) region.metadataRevision = std::stoull(metadataRevision);
    if (!leaderPeerId.empty()) region.leaderPeerId = std::stoull(leaderPeerId);

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
      RegionPeerLocation peer{std::stoi(nodeId), host, static_cast<short>(std::stoi(port))};
      const std::string storeId = config.Load(peerPrefix + "store_id");
      const std::string peerId = config.Load(peerPrefix + "peer_id");
      if (!storeId.empty()) peer.storeId = std::stoull(storeId);
      if (!peerId.empty()) peer.peerId = std::stoull(peerId);
      region.peers.push_back(std::move(peer));
    }
    regions.push_back(std::move(region));
  }
  return RegionCatalog(std::move(regions));
}

RegionCatalog RegionCatalog::FromProto(
    const google::protobuf::RepeatedPtrField<stratakv::region::RegionDescriptor>& descriptors) {
  std::vector<RegionMetadata> regions;
  regions.reserve(static_cast<size_t>(descriptors.size()));
  for (const auto& descriptor : descriptors) regions.push_back(FromProtoRegion(descriptor));
  return RegionCatalog(std::move(regions));
}

const RegionMetadata& RegionCatalog::FindByKey(const std::string& key) const {
  auto it = std::upper_bound(regions_.begin(), regions_.end(), key,
                             [](const std::string& value, const RegionMetadata& region) {
                               return RegionBytewiseLess(value, region.startKey);
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
                    [nodeId](const RegionPeerLocation& peer) { return peer.nodeId == nodeId; })) {
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

std::vector<stratakv::region::RegionDescriptor> RegionCatalog::ToProto() const {
  std::vector<stratakv::region::RegionDescriptor> result;
  result.reserve(regions_.size());
  for (const auto& region : regions_) result.push_back(ToProtoRegion(region));
  return result;
}

std::string RegionCatalog::Digest() const {
  // Stable FNV-1a over deterministic protobuf serialization. The digest is
  // used to reject a second bootstrap with a different static catalog.
  uint64_t hash = 1469598103934665603ULL;
  for (const auto& region : regions_) {
    std::string bytes;
    ToProtoRegion(region).SerializeToString(&bytes);
    for (unsigned char byte : bytes) {
      hash ^= byte;
      hash *= 1099511628211ULL;
    }
  }
  std::ostringstream output;
  output << std::hex << std::setw(16) << std::setfill('0') << hash;
  return output.str();
}

stratakv::region::RegionDescriptor ToProtoRegion(const RegionMetadata& region) {
  stratakv::region::RegionDescriptor descriptor;
  descriptor.set_regionid(static_cast<uint64_t>(region.regionId));
  descriptor.set_startkey(region.startKey);
  descriptor.set_endkey(region.endKey);
  descriptor.mutable_epoch()->set_version(region.epoch.version);
  descriptor.mutable_epoch()->set_confversion(region.epoch.confVersion);
  descriptor.set_metadatarevision(region.metadataRevision);
  descriptor.set_leaderpeerid(region.leaderPeerId);
  if (region.splitPending) {
    auto* pending = descriptor.mutable_splitpending();
    pending->set_splitkey(region.splitPending->splitKey);
    *pending->mutable_child() = ToProtoRegion(*region.splitPending->child);
  }
  for (const auto& peer : region.peers) {
    auto* encoded = descriptor.add_peers();
    encoded->set_peerid(peer.peerId);
    encoded->set_storeid(peer.storeId);
    encoded->set_host(peer.host);
    encoded->set_port(static_cast<uint32_t>(peer.port));
    encoded->set_islearner(peer.isLearner);
  }
  return descriptor;
}

RegionMetadata FromProtoRegion(const stratakv::region::RegionDescriptor& descriptor) {
  if (descriptor.regionid() > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
    throw std::invalid_argument("Region ID exceeds the local implementation limit");
  }
  RegionMetadata region;
  region.regionId = static_cast<int>(descriptor.regionid());
  region.startKey = descriptor.startkey();
  region.endKey = descriptor.endkey();
  region.epoch.version = descriptor.epoch().version();
  region.epoch.confVersion = descriptor.epoch().confversion();
  region.metadataRevision = descriptor.metadatarevision();
  region.leaderPeerId = descriptor.leaderpeerid();
  if (descriptor.has_splitpending()) {
    auto pending = std::make_shared<RegionSplitPending>();
    pending->splitKey = descriptor.splitpending().splitkey();
    pending->child = std::make_shared<const RegionMetadata>(
        FromProtoRegion(descriptor.splitpending().child()));
    region.splitPending = std::move(pending);
  }
  region.peers.reserve(static_cast<size_t>(descriptor.peers_size()));
  for (const auto& encoded : descriptor.peers()) {
    if (encoded.storeid() == 0 || encoded.peerid() == 0 || encoded.port() == 0 ||
        encoded.port() > static_cast<uint32_t>(std::numeric_limits<short>::max())) {
      throw std::invalid_argument("invalid protobuf Region peer");
    }
    // Compatibility store IDs are nodeId + 1. Dynamic descriptors retain a
    // distinct StoreId while nodeId is the local process index where possible.
    const int nodeId = encoded.storeid() <= static_cast<uint64_t>(std::numeric_limits<int>::max())
                           ? static_cast<int>(encoded.storeid() - 1)
                           : -1;
    region.peers.push_back({nodeId, encoded.host(), static_cast<short>(encoded.port()),
                            encoded.storeid(), encoded.peerid(), encoded.islearner()});
  }
  return region;
}
