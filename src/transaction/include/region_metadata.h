#ifndef STRATAKV_TRANSACTION_REGION_METADATA_H
#define STRATAKV_TRANSACTION_REGION_METADATA_H

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>

#include "region.pb.h"

struct RegionEpoch {
  uint64_t version = 1;
  uint64_t confVersion = 1;

  bool operator==(const RegionEpoch& other) const {
    return version == other.version && confVersion == other.confVersion;
  }
  bool operator!=(const RegionEpoch& other) const { return !(*this == other); }
};

// 一个 Region 是连续的半开 Key 区间 [startKey, endKey)。空 endKey 表示正无穷。
// 每个 Region 对应一个独立 Raft group；同一物理节点可出现在多个 Region 的 peers 中。
struct RegionPeerLocation {
  int nodeId = -1;
  std::string host;
  short port = 0;
  uint64_t storeId = 0;
  uint64_t peerId = 0;
  bool isLearner = false;
};

// Metadata-layer marker: set only between the split phases. Not part of
// registry identity and never present on descriptors served by RegionPeers.
struct RegionMetadata;
struct RegionSplitPending {
  std::string splitKey;
  std::shared_ptr<const RegionMetadata> child;
};

struct RegionMetadata {
  int regionId = -1;
  std::string startKey;
  std::string endKey;
  std::vector<RegionPeerLocation> peers;
  RegionEpoch epoch;
  uint64_t metadataRevision = 1;
  uint64_t leaderPeerId = 0;
  std::shared_ptr<const RegionSplitPending> splitPending;

  bool Contains(const std::string& key) const;
  const RegionPeerLocation* FindPeer(uint64_t peerId) const;
};

// One concrete peer assigned to a physical node. peerIndex is the Raft member
// index and stays stable even when one node hosts multiple peers for a Region.
struct RegionPeerAssignment {
  RegionMetadata region;
  size_t peerIndex = 0;
};

class RegionCatalog {
 public:
  explicit RegionCatalog(std::vector<RegionMetadata> regions);

  // 文件格式见 chaos_run/regions.conf。元数据在客户端和节点启动时共同加载。
  static RegionCatalog LoadFromConfig(const std::string& configPath);
  static RegionCatalog FromProto(
      const google::protobuf::RepeatedPtrField<stratakv::region::RegionDescriptor>& descriptors);

  const RegionMetadata& FindByKey(const std::string& key) const;
  const RegionMetadata& FindById(int regionId) const;
  const std::vector<RegionMetadata>& Regions() const;
  std::vector<RegionMetadata> RegionsOnNode(int nodeId) const;
  std::vector<RegionPeerAssignment> PeersOnNode(int nodeId) const;
  std::vector<stratakv::region::RegionDescriptor> ToProto() const;
  std::string Digest() const;

 private:
  std::vector<RegionMetadata> regions_;
};

bool RegionBytewiseLess(const std::string& lhs, const std::string& rhs);
stratakv::region::RegionDescriptor ToProtoRegion(const RegionMetadata& region);
RegionMetadata FromProtoRegion(const stratakv::region::RegionDescriptor& region);

#endif  // STRATAKV_TRANSACTION_REGION_METADATA_H
