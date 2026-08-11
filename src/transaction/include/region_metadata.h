#ifndef STRATAKV_TRANSACTION_REGION_METADATA_H
#define STRATAKV_TRANSACTION_REGION_METADATA_H

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>

// 一个 Region 是连续的半开 Key 区间 [startKey, endKey)。空 endKey 表示正无穷。
// 每个 Region 对应一个独立 Raft group；同一物理节点可出现在多个 Region 的 peers 中。
struct RegionPeerLocation {
  int nodeId = -1;
  std::string host;
  short port = 0;
};

struct RegionMetadata {
  int regionId = -1;
  std::string startKey;
  std::string endKey;
  std::vector<RegionPeerLocation> peers;

  bool Contains(const std::string& key) const;
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

  const RegionMetadata& FindByKey(const std::string& key) const;
  const RegionMetadata& FindById(int regionId) const;
  const std::vector<RegionMetadata>& Regions() const;
  std::vector<RegionMetadata> RegionsOnNode(int nodeId) const;
  std::vector<RegionPeerAssignment> PeersOnNode(int nodeId) const;

 private:
  std::vector<RegionMetadata> regions_;
};

#endif  // STRATAKV_TRANSACTION_REGION_METADATA_H
