// Transaction subsystem: key-to-Region routing.
#include "shard_router.h"

#include <algorithm>
#include <functional>
#include <stdexcept>

ShardRouter::ShardRouter(std::vector<std::shared_ptr<MvccStorage>> shards) : shards_(std::move(shards)) {
  if (shards_.empty()) {
    throw std::invalid_argument("ShardRouter needs at least one shard");
  }
}

ShardRouter::ShardRouter(std::vector<RegionRoute> regions) {
  if (regions.empty()) {
    throw std::invalid_argument("ShardRouter needs at least one Region");
  }
  std::sort(regions.begin(), regions.end(), [](const RegionRoute& lhs, const RegionRoute& rhs) {
    return lhs.metadata.startKey < rhs.metadata.startKey;
  });
  for (auto& route : regions) {
    if (!route.storage) {
      throw std::invalid_argument("Region route needs storage");
    }
    regions_.push_back(route.metadata);
    shards_.push_back(std::move(route.storage));
  }
  // Reuse the catalog validation so Region ranges cannot overlap or leave gaps.
  RegionCatalog(std::vector<RegionMetadata>(regions_));
}

std::shared_ptr<MvccStorage> ShardRouter::Route(const std::string& key) const { return shards_[RouteIndex(key)]; }

size_t ShardRouter::ShardId(const std::string& key) const {
  if (!regions_.empty()) {
    return RouteIndex(key);
  }
  // Phase 4: Range-based Sharding (Region)
  // Instead of hash(key) % N, we divide the key space continuously.
  // For simplicity, we use the first character of the key to route to shards.
  if (key.empty()) return 0;
  unsigned char first_byte = static_cast<unsigned char>(key[0]);
  size_t shard_index = (first_byte * shards_.size()) / 256;
  return shard_index < shards_.size() ? shard_index : shards_.size() - 1;
}

int ShardRouter::RegionId(const std::string& key) const {
  const size_t index = RouteIndex(key);
  return regions_.empty() ? static_cast<int>(index) : regions_[index].regionId;
}

size_t ShardRouter::RouteIndex(const std::string& key) const {
  if (regions_.empty()) {
    return ShardId(key);
  }
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
  return static_cast<size_t>(std::distance(regions_.begin(), it));
}

const std::vector<std::shared_ptr<MvccStorage>>& ShardRouter::Shards() const { return shards_; }
