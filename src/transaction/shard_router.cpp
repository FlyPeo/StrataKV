// Transaction subsystem: key-to-Region routing.
#include "shard_router.h"

#include <algorithm>
#include <chrono>
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

ShardRouter::ShardRouter(std::shared_ptr<RegionCache> cache, StorageFactory factory,
                         RegionCache::RefreshFunction refresh)
    : cache_(std::move(cache)), factory_(std::move(factory)), refresh_(std::move(refresh)) {
  if (!cache_) throw std::invalid_argument("ShardRouter needs a Region cache");
  if (!factory_) throw std::invalid_argument("ShardRouter needs a storage factory");
}

RegionRouteHandle ShardRouter::ResolveHandle(const std::string& key) const {
  if (!cache_) return {};
  try {
    return cache_->LookupKey(key);
  } catch (const std::out_of_range&) {
    if (!refresh_) throw;
    return cache_->LookupOrRefresh(
        key, std::chrono::steady_clock::now() + std::chrono::milliseconds(2000), refresh_);
  }
}

std::shared_ptr<MvccStorage> ShardRouter::Route(const std::string& key) const {
  if (cache_) {
    return StorageFor(ResolveHandle(key));
  }
  if (shards_.empty()) throw std::out_of_range("no shard available for routing");
  return shards_[RouteIndex(key)];
}

std::vector<ShardRouter::RegionRoute> ShardRouter::RegionsInRange(const std::string& startKey,
                                                                  const std::string& endKey) const {
  std::vector<RegionRoute> result;
  if (cache_) {
    // 空起点从全空间第一个 Region 开始;否则先解析起点所在 Region,再沿
    // 路由表顺序前进,直到 Region 起点越过 endKey。
    RegionRouteHandle handle;
    if (startKey.empty()) {
      auto table = cache_->Snapshot();
      if (table->regions.empty()) return result;
      handle.table = table;
      handle.index = 0;
    } else {
      handle = ResolveHandle(startKey);
    }
    while (handle) {
      const RegionMetadata& region = handle.Descriptor();
      if (!endKey.empty() && region.startKey >= endKey) break;
      auto storage = StorageFor(handle);
      if (!storage) throw std::out_of_range("no storage available for Region " +
                                            std::to_string(region.regionId));
      result.push_back({region, std::move(storage)});
      ++handle.index;
      if (handle.index >= handle.table->regions.size()) break;
    }
    return result;
  }
  for (size_t i = 0; i < regions_.size(); ++i) {
    const RegionMetadata& region = regions_[i];
    if (!endKey.empty() && region.startKey >= endKey) continue;
    if (!region.endKey.empty() && !startKey.empty() && region.endKey <= startKey) continue;
    if (i >= shards_.size()) continue;
    result.push_back({region, shards_[i]});
  }
  std::sort(result.begin(), result.end(),
            [](const RegionRoute& a, const RegionRoute& b) { return a.metadata.startKey < b.metadata.startKey; });
  return result;
}

std::shared_ptr<MvccStorage> ShardRouter::StorageFor(const RegionRouteHandle& handle) const {
  const int regionId = handle.Descriptor().regionId;
  {
    std::shared_lock<std::shared_mutex> lock(storageMutex_);
    const auto found = storages_.find(regionId);
    if (found != storages_.end()) return found->second;
  }
  auto storage = factory_(handle.Descriptor());
  if (!storage) {
    throw std::out_of_range("no storage available for Region " + std::to_string(regionId));
  }
  std::unique_lock<std::shared_mutex> lock(storageMutex_);
  auto& slot = storages_[regionId];
  if (!slot) slot = storage;
  return slot;
}

size_t ShardRouter::ShardId(const std::string& key) const {
  if (cache_) return static_cast<size_t>(RegionId(key));
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
  if (cache_) return ResolveHandle(key).Descriptor().regionId;
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

std::vector<std::shared_ptr<MvccStorage>> ShardRouter::Shards() const {
  if (cache_) {
    std::shared_lock<std::shared_mutex> lock(storageMutex_);
    std::vector<std::shared_ptr<MvccStorage>> result;
    result.reserve(storages_.size());
    for (const auto& entry : storages_) result.push_back(entry.second);
    return result;
  }
  return shards_;
}
