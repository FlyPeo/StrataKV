#ifndef STRATAKV_TRANSACTION_SHARD_ROUTER_H
#define STRATAKV_TRANSACTION_SHARD_ROUTER_H

#include <memory>
#include <string>
#include <vector>

#include <functional>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "mvcc_storage.h"
#include "region_cache.h"
#include "region_metadata.h"

// ShardRouter 负责把 key 映射到具体的 MVCC 分片，
// 是分布式事务协调器选择读写目标 shard 的统一入口。
class ShardRouter {
 public:
  struct RegionRoute {
    RegionMetadata metadata;
    std::shared_ptr<MvccStorage> storage;
  };

  // 构造路由器，并接管全局可用的分片列表。
  // 保留该兼容构造器，供单机 demo 使用。
  explicit ShardRouter(std::vector<std::shared_ptr<MvccStorage>> shards);
  // Region 构造器：路由按连续 key range 查表，而不是按固定 shard 数取模。
  explicit ShardRouter(std::vector<RegionRoute> regions);
  // Region cache-backed 构造器：路由决策来自实时 cache，split/merge 之后
  // 新出现的 Region 由 factory 惰性建存储，接口对上层保持不变。
  using StorageFactory = std::function<std::shared_ptr<MvccStorage>(const RegionMetadata&)>;
  // refresh enables on-miss topology refresh for router-level lookups: the
  // coordinator routes before any sender exists, so an invalidated Region
  // must trigger exactly one metadata refresh instead of failing forever.
  ShardRouter(std::shared_ptr<RegionCache> cache, StorageFactory factory,
              RegionCache::RefreshFunction refresh = {});

  // 根据 key 选择对应的 shard，用于读写请求的实际落点定位。
  std::shared_ptr<MvccStorage> Route(const std::string& key) const;
  // 返回 key 对应的分片编号，用于调试、统计或分布式协调。
  size_t ShardId(const std::string& key) const;
  int RegionId(const std::string& key) const;
  // 暴露当前路由器持有的全部分片，供协调器扫描或恢复使用。
  std::vector<std::shared_ptr<MvccStorage>> Shards() const;
  // 动态模式下暴露底层 cache，供协调器在 regroup 前刷新拓扑。
  std::shared_ptr<RegionCache> Cache() const { return cache_; }

 private:
  size_t RouteIndex(const std::string& key) const;
  // Looks up the Region handle in cache, refreshing once on cache miss or invalidation.
  RegionRouteHandle ResolveHandle(const std::string& key) const;
  // Returns the live storage for one cache descriptor, creating it through the
  // factory on first touch. Lookups share the lock; only a first-create takes
  // it exclusively, so the per-request routing path stays read-parallel.
  std::shared_ptr<MvccStorage> StorageFor(const RegionRouteHandle& handle) const;
  // 路由器管理的全部 MVCC 分片。动态模式下 split/merge 会惰性增删条目，
  // 因此这两个向量在 const 路由路径上也是可变的。
  mutable std::vector<std::shared_ptr<MvccStorage>> shards_;
  mutable std::vector<RegionMetadata> regions_;
  std::shared_ptr<RegionCache> cache_;
  StorageFactory factory_;
  RegionCache::RefreshFunction refresh_;
  // Dynamic-mode storage table keyed by RegionId. Shared for lookups,
  // exclusive for factory creation; no global lock serializes data reads.
  mutable std::shared_mutex storageMutex_;
  mutable std::unordered_map<int, std::shared_ptr<MvccStorage>> storages_;
};

#endif  // STRATAKV_TRANSACTION_SHARD_ROUTER_H
