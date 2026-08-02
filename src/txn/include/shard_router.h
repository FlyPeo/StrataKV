#ifndef STRATAKV_SHARD_ROUTER_H
#define STRATAKV_SHARD_ROUTER_H

#include <memory>
#include <string>
#include <vector>

#include "mvcc_storage.h"
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

  // 根据 key 选择对应的 shard，用于读写请求的实际落点定位。
  std::shared_ptr<MvccStorage> Route(const std::string& key) const;
  // 返回 key 对应的分片编号，用于调试、统计或分布式协调。
  size_t ShardId(const std::string& key) const;
  int RegionId(const std::string& key) const;
  // 暴露当前路由器持有的全部分片，供协调器扫描或恢复使用。
  const std::vector<std::shared_ptr<MvccStorage>>& Shards() const;

 private:
  size_t RouteIndex(const std::string& key) const;
  // 路由器管理的全部 MVCC 分片。
  std::vector<std::shared_ptr<MvccStorage>> shards_;
  std::vector<RegionMetadata> regions_;
};

#endif  // STRATAKV_SHARD_ROUTER_H
