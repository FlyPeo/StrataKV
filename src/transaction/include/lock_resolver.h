#ifndef STRATAKV_TRANSACTION_LOCK_RESOLVER_H
#define STRATAKV_TRANSACTION_LOCK_RESOLVER_H

#include <memory>
#include <optional>
#include <string>

#include "shard_router.h"

// PrimaryTxnState 表示 primary 事务当前在全局恢复流程中的状态。
enum class PrimaryTxnState {
  // primary 已经完成提交，secondary 应该跟进提交。
  Committed,
  // primary 已经被判定回滚，secondary 应该同步回滚。
  RolledBack,
  // primary 仍然持有锁，说明事务可能还在进行中。
  Locked,
  // primary 状态缺失，通常意味着需要结合 TTL 或历史记录再做裁决。
  Missing,
};

// PrimaryTxnStatus 汇总 primary 事务的裁决结果，供 secondary 恢复时使用。
struct PrimaryTxnStatus {
  // primary 当前状态。
  PrimaryTxnState state = PrimaryTxnState::Missing;
  // primary 对应的提交时间戳，只有已提交时才有效。
  uint64_t commitTs = 0;
  // 如果 primary 仍在持锁，会把锁内容带回来供后续判断。
  std::optional<MvccLock> lock;
};

// LockResolver 负责跨 shard 查询 primary 状态，
// 并据此决定一个 secondary 锁应该提交、回滚还是继续等待。
class LockResolver {
 public:
  // 构造锁解析器，并接管分片路由器，用来访问 primary 和 secondary 所在 shard。
  explicit LockResolver(std::shared_ptr<ShardRouter> router);

  // 查询某个 primary 事务的全局状态，给恢复逻辑提供裁决依据。
  PrimaryTxnStatus CheckPrimary(const std::string& primaryKey, uint64_t startTs);
  // 根据 primary 裁决结果处理某个具体 key 上的锁。
  TxnStatus ResolveLock(const std::string& key, const MvccLock& lock);
  // 扫描所有分片上的过期锁，并批量触发恢复处理。
  size_t ResolveExpiredLocks(uint64_t nowMs);

 private:
  // 用于定位 primary/secondary 分片的路由器。
  std::shared_ptr<ShardRouter> router_;
};

#endif  // STRATAKV_TRANSACTION_LOCK_RESOLVER_H
