#include "lock_resolver.h"

#include <chrono>

namespace {
uint64_t NowMs() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

bool IsExpired(const MvccLock& lock, uint64_t nowMs) {
  return lock.ttlMs > 0 && lock.createTimeMs + lock.ttlMs <= nowMs;
}
}  // namespace

LockResolver::LockResolver(std::shared_ptr<ShardRouter> router) : router_(std::move(router)) {}

PrimaryTxnStatus LockResolver::CheckPrimary(const std::string& primaryKey, uint64_t startTs) {
  auto primaryShard = router_->Route(primaryKey);
  const auto commitTs = primaryShard->FindCommitTs(primaryKey, startTs);
  if (commitTs.has_value()) {
    return PrimaryTxnStatus{PrimaryTxnState::Committed, commitTs.value(), std::nullopt};
  }

  const auto lock = primaryShard->GetLock(primaryKey);
  if (lock.has_value() && lock->startTs == startTs) {
    return PrimaryTxnStatus{PrimaryTxnState::Locked, 0, lock};
  }

  return PrimaryTxnStatus{PrimaryTxnState::Missing, 0, std::nullopt};
}

TxnStatus LockResolver::ResolveLock(const std::string& key, const MvccLock& lock) {
  auto shard = router_->Route(key);
  const PrimaryTxnStatus primary = CheckPrimary(lock.primaryKey, lock.startTs);
  if (primary.state == PrimaryTxnState::Committed) {
    return shard->Commit(key, lock.startTs, primary.commitTs);
  }
  const bool secondaryExpired = IsExpired(lock, NowMs());
  if (primary.state == PrimaryTxnState::Locked && primary.lock.has_value() && !IsExpired(primary.lock.value(), NowMs())) {
    return TxnStatus::LockConflict;
  }
  if (primary.state == PrimaryTxnState::Missing && !secondaryExpired) {
    return TxnStatus::LockConflict;
  }
  return shard->Rollback(key, lock.startTs);
}

size_t LockResolver::ResolveExpiredLocks(uint64_t nowMs) {
  size_t resolved = 0;
  for (const auto& shard : router_->Shards()) {
    for (const auto& item : shard->ExpiredLocks(nowMs)) {
      if (ResolveLock(item.first, item.second) == TxnStatus::Ok) {
        ++resolved;
      }
    }
  }
  return resolved;
}
