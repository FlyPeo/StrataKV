// Transaction subsystem: expired and orphaned lock resolution.
#include "lock_resolver.h"

#include "timestamp_oracle.h"

namespace {
bool IsExpired(const MvccLock& lock, uint64_t nowMs) {
  if (!lock.legacyExpiry && lock.expireAtPhysicalMs != 0) {
    return lock.expireAtPhysicalMs <= nowMs;
  }
  return lock.createTimeMs != 0 && lock.ttlMs != 0 &&
         lock.createTimeMs + lock.ttlMs <= nowMs;
}
}  // namespace

LockResolver::LockResolver(std::shared_ptr<ShardRouter> router) : router_(std::move(router)) {}

PrimaryTxnStatus LockResolver::CheckPrimary(const std::string& primaryKey, uint64_t startTs,
                                            uint64_t currentPhysicalMs,
                                            bool rollbackIfExpired) {
  auto primaryShard = router_->Route(primaryKey);
  TxnRecordStatus record;
  const TxnStatus query = primaryShard->CheckTxnStatus(
      primaryKey, startTs, currentPhysicalMs, rollbackIfExpired, 5000, &record);
  if (query != TxnStatus::Ok) return PrimaryTxnStatus{query};
  switch (record.state) {
    case TxnRecordState::Committed:
      return PrimaryTxnStatus{TxnStatus::Ok, PrimaryTxnState::Committed,
                              record.commitTs, std::nullopt};
    case TxnRecordState::RolledBack:
      return PrimaryTxnStatus{TxnStatus::Ok, PrimaryTxnState::RolledBack, 0, std::nullopt};
    case TxnRecordState::Locked:
      return PrimaryTxnStatus{TxnStatus::Ok, PrimaryTxnState::Locked, 0, record.lock};
    case TxnRecordState::NotFound:
      return PrimaryTxnStatus{TxnStatus::Ok, PrimaryTxnState::Missing, 0, std::nullopt};
  }
  return PrimaryTxnStatus{TxnStatus::StorageError};
}

TxnStatus LockResolver::ResolveLock(const std::string& key, const MvccLock& lock, uint64_t nowMs) {
  auto shard = router_->Route(key);
  if (nowMs == 0) nowMs = HlcTimestamp::WallClockMs();
  if (!IsExpired(lock, nowMs)) return TxnStatus::LockConflict;
  const PrimaryTxnStatus primary = CheckPrimary(lock.primaryKey, lock.startTs, nowMs, true);
  if (primary.queryStatus != TxnStatus::Ok) return primary.queryStatus;
  if (primary.state == PrimaryTxnState::Committed) {
    return shard->ResolveLock(key, lock.startTs, TxnRecordState::Committed, primary.commitTs);
  }
  if (primary.state == PrimaryTxnState::Locked) {
    return TxnStatus::LockConflict;
  }
  return shard->ResolveLock(key, lock.startTs, TxnRecordState::RolledBack);
}

size_t LockResolver::ResolveExpiredLocks(uint64_t nowMs) {
  size_t resolved = 0;
  for (const auto& shard : router_->Shards()) {
    for (const auto& item : shard->ExpiredLocks(nowMs)) {
      if (ResolveLock(item.first, item.second, nowMs) == TxnStatus::Ok) {
        ++resolved;
      }
    }
  }
  return resolved;
}
