// Transaction subsystem: cross-region transaction orchestration.
#include "distributed_transaction_coordinator.h"

#include <algorithm>
#include <iostream>
#include <future>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "bounded_thread_pool.h"

namespace {
constexpr size_t kRegionTaskWorkers = 8;
constexpr size_t kRegionTaskQueueCapacity = 2048;

bool HasMutation(const Transaction& txn, const std::string& key) {
  return txn.Mutations().find(key) != txn.Mutations().end();
}

bool IsCommitSuccess(TxnStatus status) {
  return status == TxnStatus::Ok || status == TxnStatus::AlreadyCommitted;
}

uint64_t SaturatingAdd(uint64_t value, uint64_t delta) {
  return value > std::numeric_limits<uint64_t>::max() - delta
             ? std::numeric_limits<uint64_t>::max()
             : value + delta;
}

const char* TxnStatusName(TxnStatus status) {
  switch (status) {
    case TxnStatus::Ok: return "Ok";
    case TxnStatus::NotFound: return "NotFound";
    case TxnStatus::LockConflict: return "LockConflict";
    case TxnStatus::WriteConflict: return "WriteConflict";
    case TxnStatus::AlreadyCommitted: return "AlreadyCommitted";
    case TxnStatus::Timeout: return "Timeout";
    case TxnStatus::AbortOnly: return "AbortOnly";
    case TxnStatus::CleanupPending: return "CleanupPending";
    case TxnStatus::ResultUnknown: return "ResultUnknown";
    case TxnStatus::StorageError: return "StorageError";
  }
  return "Unknown";
}

std::vector<std::string> SortedUnique(std::vector<std::string> keys) {
  std::sort(keys.begin(), keys.end());
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  return keys;
}

std::vector<std::string> CleanupKeys(const Transaction& txn) {
  std::vector<std::string> keys;
  keys.reserve(txn.Mutations().size() + txn.PessimisticLocks().size() +
               txn.UncertainPessimisticLocks().size());
  for (const auto& mutation : txn.Mutations()) keys.push_back(mutation.first);
  keys.insert(keys.end(), txn.PessimisticLocks().begin(), txn.PessimisticLocks().end());
  keys.insert(keys.end(), txn.UncertainPessimisticLocks().begin(),
              txn.UncertainPessimisticLocks().end());
  return SortedUnique(std::move(keys));
}

struct RegionKeyBatch {
  int regionId = -1;
  std::shared_ptr<MvccStorage> storage;
  std::vector<std::string> keys;
};

std::vector<RegionKeyBatch> GroupKeysByRegion(const std::shared_ptr<ShardRouter>& router,
                                               const std::vector<std::string>& keys) {
  std::unordered_map<int, size_t> positions;
  std::vector<RegionKeyBatch> batches;
  for (const auto& key : SortedUnique(keys)) {
    const int regionId = router->RegionId(key);
    auto inserted = positions.emplace(regionId, batches.size());
    if (inserted.second) batches.push_back({regionId, router->Route(key), {}});
    batches[inserted.first->second].keys.push_back(key);
  }
  std::sort(batches.begin(), batches.end(), [](const RegionKeyBatch& lhs, const RegionKeyBatch& rhs) {
    return lhs.regionId < rhs.regionId;
  });
  return batches;
}
}  // namespace

DistributedTransactionCoordinator::DistributedTransactionCoordinator(
    std::shared_ptr<ShardRouter> router, std::shared_ptr<TimestampOracle> tso)
    : router_(std::move(router)),
      tso_(std::move(tso)),
      regionExecutor_(std::make_unique<BoundedThreadPool>(kRegionTaskWorkers,
                                                          kRegionTaskQueueCapacity)),
      lockResolver_(std::make_shared<LockResolver>(router_)) {
  uint64_t maxObservedTs = 0;
  bool clusterSupportsPessimistic = true;
  bool clusterSupportsBatch = true;
  for (const auto& shard : router_->Shards()) {
    maxObservedTs = std::max(maxObservedTs, shard->MaxObservedTs());
    const ProtocolCapabilities capabilities = shard->Capabilities();
    if (capabilities.protocolVersion < kPessimisticTxnProtocolVersion)
      clusterSupportsPessimistic = false;
    if (capabilities.protocolVersion < kBatchTxnProtocolVersion) clusterSupportsBatch = false;
  }
  pessimisticEnabled_ = clusterSupportsPessimistic;
  batchEnabled_ = clusterSupportsBatch;
  // Client-side steady-clock recovery and 10-second GC are not authoritative
  // across processes. Keep them disabled until the Node-owned manager lands.
  tso_->Observe(maxObservedTs);
}

DistributedTransactionCoordinator::~DistributedTransactionCoordinator() = default;

Transaction DistributedTransactionCoordinator::Begin() { return Transaction(tso_->Next()); }

TxnStatus DistributedTransactionCoordinator::Validate(Transaction* txn,
                                                       const TxnOptions& options) {
  return CheckActiveAndDeadline(txn, options);
}

TxnStatus DistributedTransactionCoordinator::CheckActiveAndDeadline(
    Transaction* txn, const TxnOptions& options, uint64_t* nowTs) {
  if (txn == nullptr) return TxnStatus::StorageError;
  switch (txn->State()) {
    case TransactionState::Active: break;
    case TransactionState::AbortOnly: return TxnStatus::AbortOnly;
    case TransactionState::CleanupPending: return TxnStatus::CleanupPending;
    case TransactionState::ResultUnknown: return TxnStatus::ResultUnknown;
    case TransactionState::Finished: return TxnStatus::AbortOnly;
  }
  if (options.lockTtlMs <= options.transactionTimeoutMs || options.rpcBudgetMs == 0) {
    return TxnStatus::StorageError;
  }

  uint64_t current = 0;
  try {
    current = tso_->Next();
  } catch (...) {
    return TxnStatus::StorageError;
  }
  if (nowTs != nullptr) *nowTs = current;

  const uint64_t startPhysical = HlcTimestamp::PhysicalMs(txn->StartTs());
  const uint64_t currentPhysical = HlcTimestamp::PhysicalMs(current);
  if (startPhysical != 0 &&
      currentPhysical >= SaturatingAdd(startPhysical, options.transactionTimeoutMs)) {
    txn->MarkAbortOnly();
    return TxnStatus::Timeout;
  }
  return TxnStatus::Ok;
}

TxnStatus DistributedTransactionCoordinator::Get(Transaction* txn, const std::string& key,
                                                  std::string* value,
                                                  const TxnOptions& options) {
  if (key.empty() || value == nullptr) return TxnStatus::StorageError;
  const TxnStatus active = CheckActiveAndDeadline(txn, options);
  if (active != TxnStatus::Ok) return active;
  // 普通 Get 始终保持 startTs 快照语义，不能通过非原子旧探针猜测锁状态。
  return router_->Route(key)->Get(key, txn->StartTs(), value);
}

BatchLockingReadResult DistributedTransactionCoordinator::AcquireKeys(
    Transaction* txn, const std::vector<std::string>& requestedKeys, bool returnValues,
    const TxnOptions& options) {
  BatchLockingReadResult batch;
  if (txn == nullptr) return batch;
  if (!pessimisticEnabled_) {
    batch.status = TxnStatus::StorageError;
    return batch;
  }
  std::vector<std::string> keys = SortedUnique(requestedKeys);
  if (keys.empty()) {
    batch.status = TxnStatus::Ok;
    return batch;
  }
  if (std::any_of(keys.begin(), keys.end(), [](const std::string& key) { return key.empty(); })) {
    batch.status = TxnStatus::StorageError;
    return batch;
  }

  uint64_t forUpdateTs = 0;
  batch.status = CheckActiveAndDeadline(txn, options, &forUpdateTs);
  if (batch.status != TxnStatus::Ok) return batch;
  const uint64_t expireAt =
      SaturatingAdd(HlcTimestamp::PhysicalMs(forUpdateTs), options.lockTtlMs);

  for (const auto& key : keys) {
    const std::string primary = txn->ProposedPrimaryKey(key);
    txn->TrackUncertainPessimisticLock(key);
    txn->ObserveForUpdateTs(forUpdateTs);
    PessimisticLockResult result = router_->Route(key)->AcquirePessimisticLockForUpdate(
        key, primary, txn->StartTs(), options.lockTtlMs, forUpdateTs, expireAt,
        options.rpcBudgetMs);
    if (result.status == TxnStatus::Ok && !result.applied) {
      result.status = TxnStatus::ResultUnknown;
    }
    if (result.status == TxnStatus::Ok) {
      txn->ConfirmPessimisticLock(key, forUpdateTs);
      if (!returnValues) {
        result.found = false;
        result.value.clear();
        result.valueCommitTs = 0;
      }
      batch.values.emplace_back(key, std::move(result));
      continue;
    }

    if (result.status != TxnStatus::ResultUnknown && result.status != TxnStatus::Timeout &&
        result.status != TxnStatus::StorageError) {
      txn->ForgetPessimisticLock(key);
    }
    if (result.status == TxnStatus::Timeout) {
      lockTimeouts_.fetch_add(1, std::memory_order_relaxed);
    } else if (result.status == TxnStatus::LockConflict || result.status == TxnStatus::WriteConflict) {
      lockConflicts_.fetch_add(1, std::memory_order_relaxed);
    }
    txn->MarkAbortOnly();
    const TxnStatus failure = result.status;
    batch.values.clear();
    const TxnStatus cleanup = CleanupTransaction(txn);
    if (cleanup != TxnStatus::Ok) {
      txn->MarkCleanupPending();
      batch.status = TxnStatus::CleanupPending;
    } else {
      batch.status = failure;
    }
    return batch;
  }

  batch.status = TxnStatus::Ok;
  return batch;
}

PessimisticLockResult DistributedTransactionCoordinator::GetForUpdate(
    Transaction* txn, const std::string& key, const TxnOptions& options) {
  BatchLockingReadResult batch = AcquireKeys(txn, {key}, true, options);
  if (batch.status != TxnStatus::Ok || batch.values.empty()) {
    PessimisticLockResult result;
    result.status = batch.status;
    return result;
  }
  return std::move(batch.values.front().second);
}

BatchLockingReadResult DistributedTransactionCoordinator::BatchGetForUpdate(
    Transaction* txn, const std::vector<std::string>& keys, const TxnOptions& options) {
  BatchLockingReadResult acquired = AcquireKeys(txn, keys, true, options);
  if (acquired.status != TxnStatus::Ok) return acquired;

  std::unordered_map<std::string, PessimisticLockResult> byKey;
  for (auto& item : acquired.values) byKey.emplace(item.first, std::move(item.second));
  acquired.values.clear();
  acquired.values.reserve(keys.size());
  for (const auto& key : keys) {
    const auto found = byKey.find(key);
    if (found != byKey.end()) acquired.values.emplace_back(key, found->second);
  }
  return acquired;
}

TxnStatus DistributedTransactionCoordinator::LockKeys(Transaction* txn,
                                                      const std::vector<std::string>& keys,
                                                      const TxnOptions& options) {
  return AcquireKeys(txn, keys, false, options).status;
}

TxnStatus DistributedTransactionCoordinator::PessimisticLock(Transaction* txn,
                                                             const std::string& key,
                                                             const TxnOptions& options) {
  return LockKeys(txn, {key}, options);
}

TxnStatus DistributedTransactionCoordinator::CleanupTransaction(Transaction* txn) {
  if (txn == nullptr) return TxnStatus::StorageError;
  const std::vector<std::string> keys = CleanupKeys(*txn);
  TxnStatus aggregate = TxnStatus::Ok;
  if (!batchEnabled_) {
    for (const auto& key : keys) {
      const TxnStatus status = router_->Route(key)->Rollback(key, txn->StartTs());
      if (status == TxnStatus::Ok) {
        txn->ForgetPessimisticLock(key);
      } else if (status == TxnStatus::AlreadyCommitted) {
        aggregate = TxnStatus::AlreadyCommitted;
      } else {
        aggregate = TxnStatus::CleanupPending;
      }
    }
    RecordRollbackRegions(keys);
    if (aggregate == TxnStatus::Ok) txn->ClearPessimisticLocks();
    return aggregate;
  }
  const auto batches = GroupKeysByRegion(router_, keys);
  std::vector<std::future<TxnStatus>> futures;
  futures.reserve(batches.size());
  for (const auto& batch : batches) {
    futures.push_back(regionExecutor_->Submit([batch, startTs = txn->StartTs()]() {
      return batch.storage->BatchRollback(batch.keys, startTs);
    }));
  }
  for (size_t index = 0; index < batches.size(); ++index) {
    const TxnStatus status = futures[index].get();
    if (status == TxnStatus::Ok) {
      for (const auto& key : batches[index].keys) txn->ForgetPessimisticLock(key);
    } else if (status == TxnStatus::AlreadyCommitted) {
      aggregate = TxnStatus::AlreadyCommitted;
    } else {
      aggregate = TxnStatus::CleanupPending;
    }
  }
  RecordRollbackRegions(keys);
  if (aggregate == TxnStatus::Ok) txn->ClearPessimisticLocks();
  return aggregate;
}

TxnStatus DistributedTransactionCoordinator::QueryStatus(const Transaction& txn,
                                                         TxnRecordStatus* status,
                                                         const TxnOptions& options) {
  if (status == nullptr || txn.PrimaryKey().empty()) return TxnStatus::StorageError;
  uint64_t nowTs = 0;
  try {
    nowTs = tso_->Next();
  } catch (...) {
    return TxnStatus::StorageError;
  }
  return router_->Route(txn.PrimaryKey())
      ->CheckTxnStatus(txn.PrimaryKey(), txn.StartTs(), HlcTimestamp::PhysicalMs(nowTs),
                       false, options.rpcBudgetMs, status);
}

TxnStatus DistributedTransactionCoordinator::Commit(Transaction* txn,
                                                    const TxnOptions& options) {
  if (txn == nullptr) return TxnStatus::StorageError;
  const TxnStatus active = CheckActiveAndDeadline(txn, options);
  if (active != TxnStatus::Ok) return active;

  if (txn->Mutations().empty()) {
    const TxnStatus cleanup = CleanupTransaction(txn);
    if (cleanup == TxnStatus::Ok) {
      txn->MarkFinished();
      return TxnStatus::Ok;
    }
    txn->MarkCleanupPending();
    return TxnStatus::CleanupPending;
  }

  const std::string primaryKey = txn->PrimaryKey();
  if (primaryKey.empty()) return TxnStatus::StorageError;
  std::vector<std::string> mutationKeys;
  mutationKeys.reserve(txn->Mutations().size());
  for (const auto& mutation : txn->Mutations()) mutationKeys.push_back(mutation.first);
  mutationKeys = SortedUnique(std::move(mutationKeys));

  std::vector<std::string> prewritten;
  TxnStatus prewriteStatus = TxnStatus::Ok;
  if (!batchEnabled_) {
    if (!HasMutation(*txn, primaryKey)) {
      prewriteStatus = router_->Route(primaryKey)->PrewriteLock(
          primaryKey, primaryKey, txn->StartTs(), options.lockTtlMs,
          txn->MaxForUpdateTs(), options.rpcBudgetMs);
      if (prewriteStatus == TxnStatus::Ok) prewritten.push_back(primaryKey);
    }
    for (const auto& key : mutationKeys) {
      if (prewriteStatus != TxnStatus::Ok) break;
      const TxnMutation& mutation = txn->Mutations().at(key);
      prewriteStatus = mutation.isDelete
                           ? router_->Route(key)->PrewriteDelete(
                                 key, primaryKey, txn->StartTs(), options.lockTtlMs,
                                 txn->MaxForUpdateTs(), options.rpcBudgetMs)
                           : router_->Route(key)->Prewrite(
                                 key, mutation.value, primaryKey, txn->StartTs(),
                                 options.lockTtlMs, txn->MaxForUpdateTs(), options.rpcBudgetMs);
      if (prewriteStatus == TxnStatus::Ok) prewritten.push_back(key);
    }
  } else {
    struct MutationBatch {
      int regionId = -1;
      std::shared_ptr<MvccStorage> storage;
      std::vector<MvccMutation> mutations;
    };
    std::unordered_map<int, size_t> batchPositions;
    std::vector<MutationBatch> mutationBatches;
    const auto appendMutation = [&](MvccMutation mutation) {
      const int regionId = router_->RegionId(mutation.key);
      auto inserted = batchPositions.emplace(regionId, mutationBatches.size());
      if (inserted.second) mutationBatches.push_back({regionId, router_->Route(mutation.key), {}});
      mutationBatches[inserted.first->second].mutations.push_back(std::move(mutation));
    };
    if (!HasMutation(*txn, primaryKey)) appendMutation({primaryKey, "", false, true});
    for (const auto& key : mutationKeys) {
      const TxnMutation& mutation = txn->Mutations().at(key);
      appendMutation({key, mutation.value, mutation.isDelete, false});
    }
    std::sort(mutationBatches.begin(), mutationBatches.end(),
              [](const MutationBatch& lhs, const MutationBatch& rhs) {
                return lhs.regionId < rhs.regionId;
              });
    std::vector<std::future<TxnStatus>> prewriteFutures;
    prewriteFutures.reserve(mutationBatches.size());
    for (auto& batch : mutationBatches) {
      std::sort(batch.mutations.begin(), batch.mutations.end(),
                [](const MvccMutation& lhs, const MvccMutation& rhs) { return lhs.key < rhs.key; });
      prewriteFutures.push_back(regionExecutor_->Submit(
          [storage = batch.storage, mutations = batch.mutations, primaryKey,
           startTs = txn->StartTs(), ttlMs = options.lockTtlMs,
           forUpdateTs = txn->MaxForUpdateTs(), budgetMs = options.rpcBudgetMs]() {
            return storage->BatchPrewrite(mutations, primaryKey, startTs, ttlMs,
                                          forUpdateTs, budgetMs);
          }));
    }
    for (size_t index = 0; index < mutationBatches.size(); ++index) {
      const TxnStatus status = prewriteFutures[index].get();
      if (status == TxnStatus::Ok) {
        for (const auto& mutation : mutationBatches[index].mutations)
          prewritten.push_back(mutation.key);
      } else if (prewriteStatus == TxnStatus::Ok) {
        prewriteStatus = status;
      }
    }
  }
  if (prewriteStatus != TxnStatus::Ok) {
    txn->MarkAbortOnly();
    const TxnStatus cleanup = CleanupTransaction(txn);
    if (cleanup != TxnStatus::Ok) {
      txn->MarkCleanupPending();
      return TxnStatus::CleanupPending;
    }
    return prewriteStatus;
  }

  uint64_t commitTs = 0;
  try {
    do {
      commitTs = tso_->Next();
    } while (commitTs <= std::max(txn->StartTs(), txn->MaxForUpdateTs()));
  } catch (...) {
    txn->MarkAbortOnly();
    const TxnStatus cleanup = CleanupTransaction(txn);
    if (cleanup != TxnStatus::Ok) {
      txn->MarkCleanupPending();
      return TxnStatus::CleanupPending;
    }
    return TxnStatus::StorageError;
  }

  TxnStatus primaryStatus =
      router_->Route(primaryKey)->Commit(primaryKey, txn->StartTs(), commitTs);
  if (!IsCommitSuccess(primaryStatus)) {
    TxnRecordStatus authoritative;
    const TxnStatus query = QueryStatus(*txn, &authoritative, options);
    if (query == TxnStatus::Ok && authoritative.state == TxnRecordState::Committed) {
      commitTs = authoritative.commitTs;
      primaryStatus = TxnStatus::Ok;
    } else if (query == TxnStatus::Ok && authoritative.state == TxnRecordState::RolledBack) {
      txn->MarkAbortOnly();
      const TxnStatus cleanup = CleanupTransaction(txn);
      if (cleanup != TxnStatus::Ok) {
        txn->MarkCleanupPending();
        return TxnStatus::CleanupPending;
      }
      return TxnStatus::WriteConflict;
    } else {
      txn->MarkResultUnknown();
      return TxnStatus::ResultUnknown;
    }
  }

  std::vector<std::string> secondaryKeys;
  for (const auto& key : prewritten) {
    if (key != primaryKey) secondaryKeys.push_back(key);
  }
  if (!batchEnabled_) {
    for (const auto& key : secondaryKeys) {
      const TxnStatus status = router_->Route(key)->Commit(key, txn->StartTs(), commitTs);
      if (!IsCommitSuccess(status)) {
        std::cerr << "Secondary key commit remains pending: key=" << key
                  << " status=" << TxnStatusName(status)
                  << "; committed Primary remains authoritative." << std::endl;
      }
    }
  } else {
    const auto secondaryBatches = GroupKeysByRegion(router_, secondaryKeys);
    std::vector<std::future<TxnStatus>> secondaryFutures;
    secondaryFutures.reserve(secondaryBatches.size());
    for (const auto& batch : secondaryBatches) {
      secondaryFutures.push_back(regionExecutor_->Submit(
          [batch, startTs = txn->StartTs(), commitTs, budgetMs = options.rpcBudgetMs]() {
            return batch.storage->BatchCommit(batch.keys, startTs, commitTs, budgetMs);
          }));
    }
    for (size_t index = 0; index < secondaryBatches.size(); ++index) {
      const TxnStatus status = secondaryFutures[index].get();
      if (!IsCommitSuccess(status)) {
        std::cerr << "Secondary Region commit remains pending: region="
                  << secondaryBatches[index].regionId
                  << " status=" << TxnStatusName(status)
                  << "; committed Primary remains authoritative." << std::endl;
      }
    }
  }
  std::vector<std::string> purePessimisticLocks;
  for (const auto& key : txn->PessimisticLocks()) {
    if (key == primaryKey || HasMutation(*txn, key)) continue;
    purePessimisticLocks.push_back(key);
  }
  if (!batchEnabled_) {
    for (const auto& key : purePessimisticLocks) {
      const TxnStatus release = router_->Route(key)->Rollback(key, txn->StartTs());
      if (release != TxnStatus::Ok) {
        std::cerr << "Pure pessimistic lock release remains pending: key=" << key
                  << " status=" << TxnStatusName(release) << std::endl;
      }
    }
  } else {
    const auto releaseBatches = GroupKeysByRegion(router_, purePessimisticLocks);
    std::vector<std::future<TxnStatus>> releaseFutures;
    for (const auto& batch : releaseBatches) {
      releaseFutures.push_back(regionExecutor_->Submit([batch, startTs = txn->StartTs()]() {
        return batch.storage->BatchRollback(batch.keys, startTs);
      }));
    }
    for (size_t index = 0; index < releaseBatches.size(); ++index) {
      const TxnStatus release = releaseFutures[index].get();
      if (release != TxnStatus::Ok) {
        std::cerr << "Pure pessimistic lock release remains pending: region="
                  << releaseBatches[index].regionId
                  << " status=" << TxnStatusName(release) << std::endl;
      }
    }
  }
  txn->ClearPessimisticLocks();
  txn->MarkFinished();
  return TxnStatus::Ok;
}

TxnStatus DistributedTransactionCoordinator::Rollback(Transaction* txn,
                                                      const TxnOptions& options) {
  if (txn == nullptr) return TxnStatus::StorageError;
  if (txn->State() == TransactionState::Finished) return TxnStatus::Ok;
  if (txn->State() == TransactionState::ResultUnknown) {
    TxnRecordStatus authoritative;
    const TxnStatus query = QueryStatus(*txn, &authoritative, options);
    if (query != TxnStatus::Ok ||
        (authoritative.state != TxnRecordState::Committed &&
         authoritative.state != TxnRecordState::RolledBack)) {
      return TxnStatus::ResultUnknown;
    }
    const std::vector<std::string> keys = CleanupKeys(*txn);
    TxnStatus resolved = TxnStatus::Ok;
    for (const auto& key : keys) {
      const TxnStatus status = router_->Route(key)->ResolveLock(
          key, txn->StartTs(), authoritative.state, authoritative.commitTs);
      if (status != TxnStatus::Ok) resolved = TxnStatus::CleanupPending;
    }
    if (resolved != TxnStatus::Ok) {
      txn->MarkCleanupPending();
      return resolved;
    }
    txn->ClearPessimisticLocks();
    txn->MarkFinished();
    return authoritative.state == TxnRecordState::Committed ? TxnStatus::AlreadyCommitted
                                                             : TxnStatus::Ok;
  }

  const TxnStatus cleanup = CleanupTransaction(txn);
  if (cleanup == TxnStatus::Ok) {
    txn->MarkFinished();
    return TxnStatus::Ok;
  }
  if (cleanup == TxnStatus::AlreadyCommitted) {
    txn->MarkFinished();
    return TxnStatus::AlreadyCommitted;
  }
  txn->MarkCleanupPending();
  return TxnStatus::CleanupPending;
}

void DistributedTransactionCoordinator::RecordRollbackRegions(
    const std::vector<std::string>& keys) {
  std::unordered_set<size_t> regions;
  for (const auto& key : keys) regions.insert(router_->ShardId(key));
  rollbackRegionCount_.fetch_add(regions.size(), std::memory_order_relaxed);
}

DistributedTxnMetrics DistributedTransactionCoordinator::Metrics() const {
  DistributedTxnMetrics metrics;
  metrics.rollbackRegionCount = rollbackRegionCount_.load(std::memory_order_relaxed);
  metrics.lockTimeouts = lockTimeouts_.load(std::memory_order_relaxed);
  metrics.lockConflicts = lockConflicts_.load(std::memory_order_relaxed);
  
  for (const auto& shard : router_->Shards()) {
    metrics.activeLocks += shard->Stats().pessimisticLockCount;
  }
  return metrics;
}

size_t DistributedTransactionCoordinator::ResolveExpiredLocks() {
  uint64_t nowTs = 0;
  try {
    nowTs = tso_->Next();
  } catch (...) {
    return 0;
  }
  return lockResolver_->ResolveExpiredLocks(HlcTimestamp::PhysicalMs(nowTs));
}

size_t DistributedTransactionCoordinator::GarbageCollect(uint64_t safePointTs) {
  size_t removed = 0;
  for (const auto& shard : router_->Shards()) removed += shard->GarbageCollect(safePointTs);
  return removed;
}
