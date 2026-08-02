// Transaction subsystem: cross-region transaction orchestration.
#include "distributed_transaction_coordinator.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <iostream>
#include <map>
#include <thread>
#include <vector>

namespace {
// 生成当前时间的毫秒值，供锁过期扫描和恢复逻辑使用。
uint64_t NowMs() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

// 判断事务中是否记录了某个 key 的 mutation，用于区分提交 key 和纯悲观锁 key。
bool HasMutation(const Transaction& txn, const std::string& key) {
  return txn.Mutations().find(key) != txn.Mutations().end();
}

// 选择分布式提交里的 primary key，确保跨 shard 提交有一个稳定的锚点。
std::string CommitPrimaryKey(const Transaction& txn) {
  if (!txn.PrimaryKey().empty() && HasMutation(txn, txn.PrimaryKey())) {
    return txn.PrimaryKey();
  }
  return txn.Mutations().begin()->first;
}

struct PrewriteResult {
  TxnStatus status = TxnStatus::Ok;
  std::vector<std::string> prewrittenKeys;
};

struct SecondaryCommitFailure {
  std::string key;
  TxnStatus status = TxnStatus::StorageError;
};

const char* TxnStatusName(TxnStatus status) {
  switch (status) {
    case TxnStatus::Ok: return "Ok";
    case TxnStatus::NotFound: return "NotFound";
    case TxnStatus::LockConflict: return "LockConflict";
    case TxnStatus::WriteConflict: return "WriteConflict";
    case TxnStatus::AlreadyCommitted: return "AlreadyCommitted";
    case TxnStatus::StorageError: return "StorageError";
  }
  return "Unknown";
}
}  // namespace

// 构造分布式事务协调器，建立 shard 路由、锁解析器和各 shard 的后台锁管理线程。
DistributedTransactionCoordinator::DistributedTransactionCoordinator(std::shared_ptr<ShardRouter> router,
                                                                     std::shared_ptr<TimestampOracle> tso)
    : router_(std::move(router)), tso_(std::move(tso)), lockResolver_(std::make_shared<LockResolver>(router_)) {
  uint64_t maxObservedTs = 0;
  for (const auto& shard : router_->Shards()) {
    maxObservedTs = std::max(maxObservedTs, shard->MaxObservedTs());
    lockManagers_.emplace_back(
        new LockManager(shard, [resolver = lockResolver_](const std::string& key, const MvccLock& lock) {
          resolver->ResolveLock(key, lock);
        }));
    lockManagers_.back()->Start();
    dataGcManagers_.emplace_back(new DataGcManager(shard, tso_));
    dataGcManagers_.back()->Start();
  }
  tso_->Observe(maxObservedTs);
}

// 析构时停止所有 shard 的后台锁扫描线程，避免恢复逻辑在协调器退出后继续运行。
DistributedTransactionCoordinator::~DistributedTransactionCoordinator() {
  for (auto& manager : dataGcManagers_) {
    manager->Stop();
  }
  for (auto& manager : lockManagers_) {
    manager->Stop();
  }
}

// 为分布式事务分配全局 startTs，开启其跨 shard 生命周期。
Transaction DistributedTransactionCoordinator::Begin() { return Transaction(tso_->Next()); }

// 按事务快照时间从对应 shard 读取数据；若读到锁冲突，会先触发锁解析再重试。
TxnStatus DistributedTransactionCoordinator::Get(const Transaction& txn, const std::string& key, std::string* value) {
  const TxnStatus status = router_->Route(key)->Get(key, txn.StartTs(), value);
  if (status != TxnStatus::LockConflict) {
    return status;
  }

  const auto lock = router_->Route(key)->GetLock(key);
  if (!lock.has_value()) {
    return status;
  }
  lockResolver_->ResolveLock(key, lock.value());
  return router_->Route(key)->Get(key, txn.StartTs(), value);
}

// 在指定 shard 上为事务申请悲观锁，让写冲突尽量前移到执行阶段。
TxnStatus DistributedTransactionCoordinator::PessimisticLock(Transaction* txn, const std::string& key,
                                                            const TxnOptions& options) {
  if (txn == nullptr) {
    return TxnStatus::StorageError;
  }
  txn->TrackPessimisticLock(key);
  return router_->Route(key)->AcquirePessimisticLock(key, txn->PrimaryKey(), txn->StartTs(), options.lockTtlMs);
}

// 执行跨 shard 的两阶段提交流程：并发预写、提交 primary、再提交 secondary。
TxnStatus DistributedTransactionCoordinator::Commit(const Transaction& txn, const TxnOptions& options) {
  if (txn.Mutations().empty()) {
    for (const auto& key : txn.PessimisticLocks()) {
      router_->Route(key)->Rollback(key, txn.StartTs());
    }
    return TxnStatus::Ok;
  }

  const std::string primaryKey = CommitPrimaryKey(txn);
  const std::vector<std::pair<std::string, TxnMutation>> mutations(txn.Mutations().begin(), txn.Mutations().end());
  std::map<size_t, std::vector<std::pair<std::string, TxnMutation>>> mutationsByShard;
  for (const auto& mutation : mutations) {
    mutationsByShard[router_->ShardId(mutation.first)].push_back(mutation);
  }

  std::vector<std::string> prewritten;
  std::vector<std::future<PrewriteResult>> prewriteFutures;
  for (const auto& shardMutations : mutationsByShard) {
    prewriteFutures.emplace_back(std::async(std::launch::async, [this, shardMutations, primaryKey, &txn, options]() {
      PrewriteResult result;
      for (const auto& mutation : shardMutations.second) {
        TxnStatus status = TxnStatus::Ok;
        if (mutation.second.isDelete) {
          status = router_->Route(mutation.first)->PrewriteDelete(mutation.first, primaryKey, txn.StartTs(),
                                                                  options.lockTtlMs);
        } else {
          status = router_->Route(mutation.first)->Prewrite(mutation.first, mutation.second.value, primaryKey,
                                                            txn.StartTs(), options.lockTtlMs);
        }
        if (status != TxnStatus::Ok) {
          result.status = status;
          return result;
        }
        result.prewrittenKeys.push_back(mutation.first);
      }
      return result;
    }));
  }

  TxnStatus firstError = TxnStatus::Ok;
  for (auto& future : prewriteFutures) {
    PrewriteResult result = future.get();
    if (result.status != TxnStatus::Ok) {
      if (firstError == TxnStatus::Ok) {
        firstError = result.status;
      }
    }
    prewritten.insert(prewritten.end(), result.prewrittenKeys.begin(), result.prewrittenKeys.end());
  }
  if (firstError != TxnStatus::Ok) {
    for (const auto& key : prewritten) {
      router_->Route(key)->Rollback(key, txn.StartTs());
    }
    for (const auto& key : txn.PessimisticLocks()) {
      router_->Route(key)->Rollback(key, txn.StartTs());
    }
    return firstError;
  }

  const uint64_t commitTs = tso_->Next();
  TxnStatus primaryStatus = router_->Route(primaryKey)->Commit(primaryKey, txn.StartTs(), commitTs);
  if (primaryStatus != TxnStatus::Ok && primaryStatus != TxnStatus::AlreadyCommitted) {
    for (const auto& key : prewritten) {
      router_->Route(key)->Rollback(key, txn.StartTs());
    }
    for (const auto& key : txn.PessimisticLocks()) {
      if (!HasMutation(txn, key)) {
        router_->Route(key)->Rollback(key, txn.StartTs());
      }
    }
    return primaryStatus;
  }

  std::map<size_t, std::vector<std::string>> secondaryKeysByShard;
  for (const auto& key : prewritten) {
    if (key == primaryKey) {
      continue;
    }
    secondaryKeysByShard[router_->ShardId(key)].push_back(key);
  }

  std::vector<std::future<std::vector<SecondaryCommitFailure>>> commitFutures;
  for (const auto& shardKeys : secondaryKeysByShard) {
    commitFutures.emplace_back(std::async(std::launch::async, [this, shardKeys, &txn, commitTs]() {
      std::vector<SecondaryCommitFailure> failures;
      for (const auto& key : shardKeys.second) {
        TxnStatus status = TxnStatus::StorageError;
        for (int attempt = 0; attempt < 3; ++attempt) {
          status = router_->Route(key)->Commit(key, txn.StartTs(), commitTs);
          if (status == TxnStatus::Ok || status == TxnStatus::AlreadyCommitted) {
            break;
          }
          if (attempt < 2) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25 * (1 << attempt)));
          }
        }
        if (status != TxnStatus::Ok && status != TxnStatus::AlreadyCommitted) {
          failures.push_back({key, status});
        }
      }
      return failures;
    }));
  }
  for (auto& future : commitFutures) {
    for (const auto& failure : future.get()) {
      // Primary 已经提交，因此事务在逻辑上已提交，不能再向客户端返回失败。
      // 后台锁解析器会根据 primary 的 commitTs 继续收尾；这里保留明确状态，
      // 避免把并不存在的“异步队列”写进日志并掩盖真正的失败原因。
      std::cerr << "Secondary key commit remains pending after retries: key=" << failure.key
                << " status=" << TxnStatusName(failure.status)
                << "; lock resolver will finish it from the committed primary." << std::endl;
    }
  }
  for (const auto& key : txn.PessimisticLocks()) {
    if (!HasMutation(txn, key)) {
      router_->Route(key)->Rollback(key, txn.StartTs());
    }
  }
  return TxnStatus::Ok;
}

// 主动回滚一个分布式事务，把它在各个 shard 上留下的记录都清理掉。
void DistributedTransactionCoordinator::Rollback(const Transaction& txn) {
  for (const auto& mutation : txn.Mutations()) {
    router_->Route(mutation.first)->Rollback(mutation.first, txn.StartTs());
  }
  for (const auto& key : txn.PessimisticLocks()) {
    if (txn.Mutations().find(key) == txn.Mutations().end()) {
      router_->Route(key)->Rollback(key, txn.StartTs());
    }
  }
}

// 触发全局锁解析器扫描所有 shard 的过期锁，并推动它们进入提交或回滚。
size_t DistributedTransactionCoordinator::ResolveExpiredLocks() { return lockResolver_->ResolveExpiredLocks(NowMs()); }

// 对所有 shard 做垃圾回收，清理 safe point 之前不再需要的版本记录。
size_t DistributedTransactionCoordinator::GarbageCollect(uint64_t safePointTs) {
  size_t removed = 0;
  for (const auto& shard : router_->Shards()) {
    removed += shard->GarbageCollect(safePointTs);
  }
  return removed;
}
