#ifndef STRATAKV_TRANSACTION_DISTRIBUTED_TRANSACTION_COORDINATOR_H
#define STRATAKV_TRANSACTION_DISTRIBUTED_TRANSACTION_COORDINATOR_H

#include <memory>
#include <atomic>
#include <string>
#include <utility>
#include <vector>

#include "lock_resolver.h"
#include "shard_router.h"
#include "timestamp_oracle.h"
#include "transaction_coordinator.h"

class BoundedThreadPool;

struct DistributedTxnMetrics {
  uint64_t rollbackRegionCount = 0;
  uint64_t activeLocks = 0;
  uint64_t lockTimeouts = 0;
  uint64_t lockConflicts = 0;
};

struct BatchLockingReadResult {
  TxnStatus status = TxnStatus::StorageError;
  std::vector<std::pair<std::string, PessimisticLockResult>> values;
};

class DistributedTransactionCoordinator {
 public:
  DistributedTransactionCoordinator(std::shared_ptr<ShardRouter> router, std::shared_ptr<TimestampOracle> tso);
  ~DistributedTransactionCoordinator();

  Transaction Begin();
  TxnStatus Validate(Transaction* txn, const TxnOptions& options = TxnOptions());
  TxnStatus Get(Transaction* txn, const std::string& key, std::string* value,
                const TxnOptions& options = TxnOptions());
  // 快照范围读:[startKey, endKey),endKey 为空表示无界;limit 为 0 表示
  // 不限量。跨 Region 聚合,输出按键升序;任一 Region 冲突即整批 LockConflict。
  TxnStatus Scan(Transaction* txn, const std::string& startKey, const std::string& endKey,
                 size_t limit, std::vector<std::pair<std::string, std::string>>* entries,
                 const TxnOptions& options = TxnOptions());
  TxnStatus PessimisticLock(Transaction* txn, const std::string& key, const TxnOptions& options = TxnOptions());
  PessimisticLockResult GetForUpdate(Transaction* txn, const std::string& key,
                                     const TxnOptions& options = TxnOptions());
  BatchLockingReadResult BatchGetForUpdate(Transaction* txn, const std::vector<std::string>& keys,
                                           const TxnOptions& options = TxnOptions());
  TxnStatus LockKeys(Transaction* txn, const std::vector<std::string>& keys,
                     const TxnOptions& options = TxnOptions());
  TxnStatus Commit(Transaction* txn, const TxnOptions& options = TxnOptions());
  TxnStatus Rollback(Transaction* txn, const TxnOptions& options = TxnOptions());
  TxnStatus QueryStatus(const Transaction& txn, TxnRecordStatus* status,
                        const TxnOptions& options = TxnOptions());
  size_t ResolveExpiredLocks();
  size_t GarbageCollect(uint64_t safePointTs);
  DistributedTxnMetrics Metrics() const;

 private:
  std::shared_ptr<ShardRouter> router_;
  std::shared_ptr<TimestampOracle> tso_;
  // Cross-Region RPC/storage work is blocking and therefore belongs on a
  // bounded native executor; no RPC thread may block on it.
  std::unique_ptr<BoundedThreadPool> regionExecutor_;
  std::shared_ptr<LockResolver> lockResolver_;
  std::atomic<uint64_t> rollbackRegionCount_{0};
  std::atomic<uint64_t> activeLocks_{0};
  std::atomic<uint64_t> lockTimeouts_{0};
  std::atomic<uint64_t> lockConflicts_{0};
  bool pessimisticEnabled_ = false;
  bool batchEnabled_ = false;

  void RecordRollbackRegions(const std::vector<std::string>& keys);
  TxnStatus CheckActiveAndDeadline(Transaction* txn, const TxnOptions& options,
                                   uint64_t* nowTs = nullptr);
  TxnStatus CleanupTransaction(Transaction* txn);
  BatchLockingReadResult AcquireKeys(Transaction* txn, const std::vector<std::string>& keys,
                                     bool returnValues, const TxnOptions& options);
};

#endif  // STRATAKV_TRANSACTION_DISTRIBUTED_TRANSACTION_COORDINATOR_H
