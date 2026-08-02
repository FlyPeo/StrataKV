#ifndef STRATAKV_DISTRIBUTED_TRANSACTION_COORDINATOR_H
#define STRATAKV_DISTRIBUTED_TRANSACTION_COORDINATOR_H

#include <memory>
#include <string>
#include <vector>

#include "data_gc_manager.h"
#include "lock_manager.h"
#include "lock_resolver.h"
#include "shard_router.h"
#include "timestamp_oracle.h"
#include "transaction_coordinator.h"

class DistributedTransactionCoordinator {
 public:
  DistributedTransactionCoordinator(std::shared_ptr<ShardRouter> router, std::shared_ptr<TimestampOracle> tso);
  ~DistributedTransactionCoordinator();

  Transaction Begin();
  TxnStatus Get(const Transaction& txn, const std::string& key, std::string* value);
  TxnStatus PessimisticLock(Transaction* txn, const std::string& key, const TxnOptions& options = TxnOptions());
  TxnStatus Commit(const Transaction& txn, const TxnOptions& options = TxnOptions());
  void Rollback(const Transaction& txn);
  size_t ResolveExpiredLocks();
  size_t GarbageCollect(uint64_t safePointTs);

 private:
  std::shared_ptr<ShardRouter> router_;
  std::shared_ptr<TimestampOracle> tso_;
  std::shared_ptr<LockResolver> lockResolver_;
  std::vector<std::unique_ptr<LockManager>> lockManagers_;
  std::vector<std::unique_ptr<DataGcManager>> dataGcManagers_;
};

#endif  // STRATAKV_DISTRIBUTED_TRANSACTION_COORDINATOR_H
