#ifndef STRATAKV_TRANSACTION_TXN_RECOVERY_MANAGER_H
#define STRATAKV_TRANSACTION_TXN_RECOVERY_MANAGER_H

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <string>
#include <vector>

#include "region_metadata.h"
#include "shard_router.h"
#include "timestamp_oracle.h"

class NodeTxnScheduler;
class LockResolver;

// Returns the routes for the *current* topology. Called when the reported
// revision advances; the result rebuilds the lock resolver so recovery traffic
// tracks dynamic Region add/remove instead of retaining the startup catalog.
// In-flight callers hold shared_ptrs to the previous resolver, so a resolver
// swap never invalidates an ongoing CheckPrimary.
using TxnRecoveryRouteResolver = std::function<std::vector<ShardRouter::RegionRoute>()>;
using TxnRecoveryRouteRevision = std::function<uint64_t()>;

class TxnRecoveryManager {
 public:
  TxnRecoveryManager(std::shared_ptr<NodeTxnScheduler> scheduler,
                     std::shared_ptr<TimestampOracle> tsoClient,
                     TxnRecoveryRouteResolver routeResolver,
                     TxnRecoveryRouteRevision routeRevision,
                     std::chrono::milliseconds checkInterval = std::chrono::milliseconds(200));
  ~TxnRecoveryManager();

  TxnRecoveryManager(const TxnRecoveryManager&) = delete;
  TxnRecoveryManager& operator=(const TxnRecoveryManager&) = delete;

  void Start();
  void Stop();
  void ScanOnce();
  void AdvanceGc();

  // Number of times routes were rebuilt because the topology revision advanced.
  uint64_t RouteRefreshCount() const;

 private:
  void WorkerLoop();
  void RefreshRoutesIfNeeded();
  std::shared_ptr<LockResolver> CurrentLockResolver();

  std::shared_ptr<NodeTxnScheduler> scheduler_;
  std::shared_ptr<TimestampOracle> tsoClient_;
  TxnRecoveryRouteResolver routeResolver_;
  TxnRecoveryRouteRevision routeRevision_;
  std::mutex routesMutex_;
  std::shared_ptr<LockResolver> lockResolver_;
  uint64_t lastRouteRevision_ = 0;
  std::atomic<uint64_t> routeRefreshCount_{0};
  std::chrono::milliseconds checkInterval_;
  std::atomic<bool> stopped_{true};
  std::atomic<int> requestId_{0};
  std::thread worker_;
  std::mutex stateMutex_;
};

#endif  // STRATAKV_TRANSACTION_TXN_RECOVERY_MANAGER_H
