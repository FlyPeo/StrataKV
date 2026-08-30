#ifndef STRATAKV_TRANSACTION_TXN_RECOVERY_MANAGER_H
#define STRATAKV_TRANSACTION_TXN_RECOVERY_MANAGER_H

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <string>

#include "region_metadata.h"

class NodeTxnScheduler;
class RemoteTimestampOracle;
class LockResolver;

class TxnRecoveryManager {
 public:
  TxnRecoveryManager(std::shared_ptr<NodeTxnScheduler> scheduler,
                     std::shared_ptr<RemoteTimestampOracle> tsoClient,
                     const RegionCatalog& catalog,
                     std::chrono::milliseconds checkInterval = std::chrono::milliseconds(200));
  ~TxnRecoveryManager();

  TxnRecoveryManager(const TxnRecoveryManager&) = delete;
  TxnRecoveryManager& operator=(const TxnRecoveryManager&) = delete;

  void Start();
  void Stop();
  void ScanOnce();
  void AdvanceGc();

 private:
  void WorkerLoop();

  std::shared_ptr<NodeTxnScheduler> scheduler_;
  std::shared_ptr<RemoteTimestampOracle> tsoClient_;
  std::shared_ptr<LockResolver> lockResolver_;
  std::chrono::milliseconds checkInterval_;
  std::atomic<bool> stopped_{true};
  std::atomic<int> requestId_{0};
  std::thread worker_;
  std::mutex stateMutex_;
};

#endif  // STRATAKV_TRANSACTION_TXN_RECOVERY_MANAGER_H
