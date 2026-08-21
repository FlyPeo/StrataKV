#include "txn_recovery_manager.h"

#include <chrono>
#include <future>
#include <iostream>

#include "lock_resolver.h"
#include "raft_mvcc_storage.h"
#include "remote_timestamp_oracle.h"
#include "shard_router.h"
#include "txn_scheduler.h"

TxnRecoveryManager::TxnRecoveryManager(std::shared_ptr<NodeTxnScheduler> scheduler,
                                       std::shared_ptr<RemoteTimestampOracle> tsoClient,
                                       const RegionCatalog& catalog,
                                       std::chrono::milliseconds checkInterval)
    : scheduler_(std::move(scheduler)),
      tsoClient_(std::move(tsoClient)),
      checkInterval_(checkInterval) {
  std::vector<ShardRouter::RegionRoute> routes;
  routes.reserve(catalog.Regions().size());
  for (const auto& region : catalog.Regions()) {
    std::vector<std::pair<std::string, short>> endpoints;
    endpoints.reserve(region.peers.size());
    for (const auto& peer : region.peers) endpoints.emplace_back(peer.host, peer.port);
    routes.push_back({region, std::make_shared<RaftMvccStorage>(region.regionId, endpoints)});
  }
  auto router = std::make_shared<ShardRouter>(std::move(routes));
  lockResolver_ = std::make_shared<LockResolver>(std::move(router));
}

TxnRecoveryManager::~TxnRecoveryManager() {
  Stop();
}

void TxnRecoveryManager::Start() {
  std::lock_guard<std::mutex> lock(stateMutex_);
  if (!stopped_) return;
  stopped_ = false;
  worker_ = std::thread(&TxnRecoveryManager::WorkerLoop, this);
}

void TxnRecoveryManager::Stop() {
  std::lock_guard<std::mutex> lock(stateMutex_);
  if (stopped_) return;
  stopped_ = true;
  if (worker_.joinable()) {
    worker_.join();
  }
}

void TxnRecoveryManager::WorkerLoop() {
  while (!stopped_) {
    ScanOnce();
    AdvanceGc();
    std::this_thread::sleep_for(checkInterval_);
  }
}

void TxnRecoveryManager::ScanOnce() {
  uint64_t currentPhysicalMs = tsoClient_->Next() >> 18;

  for (auto region : scheduler_->Regions()) {
    for (const auto& item : region->ExpiredLocks(currentPhysicalMs)) {
      const std::string& key = item.first;
      const MvccLock& lock = item.second;

      PrimaryTxnStatus status;
      if (key == lock.primaryKey) {
        status.state = PrimaryTxnState::RolledBack;
        status.queryStatus = TxnStatus::Ok;
      } else {
        status = lockResolver_->CheckPrimary(lock.primaryKey, lock.startTs, currentPhysicalMs, true);
      }

      if (status.queryStatus != TxnStatus::Ok || status.state == PrimaryTxnState::Missing ||
          status.state == PrimaryTxnState::Locked) {
        continue;
      }

      TxnCommand command;
      command.type = TxnCommandType::ResolveLock;
      command.regionId = region->TxnRegionId();
      command.key = key;
      command.keys = {key};
      command.startTs = lock.startTs;
      command.clientId = "TxnRecoveryManager";
      
      if (status.state == PrimaryTxnState::Committed) {
        command.resolutionState = TxnRecordState::Committed;
        command.commitTs = status.commitTs;
      } else {
        command.resolutionState = TxnRecordState::RolledBack;
      }

      std::promise<void> p;
      scheduler_->Schedule(command, [&p](const TxnScheduleResult&) { p.set_value(); });
      p.get_future().get();
    }
  }
}

void TxnRecoveryManager::AdvanceGc() {
  uint64_t currentTso = 0;
  try {
    currentTso = tsoClient_->Next();
  } catch (...) {
    return;
  }
  
  uint64_t currentPhysicalMs = currentTso >> 18;
  const uint64_t retentionMs = 5 * 60 * 1000;
  if (currentPhysicalMs <= retentionMs) return;
  
  uint64_t safePointTs = (currentPhysicalMs - retentionMs) << 18;
  
  for (auto region : scheduler_->Regions()) {
    TxnCommand command;
    command.type = TxnCommandType::GarbageCollect;
    command.regionId = region->TxnRegionId();
    command.safePointTs = safePointTs;
    command.clientId = "TxnRecoveryManager";
    
    std::promise<void> p;
    scheduler_->Schedule(command, [&p](const TxnScheduleResult&) { p.set_value(); });
    p.get_future().get();
  }
}
