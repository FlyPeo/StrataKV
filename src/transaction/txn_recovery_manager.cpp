#include "txn_recovery_manager.h"

#include <chrono>
#include <future>
#include <iostream>
#include <stdexcept>

#include "lock_resolver.h"
#include "txn_scheduler.h"

namespace {

std::shared_ptr<LockResolver> BuildLockResolver(
    const std::vector<ShardRouter::RegionRoute>& routes) {
  // An empty view (registry not yet published) is legal: the scan simply skips
  // secondary locks until the first refresh installs real routes.
  if (routes.empty()) return nullptr;
  return std::make_shared<LockResolver>(std::make_shared<ShardRouter>(routes));
}

}  // namespace

TxnRecoveryManager::TxnRecoveryManager(std::shared_ptr<NodeTxnScheduler> scheduler,
                                       std::shared_ptr<TimestampOracle> tsoClient,
                                       TxnRecoveryRouteResolver routeResolver,
                                       TxnRecoveryRouteRevision routeRevision,
                                       std::chrono::milliseconds checkInterval)
    : scheduler_(std::move(scheduler)),
      tsoClient_(std::move(tsoClient)),
      routeResolver_(std::move(routeResolver)),
      routeRevision_(std::move(routeRevision)),
      checkInterval_(checkInterval) {
  if (!routeResolver_ || !routeRevision_) {
    throw std::invalid_argument("TxnRecoveryManager requires a route resolver and revision");
  }
  // Build the initial routes eagerly (possibly an empty view before the registry
  // is published). The first scan after publication refreshes them as the
  // revision advances.
  lockResolver_ = BuildLockResolver(routeResolver_());
  lastRouteRevision_ = routeRevision_();
}

std::shared_ptr<LockResolver> TxnRecoveryManager::CurrentLockResolver() {
  RefreshRoutesIfNeeded();
  std::lock_guard<std::mutex> lock(routesMutex_);
  return lockResolver_;
}

void TxnRecoveryManager::RefreshRoutesIfNeeded() {
  const uint64_t revision = routeRevision_();
  {
    std::lock_guard<std::mutex> lock(routesMutex_);
    if (revision == lastRouteRevision_) return;
    lastRouteRevision_ = revision;
  }
  // Resolve outside the lock so the resolver can call back into the registry
  // without serializing against CheckPrimary users.
  const auto routes = routeResolver_();
  auto resolver = BuildLockResolver(routes);
  {
    std::lock_guard<std::mutex> lock(routesMutex_);
    if (lastRouteRevision_ == revision) lockResolver_ = std::move(resolver);
    routeRefreshCount_.fetch_add(1, std::memory_order_relaxed);
  }
}

uint64_t TxnRecoveryManager::RouteRefreshCount() const {
  return routeRefreshCount_.load(std::memory_order_relaxed);
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
    try {
      ScanOnce();
      AdvanceGc();
    } catch (const std::exception& e) {
      std::cerr << "[TxnRecoveryManager] WorkerLoop caught exception: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "[TxnRecoveryManager] WorkerLoop caught unknown exception" << std::endl;
    }
    std::this_thread::sleep_for(checkInterval_);
  }
}

void TxnRecoveryManager::ScanOnce() {
  // Resolve routes once per scan: every CheckPrimary in this round shares one
  // topology view even if the revision advances mid-scan.
  const auto lockResolver = CurrentLockResolver();
  uint64_t currentPhysicalMs = 0;
  try {
    currentPhysicalMs = tsoClient_->Next() >> 18;
  } catch (const std::exception& e) {
    return;
  }

  for (auto region : scheduler_->Regions()) {
    if (!region || !region->IsTxnLeader()) continue;
    std::vector<std::pair<std::string, MvccLock>> expired;
    try {
      expired = region->ExpiredLocks(currentPhysicalMs);
    } catch (...) {
      continue;
    }
    for (const auto& item : expired) {
      const std::string& key = item.first;
      const MvccLock& lock = item.second;

      PrimaryTxnStatus status;
      try {
        if (key == lock.primaryKey) {
          status.state = PrimaryTxnState::RolledBack;
          status.queryStatus = TxnStatus::Ok;
        } else {
          if (lockResolver == nullptr) {
            // Registry not yet published: no route for the Primary; retry next round.
            continue;
          }
          status = lockResolver->CheckPrimary(lock.primaryKey, lock.startTs, currentPhysicalMs, true);
        }
      } catch (const std::exception&) {
        continue;
      }

      if (status.queryStatus != TxnStatus::Ok || status.state == PrimaryTxnState::Locked) {
        continue;
      }

      TxnCommand command;
      command.type = TxnCommandType::ResolveLock;
      command.regionId = region->TxnRegionId();
      command.key = key;
      command.keys = {key};
      command.startTs = lock.startTs;
      command.clientId = "TxnRecoveryManager";
      command.requestId = requestId_.fetch_add(1, std::memory_order_relaxed) + 1;
      
      if (status.state == PrimaryTxnState::Committed) {
        command.resolutionState = TxnRecordState::Committed;
        command.commitTs = status.commitTs;
      } else {
        command.resolutionState = TxnRecordState::RolledBack;
      }

      try {
        auto p = std::make_shared<std::promise<void>>();
        auto f = p->get_future();
        scheduler_->Schedule(command, [p](const TxnScheduleResult&) {
          try {
            p->set_value();
          } catch (...) {
          }
        });
        if (f.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
          f.get();
        }
      } catch (...) {
      }
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
    if (!region || !region->IsTxnLeader()) continue;
    TxnCommand command;
    command.type = TxnCommandType::GarbageCollect;
    command.latchMode = TxnLatchMode::RegionExclusive;
    command.regionId = region->TxnRegionId();
    command.safePointTs = safePointTs;
    command.clientId = "TxnRecoveryManager";
    command.requestId = requestId_.fetch_add(1, std::memory_order_relaxed) + 1;
    
    try {
      auto p = std::make_shared<std::promise<void>>();
      auto f = p->get_future();
      scheduler_->Schedule(command, [p](const TxnScheduleResult&) {
        try {
          p->set_value();
        } catch (...) {
        }
      });
      if (f.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
        f.get();
      }
    } catch (...) {
    }
  }
}

