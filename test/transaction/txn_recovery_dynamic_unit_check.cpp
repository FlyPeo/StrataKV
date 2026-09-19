/*
 * 测试目标：验证 TxnRecoveryManager 按拓扑 revision 重建 Region 路由（而非启动期目录），并在
 *           Region 并发注销 churn 下安全完成恢复扫描。
 * 测试策略：FakeTso/FakeMvccStorage/FakeRegionExecutor 隔离恢复路径，revision provider 可控；
 *           用一个线程循环 ScanOnce/AdvanceGc，另一线程 200 次 OnRegionRemoved+Register churn，
 *           检查刷新计数与最终注册可见性。
 * 测试规模：3 个场景：路由刷新计数、空路由安全跳过、并发注销与恢复（200 轮 churn）。
 * 验证内容：构造不计为刷新，revision 不变不刷新、每次推进恰好刷新一次；registry 未发布任何
 *           peer 时 secondary 锁被安全跳过（曾经是 UB）；并发扫描与注销全程无异常无死锁，
 *           churn 期间确有路由刷新发生且 Region 100 最终保持注册。
 */
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mvcc_storage.h"
#include "region_metadata.h"
#include "timestamp_oracle.h"
#include "txn_recovery_manager.h"
#include "txn_scheduler.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

class FakeTso final : public TimestampOracle {
 public:
  explicit FakeTso(uint64_t base) : next_(base) {}
  uint64_t Next() override { return next_.fetch_add(1, std::memory_order_relaxed); }
  uint64_t Peek() override { return next_.load(std::memory_order_relaxed); }
  void Observe(uint64_t ts) override {
    uint64_t current = next_.load(std::memory_order_relaxed);
    while (ts > current &&
           !next_.compare_exchange_weak(current, ts, std::memory_order_relaxed)) {
    }
  }

 private:
  std::atomic<uint64_t> next_;
};

// Every status check reports Missing so recovery skips the lock without a real
// backend. The Primary-side query path is exercised without RPC.
class FakeMvccStorage final : public MvccStorage {
 public:
  FakeMvccStorage() : MvccStorage(nullptr) {}
  TxnStatus CheckTxnStatus(const std::string&, uint64_t, TxnRecordStatus* status) override {
    if (status != nullptr) *status = TxnRecordStatus{};
    return TxnStatus::Ok;
  }
  TxnStatus CheckTxnStatus(const std::string&, uint64_t, uint64_t, bool, uint64_t,
                           TxnRecordStatus* status) override {
    if (status != nullptr) *status = TxnRecordStatus{};
    return TxnStatus::Ok;
  }
};

class FakeRegionExecutor final : public TxnRegionExecutor {
 public:
  explicit FakeRegionExecutor(int regionId) : regionId_(regionId) {}
  int TxnRegionId() const override { return regionId_; }
  bool IsTxnLeader() override { return true; }
  PreparedMvccWrite PrepareTxn(const TxnCommand&) override {
    PreparedMvccWrite result;
    result.status = TxnStatus::Ok;
    return result;
  }
  bool ProposeTxn(const Op&, int*) override { return false; }
  std::vector<std::pair<std::string, MvccLock>> ExpiredLocks(uint64_t) override {
    std::lock_guard<std::mutex> lock(mutex_);
    return expired_;
  }
  void SetExpiredLocks(std::vector<std::pair<std::string, MvccLock>> locks) {
    std::lock_guard<std::mutex> lock(mutex_);
    expired_ = std::move(locks);
  }

 private:
  int regionId_;
  std::mutex mutex_;
  std::vector<std::pair<std::string, MvccLock>> expired_;
};

RegionMetadata MakeRegion(int regionId) {
  RegionMetadata metadata;
  metadata.regionId = regionId;
  metadata.endKey = "";  // empty endKey = unbounded upper bound (required by catalog validation)
  RegionPeerLocation location;
  location.nodeId = 0;
  location.host = "127.0.0.1";
  location.port = 1;
  metadata.peers.push_back(location);
  return metadata;
}

std::shared_ptr<TxnRecoveryManager> MakeManager(
    const std::shared_ptr<NodeTxnScheduler>& scheduler,
    const std::shared_ptr<TimestampOracle>& tso, std::atomic<uint64_t>& revision,
    std::shared_ptr<MvccStorage> storage) {
  // storage is captured by value: the temporary passed by callers dies before
  // the resolver is ever invoked, so a reference capture would dangle.
  return std::make_shared<TxnRecoveryManager>(
      scheduler, tso,
      [storage]() {
        return std::vector<ShardRouter::RegionRoute>{{MakeRegion(100), storage}};
      },
      [&revision]() { return revision.load(std::memory_order_relaxed); },
      std::chrono::milliseconds(10));
}

// Routes must be rebuilt exactly once per revision advance, and construction
// itself must not count as a refresh.
void CheckRouteRefresh() {
  auto scheduler = std::make_shared<NodeTxnScheduler>();
  auto tso = std::make_shared<FakeTso>(1000ULL << 18);
  std::atomic<uint64_t> revision{1};
  auto manager = MakeManager(scheduler, tso, revision, std::make_shared<FakeMvccStorage>());

  Require(manager->RouteRefreshCount() == 0, "construction must not count as a route refresh");
  manager->ScanOnce();
  Require(manager->RouteRefreshCount() == 0, "unchanged revision must not refresh routes");

  revision.store(2, std::memory_order_relaxed);
  manager->ScanOnce();
  Require(manager->RouteRefreshCount() == 1, "advanced revision must refresh routes once");

  manager->ScanOnce();
  Require(manager->RouteRefreshCount() == 1, "stable revision must not refresh again");

  revision.store(3, std::memory_order_relaxed);
  manager->ScanOnce();
  Require(manager->RouteRefreshCount() == 2, "each revision advance refreshes routes once");
  std::cout << "[PASS] route refresh tracks topology revision" << std::endl;
}

// Before the registry publishes any peer, routes are empty: secondary locks are
// skipped without crashing (previously Route() on an empty view was UB).
void CheckEmptyRoutesAreSafe() {
  auto scheduler = std::make_shared<NodeTxnScheduler>();
  auto tso = std::make_shared<FakeTso>(1000ULL << 18);
  std::atomic<uint64_t> revision{0};
  TxnRecoveryManager manager(
      scheduler, tso, [] { return std::vector<ShardRouter::RegionRoute>{}; },
      [&]() { return revision.load(std::memory_order_relaxed); },
      std::chrono::milliseconds(10));

  auto region = std::make_shared<FakeRegionExecutor>(100);
  MvccLock lock;
  lock.primaryKey = "other-key";  // secondary lock → would route to the Primary
  lock.startTs = 100;
  region->SetExpiredLocks({{"locked-key", lock}});
  scheduler->RegisterRegion(region);

  manager.ScanOnce();
  std::cout << "[PASS] empty routes skip secondary locks safely" << std::endl;
}

// Concurrent Region unregister + recovery scan must complete without dangling
// access, exceptions, or deadlock.
void CheckConcurrentUnregisterAndRecovery() {
  auto scheduler = std::make_shared<NodeTxnScheduler>();
  auto tso = std::make_shared<FakeTso>(1000ULL << 18);
  std::atomic<uint64_t> revision{1};
  auto manager = MakeManager(scheduler, tso, revision, std::make_shared<FakeMvccStorage>());

  auto region1 = std::make_shared<FakeRegionExecutor>(100);
  auto region2 = std::make_shared<FakeRegionExecutor>(101);
  MvccLock localLock;
  localLock.primaryKey = "locked-key";  // == key → local RolledBack path, no RPC
  localLock.startTs = 100;
  region1->SetExpiredLocks({{"locked-key", localLock}});
  region2->SetExpiredLocks({{"locked-key", localLock}});
  scheduler->RegisterRegion(region1);
  scheduler->RegisterRegion(region2);

  std::atomic<bool> stop{false};
  std::atomic<bool> failed{false};
  std::thread scanner([&]() {
    try {
      while (!stop.load(std::memory_order_relaxed)) {
        manager->ScanOnce();
        manager->AdvanceGc();
      }
    } catch (const std::exception& e) {
      std::cerr << "scanner exception: " << e.what() << std::endl;
      failed.store(true, std::memory_order_relaxed);
    } catch (...) {
      failed.store(true, std::memory_order_relaxed);
    }
  });
  std::thread churner([&]() {
    try {
      for (int i = 0; i < 200; ++i) {
        scheduler->OnRegionRemoved(100);
        scheduler->RegisterRegion(region1);
        revision.fetch_add(1, std::memory_order_relaxed);
        // Without a pace the churn finishes before the scanner thread is ever
        // scheduled, so no refresh would be observable.
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    } catch (const std::exception& e) {
      std::cerr << "churner exception: " << e.what() << std::endl;
      failed.store(true, std::memory_order_relaxed);
    } catch (...) {
      failed.store(true, std::memory_order_relaxed);
    }
  });
  churner.join();
  stop.store(true, std::memory_order_relaxed);
  scanner.join();

  Require(!failed.load(std::memory_order_relaxed),
          "concurrent scan and unregister must not throw");
  bool found100 = false;
  for (const auto& region : scheduler->Regions()) {
    found100 = found100 || region->TxnRegionId() == 100;
  }
  Require(found100, "final registration of Region 100 must be visible");
  Require(manager->RouteRefreshCount() > 0, "route refreshes must have occurred during churn");
  std::cout << "[PASS] concurrent unregister and recovery complete safely" << std::endl;
}

}  // namespace

int main() {
  try {
    CheckRouteRefresh();
    CheckEmptyRoutesAreSafe();
    CheckConcurrentUnregisterAndRecovery();
  } catch (const std::exception& e) {
    std::cerr << "FAILED: " << e.what() << std::endl;
    return 1;
  }
  std::cout << "ALL DYNAMIC RECOVERY CHECKS PASSED" << std::endl;
  return 0;
}
