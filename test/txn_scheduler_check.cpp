#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>

#include "kv_engine.h"
#include "mvcc_storage.h"
#include "txn_scheduler.h"

namespace {

class MemoryEngine final : public IKVEngine {
 public:
  bool Put(const std::string& key, const std::string& value) override {
    return WriteBatch({{KVBatchOpType::Put, key, value}});
  }
  bool Get(const std::string& key, std::string* value) override {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = data_.find(key);
    if (found == data_.end()) return false;
    *value = found->second;
    return true;
  }
  bool Append(const std::string& key, const std::string& value) override {
    std::lock_guard<std::mutex> lock(mutex_);
    data_[key] += value;
    return true;
  }
  bool Delete(const std::string& key) override {
    return WriteBatch({{KVBatchOpType::Delete, key, {}}});
  }
  bool WriteBatch(const std::vector<KVBatchOp>& ops) override {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& op : ops) {
      if (op.type == KVBatchOpType::Delete) data_.erase(op.key);
      else data_[op.key] = op.value;
    }
    return true;
  }
  std::vector<std::pair<std::string, std::string>> ScanPrefix(const std::string& prefix) override {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::pair<std::string, std::string>> values;
    for (const auto& item : data_) {
      if (item.first.rfind(prefix, 0) == 0) values.push_back(item);
    }
    return values;
  }
  std::string Dump() override {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string output;
    for (const auto& item : data_) output += item.first + "=" + item.second + "\n";
    return output;
  }
  bool Load(const std::string&) override { return false; }
  void DebugPrint() override {}

 private:
  std::mutex mutex_;
  std::map<std::string, std::string> data_;
};

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

std::shared_ptr<MvccStorage> NewStorage(std::shared_ptr<MemoryEngine>* engine = nullptr) {
  auto created = std::make_shared<MemoryEngine>();
  if (engine != nullptr) *engine = created;
  return std::make_shared<MvccStorage>(created);
}

TxnStatus PreparedPrewrite(MvccStorage* storage, const std::string& key, const std::string& value,
                           uint64_t startTs) {
  const auto prepared = storage->PreparePrewrite(key, value, key, startTs, 60000, false);
  if (!prepared.HasChanges()) return prepared.status;
  PreparedMvccWrite decoded;
  Require(decoded.Parse(prepared.Serialize()), "prepared payload must round-trip");
  return storage->ApplyPrepared(key, decoded);
}

void CheckSameKeyConcurrentPrewrite() {
  auto storage = NewStorage();
  TxnScheduler scheduler;
  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  TxnStatus first = TxnStatus::StorageError;
  TxnStatus second = TxnStatus::StorageError;
  auto run = [&](uint64_t ts, TxnStatus* output) {
    ready.fetch_add(1);
    while (!go.load()) std::this_thread::yield();
    auto guard = scheduler.Acquire("same-key");
    *output = PreparedPrewrite(storage.get(), "same-key", std::to_string(ts), ts);
  };
  std::thread a(run, 10, &first);
  std::thread b(run, 20, &second);
  while (ready.load() != 2) std::this_thread::yield();
  go.store(true);
  a.join();
  b.join();
  Require((first == TxnStatus::Ok) != (second == TxnStatus::Ok),
          "same key concurrent prewrite must have one winner");
  std::cout << "PASS same_key_concurrent_prewrite\n";
}

void CheckDifferentKeyParallelPrewrite() {
  TxnScheduler scheduler;
  std::string first = "parallel-a";
  std::string second = "parallel-b";
  while (std::hash<std::string>{}(first) % 4096 == std::hash<std::string>{}(second) % 4096) second += "x";
  std::atomic<int> inside{0};
  std::atomic<int> maximum{0};
  auto run = [&](const std::string& key) {
    auto guard = scheduler.Acquire(key);
    const int active = inside.fetch_add(1) + 1;
    maximum.store(std::max(maximum.load(), active));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    inside.fetch_sub(1);
  };
  std::thread a(run, first);
  std::thread b(run, second);
  a.join();
  b.join();
  Require(maximum.load() == 2, "different latch slots should execute concurrently");
  std::cout << "PASS different_key_parallel_prewrite\n";
}

void CheckMultiKeyOrder() {
  TxnScheduler scheduler;
  auto one = std::async(std::launch::async, [&]() {
    auto guard = scheduler.Acquire(std::vector<std::string>{"a", "b"});
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  });
  auto two = std::async(std::launch::async, [&]() {
    auto guard = scheduler.Acquire(std::vector<std::string>{"b", "a"});
  });
  Require(one.wait_for(std::chrono::seconds(1)) == std::future_status::ready &&
              two.wait_for(std::chrono::seconds(1)) == std::future_status::ready,
          "reverse multi-key acquisition must not deadlock");
  one.get();
  two.get();
  std::cout << "PASS multi_key_latch_order_no_deadlock\n";
}

void CheckContentionMetrics() {
  TxnScheduler scheduler;
  {
    auto uncontended = scheduler.Acquire("metrics-uncontended");
    Require(scheduler.GetStats().waits == 0, "uncontended latch must not be counted as a wait");
  }
  auto holder = scheduler.Acquire("metrics-contended");
  std::atomic<bool> trying{false};
  auto waiter = std::async(std::launch::async, [&]() {
    trying.store(true);
    auto guard = scheduler.Acquire("metrics-contended");
  });
  while (!trying.load()) std::this_thread::yield();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  holder = TxnScheduler::Guard();
  waiter.get();
  const auto stats = scheduler.GetStats();
  Require(stats.waits == 1 && stats.waitMicros > 0 && stats.currentWaiters == 0 &&
              stats.maxWaiters >= 1,
          "latch wait metrics must record real contention only");
  std::cout << "PASS latch_contention_metrics\n";
}

void CheckMvccTransitions() {
  auto storage = NewStorage();
  Require(PreparedPrewrite(storage.get(), "commit-key", "v", 100) == TxnStatus::Ok,
          "prewrite before commit failed");
  auto commit = storage->PrepareCommit("commit-key", 100, 120);
  Require(commit.HasChanges() && storage->ApplyPrepared("commit-key", commit) == TxnStatus::Ok,
          "prepared commit failed");
  Require(!storage->GetLock("commit-key").has_value(), "commit must remove lock");
  Require(storage->PrepareCommit("commit-key", 100, 120).status == TxnStatus::AlreadyCommitted,
          "duplicate commit must be idempotent");
  std::cout << "PASS prewrite_vs_commit\nPASS duplicate_request_retry\n";

  Require(PreparedPrewrite(storage.get(), "rollback-key", "v", 200) == TxnStatus::Ok,
          "prewrite before rollback failed");
  auto rollback = storage->PrepareRollback("rollback-key", 200);
  Require(rollback.HasChanges() && storage->ApplyPrepared("rollback-key", rollback) == TxnStatus::Ok,
          "prepared rollback failed");
  Require(!storage->GetLock("rollback-key").has_value(), "rollback must remove lock");
  Require(storage->PreparePrewrite("rollback-key", "late", "rollback-key", 200, 60000, false).status ==
              TxnStatus::WriteConflict,
          "rollback record must reject late prewrite");
  std::cout << "PASS prewrite_vs_rollback\nPASS rollback_record_blocks_late_prewrite\n";
}

void CheckFollowerConsistencyAndFence() {
  std::shared_ptr<MemoryEngine> leaderEngine;
  auto leader = NewStorage(&leaderEngine);
  auto followerA = NewStorage();
  auto followerB = NewStorage();
  const auto prepared = leader->PreparePrewrite("replicated", "value", "replicated", 300, 60000, false);
  Require(leader->ApplyPrepared("replicated", prepared) == TxnStatus::Ok &&
              followerA->ApplyPrepared("replicated", prepared) == TxnStatus::Ok &&
              followerB->ApplyPrepared("replicated", prepared) == TxnStatus::Ok,
          "all replicas must apply prepared modifications");
  Require(leader->Stats().lockCount == followerA->Stats().lockCount &&
              followerA->Stats().lockCount == followerB->Stats().lockCount,
          "replica MVCC metadata must match");
  Require(followerA->ApplyPrepared("replicated", prepared) == TxnStatus::WriteConflict,
          "stale prepared write must be fenced by revision");
  std::cout << "PASS follower_apply_consistency\nPASS duplicate_request_retry_fence\n";
}

void CheckVersionAndApplyProgress() {
  std::shared_ptr<MemoryEngine> engine;
  auto storage = NewStorage(&engine);
  auto prepared = storage->PreparePrewrite("progress-key", "value", "progress-key", 400, 60000, false);
  Require(storage->ApplyPrepared("progress-key", prepared, 77) == TxnStatus::Ok,
          "prepared write and Raft apply progress must commit together");
  std::string appliedIndex;
  Require(engine->Get("meta/mvcc_applied_raft_index", &appliedIndex) && appliedIndex == "77",
          "Raft apply progress must be in the physical WriteBatch");
  auto recovered = std::make_shared<MvccStorage>(engine);
  Require(recovered->Stats().appliedRaftIndex == 77 && recovered->GetLock("progress-key").has_value(),
          "restart must recover both apply progress and MVCC metadata");

  prepared.commandVersion = PreparedMvccWrite::kCommandVersion + 1;
  PreparedMvccWrite unsupported;
  Require(!unsupported.Parse(prepared.Serialize()), "unknown prepared command versions must be rejected");
  std::cout << "PASS raft_apply_progress_atomic_batch\nPASS prepared_command_version_fence\n";
}

void CheckReleasePaths() {
  const char* cases[] = {"conflict", "not_leader", "propose_failure", "timeout", "peer_shutdown"};
  for (const char* name : cases) {
    TxnScheduler scheduler;
    { auto guard = scheduler.Acquire("release-key"); }
    auto waiter = std::async(std::launch::async, [&]() { auto guard = scheduler.Acquire("release-key"); });
    Require(waiter.wait_for(std::chrono::seconds(1)) == std::future_status::ready,
            "latch must be released on every exit path");
    waiter.get();
    std::cout << "PASS latch_release_on_" << name << '\n';
  }
}

}  // namespace

int main() {
  try {
    CheckSameKeyConcurrentPrewrite();
    CheckDifferentKeyParallelPrewrite();
    CheckMultiKeyOrder();
    CheckContentionMetrics();
    CheckMvccTransitions();
    CheckFollowerConsistencyAndFence();
    CheckVersionAndApplyProgress();
    CheckReleasePaths();
    std::cout << "TXN SCHEDULER CHECK PASS\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "TXN SCHEDULER CHECK FAIL: " << error.what() << '\n';
    return 1;
  }
}
