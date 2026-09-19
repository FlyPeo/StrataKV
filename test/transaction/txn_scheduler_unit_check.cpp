/*
 * 测试目标：验证节点级事务 Scheduler、Latch 与 MVCC prepare/apply 管线的并发和恢复语义。
 * 测试策略：使用内存 KVEngine 和可控 Region 执行器，精确驱动 acquire、propose、apply、
 *           timeout、WrongLeader、过载回调、悲观锁升级、版本兼容与 Region 重启。
 * 测试规模：固定 17 个顶层场景，覆盖多 Region/多键 Latch、调度生命周期、MVCC 状态迁移、
 *           悲观事务编码与 apply、Leader 切换、重启以及旧锁格式降级。
 * 验证内容：确认同 Region/键严格串行、不同 Region 正确隔离，回调只在安全时机触发，
 *           applied index 与编码可恢复，并覆盖锁读取、冲突解析和旧协议降级。
 */
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "kv_engine.h"
#include "kv_server_rpc.pb.h"
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

std::string EncodeTestFields(const std::vector<std::string>& fields) {
  std::string encoded;
  for (const auto& field : fields) {
    encoded += std::to_string(field.size()) + ":" + field;
  }
  return encoded;
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

class MockRegion final : public TxnRegionExecutor {
 public:
  explicit MockRegion(int regionId) : regionId_(regionId), storage_(NewStorage()) {}

  int TxnRegionId() const override { return regionId_; }
  bool IsTxnLeader() override { return leader_.load(); }
  void SetLeader(bool leader) { leader_.store(leader); }

  PreparedMvccWrite PrepareTxn(const TxnCommand& command) override {
    switch (command.type) {
      case TxnCommandType::Prewrite:
        if (command.isLockOnly) {
          return storage_->PreparePrewriteLock(command.key, command.primaryKey,
                                               command.startTs, command.ttlMs,
                                               command.forUpdateTs);
        }
        return storage_->PreparePrewrite(command.key, command.value, command.primaryKey,
                                         command.startTs, command.ttlMs, command.isDelete);
      case TxnCommandType::Commit:
        return storage_->PrepareCommit(command.key, command.startTs, command.commitTs);
      case TxnCommandType::Rollback:
        return storage_->PrepareRollback(command.key, command.startTs);
      case TxnCommandType::PessimisticLock:
        return storage_->PreparePessimisticLock(command.key, command.primaryKey,
                                                command.startTs, command.ttlMs,
                                                command.forUpdateTs,
                                                command.expireAtPhysicalMs);
      case TxnCommandType::CheckTxnStatus:
        return storage_->PrepareCheckTxnStatus(command.key, command.startTs,
                                               command.currentPhysicalMs,
                                               command.rollbackIfExpired);
      case TxnCommandType::ResolveLock:
        return storage_->PrepareResolveLock(command.key, command.startTs,
                                            command.resolutionState,
                                            command.commitTs);
      case TxnCommandType::GarbageCollect:
        return {};
    }
    return {};
  }

  bool ProposeTxn(const Op& op, int* raftIndex) override {
    if (!leader_.load()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    *raftIndex = static_cast<int>(proposals_.size()) + 1;
    proposals_.push_back(op);
    changed_.notify_all();
    return true;
  }

  bool WaitForProposals(size_t count, std::chrono::milliseconds timeout = std::chrono::seconds(1)) {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, timeout, [&]() { return proposals_.size() >= count; });
  }

  Op Proposal(size_t index) {
    std::lock_guard<std::mutex> lock(mutex_);
    return proposals_.at(index);
  }

  Op Apply(size_t index, int raftIndex) {
    Op op = Proposal(index);
    if (op.Operation.rfind("TxnPrepared", 0) == 0) {
      PreparedMvccWrite prepared;
      const TxnStatus status = prepared.Parse(op.Value)
                                   ? storage_->ApplyPrepared(op.Key, prepared, raftIndex)
                                   : TxnStatus::StorageError;
      op.Status = std::to_string(static_cast<int>(status));
    } else if (op.Operation == "TxnGarbageCollect") {
      TxnOpPayload payload;
      op.Status = payload.parseFromString(op.Value)
                      ? std::to_string(storage_->GarbageCollect(payload.startTs))
                      : "invalid";
    }
    return op;
  }

  std::vector<std::pair<std::string, MvccLock>> ExpiredLocks(uint64_t currentPhysicalMs) override {
    return {};
  }

  void StepDown() { leader_.store(false); }

 private:
  int regionId_;
  std::atomic<bool> leader_{true};
  std::shared_ptr<MvccStorage> storage_;
  std::mutex mutex_;
  std::condition_variable changed_;
  std::vector<Op> proposals_;
};

TxnCommand PrewriteCommand(int regionId, std::string key, std::string clientId, int requestId,
                           uint64_t startTs, std::chrono::milliseconds timeout = std::chrono::seconds(1)) {
  TxnCommand command;
  command.type = TxnCommandType::Prewrite;
  command.regionId = regionId;
  command.key = std::move(key);
  command.keys = {command.key};
  command.value = "value";
  command.primaryKey = command.key;
  command.clientId = std::move(clientId);
  command.requestId = requestId;
  command.startTs = startTs;
  command.ttlMs = 60000;
  command.deadline = std::chrono::steady_clock::now() + timeout;
  return command;
}

TxnCommand CommitCommand(int regionId, std::string key, std::string clientId, int requestId,
                         uint64_t startTs, uint64_t commitTs) {
  TxnCommand command;
  command.type = TxnCommandType::Commit;
  command.regionId = regionId;
  command.key = std::move(key);
  command.keys = {command.key};
  command.clientId = std::move(clientId);
  command.requestId = requestId;
  command.startTs = startTs;
  command.commitTs = commitTs;
  command.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  return command;
}

void CheckNodeLatchIsolationAndOrdering() {
  NodeLatchManager latches(64);
  Require(latches.Acquire(1, 100, {"same"}, TxnLatchMode::Keys), "first same-key latch must acquire");
  Require(!latches.Acquire(2, 100, {"same"}, TxnLatchMode::Keys), "same Region/key must queue");
  Require(latches.Acquire(3, 101, {"same"}, TxnLatchMode::Keys),
          "same key string in a different Region must not queue");
  const auto wake = latches.Release(1);
  Require(wake.size() == 1 && wake[0] == 2 && latches.Holds(2), "release must wake same-key successor");
  latches.Release(2);
  latches.Release(3);
  std::cout << "PASS node_latch_same_key_serial\nPASS node_latch_cross_region_isolation\n";

  Require(latches.Acquire(10, 100, {"a", "b"}, TxnLatchMode::Keys), "multi-key owner failed");
  Require(!latches.Acquire(11, 100, {"b", "a"}, TxnLatchMode::Keys),
          "reverse multi-key command must queue");
  const auto reverseWake = latches.Release(10);
  Require(reverseWake.size() == 1 && reverseWake[0] == 11,
          "reverse multi-key acquisition must make progress without deadlock");
  latches.Release(11);
  std::cout << "PASS multi_key_latch_order_no_deadlock\n";
}

void CheckRegionExclusiveIsolation() {
  NodeLatchManager latches(64);
  Require(latches.Acquire(20, 100, {"key"}, TxnLatchMode::Keys), "Region 100 writer failed");
  Require(!latches.Acquire(21, 100, {}, TxnLatchMode::RegionExclusive),
          "Region 100 GC must wait for Region 100 writer");
  Require(latches.Acquire(22, 101, {"key"}, TxnLatchMode::Keys),
          "Region 100 GC must not stop Region 101");
  const auto wake = latches.Release(20);
  Require(wake.size() == 1 && wake[0] == 21, "Region GC must wake after local writers drain");
  latches.Release(21);
  latches.Release(22);
  std::cout << "PASS region_gc_local_exclusive\nPASS region_gc_cross_region_parallel\n";
}

void CheckLatchMetrics() {
  NodeLatchManager latches(64);
  Require(latches.Acquire(30, 100, {"metrics"}, TxnLatchMode::Keys), "metrics owner failed");
  Require(!latches.Acquire(31, 100, {"metrics"}, TxnLatchMode::Keys), "metrics waiter failed");
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  latches.Release(30);
  const auto stats = latches.GetStats();
  Require(stats.waits == 1 && stats.waitMicros > 0 && stats.currentWaiters == 0 && stats.maxWaiters == 1,
          "queue latch metrics must record contention");
  latches.Release(31);
  std::cout << "PASS latch_contention_metrics\n";
}

void CheckSchedulerPrepareProposeApplyLifecycle() {
  auto scheduler = std::make_shared<NodeTxnScheduler>(64, 2, 64, 64);
  auto region = std::make_shared<MockRegion>(100);
  scheduler->RegisterRegion(region);

  std::mutex mutex;
  std::condition_variable changed;
  int completed = 0;
  std::vector<TxnScheduleResult> results;
  auto completion = [&](const TxnScheduleResult& result) {
    std::lock_guard<std::mutex> lock(mutex);
    results.push_back(result);
    ++completed;
    changed.notify_all();
  };

  scheduler->Schedule(PrewriteCommand(100, "serial", "client", 1, 10), completion);
  scheduler->Schedule(CommitCommand(100, "serial", "client", 2, 10, 20), completion);
  Require(region->WaitForProposals(1), "prewrite was not proposed");
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  Require(!region->WaitForProposals(2, std::chrono::milliseconds(20)),
          "same-key commit must not pass prewrite before Apply");
  scheduler->OnApplied(100, region->Apply(0, 1), 1);
  Require(region->WaitForProposals(2), "Apply must release latch and wake commit");
  scheduler->OnApplied(100, region->Apply(1, 2), 2);

  std::unique_lock<std::mutex> lock(mutex);
  Require(changed.wait_for(lock, std::chrono::seconds(1), [&]() { return completed == 2; }),
          "both scheduled commands must complete");
  Require(results[0].applied && results[1].applied, "callbacks must run only after Apply");
  lock.unlock();
  Require(scheduler->GetStats().pendingTasks == 0, "completed tasks must leave pending table");
  std::cout << "PASS scheduler_prepare_propose_apply\nPASS latch_held_until_apply\n";
}

void CheckTimeoutDoesNotReleaseUnknownProposal() {
  auto scheduler = std::make_shared<NodeTxnScheduler>(64, 2, 64, 64);
  auto region = std::make_shared<MockRegion>(100);
  scheduler->RegisterRegion(region);
  std::mutex mutex;
  std::condition_variable changed;
  bool timedOut = false;
  bool commitFinished = false;

  scheduler->Schedule(PrewriteCommand(100, "timeout", "client-timeout", 1, 100,
                                      std::chrono::milliseconds(40)),
                      [&](const TxnScheduleResult& result) {
                        std::lock_guard<std::mutex> lock(mutex);
                        timedOut = result.responseTimedOut;
                        changed.notify_all();
                      });
  Require(region->WaitForProposals(1), "timeout test proposal missing");
  {
    std::unique_lock<std::mutex> lock(mutex);
    Require(changed.wait_for(lock, std::chrono::seconds(1), [&]() { return timedOut; }),
            "RPC response deadline must fire");
  }

  scheduler->Schedule(CommitCommand(100, "timeout", "client-timeout", 2, 100, 120),
                      [&](const TxnScheduleResult&) {
                        std::lock_guard<std::mutex> lock(mutex);
                        commitFinished = true;
                        changed.notify_all();
                      });
  Require(!region->WaitForProposals(2, std::chrono::milliseconds(80)),
          "response timeout must not release an unknown Raft proposal latch");
  scheduler->OnApplied(100, region->Apply(0, 1), 1);
  Require(region->WaitForProposals(2), "late Apply must release latch and wake successor");
  scheduler->OnApplied(100, region->Apply(1, 2), 2);
  {
    std::unique_lock<std::mutex> lock(mutex);
    Require(changed.wait_for(lock, std::chrono::seconds(1), [&]() { return commitFinished; }),
            "successor did not finish after late Apply");
  }
  Require(scheduler->GetStats().responseTimeouts == 1, "timeout metric mismatch");
  std::cout << "PASS rpc_timeout_keeps_unknown_proposal\nPASS late_apply_releases_latch\n";
}

void CheckAppliedRequestIncludesRegionId() {
  auto scheduler = std::make_shared<NodeTxnScheduler>(64, 2, 64, 64);
  auto region100 = std::make_shared<MockRegion>(100);
  auto region101 = std::make_shared<MockRegion>(101);
  scheduler->RegisterRegion(region100);
  scheduler->RegisterRegion(region101);
  std::atomic<int> done100{0};
  std::atomic<int> done101{0};
  scheduler->Schedule(PrewriteCommand(100, "same", "same-client", 7, 200),
                      [&](const TxnScheduleResult&) { done100.fetch_add(1); });
  scheduler->Schedule(PrewriteCommand(101, "same", "same-client", 7, 300),
                      [&](const TxnScheduleResult&) { done101.fetch_add(1); });
  Require(region100->WaitForProposals(1) && region101->WaitForProposals(1),
          "different Regions must propose concurrently");
  scheduler->OnApplied(100, region100->Apply(0, 1), 1);
  for (int attempt = 0; attempt < 100 && done100.load() != 1; ++attempt)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  Require(done100.load() == 1 && done101.load() == 0,
          "OnApplied must match RegionId, clientId and requestId");
  scheduler->OnApplied(101, region101->Apply(0, 1), 1);
  for (int attempt = 0; attempt < 100 && done101.load() != 1; ++attempt)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  Require(done101.load() == 1, "Region 101 completion missing");
  std::cout << "PASS apply_identity_includes_region\nPASS cross_region_same_key_parallel\n";
}

void CheckWrongLeaderReleasesTask() {
  auto scheduler = std::make_shared<NodeTxnScheduler>(64, 1, 16, 16);
  auto region = std::make_shared<MockRegion>(100);
  scheduler->RegisterRegion(region);
  region->SetLeader(false);
  std::atomic<int> failed{0};
  scheduler->Schedule(PrewriteCommand(100, "leader", "client", 1, 10),
                      [&](const TxnScheduleResult& result) {
                        if (result.status == ErrWrongLeader) failed.fetch_add(1);
                      });
  Require(failed.load() == 1 && scheduler->GetStats().pendingTasks == 0,
          "fast WrongLeader must not retain task/latch");
  region->SetLeader(true);
  scheduler->Schedule(PrewriteCommand(100, "leader", "client", 2, 20),
                      [&](const TxnScheduleResult&) {});
  Require(region->WaitForProposals(1), "latch was not reusable after WrongLeader");
  scheduler->OnApplied(100, region->Apply(0, 1), 1);
  std::cout << "PASS latch_release_on_wrong_leader\n";
}

void CheckOverloadCallbackCanReenterScheduler() {
  auto scheduler = std::make_shared<NodeTxnScheduler>(64, 1, 16, 1);
  auto region = std::make_shared<MockRegion>(100);
  scheduler->RegisterRegion(region);
  scheduler->Schedule(PrewriteCommand(100, "pending", "client", 1, 10),
                      [&](const TxnScheduleResult&) {});
  Require(region->WaitForProposals(1), "overload test proposal missing");

  std::atomic<bool> reentered{false};
  scheduler->Schedule(PrewriteCommand(100, "overflow", "client", 2, 20),
                      [&](const TxnScheduleResult& result) {
                        // RPC completions are allowed to inspect or re-enter
                        // the scheduler. This would deadlock if overload
                        // rejection ran under the pending-table mutex.
                        reentered.store(result.status == ErrWrongLeader &&
                                        scheduler->GetStats().pendingTasks == 1);
                      });
  Require(reentered.load(), "overload callback must run outside scheduler mutex");
  scheduler->OnApplied(100, region->Apply(0, 1), 1);
  std::cout << "PASS overload_callback_reentrant\n";
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

  Require(PreparedPrewrite(storage.get(), "rollback-key", "v", 200) == TxnStatus::Ok,
          "prewrite before rollback failed");
  auto rollback = storage->PrepareRollback("rollback-key", 200);
  Require(rollback.HasChanges() && storage->ApplyPrepared("rollback-key", rollback) == TxnStatus::Ok,
          "prepared rollback failed");
  Require(storage->PreparePrewrite("rollback-key", "late", "rollback-key", 200, 60000, false).status ==
              TxnStatus::WriteConflict,
          "rollback record must reject late prewrite");
  std::cout << "PASS prewrite_commit_rollback_transitions\n";
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
  PreparedMvccWrite legacyDecoded;
  Require(legacyDecoded.Parse(prepared.Serialize()) &&
              legacyDecoded.commandVersion == PreparedMvccWrite::kLegacyCommandVersion,
          "legacy prepared command must remain readable");
  raftKVRpcProctoc::TxnAcquirePessimisticLockArgs oldRequest;
  Require(oldRequest.protocolversion() == 0 && oldRequest.forupdatets() == 0 &&
              oldRequest.remainingbudgetms() == 0,
          "new protobuf fields must preserve old-message defaults");
  prepared.commandVersion = PreparedMvccWrite::kCommandVersion + 1;
  PreparedMvccWrite unsupported;
  Require(!unsupported.Parse(prepared.Serialize()), "unknown prepared command versions must be rejected");
  std::cout << "PASS raft_apply_progress_atomic_batch\nPASS prepared_command_version_fence\n"
               "PASS protobuf_compatibility_defaults\n";
}

void CheckPessimisticLockEncodingRecovery() {
  std::shared_ptr<MemoryEngine> engine;
  auto storage = NewStorage(&engine);
  auto prepared = storage->PreparePessimisticLock("new-lock", "primary", 500, 120000, 550, 123456);
  Require(prepared.HasChanges() && storage->ApplyPrepared("new-lock", prepared) == TxnStatus::Ok,
          "new pessimistic lock failed");
  auto lock = storage->GetLock("new-lock");
  Require(lock.has_value() && lock->forUpdateTs == 550 && lock->expireAtPhysicalMs == 123456 &&
              !lock->legacyExpiry && lock->primaryKey == "primary",
          "new pessimistic lock fields did not round-trip");

  engine->Put("lock/legacy-lock",
              EncodeTestFields({"legacy-primary", "600", "120000", "1000", "0", "1"}));
  auto recovered = std::make_shared<MvccStorage>(engine);
  lock = recovered->GetLock("new-lock");
  Require(lock.has_value() && lock->forUpdateTs == 550 && lock->expireAtPhysicalMs == 123456,
          "new pessimistic lock fields did not survive recovery");
  auto legacy = recovered->GetLock("legacy-lock");
  Require(legacy.has_value() && legacy->forUpdateTs == 600 && legacy->legacyExpiry &&
              legacy->expireAtPhysicalMs == 0,
          "legacy pessimistic lock compatibility defaults are wrong");
  std::cout << "PASS pessimistic_lock_encoding_recovery\nPASS legacy_lock_compatibility\n";
}

void CheckPessimisticLockCurrentRead() {
  auto storage = NewStorage();
  Require(PreparedPrewrite(storage.get(), "present", "current", 100) == TxnStatus::Ok,
          "seed prewrite failed");
  auto seedCommit = storage->PrepareCommit("present", 100, 110);
  Require(seedCommit.HasChanges() && storage->ApplyPrepared("present", seedCommit) == TxnStatus::Ok,
          "seed commit failed");

  auto present = storage->PreparePessimisticLock("present", "present", 120, 120000, 130, 10000);
  Require(present.HasChanges() && present.readStatus == TxnStatus::Ok &&
              present.readValue == "current" && present.readCommitTs == 110,
          "locking read did not return current committed value");
  Require(storage->ApplyPrepared("present", present) == TxnStatus::Ok,
          "locking read lock apply failed");

  auto repeated = storage->PreparePessimisticLock("present", "present", 120, 120000, 130, 10000);
  Require(repeated.status == TxnStatus::Ok && !repeated.HasChanges() &&
              repeated.readStatus == TxnStatus::Ok && repeated.readValue == "current",
          "same-owner lock retry was not idempotent");

  auto advanced = storage->PreparePessimisticLock("present", "present", 120, 120000, 150, 12000);
  Require(advanced.HasChanges() && advanced.readValue == "current" &&
              storage->ApplyPrepared("present", advanced) == TxnStatus::Ok,
          "same-owner lock did not advance monotonically");
  auto advancedLock = storage->GetLock("present");
  Require(advancedLock.has_value() && advancedLock->forUpdateTs == 150 &&
              advancedLock->expireAtPhysicalMs == 12000,
          "advanced lock fields mismatch");

  auto absent = storage->PreparePessimisticLock("absent", "absent", 200, 120000, 210, 20000);
  Require(absent.HasChanges() && absent.readStatus == TxnStatus::NotFound,
          "absent exact key was not lockable");
  Require(storage->ApplyPrepared("absent", absent) == TxnStatus::Ok &&
              storage->GetLock("absent").has_value(),
          "absent exact key lock did not persist");

  auto conflict = storage->PreparePessimisticLock("present", "other", 300, 120000, 310, 30000);
  Require(conflict.status == TxnStatus::LockConflict,
          "foreign pessimistic lock did not fail fast");

  Require(PreparedPrewrite(storage.get(), "race", "newer", 400) == TxnStatus::Ok,
          "race seed prewrite failed");
  auto raceCommit = storage->PrepareCommit("race", 400, 450);
  Require(raceCommit.HasChanges() && storage->ApplyPrepared("race", raceCommit) == TxnStatus::Ok,
          "race seed commit failed");
  auto stale = storage->PreparePessimisticLock("race", "race", 420, 120000, 440, 40000);
  Require(stale.status == TxnStatus::WriteConflict,
          "version newer than forUpdateTs did not win the lock race");
  std::cout << "PASS pessimistic_lock_current_read\nPASS absent_key_lock\n"
               "PASS pessimistic_lock_idempotency\nPASS for_update_version_race\n";
}

void CheckPessimisticPrewriteUpgrade() {
  auto storage = NewStorage();
  auto lock = storage->PreparePessimisticLock("upgrade", "primary", 500, 120000, 550, 50000);
  Require(lock.HasChanges() && storage->ApplyPrepared("upgrade", lock) == TxnStatus::Ok,
          "upgrade seed lock failed");

  Require(storage->PreparePrewrite("upgrade", "value", "other-primary", 500, 120000, false, 550).status ==
              TxnStatus::LockConflict,
          "prewrite changed a fixed primary");
  Require(storage->PreparePrewrite("upgrade", "value", "primary", 500, 120000, false, 540).status ==
              TxnStatus::WriteConflict,
          "prewrite accepted a stale forUpdateTs");

  auto upgrade = storage->PreparePrewrite("upgrade", "value", "primary", 500, 120000, false, 550);
  Require(upgrade.HasChanges() && storage->ApplyPrepared("upgrade", upgrade) == TxnStatus::Ok,
          "owned pessimistic lock did not upgrade");
  auto upgradedLock = storage->GetLock("upgrade");
  Require(upgradedLock.has_value() && !upgradedLock->isPessimistic &&
              upgradedLock->primaryKey == "primary" && upgradedLock->forUpdateTs == 550,
          "upgraded Prewrite lock lost pessimistic metadata");
  Require(storage->PreparePrewrite("upgrade", "value", "primary", 500, 120000, false, 550).status ==
              TxnStatus::Ok,
          "duplicate Prewrite after upgrade was not idempotent");

  Require(storage->PrepareCommit("upgrade", 500, 550).status == TxnStatus::WriteConflict,
          "commitTs equal to forUpdateTs was accepted");
  auto commit = storage->PrepareCommit("upgrade", 500, 551);
  Require(commit.HasChanges() && storage->ApplyPrepared("upgrade", commit) == TxnStatus::Ok,
          "commit after forUpdateTs failed");

  auto foreignLock = storage->PreparePessimisticLock("foreign", "foreign", 600, 120000, 610, 60000);
  Require(foreignLock.HasChanges() && storage->ApplyPrepared("foreign", foreignLock) == TxnStatus::Ok,
          "foreign seed lock failed");
  Require(storage->PreparePrewrite("foreign", "bad", "foreign", 700, 120000, false, 710).status ==
              TxnStatus::LockConflict,
          "foreign owner lock was overwritten");
  std::cout << "PASS pessimistic_prewrite_upgrade\nPASS fixed_primary_validation\n"
               "PASS commit_after_for_update_ts\n";
}

void CheckTypedTxnStatusAndResolution() {
  auto storage = NewStorage();
  TxnRecordStatus status;
  Require(storage->CheckTxnStatus("missing", 1, &status) == TxnStatus::Ok &&
              status.state == TxnRecordState::NotFound,
          "missing transaction status mismatch");

  Require(PreparedPrewrite(storage.get(), "committed", "v", 100) == TxnStatus::Ok,
          "committed seed prewrite failed");
  auto committed = storage->PrepareCommit("committed", 100, 120);
  Require(committed.HasChanges() && storage->ApplyPrepared("committed", committed) == TxnStatus::Ok,
          "committed seed apply failed");
  Require(storage->CheckTxnStatus("committed", 100, &status) == TxnStatus::Ok &&
              status.state == TxnRecordState::Committed && status.commitTs == 120,
          "committed transaction status mismatch");

  auto rollback = storage->PrepareRollback("rolled-back", 200);
  Require(rollback.HasChanges() && storage->ApplyPrepared("rolled-back", rollback) == TxnStatus::Ok,
          "rollback seed apply failed");
  Require(storage->CheckTxnStatus("rolled-back", 200, &status) == TxnStatus::Ok &&
              status.state == TxnRecordState::RolledBack &&
              !storage->FindCommitTs("rolled-back", 200).has_value(),
          "rollback record was treated as a commit");

  auto live = storage->PreparePessimisticLock("live", "live", 300, 120000, 310, 10000);
  Require(live.HasChanges() && storage->ApplyPrepared("live", live) == TxnStatus::Ok,
          "live lock seed failed");
  Require(storage->CheckTxnStatus("live", 300, &status) == TxnStatus::Ok &&
              status.state == TxnRecordState::Locked && status.lock.has_value(),
          "locked transaction status mismatch");
  auto notExpired = storage->PrepareCheckTxnStatus("live", 300, 9999, true);
  Require(notExpired.status == TxnStatus::Ok && !notExpired.HasChanges() &&
              notExpired.txnRecordStatus.state == TxnRecordState::Locked,
          "live lock was rolled back before expiry");
  auto expired = storage->PrepareCheckTxnStatus("live", 300, 10000, true);
  Require(expired.HasChanges() &&
              expired.txnRecordStatus.state == TxnRecordState::RolledBack &&
              storage->ApplyPrepared("live", expired) == TxnStatus::Ok,
          "expired lock was not atomically rolled back");
  Require(storage->CheckTxnStatus("live", 300, &status) == TxnStatus::Ok &&
              status.state == TxnRecordState::RolledBack,
          "expired transaction did not persist rollback status");

  Require(PreparedPrewrite(storage.get(), "secondary-commit", "v", 400) == TxnStatus::Ok,
          "secondary commit prewrite failed");
  auto resolveCommit =
      storage->PrepareResolveLock("secondary-commit", 400, TxnRecordState::Committed, 450);
  Require(resolveCommit.HasChanges() &&
              storage->ApplyPrepared("secondary-commit", resolveCommit) == TxnStatus::Ok,
          "committed resolution failed");
  Require(storage->PrepareResolveLock("secondary-commit", 400, TxnRecordState::Committed, 450).status ==
              TxnStatus::AlreadyCommitted,
          "committed resolution retry was not idempotent");

  Require(PreparedPrewrite(storage.get(), "secondary-rollback", "v", 500) == TxnStatus::Ok,
          "secondary rollback prewrite failed");
  auto resolveRollback =
      storage->PrepareResolveLock("secondary-rollback", 500, TxnRecordState::RolledBack);
  Require(resolveRollback.HasChanges() &&
              storage->ApplyPrepared("secondary-rollback", resolveRollback) == TxnStatus::Ok,
          "rollback resolution failed");
  Require(storage->PrepareResolveLock("secondary-rollback", 500, TxnRecordState::RolledBack).status ==
              TxnStatus::Ok,
          "rollback resolution retry was not idempotent");
  std::cout << "PASS typed_txn_status\nPASS rollback_not_commit\n"
               "PASS expired_primary_atomic_rollback\nPASS idempotent_lock_resolution\n";
}

void CheckPessimisticSchedulerApplyResults() {
  auto scheduler = std::make_shared<NodeTxnScheduler>(64, 2, 64, 64);
  auto region = std::make_shared<MockRegion>(100);
  scheduler->RegisterRegion(region);

  TxnCommand lock;
  lock.type = TxnCommandType::PessimisticLock;
  lock.regionId = 100;
  lock.key = "scheduler-lock";
  lock.keys = {lock.key};
  lock.primaryKey = lock.key;
  lock.clientId = "pessimistic-client";
  lock.requestId = 1;
  lock.startTs = 800;
  lock.forUpdateTs = 810;
  lock.ttlMs = 120000;
  lock.expireAtPhysicalMs = 90000;
  lock.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);

  std::mutex mutex;
  std::condition_variable changed;
  bool finished = false;
  TxnScheduleResult lockResult;
  scheduler->Schedule(lock, [&](const TxnScheduleResult& result) {
    std::lock_guard<std::mutex> guard(mutex);
    lockResult = result;
    finished = true;
    changed.notify_all();
  });
  Require(region->WaitForProposals(1), "pessimistic lock proposal missing");
  {
    std::lock_guard<std::mutex> guard(mutex);
    Require(!finished, "pessimistic lock returned before Raft Apply");
  }
  scheduler->OnApplied(100, region->Apply(0, 1), 1);
  {
    std::unique_lock<std::mutex> guard(mutex);
    Require(changed.wait_for(guard, std::chrono::seconds(1), [&] { return finished; }),
            "pessimistic lock completion missing");
  }
  Require(lockResult.status == std::to_string(static_cast<int>(TxnStatus::Ok)) &&
              lockResult.applied && lockResult.readStatus == TxnStatus::NotFound,
          "pessimistic Apply result lost locking-read metadata");

  TxnCommand check;
  check.type = TxnCommandType::CheckTxnStatus;
  check.regionId = 100;
  check.key = lock.key;
  check.keys = {check.key};
  check.clientId = "pessimistic-client";
  check.requestId = 2;
  check.startTs = 800;
  check.currentPhysicalMs = 80000;
  check.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  bool checked = false;
  TxnScheduleResult checkResult;
  scheduler->Schedule(check, [&](const TxnScheduleResult& result) {
    checkResult = result;
    checked = true;
  });
  for (int attempt = 0; attempt < 100 && !checked; ++attempt)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  Require(checked && checkResult.txnRecordStatus.state == TxnRecordState::Locked,
          "scheduler typed status result mismatch");

  TxnCommand resolve;
  resolve.type = TxnCommandType::ResolveLock;
  resolve.regionId = 100;
  resolve.key = lock.key;
  resolve.keys = {resolve.key};
  resolve.clientId = "pessimistic-client";
  resolve.requestId = 3;
  resolve.startTs = 800;
  resolve.resolutionState = TxnRecordState::RolledBack;
  resolve.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  bool resolved = false;
  scheduler->Schedule(resolve, [&](const TxnScheduleResult& result) {
    resolved = result.status == std::to_string(static_cast<int>(TxnStatus::Ok));
  });
  Require(region->WaitForProposals(2), "resolve proposal missing");
  scheduler->OnApplied(100, region->Apply(1, 2), 2);
  for (int attempt = 0; attempt < 100 && !resolved; ++attempt)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  Require(resolved, "resolve did not complete after Apply");
  Require(resolved, "resolve did not complete after Apply");
  std::cout << "PASS pessimistic_scheduler_apply_response\n"
               "PASS scheduler_typed_status_and_resolve\n";
}

void CheckRegionRestartAndLeaderChange() {
  auto scheduler = std::make_shared<NodeTxnScheduler>(64, 1, 16, 16);
  auto region = std::make_shared<MockRegion>(100);
  scheduler->RegisterRegion(region);

  std::atomic<int> failed{0};
  TxnCommand lock = PrewriteCommand(100, "pending-restart", "client", 1, 10);
  lock.type = TxnCommandType::PessimisticLock;
  lock.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
  scheduler->Schedule(lock, [&](const TxnScheduleResult& result) {
    if (result.status == ErrWrongLeader || result.status == std::to_string(static_cast<int>(TxnStatus::Timeout))) failed.fetch_add(1);
  });

  Require(region->WaitForProposals(1), "proposal must reach region");
  scheduler->OnRegionRemoved(100); // Simulate restart / leader change

  auto newRegion = std::make_shared<MockRegion>(100);
  scheduler->RegisterRegion(newRegion);

  TxnCommand lock2 = PrewriteCommand(100, "pending-restart", "client", 2, 20);
  lock2.type = TxnCommandType::PessimisticLock;
  std::atomic<int> oks{0};
  scheduler->Schedule(lock2, [&](const TxnScheduleResult& result) {
    if (result.status == std::to_string(static_cast<int>(TxnStatus::Ok))) oks.fetch_add(1);
  });

  Require(newRegion->WaitForProposals(1), "new proposal must reach new region");
  scheduler->OnApplied(100, newRegion->Apply(0, 1), 1);
  for (int attempt = 0; attempt < 500 && oks.load() == 0; ++attempt)
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  Require(oks.load() == 1, "task must succeed after restart and timeout");
}

void CheckLegacyLockDowngrade() {
  auto storage = NewStorage();
  auto lock = storage->PreparePessimisticLock("downgrade", "primary", 500, 120000, 550, 50000);
  Require(lock.HasChanges() && storage->ApplyPrepared("downgrade", lock) == TxnStatus::Ok,
          "seed lock failed");

  auto prewrite = storage->PreparePrewrite("downgrade", "value", "primary", 500, 120000, false, 0);
  Require(prewrite.status == TxnStatus::WriteConflict,
          "legacy prewrite without forUpdateTs must not downgrade a V2 lock");
}

}  // namespace

int main() {
  try {
    CheckNodeLatchIsolationAndOrdering();
    CheckRegionExclusiveIsolation();
    CheckLatchMetrics();
    CheckSchedulerPrepareProposeApplyLifecycle();
    CheckTimeoutDoesNotReleaseUnknownProposal();
    CheckAppliedRequestIncludesRegionId();
    CheckWrongLeaderReleasesTask();
    CheckOverloadCallbackCanReenterScheduler();
    CheckMvccTransitions();
    CheckVersionAndApplyProgress();
    CheckPessimisticLockEncodingRecovery();
    CheckPessimisticLockCurrentRead();
    CheckPessimisticPrewriteUpgrade();
    CheckTypedTxnStatusAndResolution();
    CheckPessimisticSchedulerApplyResults();
    CheckRegionRestartAndLeaderChange();
    CheckLegacyLockDowngrade();
    std::cout << "TXN SCHEDULER CHECK PASS\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "TXN SCHEDULER CHECK FAIL: " << error.what() << '\n';
    return 1;
  }
}
