#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "distributed_transaction_coordinator.h"
#include "kv_engine.h"
#include "raft_mvcc_storage.h"
#include "timestamp_oracle.h"

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
  std::vector<std::pair<std::string, std::string>> ScanPrefix(
      const std::string& prefix) override {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::pair<std::string, std::string>> values;
    for (const auto& item : data_) {
      if (item.first.rfind(prefix, 0) == 0) values.push_back(item);
    }
    return values;
  }
  std::string Dump() override { return {}; }
  bool Load(const std::string&) override { return false; }
  void DebugPrint() override {}

 private:
  std::mutex mutex_;
  std::map<std::string, std::string> data_;
};

class TestTimestampOracle final : public TimestampOracle {
 public:
  explicit TestTimestampOracle(uint64_t physicalMs) : physicalMs_(physicalMs) {}

  uint64_t Next() override {
    std::lock_guard<std::mutex> lock(mutex_);
    const uint64_t wall = HlcTimestamp::Compose(physicalMs_, 0);
    highWater_ = std::max(wall, highWater_ + 1);
    return highWater_;
  }
  uint64_t Peek() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::max(HlcTimestamp::Compose(physicalMs_, 0), highWater_);
  }
  void Observe(uint64_t ts) override {
    std::lock_guard<std::mutex> lock(mutex_);
    highWater_ = std::max(highWater_, ts);
  }
  void AdvanceMs(uint64_t delta) {
    std::lock_guard<std::mutex> lock(mutex_);
    physicalMs_ += delta;
  }

 private:
  std::mutex mutex_;
  uint64_t physicalMs_;
  uint64_t highWater_ = 0;
};

class CommitResponseLossStorage final : public MvccStorage {
 public:
  CommitResponseLossStorage() : MvccStorage(std::make_shared<MemoryEngine>()) {}
  void Arm() { armed_ = true; }
  TxnStatus Commit(const std::string& key, uint64_t startTs, uint64_t commitTs) override {
    const TxnStatus applied = MvccStorage::Commit(key, startTs, commitTs);
    if (armed_ && !lost_ && applied == TxnStatus::Ok) {
      lost_ = true;
      return TxnStatus::StorageError;
    }
    return applied;
  }

 private:
  bool armed_ = false;
  bool lost_ = false;
};

class IndeterminateCommitStorage final : public MvccStorage {
 public:
  IndeterminateCommitStorage() : MvccStorage(std::make_shared<MemoryEngine>()) {}
  TxnStatus Commit(const std::string&, uint64_t, uint64_t) override {
    return TxnStatus::StorageError;
  }
  TxnStatus CheckTxnStatus(const std::string&, uint64_t, uint64_t, bool, uint64_t,
                           TxnRecordStatus*) override {
    return TxnStatus::StorageError;
  }
};

class RolledBackCommitStorage final : public MvccStorage {
 public:
  RolledBackCommitStorage() : MvccStorage(std::make_shared<MemoryEngine>()) {}
  TxnStatus Commit(const std::string& key, uint64_t startTs, uint64_t) override {
    const TxnStatus rolledBack = MvccStorage::Rollback(key, startTs);
    return rolledBack == TxnStatus::Ok ? TxnStatus::StorageError : rolledBack;
  }
};

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

struct Fixture {
  explicit Fixture(std::vector<std::shared_ptr<MvccStorage>> supplied = {}) {
    if (supplied.empty()) {
      supplied.push_back(std::make_shared<MvccStorage>(std::make_shared<MemoryEngine>()));
      supplied.push_back(std::make_shared<MvccStorage>(std::make_shared<MemoryEngine>()));
    }
    storages = supplied;
    tso = std::make_shared<TestTimestampOracle>(1700000000000ULL);
    coordinator = std::make_unique<DistributedTransactionCoordinator>(
        std::make_shared<ShardRouter>(storages), tso);
  }

  void PutCommitted(const std::string& key, const std::string& value) {
    Transaction txn = coordinator->Begin();
    txn.Put(key, value);
    Require(coordinator->Commit(&txn) == TxnStatus::Ok, "seed commit must succeed");
  }

  std::string Read(const std::string& key) {
    Transaction txn = coordinator->Begin();
    std::string value;
    Require(coordinator->Get(&txn, key, &value) == TxnStatus::Ok, "read must succeed");
    return value;
  }

  std::vector<std::shared_ptr<MvccStorage>> storages;
  std::shared_ptr<TestTimestampOracle> tso;
  std::unique_ptr<DistributedTransactionCoordinator> coordinator;
};

const std::string kLowKey = std::string(1, static_cast<char>(0x10)) + "-a";
const std::string kHighKey = std::string(1, static_cast<char>(0xe0)) + "-b";

void CheckStateAndPartialCleanup() {
  Fixture fixture;
  fixture.PutCommitted(kLowKey, "1");
  fixture.PutCommitted(kHighKey, "1");

  Transaction holder = fixture.coordinator->Begin();
  Require(fixture.coordinator->LockKeys(&holder, {kHighKey}) == TxnStatus::Ok,
          "holder must lock high key");

  Transaction contender = fixture.coordinator->Begin();
  const BatchLockingReadResult failed =
      fixture.coordinator->BatchGetForUpdate(&contender, {kHighKey, kLowKey, kLowKey});
  Require(failed.status == TxnStatus::LockConflict, "batch must expose lock conflict");
  Require(failed.values.empty(), "failed batch must not expose partial values");
  Require(contender.State() == TransactionState::AbortOnly,
          "failed batch must become abort-only");
  Require(fixture.coordinator->Validate(&contender) == TxnStatus::AbortOnly,
          "abort-only transaction must reject more operations");

  Transaction probe = fixture.coordinator->Begin();
  Require(fixture.coordinator->LockKeys(&probe, {kLowKey}) == TxnStatus::Ok,
          "earlier cross-region lock must be cleaned");
  Require(fixture.coordinator->Rollback(&probe) == TxnStatus::Ok, "probe rollback must succeed");
  Require(fixture.coordinator->Rollback(&holder) == TxnStatus::Ok, "holder rollback must succeed");
  Require(fixture.coordinator->Rollback(&contender) == TxnStatus::Ok,
          "abort-only rollback must be idempotent");
}

void CheckStableLockOnlyPrimary() {
  Fixture fixture;
  fixture.PutCommitted(kLowKey, "guard");
  fixture.PutCommitted(kHighKey, "old");

  Transaction txn = fixture.coordinator->Begin();
  const PessimisticLockResult guard = fixture.coordinator->GetForUpdate(&txn, kLowKey);
  Require(guard.status == TxnStatus::Ok && guard.found && guard.value == "guard",
          "locking read must return current guard value");
  Require(txn.PrimaryKey() == kLowKey, "first successful lock must remain Primary");
  txn.Put(kHighKey, "new");
  Require(txn.PrimaryKey() == kLowKey, "later mutation must not replace Primary");
  Require(fixture.coordinator->Commit(&txn) == TxnStatus::Ok,
          "lock-only Primary transaction must commit");
  Require(fixture.Read(kLowKey) == "guard", "lock-only Primary must not overwrite guard data");
  Require(fixture.Read(kHighKey) == "new", "secondary mutation must commit");

  TxnRecordStatus record;
  Require(fixture.coordinator->QueryStatus(txn, &record) == TxnStatus::Ok &&
              record.state == TxnRecordState::Committed,
          "lock-only Primary must leave authoritative committed status");

  Transaction lockOnly = fixture.coordinator->Begin();
  Require(fixture.coordinator->LockKeys(&lockOnly, {kLowKey, kHighKey}) == TxnStatus::Ok,
          "lock-only transaction must acquire both keys");
  Require(fixture.coordinator->Commit(&lockOnly) == TxnStatus::Ok,
          "lock-only Commit must succeed by releasing locks");
  Transaction after = fixture.coordinator->Begin();
  Require(fixture.coordinator->LockKeys(&after, {kLowKey, kHighKey}) == TxnStatus::Ok,
          "lock-only Commit must release every lock");
  Require(fixture.coordinator->Rollback(&after) == TxnStatus::Ok, "cleanup must succeed");
}

void CheckWriteSkewControlAndProtection() {
  Fixture control;
  control.PutCommitted(kLowKey, "1");
  control.PutCommitted(kHighKey, "1");
  Transaction t1 = control.coordinator->Begin();
  Transaction t2 = control.coordinator->Begin();
  std::string value;
  Require(control.coordinator->Get(&t1, kLowKey, &value) == TxnStatus::Ok && value == "1",
          "t1 must see low=1");
  Require(control.coordinator->Get(&t1, kHighKey, &value) == TxnStatus::Ok && value == "1",
          "t1 must see high=1");
  Require(control.coordinator->Get(&t2, kLowKey, &value) == TxnStatus::Ok && value == "1",
          "t2 must see low=1");
  Require(control.coordinator->Get(&t2, kHighKey, &value) == TxnStatus::Ok && value == "1",
          "t2 must see high=1");
  t1.Put(kLowKey, "0");
  t2.Put(kHighKey, "0");
  Require(control.coordinator->Commit(&t1) == TxnStatus::Ok, "first disjoint write must commit");
  Require(control.coordinator->Commit(&t2) == TxnStatus::Ok, "second disjoint write must commit");
  Require(control.Read(kLowKey) == "0" && control.Read(kHighKey) == "0",
          "SI control must deterministically demonstrate write skew");

  Fixture protectedRun;
  protectedRun.PutCommitted(kLowKey, "1");
  protectedRun.PutCommitted(kHighKey, "1");
  Transaction p1 = protectedRun.coordinator->Begin();
  Transaction p2 = protectedRun.coordinator->Begin();
  const BatchLockingReadResult first =
      protectedRun.coordinator->BatchGetForUpdate(&p1, {kLowKey, kHighKey});
  Require(first.status == TxnStatus::Ok && first.values.size() == 2,
          "first protected transaction must lock shared read set");
  const BatchLockingReadResult second =
      protectedRun.coordinator->BatchGetForUpdate(&p2, {kLowKey, kHighKey});
  Require(second.status == TxnStatus::LockConflict && p2.State() == TransactionState::AbortOnly,
          "second protected transaction must fail fast and retry whole transaction");
  p1.Put(kLowKey, "0");
  Require(protectedRun.coordinator->Commit(&p1) == TxnStatus::Ok,
          "winner must commit protected decision");
  Require(protectedRun.coordinator->Rollback(&p2) == TxnStatus::Ok,
          "loser cleanup must succeed");

  Transaction retry = protectedRun.coordinator->Begin();
  const BatchLockingReadResult retried =
      protectedRun.coordinator->BatchGetForUpdate(&retry, {kLowKey, kHighKey});
  Require(retried.status == TxnStatus::Ok, "retry must lock the fresh state");
  Require(retried.values[0].second.value == "0" && retried.values[1].second.value == "1",
          "retry must observe current protected values");
  Require(protectedRun.coordinator->Commit(&retry) == TxnStatus::Ok,
          "read-only protected retry must release locks");
  Require(protectedRun.Read(kLowKey) == "0" && protectedRun.Read(kHighKey) == "1",
          "shared exact-key locks must preserve the tested invariant");
}

void CheckDeadlineAndUnknownCommit() {
  Fixture timeout;
  Transaction expired = timeout.coordinator->Begin();
  timeout.tso->AdvanceMs(60000);
  Require(timeout.coordinator->Validate(&expired) == TxnStatus::Timeout &&
              expired.State() == TransactionState::AbortOnly,
          "hard transaction deadline must transition to abort-only");

  auto responseLoss = std::make_shared<CommitResponseLossStorage>();
  Fixture recovered({responseLoss});
  responseLoss->Arm();
  Transaction committed = recovered.coordinator->Begin();
  committed.Put("key", "value");
  Require(recovered.coordinator->Commit(&committed) == TxnStatus::Ok,
          "lost Primary response must resolve through committed status");
  Require(recovered.Read("key") == "value", "resolved commit must retain data");

  auto rolledBackStorage = std::make_shared<RolledBackCommitStorage>();
  Fixture rolledBack({rolledBackStorage});
  Transaction rejected = rolledBack.coordinator->Begin();
  rejected.Put("key", "value");
  Require(rolledBack.coordinator->Commit(&rejected) == TxnStatus::WriteConflict &&
              rejected.State() == TransactionState::AbortOnly,
          "authoritative rolled-back Primary must clean up without guessing commit");

  auto unavailable = std::make_shared<IndeterminateCommitStorage>();
  Fixture unknown({unavailable});
  Transaction uncertain = unknown.coordinator->Begin();
  uncertain.Put("key", "value");
  Require(unknown.coordinator->Commit(&uncertain) == TxnStatus::ResultUnknown &&
              uncertain.State() == TransactionState::ResultUnknown,
          "unavailable status check must remain ResultUnknown, never NotFound");
  Require(unknown.coordinator->Rollback(&uncertain) == TxnStatus::ResultUnknown,
          "ResultUnknown must reject blind rollback");
}

void CheckRemoteFailureClassification() {
  RaftMvccStorage unavailable(9999, {{"127.0.0.1", 1}});
  TxnRecordStatus record;
  const TxnStatus query = unavailable.CheckTxnStatus(
      "primary", HlcTimestamp::Compose(1700000000000ULL, 1), 1700000000001ULL,
      false, 1, &record);
  Require(query == TxnStatus::Timeout || query == TxnStatus::StorageError,
          "transport failure must not be returned as transaction NotFound");

  const PessimisticLockResult lock = unavailable.AcquirePessimisticLockForUpdate(
      "key", "key", HlcTimestamp::Compose(1700000000000ULL, 2), 120000,
      HlcTimestamp::Compose(1700000000000ULL, 3), 1700000120000ULL, 1);
  Require(lock.status == TxnStatus::ResultUnknown,
          "ambiguous mutating RPC timeout must remain ResultUnknown");
}

void CheckCoordinatorCrashRecovery() {
  Fixture fixture;
  fixture.PutCommitted(kLowKey, "guard");
  fixture.PutCommitted(kHighKey, "old");

  {
    Transaction crashed = fixture.coordinator->Begin();
    Require(fixture.coordinator->LockKeys(&crashed, {kLowKey, kHighKey}) == TxnStatus::Ok,
            "crashed coordinator must acquire locks");
  }

  fixture.tso->AdvanceMs(300000); 
  Require(fixture.coordinator->ResolveExpiredLocks() > 0, "recovery manager must resolve the expired locks");

  Transaction recovery = fixture.coordinator->Begin();

  Require(fixture.coordinator->LockKeys(&recovery, {kLowKey, kHighKey}) == TxnStatus::Ok,
          "recovery transaction must clean up expired locks and succeed");
          
  Require(fixture.coordinator->Commit(&recovery) == TxnStatus::Ok,
          "recovery transaction must commit");
}

}  // namespace

int main() {
  try {
    CheckStateAndPartialCleanup();
    CheckStableLockOnlyPrimary();
    CheckWriteSkewControlAndProtection();
    CheckDeadlineAndUnknownCommit();
    CheckRemoteFailureClassification();
    CheckCoordinatorCrashRecovery();
    std::cout << "transaction coordinator checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "transaction coordinator checks failed: " << error.what() << std::endl;
    return 1;
  }
}
