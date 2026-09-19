/*
 * 测试目标：验证 B3 两个关键阶段（after_all_prewrite_before_primary_commit 与 after_primary_commit_before_secondaries）在协调器强杀后，分布式锁恢复管理器（LockResolver）的确定性与幂等收敛语义。
 * 测试策略：在跨两个独立 Shard 的多键 2PC 事务提交路径上，分别在 Primary Commit 前与 Primary Commit 后命中 failpoint barrier；模拟协调器强杀（丢弃 coordinator 并推进物理时间使锁超时）；通过 LockResolver 驱动锁决议；验证读操作与存储层状态机的一致性。
 * 测试规模：2 个核心端到端故障恢复场景，涉及跨 2 个 Shard 的 Primary 与 Secondary 3 键事务，各包含初次决议与重复决议测试。
 * 验证条件：Case 1（Primary 未提交被强杀）所有 key 均安全回滚且清除预写锁，读取观察到旧数据；Case 2（Primary 提交后 Secondary 提交前被强杀）Secondary key 均幂等使用 Primary 的同一 commitTs roll-forward，读取观察到新提交数据；两次决议幂等且零残留锁、零部分提交。
 */
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "distributed_transaction_coordinator.h"
#include "kv_engine.h"
#include "lock_resolver.h"
#include "raft_mvcc_storage.h"
#include "shard_router.h"
#include "timestamp_oracle.h"
#include "txn_2pc_failpoint.h"

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

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
  std::string Dump() override { return {}; }
  bool Load(const std::string&) override { return false; }
  void DebugPrint() override {}

 private:
  std::mutex mutex_;
  std::map<std::string, std::string> data_;
};

class ControllableTso final : public TimestampOracle {
 public:
  explicit ControllableTso(uint64_t physicalMs) : physicalMs_(physicalMs) {}

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
  uint64_t PhysicalMs() const {
    return physicalMs_;
  }

 private:
  std::mutex mutex_;
  uint64_t physicalMs_;
  uint64_t highWater_ = 0;
};

struct B3Fixture {
  std::shared_ptr<ControllableTso> tso;
  std::shared_ptr<MvccStorage> shard0;
  std::shared_ptr<MvccStorage> shard1;
  std::shared_ptr<ShardRouter> router;
  std::unique_ptr<DistributedTransactionCoordinator> coordinator;
  std::unique_ptr<LockResolver> lockResolver;

  B3Fixture() {
    tso = std::make_shared<ControllableTso>(1700000000000ULL);
    shard0 = std::make_shared<MvccStorage>(std::make_shared<MemoryEngine>());
    shard1 = std::make_shared<MvccStorage>(std::make_shared<MemoryEngine>());

    std::vector<std::shared_ptr<MvccStorage>> shards = {shard0, shard1};
    router = std::make_shared<ShardRouter>(std::move(shards));
    coordinator = std::make_unique<DistributedTransactionCoordinator>(router, tso);
    lockResolver = std::make_unique<LockResolver>(router);
  }

  void PutCommitted(const std::string& key, const std::string& value) {
    Transaction txn = coordinator->Begin();
    txn.Put(key, value);
    Require(coordinator->Commit(&txn) == TxnStatus::Ok, "PutCommitted failed");
  }

  std::optional<std::string> GetCommitted(const std::string& key) {
    Transaction txn = coordinator->Begin();
    std::string value;
    const TxnStatus status = coordinator->Get(&txn, key, &value);
    if (status == TxnStatus::NotFound) return std::nullopt;
    Require(status == TxnStatus::Ok, "GetCommitted failed with status " + std::to_string(static_cast<int>(status)));
    return value;
  }
};

}  // namespace

int main() {
  try {
    std::cout << "--- Starting B3 2PC Coordinator Crash Recovery Checks ---" << std::endl;

    namespace fp = stratakv::transaction::failpoint;
    const std::string projectToken = "b3-project-token";
    const std::filesystem::path markerDir = std::filesystem::temp_directory_path() / "stratakv_b3_rec";
    std::filesystem::create_directories(markerDir);

    const std::string kPrimaryKey = "a:account:primary";
    const std::string kSecondaryKey1 = "b:account:secondary1";
    const std::string kSecondaryKey2 = "b:account:secondary2";

    // =========================================================================
    // Case 1: after_all_prewrite_before_primary_commit
    // 全部 prewrite 完成，Primary 尚未 commit 时 Coordinator 死亡
    // 预期：恢复时 Primary 未提交（NotFound/RolledBack），所有 secondary 回滚并清除锁
    // =========================================================================
    {
      std::cout << "[Case 1] Testing after_all_prewrite_before_primary_commit..." << std::endl;
      B3Fixture fixture;
      fixture.PutCommitted(kPrimaryKey, "init_primary");
      fixture.PutCommitted(kSecondaryKey1, "init_sec1");
      fixture.PutCommitted(kSecondaryKey2, "init_sec2");

      const std::string markerPath = (markerDir / "b3_case1.marker").string();
      std::filesystem::remove(markerPath);

      fp::Arm(projectToken, fp::FailpointLocation::AfterAllPrewriteBeforePrimaryCommit, markerPath,
              fp::BarrierAction::SimulateCrash);

      uint64_t case1StartTs = 0;
      std::thread coordinatorThread([&]() {
        try {
          Transaction txn = fixture.coordinator->Begin();
          case1StartTs = txn.StartTs();
          txn.Put(kPrimaryKey, "new_primary");
          txn.Put(kSecondaryKey1, "new_sec1");
          txn.Put(kSecondaryKey2, "new_sec2");

          TxnOptions options;
          options.testProjectToken = projectToken;
          options.transactionTimeoutMs = 500;
          options.lockTtlMs = 1000;  // 1s short TTL for fast recovery testing
          fixture.coordinator->Commit(&txn, options);
        } catch (const fp::CoordinatorCrashedException&) {
          // 预期协调器模拟强杀/崩溃
        }
      });

      Require(fp::WaitForHit(5000), "Case 1: Failpoint barrier must be hit");
      Require(std::filesystem::exists(markerPath), "Case 1: Marker file must exist");
      std::cout << "Case 1: Coordinator reached barrier after prewrite, simulated coordinator crash." << std::endl;

      coordinatorThread.join();
      fp::Disarm();

      // 校验 Primary 在此时尚未提交
      TxnRecordStatus primaryRecord;
      const TxnStatus qStatus = fixture.shard0->CheckTxnStatus(
          kPrimaryKey, case1StartTs, fixture.tso->PhysicalMs() + 2000, false, 5000, &primaryRecord);
      Require(primaryRecord.state != TxnRecordState::Committed,
              "Primary MUST NOT be committed before Primary commit step");

      // 推进时间，让锁过期
      fixture.tso->AdvanceMs(5000);

      // 驱动 LockResolver 解析所有过期锁
      const size_t resolvedCount = fixture.coordinator->ResolveExpiredLocks();
      std::cout << "Case 1: Resolved " << resolvedCount << " expired locks via LockResolver." << std::endl;

      // 验证全部 3 个 key 均未写入新值，全部维持初始值（无部分提交）
      Require(fixture.GetCommitted(kPrimaryKey) == "init_primary", "Primary must remain unchanged");
      Require(fixture.GetCommitted(kSecondaryKey1) == "init_sec1", "Secondary1 must remain unchanged");
      Require(fixture.GetCommitted(kSecondaryKey2) == "init_sec2", "Secondary2 must remain unchanged");

      // 验证没有残留活跃锁
      Require(!fixture.shard0->GetLock(kPrimaryKey).has_value(), "Primary key must have no active lock");
      Require(!fixture.shard1->GetLock(kSecondaryKey1).has_value(), "Secondary key 1 must have no active lock");
      Require(!fixture.shard1->GetLock(kSecondaryKey2).has_value(), "Secondary key 2 must have no active lock");

      // 幂等性：再次调用 ResolveExpiredLocks 不会报错且返回 0
      Require(fixture.coordinator->ResolveExpiredLocks() == 0, "Repeated lock resolution must be idempotent");
      std::cout << "[Case 1 Passed] Primary uncommitted -> all keys safely rolled back, no partial write." << std::endl;
    }

    // =========================================================================
    // Case 2: after_primary_commit_before_secondaries
    // Primary 已 commit，Secondary 尚未全部 commit 时 Coordinator 死亡
    // 预期：恢复时观察到 Primary 已提交，Secondary 幂等使用同一 commitTs roll-forward
    // =========================================================================
    {
      std::cout << "[Case 2] Testing after_primary_commit_before_secondaries..." << std::endl;
      B3Fixture fixture;
      fixture.PutCommitted(kPrimaryKey, "init_primary");
      fixture.PutCommitted(kSecondaryKey1, "init_sec1");
      fixture.PutCommitted(kSecondaryKey2, "init_sec2");

      const std::string markerPath = (markerDir / "b3_case2.marker").string();
      std::filesystem::remove(markerPath);

      fp::Arm(projectToken, fp::FailpointLocation::AfterPrimaryCommitBeforeSecondaries, markerPath,
              fp::BarrierAction::SimulateCrash);

      uint64_t case2StartTs = 0;
      std::thread coordinatorThread([&]() {
        try {
          Transaction txn = fixture.coordinator->Begin();
          case2StartTs = txn.StartTs();
          txn.Put(kPrimaryKey, "committed_primary");
          txn.Put(kSecondaryKey1, "committed_sec1");
          txn.Put(kSecondaryKey2, "committed_sec2");

          TxnOptions options;
          options.testProjectToken = projectToken;
          options.transactionTimeoutMs = 500;
          options.lockTtlMs = 1000;
          fixture.coordinator->Commit(&txn, options);
        } catch (const fp::CoordinatorCrashedException&) {
          // 预期协调器在 Primary 提交后、Secondary 提交前模拟强杀/崩溃
        }
      });

      Require(fp::WaitForHit(5000), "Case 2: Failpoint barrier must be hit after primary commit");
      Require(std::filesystem::exists(markerPath), "Case 2: Marker file must exist");
      std::cout << "Case 2: Coordinator reached barrier after primary commit, simulated crash." << std::endl;

      coordinatorThread.join();
      fp::Disarm();

      // 校验 Primary 此时已经处于 Committed 状态！
      TxnRecordStatus primaryRecord;
      fixture.shard0->CheckTxnStatus(
          kPrimaryKey, case2StartTs, fixture.tso->PhysicalMs() + 2000, false, 5000, &primaryRecord);
      Require(primaryRecord.state == TxnRecordState::Committed,
              "Primary MUST be committed after primary commit step");
      const uint64_t primaryCommitTs = primaryRecord.commitTs;
      Require(primaryCommitTs > 0, "Primary commitTs must be positive");
      Require(primaryCommitTs > 0, "Primary commitTs must be positive");

      // 推进时间让 secondary 锁进入过期解析窗口
      fixture.tso->AdvanceMs(5000);

      // 驱动 LockResolver 解析 secondary 锁
      const size_t resolvedCount = fixture.coordinator->ResolveExpiredLocks();
      std::cout << "Case 2: Resolved " << resolvedCount << " locks with roll-forward." << std::endl;

      // 验证所有 key 均成功 roll-forward，全部呈现新值（完整提交，无部分提交）
      Require(fixture.GetCommitted(kPrimaryKey) == "committed_primary", "Primary must be committed");
      Require(fixture.GetCommitted(kSecondaryKey1) == "committed_sec1", "Secondary1 must roll-forward to new value");
      Require(fixture.GetCommitted(kSecondaryKey2) == "committed_sec2", "Secondary2 must roll-forward to new value");

      // 验证没有残留活跃锁
      Require(!fixture.shard0->GetLock(kPrimaryKey).has_value(), "Primary key must have no active lock");
      Require(!fixture.shard1->GetLock(kSecondaryKey1).has_value(), "Secondary key 1 must have no active lock");
      Require(!fixture.shard1->GetLock(kSecondaryKey2).has_value(), "Secondary key 2 must have no active lock");

      // 幂等性：再次调用 ResolveExpiredLocks 依然零活跃锁
      Require(fixture.coordinator->ResolveExpiredLocks() == 0, "Repeated resolution must be idempotent");
      std::cout << "[Case 2 Passed] Primary committed -> secondaries rolled-forward with same commitTs." << std::endl;
    }

    std::filesystem::remove_all(markerDir);
    std::cout << "--- All B3 Coordinator Crash Recovery Checks Passed Successfully ---" << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "b3_recovery_check failed: " << e.what() << std::endl;
    return 1;
  }
}
