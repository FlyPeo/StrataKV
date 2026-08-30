#include <atomic>
#include <chrono>
#include <memory>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "distributed_transaction_coordinator.h"

namespace {

class TestTimestampOracle final : public TimestampOracle {
 public:
  uint64_t Next() override { return next_.fetch_add(1, std::memory_order_relaxed) + 1; }
  uint64_t Peek() override { return next_.load(std::memory_order_relaxed); }
  void Observe(uint64_t timestamp) override {
    uint64_t current = next_.load(std::memory_order_relaxed);
    while (current < timestamp &&
           !next_.compare_exchange_weak(current, timestamp, std::memory_order_relaxed)) {}
  }

 private:
  std::atomic<uint64_t> next_{HlcTimestamp::Compose(1700000000000ULL, 0)};
};

struct SharedConcurrency {
  std::atomic<int> active{0};
  std::atomic<int> maximum{0};
};

class RecordingStorage final : public MvccStorage {
 public:
  RecordingStorage(std::shared_ptr<SharedConcurrency> concurrency, TxnStatus prewriteStatus,
                   uint32_t protocolVersion)
      : MvccStorage(nullptr),
        concurrency_(std::move(concurrency)),
        prewriteStatus_(prewriteStatus),
        protocolVersion_(protocolVersion) {}

  TxnStatus Prewrite(const std::string&, const std::string&, const std::string&, uint64_t,
                     uint64_t, uint64_t, uint64_t) override {
    ++singlePrewriteCalls;
    return prewriteStatus_;
  }

  TxnStatus PrewriteDelete(const std::string&, const std::string&, uint64_t, uint64_t,
                           uint64_t, uint64_t) override {
    ++singlePrewriteCalls;
    return prewriteStatus_;
  }

  TxnStatus PrewriteLock(const std::string&, const std::string&, uint64_t, uint64_t,
                         uint64_t, uint64_t) override {
    ++singlePrewriteCalls;
    return prewriteStatus_;
  }

  TxnStatus BatchPrewrite(const std::vector<MvccMutation>& mutations, const std::string&,
                          uint64_t, uint64_t, uint64_t, uint64_t) override {
    ++prewriteCalls;
    prewriteKeys = mutations.size();
    const int active = concurrency_->active.fetch_add(1, std::memory_order_relaxed) + 1;
    int maximum = concurrency_->maximum.load(std::memory_order_relaxed);
    while (maximum < active && !concurrency_->maximum.compare_exchange_weak(
                                   maximum, active, std::memory_order_relaxed)) {}
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    concurrency_->active.fetch_sub(1, std::memory_order_relaxed);
    return prewriteStatus_;
  }

  TxnStatus Commit(const std::string&, uint64_t, uint64_t) override {
    ++primaryCommitCalls;
    return TxnStatus::Ok;
  }

  TxnStatus BatchCommit(const std::vector<std::string>& keys, uint64_t, uint64_t,
                        uint64_t) override {
    ++batchCommitCalls;
    commitKeys += keys.size();
    return TxnStatus::Ok;
  }

  TxnStatus BatchRollback(const std::vector<std::string>& keys, uint64_t, uint64_t) override {
    ++rollbackCalls;
    rollbackKeys += keys.size();
    return TxnStatus::Ok;
  }

  TxnStatus Rollback(const std::string&, uint64_t) override {
    ++rollbackCalls;
    ++rollbackKeys;
    return TxnStatus::Ok;
  }

  ProtocolCapabilities Capabilities() override {
    ProtocolCapabilities capabilities;
    capabilities.protocolVersion = protocolVersion_;
    return capabilities;
  }

  std::atomic<size_t> singlePrewriteCalls{0};
  std::atomic<size_t> prewriteCalls{0};
  std::atomic<size_t> prewriteKeys{0};
  std::atomic<size_t> primaryCommitCalls{0};
  std::atomic<size_t> batchCommitCalls{0};
  std::atomic<size_t> commitKeys{0};
  std::atomic<size_t> rollbackCalls{0};
  std::atomic<size_t> rollbackKeys{0};

 private:
  std::shared_ptr<SharedConcurrency> concurrency_;
  TxnStatus prewriteStatus_;
  uint32_t protocolVersion_;
};

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

struct Fixture {
  explicit Fixture(int failingRegion = -1,
                   uint32_t protocolVersion = kBatchTxnProtocolVersion) {
    concurrency = std::make_shared<SharedConcurrency>();
    for (int region = 0; region < 3; ++region) {
      storages.push_back(std::make_shared<RecordingStorage>(
          concurrency, region == failingRegion ? TxnStatus::WriteConflict : TxnStatus::Ok,
          protocolVersion));
    }
    const std::vector<RegionPeerLocation> peers{{0, "127.0.0.1", 1}};
    std::vector<ShardRouter::RegionRoute> routes{
        {{10, "", "m", peers}, storages[0]},
        {{20, "m", "t", peers}, storages[1]},
        {{30, "t", "", peers}, storages[2]},
    };
    coordinator = std::make_unique<DistributedTransactionCoordinator>(
        std::make_shared<ShardRouter>(std::move(routes)), std::make_shared<TestTimestampOracle>());
  }

  Transaction SixKeyTransaction() {
    Transaction transaction = coordinator->Begin();
    transaction.Put("a", "1");
    transaction.Put("b", "2");
    transaction.Put("n", "3");
    transaction.Put("o", "4");
    transaction.Put("u", "5");
    transaction.Put("v", "6");
    return transaction;
  }

  std::shared_ptr<SharedConcurrency> concurrency;
  std::vector<std::shared_ptr<RecordingStorage>> storages;
  std::unique_ptr<DistributedTransactionCoordinator> coordinator;
};

void CheckGroupingAndParallelism() {
  Fixture fixture;
  Transaction transaction = fixture.SixKeyTransaction();
  Require(fixture.coordinator->Commit(&transaction) == TxnStatus::Ok,
          "three-Region batch transaction must commit");
  size_t prewriteCalls = 0;
  size_t prewriteKeys = 0;
  size_t batchCommitCalls = 0;
  size_t secondaryKeys = 0;
  for (const auto& storage : fixture.storages) {
    prewriteCalls += storage->prewriteCalls.load();
    prewriteKeys += storage->prewriteKeys.load();
    batchCommitCalls += storage->batchCommitCalls.load();
    secondaryKeys += storage->commitKeys.load();
  }
  Require(prewriteCalls == 3 && prewriteKeys == 6,
          "six mutations must become exactly three Region prewrite calls");
  Require(fixture.concurrency->maximum.load() >= 2,
          "different Region prewrites must overlap on the bounded executor");
  Require(fixture.storages[0]->primaryCommitCalls.load() == 1 &&
              batchCommitCalls == 3 && secondaryKeys == 5,
          "Primary must commit first and all five secondaries must use Region batches");
}

void CheckPartialPrewriteCleanup() {
  Fixture fixture(1);
  Transaction transaction = fixture.SixKeyTransaction();
  Require(fixture.coordinator->Commit(&transaction) == TxnStatus::WriteConflict,
          "one failed Region must fail prewrite after cleanup");
  size_t rollbackCalls = 0;
  size_t rollbackKeys = 0;
  for (const auto& storage : fixture.storages) {
    rollbackCalls += storage->rollbackCalls.load();
    rollbackKeys += storage->rollbackKeys.load();
  }
  Require(rollbackCalls == 3 && rollbackKeys == 6,
          "all possibly-prewritten Regions must receive one batch rollback");
}

void CheckProtocolV2FallsBackToSingleKeyRpcs() {
  Fixture fixture(-1, kPessimisticTxnProtocolVersion);
  Transaction transaction = fixture.SixKeyTransaction();
  Require(fixture.coordinator->Commit(&transaction) == TxnStatus::Ok,
          "protocol v2 transaction must use the compatible path");
  size_t singlePrewrites = 0;
  size_t batchPrewrites = 0;
  size_t singleCommits = 0;
  size_t batchCommits = 0;
  for (const auto& storage : fixture.storages) {
    singlePrewrites += storage->singlePrewriteCalls.load();
    batchPrewrites += storage->prewriteCalls.load();
    singleCommits += storage->primaryCommitCalls.load();
    batchCommits += storage->batchCommitCalls.load();
  }
  Require(singlePrewrites == 6 && batchPrewrites == 0,
          "protocol v2 must fall back to six single-key prewrites");
  Require(singleCommits == 6 && batchCommits == 0,
          "protocol v2 must fall back to Primary-first single-key commits");
}

}  // namespace

int main() {
  try {
    CheckGroupingAndParallelism();
    CheckPartialPrewriteCleanup();
    CheckProtocolV2FallsBackToSingleKeyRpcs();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Region batch coordinator check failed: " << error.what() << std::endl;
    return 1;
  }
}
