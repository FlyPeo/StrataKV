/*
 * 测试目标：验证 A2 本地与跨 Region 事务混合生成器、路由隔离及统计分桶。
 * 测试策略：使用内存 FakeAdapter 和真实 RegionRange 路由断言，在 0%、15%、100% 比例下执行 3 mutations 事务，收集并校验实际比例、分桶吞吐与延迟。
 * 测试规模：比例 0/15/100%，每点 1,000 transactions，workers=8，每事务 3 mutations。
 * 验证条件：除 crossPercent 比例外参数完全相同；0% 时 100% 本地且在 3 Region 间轮换；15% 时精确 15% 跨 Region/85% 本地；100% 时 100% 跨三 Region；本地事务每笔只涉及 1 Region，跨 Region 事务涉及全部 3 Region；local/distributed 分桶统计与 TPS/延迟完整输出。
 */
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "support/performance/performance_support.h"
#include "support/performance/workload_runner.h"

namespace perf = stratakv::test::performance;

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

struct FakeState {
  std::mutex mutex;
  std::map<std::string, std::string> values;
  int commits = 0;
  int rollbacks = 0;
};

class FakeTransaction final : public perf::TransactionHandle {
 public:
  std::map<std::string, std::string> writes;
};

class FakeAdapter final : public perf::ClientAdapter {
 public:
  explicit FakeAdapter(std::shared_ptr<FakeState> state) : state_(std::move(state)) {}

  std::unique_ptr<perf::TransactionHandle> Begin(uint64_t) override {
    return std::make_unique<FakeTransaction>();
  }

  perf::AdapterResult Get(perf::TransactionHandle* transaction, const std::string& key) override {
    auto* fake = dynamic_cast<FakeTransaction*>(transaction);
    if (fake == nullptr) return {perf::AdapterStatus::kInvalid};
    const auto pending = fake->writes.find(key);
    if (pending != fake->writes.end()) return {perf::AdapterStatus::kOk, pending->second, true};
    std::lock_guard<std::mutex> lock(state_->mutex);
    const auto found = state_->values.find(key);
    if (found == state_->values.end()) return {perf::AdapterStatus::kNotFound, {}, false};
    return {perf::AdapterStatus::kOk, found->second, true};
  }

  perf::AdapterResult Put(perf::TransactionHandle* transaction, const std::string& key,
                          const std::string& value) override {
    auto* fake = dynamic_cast<FakeTransaction*>(transaction);
    if (fake == nullptr) return {perf::AdapterStatus::kInvalid};
    fake->writes[key] = value;
    return {perf::AdapterStatus::kOk};
  }

  perf::AdapterResult Commit(perf::TransactionHandle* transaction) override {
    auto* fake = dynamic_cast<FakeTransaction*>(transaction);
    if (fake == nullptr) return {perf::AdapterStatus::kInvalid};
    std::lock_guard<std::mutex> lock(state_->mutex);
    for (const auto& item : fake->writes) state_->values[item.first] = item.second;
    ++state_->commits;
    return {perf::AdapterStatus::kOk};
  }

  perf::AdapterResult Rollback(perf::TransactionHandle*) override {
    std::lock_guard<std::mutex> lock(state_->mutex);
    ++state_->rollbacks;
    return {perf::AdapterStatus::kOk};
  }

 private:
  std::shared_ptr<FakeState> state_;
};

perf::AdapterFactory Factory(const std::shared_ptr<FakeState>& state) {
  return [state]() { return std::make_unique<FakeAdapter>(state); };
}

}  // namespace

int main() {
  try {
    std::cout << "--- Starting A2 Transaction Mixed Generator Validation ---" << std::endl;

    const std::vector<perf::RegionRange> ranges = {{100, "", "h"}, {101, "h", "p"}, {102, "p", ""}};
    const perf::RegionKeyCodec keys(ranges, "a2-check");

    // 1. 验证非法比例被严格拒绝
    for (int invalidRatio : {-1, 5, 10, 50, 99, 101}) {
      bool rejected = false;
      try {
        perf::A2TransactionGenerator gen(invalidRatio, 20260904, keys);
      } catch (const std::invalid_argument&) {
        rejected = true;
      }
      Require(rejected, "Invalid cross-region ratio " + std::to_string(invalidRatio) + " was not rejected");
    }

    constexpr uint64_t kTransactions = 1000;
    constexpr uint64_t kSeed = 20260904;
    constexpr size_t kValueSize = 256;
    constexpr int kWorkers = 8;

    // 2. 验证路由断言与生成器行为
    // 2a. 0% 比例: 全部 1000 笔均为本地事务，并在 3 个 Region 间轮换
    {
      perf::A2TransactionGenerator gen0(0, kSeed, keys, kValueSize);
      std::map<int, int> regionTxnCounts;
      for (uint64_t seq = 0; seq < kTransactions; ++seq) {
        const perf::A2Transaction txn = gen0.At(seq);
        Require(!txn.isDistributed, "Transaction in 0% mix must be local");
        Require(txn.mutations.size() == 3, "Each transaction must contain exactly 3 mutations");
        Require(txn.targetRegionIndex >= 0 && txn.targetRegionIndex < 3, "Invalid target region index");
        Require(txn.targetRegionId == ranges[txn.targetRegionIndex].regionId, "Target region ID mismatch");

        // 断言本地事务所有 3 个 key 均严格位于同一 Region
        for (const auto& mut : txn.mutations) {
          const int located = keys.LocateKeyRegion(mut.key);
          Require(located == txn.targetRegionId, "Local mutation key routed to wrong region");
          Require(mut.regionId == txn.targetRegionId, "Mutation regionId field mismatch");
        }
        ++regionTxnCounts[txn.targetRegionId];
      }
      Require(regionTxnCounts[100] == 334, "Region 100 count mismatch in 0% rotation");
      Require(regionTxnCounts[101] == 333, "Region 101 count mismatch in 0% rotation");
      Require(regionTxnCounts[102] == 333, "Region 102 count mismatch in 0% rotation");
      std::cout << "[Check 0%] 1000 local txns rotated across 3 regions (334/333/333), 100% single-region" << std::endl;
    }

    // 2b. 15% 比例: 恰好 150 笔跨 Region (各访问 3 Region), 850 笔本地 (单 Region)
    {
      perf::A2TransactionGenerator gen15(15, kSeed, keys, kValueSize);
      int distributedCount = 0;
      int localCount = 0;
      for (uint64_t seq = 0; seq < kTransactions; ++seq) {
        const perf::A2Transaction txn = gen15.At(seq);
        Require(txn.mutations.size() == 3, "Each transaction must contain exactly 3 mutations");
        if (txn.isDistributed) {
          ++distributedCount;
          Require(txn.targetRegionIndex == -1 && txn.targetRegionId == -1, "Distributed txn has targetRegion");
          std::set<int> touchedRegions;
          for (size_t i = 0; i < 3; ++i) {
            const int located = keys.LocateKeyRegion(txn.mutations[i].key);
            Require(located == ranges[i].regionId, "Distributed mutation key does not match designated region");
            touchedRegions.insert(located);
          }
          Require(touchedRegions.size() == 3, "Distributed transaction must touch all 3 distinct regions");
        } else {
          ++localCount;
          for (const auto& mut : txn.mutations) {
            Require(keys.LocateKeyRegion(mut.key) == txn.targetRegionId, "Local mutation key in wrong region");
          }
        }
      }
      Require(distributedCount == 150, "Expected exactly 150 distributed transactions for 15% ratio");
      Require(localCount == 850, "Expected exactly 850 local transactions for 15% ratio");
      std::cout << "[Check 15%] Exact ratio verified: 150 distributed (3 regions) / 850 local (1 region)" << std::endl;
    }

    // 2c. 100% 比例: 全部 1000 笔均为跨 3 Region 事务
    {
      perf::A2TransactionGenerator gen100(100, kSeed, keys, kValueSize);
      for (uint64_t seq = 0; seq < kTransactions; ++seq) {
        const perf::A2Transaction txn = gen100.At(seq);
        Require(txn.isDistributed, "Transaction in 100% mix must be distributed");
        Require(txn.mutations.size() == 3, "Each transaction must contain exactly 3 mutations");
        std::set<int> touchedRegions;
        for (const auto& mut : txn.mutations) {
          touchedRegions.insert(keys.LocateKeyRegion(mut.key));
        }
        Require(touchedRegions.size() == 3, "100% mix transaction must touch all 3 distinct regions");
      }
      std::cout << "[Check 100%] 1000 distributed txns verified, 100% cross-3-regions" << std::endl;
    }

    // 3. 小型集成测试：用 FakeAdapter 执行三种比例，验证 local/distributed 分桶统计与 TPS/延迟
    std::vector<perf::A2Summary> summaries;
    for (int ratio : {0, 15, 100}) {
      auto state = std::make_shared<FakeState>();
      perf::A2TransactionGenerator generator(ratio, kSeed, keys, kValueSize);
      const std::string caseId = "a2-cross-" + std::to_string(ratio);
      const perf::A2Summary summary = perf::RunA2(generator, Factory(state), caseId,
                                                  kTransactions, kWorkers, 1, 20, 180000);

      Require(summary.caseId == caseId, "Case ID mismatch");
      Require(summary.targetCrossPercent == ratio, "Target cross percent mismatch");
      Require(summary.workers == kWorkers, "Worker count mismatch");
      Require(summary.totalAttempted == kTransactions, "Total attempted mismatch");
      Require(summary.totalCommitted == kTransactions, "Total committed mismatch");
      Require(summary.totalFailed == 0, "Failed count mismatch");
      Require(summary.TotalAttemptedTps() > 0.0, "Total attempted TPS must be positive");
      Require(summary.TotalCommittedTps() > 0.0, "Total committed TPS must be positive");

      if (ratio == 0) {
        Require(summary.localAttempted == kTransactions, "0% localAttempted mismatch");
        Require(summary.localCommitted == kTransactions, "0% localCommitted mismatch");
        Require(summary.distributedAttempted == 0, "0% distributedAttempted must be 0");
        Require(summary.distributedCommitted == 0, "0% distributedCommitted must be 0");
        Require(summary.ActualCrossRatio() == 0.0, "0% actual cross ratio mismatch");
        Require(summary.LocalCommittedTps() > 0.0, "Local committed TPS must be positive");
        Require(summary.DistributedCommittedTps() == 0.0, "Distributed committed TPS must be 0");
      } else if (ratio == 15) {
        Require(summary.localAttempted == 850, "15% localAttempted mismatch");
        Require(summary.localCommitted == 850, "15% localCommitted mismatch");
        Require(summary.distributedAttempted == 150, "15% distributedAttempted mismatch");
        Require(summary.distributedCommitted == 150, "15% distributedCommitted mismatch");
        Require(std::abs(summary.ActualCrossRatio() - 0.15) < 1e-6, "15% actual cross ratio mismatch");
        Require(summary.LocalCommittedTps() > 0.0, "Local committed TPS must be positive");
        Require(summary.DistributedCommittedTps() > 0.0, "Distributed committed TPS must be positive");
      } else if (ratio == 100) {
        Require(summary.localAttempted == 0, "100% localAttempted must be 0");
        Require(summary.localCommitted == 0, "100% localCommitted must be 0");
        Require(summary.distributedAttempted == kTransactions, "100% distributedAttempted mismatch");
        Require(summary.distributedCommitted == kTransactions, "100% distributedCommitted mismatch");
        Require(summary.ActualCrossRatio() == 1.0, "100% actual cross ratio mismatch");
        Require(summary.LocalCommittedTps() == 0.0, "Local committed TPS must be 0");
        Require(summary.DistributedCommittedTps() > 0.0, "Distributed committed TPS must be positive");
      }

      Require(summary.totalLatency.Percentile(0.95) >= summary.totalLatency.Percentile(0.50), "P95 < P50");
      Require(summary.totalLatency.Percentile(0.99) >= summary.totalLatency.Percentile(0.95), "P99 < P95");
      Require(summary.totalLatency.Max() >= summary.totalLatency.Percentile(0.99), "Max < P99");

      const std::string kv = summary.ToKeyValues();
      Require(kv.find("target_cross_percent=") != std::string::npos, "KV missing target_cross_percent");
      Require(kv.find("actual_cross_ratio=") != std::string::npos, "KV missing actual_cross_ratio");
      Require(kv.find("local_committed=") != std::string::npos, "KV missing local_committed");
      Require(kv.find("distributed_committed=") != std::string::npos, "KV missing distributed_committed");
      Require(kv.find("throughput_committed_txn_per_sec=") != std::string::npos, "KV missing throughput_committed_txn_per_sec");

      const std::string json = summary.ToJson();
      Require(json.find("\"target_cross_percent\":") != std::string::npos, "JSON missing target_cross_percent");
      Require(json.find("\"actual_cross_ratio\":") != std::string::npos, "JSON missing actual_cross_ratio");
      Require(json.find("\"local\":") != std::string::npos, "JSON missing local bucket");
      Require(json.find("\"distributed\":") != std::string::npos, "JSON missing distributed bucket");

      std::cout << "[A2 Integration " << caseId << "] target_ratio=" << ratio << "%"
                << " actual_ratio=" << summary.ActualCrossRatio()
                << " total_tps=" << summary.TotalCommittedTps()
                << " local_committed=" << summary.localCommitted
                << " local_tps=" << summary.LocalCommittedTps()
                << " dist_committed=" << summary.distributedCommitted
                << " dist_tps=" << summary.DistributedCommittedTps()
                << " P50=" << summary.totalLatency.Percentile(0.50) << "us"
                << " P99=" << summary.totalLatency.Percentile(0.99) << "us" << std::endl;

      summaries.push_back(summary);
    }

    // 4. 验证除跨 Region 比例外参数完全相同
    for (size_t i = 1; i < summaries.size(); ++i) {
      Require(summaries[i].workers == summaries[0].workers, "Workers must be identical");
      Require(summaries[i].totalAttempted == summaries[0].totalAttempted, "Total attempted must be identical");
    }

    std::cout << "--- A2 Transaction Generator & Bucketed Metrics Verified Successfully ---" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "a2_transaction_check failed: " << error.what() << '\n';
    return 1;
  }
}
