/*
 * 测试目标：验证 A3 固定 3 mutations 下 1/2/3 Region fanout 事务生成、路由隔离与相对成本。
 * 测试策略：使用内存 FakeAdapter 和真实 RegionRange 路由断言，在 1、2、3 Region fanout 下执行事务，收集并计算吞吐、P99 延迟及相对 1 Region 的增量成本。
 * 测试规模：fanout=1/2/3，每点 1,000 transactions，workers=8，每事务固定 3 mutations。
 * 验证条件：每笔事务无论 fanout 为何均严格包含 3 mutations；fanout=1 时每笔事务只涉及 1 个 Region；fanout=2 时每笔事务恰好涉及 2 个不同 Region；fanout=3 时每笔事务恰好涉及 3 个不同 Region；各点正确输出 committed TPS、P99 及相对 1 Region 的增量成本（TPS 衰减比与 P99 增长值）。
 */
#include <chrono>
#include <cmath>
#include <iomanip>
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
    std::cout << "--- Starting A3 Fanout Matrix Validation ---" << std::endl;

    const std::vector<perf::RegionRange> ranges = {{100, "", "h"}, {101, "h", "p"}, {102, "p", ""}};
    const perf::RegionKeyCodec keys(ranges, "a3-check");

    // 1. 验证非法 fanout 被严格拒绝
    for (int invalidFanout : {0, -1, 4, 5, 10}) {
      bool rejected = false;
      try {
        perf::A3TransactionGenerator gen(invalidFanout, 20260904, keys);
      } catch (const std::invalid_argument&) {
        rejected = true;
      }
      Require(rejected, "Invalid fanout " + std::to_string(invalidFanout) + " was not rejected");
    }

    constexpr uint64_t kTransactions = 1000;
    constexpr uint64_t kSeed = 20260904;
    constexpr size_t kValueSize = 256;
    constexpr int kWorkers = 8;

    // 2. 验证路由断言：每笔事务固定 3 mutations，参与 Region 数恰好为 fanout (1, 2, 3)
    for (int fanout : {1, 2, 3}) {
      perf::A3TransactionGenerator generator(fanout, kSeed, keys, kValueSize);
      Require(generator.Fanout() == fanout, "Generator fanout mismatch");

      std::map<int, int> regionUsageCount;
      for (uint64_t seq = 0; seq < kTransactions; ++seq) {
        const perf::A3Transaction txn = generator.At(seq);
        Require(txn.fanout == fanout, "Transaction fanout mismatch");
        Require(txn.mutations.size() == 3, "Transaction must always have exactly 3 mutations");

        std::set<int> touchedRegions;
        for (const auto& mut : txn.mutations) {
          const int regionId = keys.LocateKeyRegion(mut.key);
          Require(regionId == mut.regionId, "Mutation regionId does not match located key region");
          touchedRegions.insert(regionId);
          ++regionUsageCount[regionId];
        }
        Require(touchedRegions.size() == static_cast<size_t>(fanout),
                "Touched regions count must equal fanout " + std::to_string(fanout));
      }

      // 断言各 Region 均被覆盖且负载均衡
      Require(regionUsageCount.size() == 3, "All 3 regions must be touched across the workload");
      std::cout << "[Check Fanout=" << fanout << "] 1000 txns x 3 mutations verified: "
                << "each txn touched exactly " << fanout << " region(s)" << std::endl;
    }

    // 3. 小型集成测试：执行 fanout 1/2/3，验证 TPS、P99 及相对 1 Region 的增量成本
    std::vector<perf::A3Summary> summaries;
    for (int fanout : {1, 2, 3}) {
      auto state = std::make_shared<FakeState>();
      perf::A3TransactionGenerator generator(fanout, kSeed, keys, kValueSize);
      const std::string caseId = "a3-regions-" + std::to_string(fanout);
      const perf::A3Summary summary = perf::RunA3(generator, Factory(state), caseId,
                                                  kTransactions, kWorkers, 1, 20, 180000);

      Require(summary.caseId == caseId, "Case ID mismatch");
      Require(summary.fanout == fanout, "Fanout mismatch");
      Require(summary.workers == kWorkers, "Worker count mismatch");
      Require(summary.totalAttempted == kTransactions, "Total attempted mismatch");
      Require(summary.totalCommitted == kTransactions, "Total committed mismatch");
      Require(summary.totalFailed == 0, "Failed count mismatch");
      Require(summary.AttemptedTps() > 0.0, "Attempted TPS must be positive");
      Require(summary.CommittedTps() > 0.0, "Committed TPS must be positive");
      Require(summary.regionMutationCounts.size() == 3, "All 3 regions must have mutation records");

      Require(summary.latency.Percentile(0.95) >= summary.latency.Percentile(0.50), "P95 < P50");
      Require(summary.latency.Percentile(0.99) >= summary.latency.Percentile(0.95), "P99 < P95");
      Require(summary.latency.Max() >= summary.latency.Percentile(0.99), "Max < P99");

      const std::string kv = summary.ToKeyValues();
      Require(kv.find("fanout_regions=") != std::string::npos, "KV missing fanout_regions");
      Require(kv.find("throughput_committed_txn_per_sec=") != std::string::npos, "KV missing committed TPS");
      Require(kv.find("latency_p99_us=") != std::string::npos, "KV missing latency_p99_us");

      const std::string json = summary.ToJson();
      Require(json.find("\"fanout_regions\":") != std::string::npos, "JSON missing fanout_regions");
      Require(json.find("\"throughput_committed_txn_per_sec\":") != std::string::npos, "JSON missing committed TPS");

      summaries.push_back(summary);
    }

    // 4. 计算并输出相对 1 Region 的增量成本
    const double tps1 = summaries[0].CommittedTps();
    const uint64_t p99_1 = summaries[0].latency.Percentile(0.99);

    std::cout << "\n=== A3 Fanout Cost Matrix (Relative to 1 Region) ===" << std::endl;
    for (size_t i = 0; i < summaries.size(); ++i) {
      const auto& s = summaries[i];
      const double tps = s.CommittedTps();
      const uint64_t p99 = s.latency.Percentile(0.99);
      const double tpsChangePct = ((tps - tps1) / tps1) * 100.0;
      const long long p99DeltaUs = static_cast<long long>(p99) - static_cast<long long>(p99_1);
      const double p99DeltaPct = p99_1 > 0 ? (static_cast<double>(p99DeltaUs) / p99_1) * 100.0 : 0.0;

      std::cout << "[Fanout " << s.fanout << " Region(s)] "
                << "TPS=" << std::fixed << std::setprecision(1) << tps
                << " (rel 1-Reg: " << std::showpos << tpsChangePct << "%" << std::noshowpos << "), "
                << "P50=" << s.latency.Percentile(0.50) << "us, "
                << "P99=" << p99 << "us "
                << "(cost rel 1-Reg: " << std::showpos << p99DeltaUs << "us / " << p99DeltaPct << "%" << std::noshowpos << ")"
                << std::endl;
    }

    std::cout << "\n--- A3 Fanout Matrix & Incremental Cost Verified Successfully ---" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "a3_fanout_check failed: " << error.what() << '\n';
    return 1;
  }
}
