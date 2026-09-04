/*
 * 测试目标：验证 A1 性能矩阵所有 24 个点恰好执行，A/B/C/F 读写计数与分位数指标完整。
 * 测试策略：使用内存 FakeAdapter 执行缩小 operation 数的 StandardA1Matrix() 与 SmokeA1Matrix()，
 *           拦截并验证各点的 path、workload、distribution、workers、读写比例与延迟分位数。
 * 测试规模：24 个矩阵点全部执行，每点 100 operations，覆盖 1/4/8/16/32 workers。
 * 验证条件：所有 24 点执行成功且无遗漏；A(50%R/50%W)、B(95%R/5%W)、C(100%R)、F(50%R/50%RMW)
 *           读写计数精确吻合；每点正确计算并输出 attempted/successful OPS 与 P50/P95/P99/Max。
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
    std::cout << "--- Starting A1 Matrix Validation ---" << std::endl;

    const std::vector<perf::A1Point> standardMatrix = perf::StandardA1Matrix();
    Require(standardMatrix.size() == 24, "StandardA1Matrix must contain exactly 24 points");

    std::set<std::string> seenCaseIds;
    int gatewayUniformCount = 0;
    int gatewayZipfianCount = 0;
    int directUniformCount = 0;

    for (const auto& point : standardMatrix) {
      Require(seenCaseIds.insert(point.caseId).second, "Duplicate caseId in matrix: " + point.caseId);
      if (point.path == "gateway" && point.distribution == perf::Distribution::kUniform) {
        ++gatewayUniformCount;
      } else if (point.path == "gateway" && point.distribution == perf::Distribution::kZipfian) {
        ++gatewayZipfianCount;
      } else if (point.path == "direct" && point.distribution == perf::Distribution::kUniform) {
        ++directUniformCount;
      } else {
        Require(false, "Unexpected matrix point specification: " + point.caseId);
      }
    }

    Require(gatewayUniformCount == 20, "Expected 20 Gateway Uniform points (4 workloads x 5 worker counts)");
    Require(gatewayZipfianCount == 2, "Expected 2 Gateway Zipfian points (A/B at workers=8)");
    Require(directUniformCount == 2, "Expected 2 Direct Uniform points (A/C at workers=8)");

    const std::vector<perf::RegionRange> ranges = {{100, "", "h"}, {101, "h", "p"}, {102, "p", ""}};
    const perf::RegionKeyCodec keys(ranges, "a1-check");

    constexpr uint64_t kRecords = 100;
    constexpr uint64_t kOperations = 100;

    auto sharedState = std::make_shared<FakeState>();
    for (uint64_t id = 0; id < kRecords; ++id) {
      sharedState->values[keys.Key(id)] = perf::StableValue(id, 64);
    }

    int executedPoints = 0;
    for (const auto& point : standardMatrix) {
      perf::WorkloadSpec spec = point.ToSpec("interview-full", kRecords, kOperations, 64, 20260904);
      spec.Validate();

      const auto summary = perf::RunRecords(spec, keys, Factory(sharedState), point.caseId);
      ++executedPoints;

      Require(summary.caseId == point.caseId, "Case ID mismatch");
      Require(summary.path == point.path, "Path mismatch");
      Require(summary.workload == perf::WorkloadName(point.workload), "Workload mismatch");
      Require(summary.distribution == perf::DistributionName(point.distribution), "Distribution mismatch");
      Require(summary.workers == point.workers, "Worker count mismatch");
      Require(summary.attempted == kOperations, "Attempted count mismatch");
      Require(summary.successful == kOperations, "Successful count mismatch");
      Require(summary.AttemptedPerSecond() > 0.0, "Attempted OPS must be positive");
      Require(summary.SuccessfulPerSecond() > 0.0, "Successful OPS must be positive");

      if (point.workload == perf::Workload::kA) {
        Require(summary.reads == 50 && summary.updates == 50 && summary.readModifyWrites == 0,
                "Workload A must have 50 reads and 50 updates per 100 ops");
      } else if (point.workload == perf::Workload::kB) {
        Require(summary.reads == 95 && summary.updates == 5 && summary.readModifyWrites == 0,
                "Workload B must have 95 reads and 5 updates per 100 ops");
      } else if (point.workload == perf::Workload::kC) {
        Require(summary.reads == 100 && summary.updates == 0 && summary.readModifyWrites == 0,
                "Workload C must have 100 reads and 0 updates per 100 ops");
      } else if (point.workload == perf::Workload::kF) {
        Require(summary.reads == 50 && summary.updates == 0 && summary.readModifyWrites == 50,
                "Workload F must have 50 reads and 50 RMWs per 100 ops");
      }

      Require(summary.latency.Percentile(0.95) >= summary.latency.Percentile(0.50), "P95 < P50");
      Require(summary.latency.Percentile(0.99) >= summary.latency.Percentile(0.95), "P99 < P95");
      Require(summary.latency.Max() >= summary.latency.Percentile(0.99), "Max < P99");

      std::cout << "[A1 Point " << executedPoints << "/24] caseId=" << point.caseId
                << " path=" << point.path
                << " wl=" << perf::WorkloadName(point.workload)
                << " dist=" << perf::DistributionName(point.distribution)
                << " w=" << point.workers
                << " attempted_ops=" << summary.attempted
                << " successful_ops=" << summary.successful
                << " attempted_ops/s=" << summary.AttemptedPerSecond()
                << " successful_ops/s=" << summary.SuccessfulPerSecond()
                << " reads=" << summary.reads
                << " updates=" << summary.updates
                << " rmws=" << summary.readModifyWrites
                << " P50=" << summary.latency.Percentile(0.50) << "us"
                << " P95=" << summary.latency.Percentile(0.95) << "us"
                << " P99=" << summary.latency.Percentile(0.99) << "us"
                << " Max=" << summary.latency.Max() << "us" << std::endl;

      const std::string json = summary.ToJson("record");
      Require(json.find("\"attempted_per_second\":") != std::string::npos, "JSON missing attempted_per_second");
      Require(json.find("\"successful_per_second\":") != std::string::npos, "JSON missing successful_per_second");
      Require(json.find("\"p50\":") != std::string::npos, "JSON missing p50");
      Require(json.find("\"p95\":") != std::string::npos, "JSON missing p95");
      Require(json.find("\"p99\":") != std::string::npos, "JSON missing p99");
      Require(json.find("\"max\":") != std::string::npos, "JSON missing max");

      const std::string kv = summary.ToKeyValues("record");
      Require(kv.find("attempted_per_second=") != std::string::npos, "KV missing attempted_per_second");
      Require(kv.find("successful_per_second=") != std::string::npos, "KV missing successful_per_second");
      Require(kv.find("latency_p50_us=") != std::string::npos, "KV missing latency_p50_us");
      Require(kv.find("latency_p95_us=") != std::string::npos, "KV missing latency_p95_us");
      Require(kv.find("latency_p99_us=") != std::string::npos, "KV missing latency_p99_us");
      Require(kv.find("latency_max_us=") != std::string::npos, "KV missing latency_max_us");
    }
    Require(executedPoints == 24, "Not all 24 points were executed");

    const std::vector<perf::A1Point> smokeMatrix = perf::SmokeA1Matrix();
    Require(smokeMatrix.size() == 4, "SmokeA1Matrix must contain exactly 4 points");
    for (const auto& point : smokeMatrix) {
      perf::WorkloadSpec spec = point.ToSpec("interview-smoke", kRecords, kOperations, 64, 20260904);
      spec.Validate();
      const auto summary = perf::RunRecords(spec, keys, Factory(sharedState), point.caseId);
      Require(summary.successful == kOperations, "Smoke point did not succeed");
    }

    std::cout << "--- All 24 A1 Matrix Points & 4 Smoke Points Verified Successfully ---" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "a1_matrix_check failed: " << error.what() << '\n';
    return 1;
  }
}
