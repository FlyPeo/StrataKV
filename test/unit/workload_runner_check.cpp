/*
 * 测试目标：验证公共 workload runner 的事务边界、幂等 Load、并发计数和重试计时。
 * 测试策略：用内存 fake adapter 记录 Begin/Get/Put/Commit/Rollback，并注入一次 timeout。
 * 测试规模：300 条记录重复 Load 两次，A/C/F 各 1,000 operations，重试场景 1 operation。
 * 验证内容：键空间不扩大、读写计数精确、成功/尝试分离、重试次数与退避进入端到端延迟。
 */
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

#include "support/performance/workload_runner.h"

namespace perf = stratakv::test::performance;

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

struct FakeState {
  std::mutex mutex;
  std::map<std::string, std::string> values;
  std::vector<std::string> operationLog;
  int commits = 0;
  int rollbacks = 0;
  bool failNextCommit = false;
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
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      state_->operationLog.push_back("GET:" + key);
    }
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
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      state_->operationLog.push_back("PUT:" + key + "=" + value);
    }
    fake->writes[key] = value;
    return {perf::AdapterStatus::kOk};
  }

  perf::AdapterResult Commit(perf::TransactionHandle* transaction) override {
    auto* fake = dynamic_cast<FakeTransaction*>(transaction);
    if (fake == nullptr) return {perf::AdapterStatus::kInvalid};
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->failNextCommit) {
      state_->failNextCommit = false;
      return {perf::AdapterStatus::kTimeout, {}, false, true, "injected timeout"};
    }
    state_->values.insert(fake->writes.begin(), fake->writes.end());
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
    auto state = std::make_shared<FakeState>();
    perf::RegionKeyCodec keys({{100, "", "h"}, {101, "h", "p"}, {102, "p", ""}}, "runner-check");
    perf::WorkloadSpec spec;
    spec.recordCount = 300;
    spec.operationCount = 1000;
    spec.valueSize = 64;
    spec.workers = 4;

    const auto firstLoad = perf::LoadRecords(spec, keys, Factory(state), "load-1");
    const auto secondLoad = perf::LoadRecords(spec, keys, Factory(state), "load-2");
    Require(firstLoad.successful == 300 && secondLoad.successful == 300, "Load did not succeed");
    Require(state->values.size() == 300, "duplicate Load enlarged the key space");

    spec.workload = perf::Workload::kA;
    const auto a = perf::RunRecords(spec, keys, Factory(state), "A");
    Require(a.attempted == 1000 && a.successful == 1000 && a.reads == 500 && a.updates == 500,
            "A counters are wrong");

    spec.workload = perf::Workload::kC;
    const auto c = perf::RunRecords(spec, keys, Factory(state), "C");
    Require(c.successful == 1000 && c.reads == 1000 && c.updates == 0, "C counters are wrong");

    spec.workload = perf::Workload::kF;
    const auto f = perf::RunRecords(spec, keys, Factory(state), "F");
    Require(f.successful == 1000 && f.reads == 500 && f.readModifyWrites == 500, "F counters are wrong");

    perf::WorkloadSpec retrySpec = spec;
    retrySpec.workload = perf::Workload::kA;
    retrySpec.seed = 0;  // sequence 0 is a read, so use sequence rotation below by choosing B? no write needed.
    retrySpec.operationCount = 1;
    retrySpec.workers = 1;
    retrySpec.maxAttempts = 2;
    retrySpec.retryDelayMs = 10;
    retrySpec.workload = perf::Workload::kF;
    retrySpec.seed = 50;  // sequence 0 becomes RMW and commits.
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->failNextCommit = true;
    }
    const auto retryStarted = std::chrono::steady_clock::now();
    const auto retried = perf::RunRecords(retrySpec, keys, Factory(state), "retry");
    const auto retryElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - retryStarted)
                                  .count();
    Require(retried.successful == 1 && retried.retries == 1, "retry was not classified correctly");
    Require(retryElapsed >= 10 && retried.latency.Max() >= 10000, "retry backoff is outside end-to-end latency");

    // 验证两路径在相同参数下生成完全一致的操作序列
    auto directState = std::make_shared<FakeState>();
    auto gatewayState = std::make_shared<FakeState>();
    perf::WorkloadSpec pathSpec = spec;
    pathSpec.workers = 1;
    pathSpec.operationCount = 200;
    pathSpec.workload = perf::Workload::kA;
    pathSpec.path = "direct";
    perf::RunRecords(pathSpec, keys, Factory(directState), "path-direct");
    pathSpec.path = "gateway";
    perf::RunRecords(pathSpec, keys, Factory(gatewayState), "path-gateway");
    Require(!directState->operationLog.empty(), "operation log must not be empty");
    Require(directState->operationLog.size() == gatewayState->operationLog.size() &&
            directState->operationLog == gatewayState->operationLog,
            "two paths did not produce the identical operation sequence");

    // 验证 Scan / E 在施压前被拒绝
    bool scanRejected = false;
    try {
      (void)perf::ParseWorkload("E");
    } catch (const std::invalid_argument&) {
      scanRejected = true;
    }
    Require(scanRejected, "Scan/E workload was not rejected before applying pressure");

    bool scanWordRejected = false;
    try {
      (void)perf::ParseWorkload("scan");
    } catch (const std::invalid_argument&) {
      scanWordRejected = true;
    }
    Require(scanWordRejected, "Scan keyword was not rejected before applying pressure");

    perf::WorkloadSpec invalidSpec = spec;
    invalidSpec.path = "invalid_path";
    bool invalidPathRejected = false;
    try {
      invalidSpec.Validate();
    } catch (const std::invalid_argument&) {
      invalidPathRejected = true;
    }
    Require(invalidPathRejected, "Invalid path was not rejected by WorkloadSpec");

    return 0;
  } catch (const std::exception& error) {
    std::cerr << "workload runner check failed: " << error.what() << '\n';
    return 1;
  }
}
