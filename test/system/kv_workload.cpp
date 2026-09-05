/*
 * 测试目标：在真实三 Region 集群上执行 YCSB A/B/C/F、A1 性能矩阵、C1 转账和 C2 register history。
 * 测试策略：复用公共 Direct/Gateway adapter；Load 与 Run 分离，支持完整 A1 矩阵与单点执行，
 *           C1 检查守恒，C2 按 quiescent epoch 对带 invocation/completion 时间的历史做有界线性化搜索。
 * 测试规模：interview-smoke 为 3,000×256 B、A/C 各 10,000 ops、1,000 transfers、
 *           300 register ops；interview-full 为 100,000×1 KiB、A1 完整 24 点 20,000 ops。
 * 验证内容：verify 全量检查 checkpoint 的初始键值；成功/失败吞吐与延迟口径明确、转账守恒、register 历史可线性化。
 */
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "support/performance/performance_support.h"
#include "support/performance/workload_runner.h"
#include "support/performance/linearizability_checker.h"

namespace perf = stratakv::test::performance;

namespace {

struct Options {
  std::string mode = "run";
  std::string path = "gateway";
  std::string gateway = "http://127.0.0.1:18080";
  std::string regionsConfig;
  std::string tsoEndpoints = "127.0.0.1:26380,127.0.0.1:26381,127.0.0.1:26382";
  std::string profile = "interview-smoke";
  std::string runId = "smoke";
  std::string caseId = "case";
  std::string workload = "A";
  std::string distribution = "uniform";
  std::string output;
  std::string history;
  uint64_t records = 3000;
  uint64_t operations = 10000;
  size_t valueSize = 256;
  uint64_t seed = 20260904;
  int workers = 1;
  int maxAttempts = 1;
  int retryDelayMs = 20;
  int timeoutMs = 180000;
};

uint64_t Positive(const std::string& value, const std::string& option) {
  size_t consumed = 0;
  const unsigned long long parsed = std::stoull(value, &consumed);
  if (consumed != value.size() || parsed == 0) throw std::invalid_argument(option + " must be positive");
  return static_cast<uint64_t>(parsed);
}

void Usage(const char* program) {
  std::cout << "Usage: " << program << " --mode load|verify|run|a1-matrix|transfer|register [options]\n"
            << "  --path gateway|direct --regions-config PATH --run-id ID --case-id ID\n"
            << "  --gateway URL --tso-endpoints CSV --profile NAME\n"
            << "  --workload A|B|C|F --distribution uniform|zipfian\n"
            << "  --records N --operations N --value-size N --workers N --seed N\n"
            << "  --max-attempts N --retry-delay-ms N --timeout-ms N\n"
            << "  --output PATH [--history PATH]\n";
}

Options Parse(int argc, char** argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    const std::string option = argv[index];
    if (option == "--help" || option == "-h") {
      Usage(argv[0]);
      std::exit(0);
    }
    if (index + 1 >= argc) throw std::invalid_argument("missing value for " + option);
    const std::string value = argv[++index];
    if (option == "--mode") options.mode = value;
    else if (option == "--path") options.path = value;
    else if (option == "--gateway") options.gateway = value;
    else if (option == "--regions-config") options.regionsConfig = value;
    else if (option == "--tso-endpoints") options.tsoEndpoints = value;
    else if (option == "--profile") options.profile = value;
    else if (option == "--run-id") options.runId = value;
    else if (option == "--case-id") options.caseId = value;
    else if (option == "--workload") options.workload = value;
    else if (option == "--distribution") options.distribution = value;
    else if (option == "--output") options.output = value;
    else if (option == "--history") options.history = value;
    else if (option == "--records") options.records = Positive(value, option);
    else if (option == "--operations") options.operations = Positive(value, option);
    else if (option == "--value-size") options.valueSize = static_cast<size_t>(Positive(value, option));
    else if (option == "--workers") options.workers = static_cast<int>(Positive(value, option));
    else if (option == "--seed") options.seed = Positive(value, option);
    else if (option == "--max-attempts") options.maxAttempts = static_cast<int>(Positive(value, option));
    else if (option == "--retry-delay-ms") options.retryDelayMs = static_cast<int>(Positive(value, option));
    else if (option == "--timeout-ms") options.timeoutMs = static_cast<int>(Positive(value, option));
    else throw std::invalid_argument("unknown option " + option);
  }
  if (options.regionsConfig.empty() || options.output.empty()) {
    throw std::invalid_argument("--regions-config and --output are required");
  }
  if (options.mode != "load" && options.mode != "verify" && options.mode != "run" && options.mode != "a1-matrix" &&
      options.mode != "transfer" && options.mode != "register") {
    throw std::invalid_argument("--mode must be load, verify, run, a1-matrix, transfer, or register");
  }
  if (options.path != "gateway" && options.path != "direct") {
    throw std::invalid_argument("--path must be gateway or direct");
  }
  return options;
}

perf::WorkloadSpec MakeSpec(const Options& options) {
  perf::WorkloadSpec spec;
  spec.profile = options.profile;
  spec.path = options.path;
  spec.workload = perf::ParseWorkload(options.workload);
  spec.distribution = perf::ParseDistribution(options.distribution);
  spec.seed = options.seed;
  spec.recordCount = options.records;
  spec.operationCount = options.operations;
  spec.valueSize = options.valueSize;
  spec.workers = options.workers;
  spec.maxAttempts = options.maxAttempts;
  spec.retryDelayMs = options.retryDelayMs;
  spec.timeoutMs = options.timeoutMs;
  spec.Validate();
  return spec;
}

perf::AdapterFactory MakeFactory(const Options& options) {
  if (options.path == "gateway") return perf::GatewayAdapterFactory(options.gateway, options.timeoutMs);
  return perf::DirectAdapterFactory(options.regionsConfig, options.tsoEndpoints);
}

void Publish(const Options& options, const perf::RunSummary& summary, const std::string& subject,
             const std::string& extraJson = {}) {
  std::string json = summary.ToJson(subject);
  if (!extraJson.empty()) {
    const size_t closing = json.rfind("\n}\n");
    if (closing == std::string::npos) throw std::runtime_error("invalid summary JSON");
    json.insert(closing, ",\n" + extraJson);
  }
  std::filesystem::path outputPath = options.output;
  if (std::filesystem::is_directory(outputPath) ||
      (!outputPath.has_extension() && options.mode == "a1-matrix")) {
    std::filesystem::create_directories(outputPath);
    outputPath = outputPath / (options.caseId + ".json");
  }
  perf::AtomicWrite(outputPath, json);
  std::cout << summary.ToKeyValues(subject);
}

perf::AdapterResult ReadOne(perf::ClientAdapter* adapter, const std::string& key) {
  auto transaction = adapter->Begin(120000);
  perf::AdapterResult result = adapter->Get(transaction.get(), key);
  const perf::AdapterResult rollback = adapter->Rollback(transaction.get());
  if (result.ok() && !rollback.ok()) return rollback;
  return result;
}

perf::AdapterResult WriteOne(perf::ClientAdapter* adapter, const std::string& key,
                             const std::string& value) {
  auto transaction = adapter->Begin(120000);
  perf::AdapterResult result = adapter->Put(transaction.get(), key, value);
  if (!result.ok()) {
    (void)adapter->Rollback(transaction.get());
    return result;
  }
  return adapter->Commit(transaction.get());
}

bool Retriable(const perf::AdapterResult& result) {
  return result.retryable || result.status == perf::AdapterStatus::kConflict ||
         result.status == perf::AdapterStatus::kTimeout || result.status == perf::AdapterStatus::kUnavailable;
}

struct TransferRecord {
  uint64_t sequence = 0;
  int source = 0;
  int destination = 0;
  bool acknowledged = false;
  uint64_t latencyUs = 0;
};

perf::RunSummary RunTransfers(const Options& options, const perf::RegionKeyCodec& keys,
                              const perf::AdapterFactory& factory) {
  constexpr int kAccounts = 30;
  constexpr long long kInitialBalance = 100000;
  auto initializer = factory();
  for (int account = 0; account < kAccounts; ++account) {
    const perf::AdapterResult result = WriteOne(initializer.get(), keys.Key(static_cast<uint64_t>(account)),
                                                std::to_string(kInitialBalance));
    if (!result.ok()) throw std::runtime_error("cannot initialize transfer account " + std::to_string(account));
  }

  std::atomic<uint64_t> next{0};
  std::atomic<uint64_t> successful{0};
  std::atomic<uint64_t> retries{0};
  std::atomic<uint64_t> conflicts{0};
  std::atomic<uint64_t> failures{0};
  std::vector<TransferRecord> history(static_cast<size_t>(options.operations));
  std::vector<perf::Histogram> histograms(static_cast<size_t>(options.workers));
  const auto runStarted = std::chrono::steady_clock::now();
  std::vector<std::thread> workers;
  for (int workerIndex = 0; workerIndex < options.workers; ++workerIndex) {
    workers.emplace_back([&, workerIndex]() {
      auto adapter = factory();
      while (true) {
        const uint64_t sequence = next.fetch_add(1);
        if (sequence >= options.operations) return;
        TransferRecord& record = history[static_cast<size_t>(sequence)];
        record.sequence = sequence;
        record.source = static_cast<int>(sequence % kAccounts);
        record.destination = static_cast<int>((sequence * 7U + 1U) % kAccounts);
        if (record.destination == record.source) record.destination = (record.destination + 1) % kAccounts;
        const auto started = std::chrono::steady_clock::now();
        for (int attempt = 1; attempt <= options.maxAttempts; ++attempt) {
          auto transaction = adapter->Begin(120000);
          const std::string sourceKey = keys.Key(static_cast<uint64_t>(record.source));
          const std::string destinationKey = keys.Key(static_cast<uint64_t>(record.destination));
          const perf::AdapterResult source = adapter->Get(transaction.get(), sourceKey);
          const perf::AdapterResult destination = adapter->Get(transaction.get(), destinationKey);
          perf::AdapterResult result = source.ok() ? destination : source;
          if (result.ok()) {
            const long long sourceBalance = std::stoll(source.value);
            const long long destinationBalance = std::stoll(destination.value);
            if (sourceBalance <= 0) {
              result = {perf::AdapterStatus::kInvalid, {}, false, false, "source balance exhausted"};
            } else {
              result = adapter->Put(transaction.get(), sourceKey, std::to_string(sourceBalance - 1));
              if (result.ok()) {
                result = adapter->Put(transaction.get(), destinationKey, std::to_string(destinationBalance + 1));
              }
              if (result.ok()) result = adapter->Commit(transaction.get());
            }
          }
          if (!result.ok()) (void)adapter->Rollback(transaction.get());
          if (result.ok()) {
            record.acknowledged = true;
            successful.fetch_add(1);
            break;
          }
          if (result.status == perf::AdapterStatus::kConflict) conflicts.fetch_add(1);
          if (!Retriable(result) || attempt == options.maxAttempts) {
            failures.fetch_add(1);
            break;
          }
          retries.fetch_add(1);
          std::this_thread::sleep_for(std::chrono::milliseconds(options.retryDelayMs * attempt));
        }
        record.latencyUs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                     std::chrono::steady_clock::now() - started)
                                                     .count());
        histograms[static_cast<size_t>(workerIndex)].Record(record.latencyUs);
      }
    });
  }
  for (auto& worker : workers) worker.join();

  long long total = 0;
  for (int account = 0; account < kAccounts; ++account) {
    const auto result = ReadOne(initializer.get(), keys.Key(static_cast<uint64_t>(account)));
    if (!result.ok()) throw std::runtime_error("cannot verify transfer account " + std::to_string(account));
    total += std::stoll(result.value);
  }
  const long long expectedTotal = kAccounts * kInitialBalance;

  if (!options.history.empty()) {
    std::ostringstream output;
    for (const auto& record : history) {
      output << "{\"type\":\"transfer\",\"sequence\":" << record.sequence
             << ",\"source\":" << record.source << ",\"destination\":" << record.destination
             << ",\"amount\":1,\"acknowledged\":" << (record.acknowledged ? "true" : "false")
             << ",\"latency_us\":" << record.latencyUs << "}\n";
    }
    perf::AtomicWrite(options.history, output.str());
  }

  perf::RunSummary summary;
  summary.caseId = options.caseId;
  summary.path = options.path;
  summary.workload = "transfer";
  summary.distribution = "deterministic";
  summary.workers = options.workers;
  summary.attempted = options.operations;
  summary.successful = successful.load();
  summary.conflicts = conflicts.load();
  summary.unavailable = failures.load();
  summary.retries = retries.load();
  summary.elapsedSeconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - runStarted).count();
  for (const auto& histogram : histograms) summary.latency.Merge(histogram);
  Publish(options, summary, "transaction",
          "  \"invariant\":{\"name\":\"account_total\",\"expected\":" +
              std::to_string(expectedTotal) + ",\"actual\":" + std::to_string(total) +
              ",\"passed\":" + (total == expectedTotal && failures.load() == 0 ? "true" : "false") + "}");
  if (total != expectedTotal || failures.load() != 0) throw std::runtime_error("transfer invariant failed");
  return summary;
}

uint64_t NowNs() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::steady_clock::now().time_since_epoch())
                                   .count());
}

perf::AdapterResult ExecuteRegister(perf::ClientAdapter* adapter, bool write, const std::string& key,
                                    const std::string& input, std::string* output) {
  auto transaction = adapter->Begin(120000);
  perf::AdapterResult result;
  if (write) {
    result = adapter->Put(transaction.get(), key, input);
    if (result.ok()) result = adapter->Commit(transaction.get());
  } else {
    result = adapter->Get(transaction.get(), key);
    if (result.ok()) *output = result.value;
    const auto rollback = adapter->Rollback(transaction.get());
    if (result.ok() && !rollback.ok()) result = rollback;
  }
  if (!result.ok()) (void)adapter->Rollback(transaction.get());
  return result;
}

perf::RunSummary RunRegister(const Options& options, const perf::RegionKeyCodec& keys,
                             const perf::AdapterFactory& factory) {
  const std::string key = keys.Key(0);
  auto boundaryAdapter = factory();
  if (!WriteOne(boundaryAdapter.get(), key, "0").ok()) throw std::runtime_error("cannot initialize register");
  std::string initialValue = "0";
  bool checkerPassed = true;
  bool checkerInconclusive = false;
  std::vector<perf::RegisterOperation> history;
  history.reserve(static_cast<size_t>(options.operations));
  perf::Histogram latency;
  const auto runStarted = std::chrono::steady_clock::now();

  constexpr size_t kEpochLimit = 8;
  for (uint64_t base = 0; base < options.operations; base += kEpochLimit) {
    const size_t epochSize = static_cast<size_t>(std::min<uint64_t>(kEpochLimit, options.operations - base));
    std::vector<perf::RegisterOperation> epoch(epochSize);
    std::atomic<size_t> ready{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    for (size_t offset = 0; offset < epochSize; ++offset) {
      threads.emplace_back([&, offset]() {
        auto adapter = factory();
        perf::RegisterOperation& operation = epoch[offset];
        operation.sequence = base + offset;
        operation.write = operation.sequence % 2U == 0;
        operation.input = operation.write ? std::to_string(operation.sequence + 1U) : "";
        ready.fetch_add(1);
        while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
        operation.invokeNs = NowNs();
        const auto started = std::chrono::steady_clock::now();
        for (int attempt = 1; attempt <= options.maxAttempts; ++attempt) {
          const perf::AdapterResult result =
              ExecuteRegister(adapter.get(), operation.write, key, operation.input, &operation.output);
          if (result.ok()) {
            operation.success = true;
            break;
          }
          operation.unknown = result.status == perf::AdapterStatus::kResultUnknown;
          if (!Retriable(result) || attempt == options.maxAttempts) break;
          std::this_thread::sleep_for(std::chrono::milliseconds(options.retryDelayMs * attempt));
        }
        operation.completeNs = NowNs();
        const uint64_t elapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                           std::chrono::steady_clock::now() - started)
                                                           .count());
        static std::mutex histogramMutex;
        std::lock_guard<std::mutex> lock(histogramMutex);
        latency.Record(elapsed);
      });
    }
    while (ready.load() != epochSize) std::this_thread::yield();
    go.store(true, std::memory_order_release);
    for (auto& thread : threads) thread.join();

    const perf::AdapterResult boundary = ReadOne(boundaryAdapter.get(), key);
    if (!boundary.ok()) throw std::runtime_error("cannot read register epoch boundary");
    perf::LinearizabilityChecker checker;
    const auto res = checker.CheckEpoch(epoch, initialValue, boundary.value);
    if (res.verdict == perf::LinearizabilityVerdict::kInconclusive) {
      checkerInconclusive = true;
    } else if (res.verdict == perf::LinearizabilityVerdict::kFail) {
      checkerPassed = false;
    }
    initialValue = boundary.value;
    history.insert(history.end(), epoch.begin(), epoch.end());
  }

  if (!options.history.empty()) {
    perf::LinearizabilityChecker::WriteHistoryJsonl(options.history, history);
  }

  const uint64_t successful = static_cast<uint64_t>(std::count_if(
      history.begin(), history.end(), [](const auto& operation) { return operation.success; }));
  perf::RunSummary summary;
  summary.caseId = options.caseId;
  summary.path = options.path;
  summary.workload = "register";
  summary.distribution = "deterministic";
  summary.workers = std::min<int>(options.workers, static_cast<int>(kEpochLimit));
  summary.attempted = history.size();
  summary.successful = successful;
  summary.unavailable = history.size() - successful;
  summary.elapsedSeconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - runStarted).count();
  summary.latency.Merge(latency);
  const std::string checker = checkerInconclusive ? "inconclusive" : checkerPassed ? "pass" : "fail";
  Publish(options, summary, "register_operation",
          "  \"linearizability\":{\"checker\":\"bounded-register-v1\",\"result\":\"" + checker + "\"}");
  if (!checkerPassed || checkerInconclusive) throw std::runtime_error("register history checker result=" + checker);
  return summary;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const Options options = Parse(argc, argv);
    const perf::WorkloadSpec spec = MakeSpec(options);
    const std::vector<perf::RegionRange> ranges = perf::LoadRegionRanges(options.regionsConfig);
    const perf::RegionKeyCodec keys(ranges, options.runId);
    const perf::AdapterFactory factory = MakeFactory(options);

    if (options.mode == "load") {
      const auto summary = perf::LoadRecords(spec, keys, factory, options.caseId);
      Publish(options, summary, "record");
      return summary.successful == summary.attempted ? 0 : 1;
    }
    if (options.mode == "verify") {
      auto adapter = factory();
      perf::RunSummary summary;
      summary.caseId = options.caseId;
      summary.path = options.path;
      summary.workload = "verify";
      summary.workers = 1;
      const auto started = std::chrono::steady_clock::now();
      for (uint64_t record = 0; record < options.records; ++record) {
        ++summary.attempted;
        const auto result = ReadOne(adapter.get(), keys.Key(record));
        if (result.ok() && result.value == perf::StableValue(record, options.valueSize)) {
          ++summary.successful;
        } else {
          ++summary.unavailable;
          std::cerr << "checkpoint mismatch record=" << record << '\n';
        }
      }
      summary.elapsedSeconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
      Publish(options, summary, "record");
      return summary.successful == options.records ? 0 : 1;
    }
    if (options.mode == "run") {
      const auto summary = perf::RunRecords(spec, keys, factory, options.caseId);
      Publish(options, summary, "record");
      // A performance point remains valid when optimistic concurrency produces
      // classified conflicts. Infrastructure/unknown outcomes are hard errors.
      return summary.timeouts == 0 && summary.unavailable == 0 && summary.cleanupPending == 0 &&
                     summary.resultUnknown == 0
                 ? 0
                 : 1;
    }
    if (options.mode == "a1-matrix") {
      const auto matrix = (options.profile == "interview-smoke")
                              ? perf::SmokeA1Matrix()
                              : perf::StandardA1Matrix();
      bool allOk = true;
      for (const auto& point : matrix) {
        perf::WorkloadSpec pointSpec = MakeSpec(options);
        pointSpec.path = point.path;
        pointSpec.workload = point.workload;
        pointSpec.distribution = point.distribution;
        pointSpec.workers = point.workers;
        pointSpec.Validate();

        Options pointOptions = options;
        pointOptions.path = point.path;
        pointOptions.caseId = point.caseId;
        pointOptions.workload = perf::WorkloadName(point.workload);
        pointOptions.distribution = perf::DistributionName(point.distribution);
        pointOptions.workers = point.workers;

        const perf::AdapterFactory pointFactory = (point.path == "gateway")
            ? perf::GatewayAdapterFactory(options.gateway, options.timeoutMs)
            : perf::DirectAdapterFactory(options.regionsConfig, options.tsoEndpoints);

        const auto summary = perf::RunRecords(pointSpec, keys, pointFactory, point.caseId);
        Publish(pointOptions, summary, "record");
        if (summary.timeouts != 0 || summary.unavailable != 0 ||
            summary.cleanupPending != 0 || summary.resultUnknown != 0) {
          allOk = false;
        }
      }
      return allOk ? 0 : 1;
    }
    if (options.mode == "transfer") {
      const auto summary = RunTransfers(options, keys, factory);
      return summary.successful == summary.attempted ? 0 : 1;
    }
    const auto summary = RunRegister(options, keys, factory);
    return summary.successful == summary.attempted ? 0 : 1;
  } catch (const std::exception& error) {
    std::cerr << "stratakv-test-kv-workload: " << error.what() << '\n';
    return 2;
  }
}
