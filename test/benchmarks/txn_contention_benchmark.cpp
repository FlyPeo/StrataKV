/*
 * 测试目标：评估乐观或悲观事务在跨 Region 热点键争用下的吞吐、冲突率和尾延迟。
 * 测试策略：通过公开 SDK 连接真实集群，多线程执行三 Region 事务，并按参数控制热点
 *           键数量、热点比例、事务数量与 workload 类型。
 * 测试规模：默认 16 个 worker 执行 1,000 个事务，每个事务在 3 个 Region 各访问 1 个键；
 *           热点键数默认 0，可通过 hot-key-count/hot-percent 放大争用。
 * 验证内容：区分成功、预期冲突和不可用错误；读取热点槽确认各 Region 值原子一致，
 *           要求不可用与原子性失败均为零，并输出延迟分位数。
 */
#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "stratakv/client.h"

namespace {
struct Options {
  std::string regions;
  std::string tsoHost = "127.0.0.1";
  int tsoPort = 26300;
  std::string tsoEndpoints = "127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302";
  std::string runId = "run";
  int workers = 16;
  int transactions = 1000;
  int hotKeyCount = 0;
  int hotPercent = 100;
  int keysPerRegion = 1;
  int maxAttempts = 3;
  int retryDelayMs = 10;
  std::string workload = "optimistic-contention";
};

int Number(const std::string& value) {
  const int parsed = std::stoi(value);
  if (parsed < 0) throw std::invalid_argument("numeric options must be non-negative");
  return parsed;
}

Options Parse(int argc, char** argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    const std::string option = argv[index];
    if (index + 1 >= argc) throw std::invalid_argument("missing value for " + option);
    const std::string value = argv[++index];
    if (option == "--regions-config") options.regions = value;
    else if (option == "--tso-host") { options.tsoHost = value; options.tsoEndpoints.clear(); }
    else if (option == "--tso-port") { options.tsoPort = Number(value); options.tsoEndpoints.clear(); }
    else if (option == "--tso-endpoints") options.tsoEndpoints = value;
    else if (option == "--run-id") options.runId = value;
    else if (option == "--workers") options.workers = Number(value);
    else if (option == "--transactions") options.transactions = Number(value);
    else if (option == "--hot-key-count") options.hotKeyCount = Number(value);
    else if (option == "--hot-percent") options.hotPercent = Number(value);
    else if (option == "--keys-per-region") options.keysPerRegion = Number(value);
    else if (option == "--max-attempts") options.maxAttempts = Number(value);
    else if (option == "--retry-delay-ms") options.retryDelayMs = Number(value);
    else if (option == "--workload") options.workload = value;
    else throw std::invalid_argument("unknown option " + option);
  }
  if (options.regions.empty() || options.tsoHost.empty() || options.tsoPort <= 0 || options.tsoPort > 65535 ||
      options.workers <= 0 || options.transactions <= 0 || options.keysPerRegion <= 0 ||
      options.maxAttempts <= 0) {
    throw std::invalid_argument("--regions-config, valid TSO endpoint, positive --workers and --transactions are required");
  }
  if (options.hotPercent > 100) throw std::invalid_argument("--hot-percent must be at most 100");
  if (options.workload != "optimistic-contention" && options.workload != "pessimistic-contention") {
    throw std::invalid_argument("--workload must be optimistic-contention or pessimistic-contention");
  }
  return options;
}

long long Percentile(std::vector<long long> values, double percentile) {
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  return values[static_cast<size_t>(percentile * static_cast<double>(values.size() - 1))];
}

long long Average(const std::vector<long long>& values) {
  if (values.empty()) return 0;
  long long total = 0;
  for (long long value : values) total += value;
  return total / static_cast<long long>(values.size());
}

std::vector<std::string> SlotKeys(const Options& options, int slot) {
  const std::string suffix = ":contention:" + options.runId + ":" + std::to_string(slot);
  std::vector<std::string> keys;
  keys.reserve(static_cast<size_t>(options.keysPerRegion) * 3);
  for (const char prefix : {'a', 'h', 'p'}) {
    for (int index = 0; index < options.keysPerRegion; ++index) {
      keys.push_back(std::string(1, prefix) + suffix + ":" + std::to_string(index));
    }
  }
  return keys;
}

std::vector<std::string> Keys(const Options& options, int index) {
  const bool hot = options.hotKeyCount > 0 && index % 100 < options.hotPercent;
  return SlotKeys(options, hot ? index % options.hotKeyCount : options.transactions + index);
}
}  // namespace

int main(int argc, char** argv) {
  try {
    Options options = Parse(argc, argv);
    if (options.tsoEndpoints.empty()) {
      options.tsoEndpoints = options.tsoHost + ":" + std::to_string(options.tsoPort);
    }
    auto client = stratakv::Client::Connect(options.regions, options.tsoEndpoints);
    std::atomic<int> next{0};
    std::atomic<int> attempted{0};
    std::atomic<int> committed{0};
    std::atomic<int> conflicts{0};
    std::atomic<int> unavailable{0};
    std::atomic<bool> reportedFailure{false};
    std::mutex latencyMutex;
    std::vector<long long> latencies;
    latencies.reserve(options.transactions);
    const auto workloadStarted = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    for (int worker = 0; worker < options.workers; ++worker) {
      workers.emplace_back([&]() {
        while (true) {
          const int index = next.fetch_add(1);
          if (index >= options.transactions) return;
          const auto started = std::chrono::steady_clock::now();
          const auto keys = Keys(options, index);
          const std::string value = "value-" + options.runId + "-" + std::to_string(index);
          for (int attempt = 1; attempt <= options.maxAttempts; ++attempt) {
            attempted.fetch_add(1);
            auto txn = client->Begin(120000);
            stratakv::Result result;
            if (options.workload == "pessimistic-contention") {
              result = client->LockKeys(txn, keys);
              if (result.ok()) {
                for (const auto& key : keys) client->Put(txn, key, value);
                result = client->Commit(txn);
              } else if (result.status == stratakv::Status::kTimeout) {
                result.status = stratakv::Status::kLockConflict;
              }
            } else {
              for (const auto& key : keys) client->Put(txn, key, value);
              result = client->Commit(txn);
            }

            if (result.ok()) {
              committed.fetch_add(1);
              break;
            }
            (void)client->Rollback(txn);
            if (result.status == stratakv::Status::kLockConflict ||
                result.status == stratakv::Status::kWriteConflict) {
              conflicts.fetch_add(1);
              if (attempt < options.maxAttempts) {
                std::this_thread::sleep_for(std::chrono::milliseconds(options.retryDelayMs * attempt));
                continue;
              }
              break;
            }
            unavailable.fetch_add(1);
            if (!reportedFailure.exchange(true)) {
              std::cerr << "first_unavailable_status=" << stratakv::StatusName(result.status)
                        << " message=" << result.message << '\n';
            }
            break;
          }
          const long long latency = std::chrono::duration_cast<std::chrono::microseconds>(
                                        std::chrono::steady_clock::now() - started)
                                        .count();
          std::lock_guard<std::mutex> lock(latencyMutex);
          latencies.push_back(latency);
        }
      });
    }
    for (auto& worker : workers) worker.join();
    const long long elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - workloadStarted)
                                    .count();

    int atomicityFailures = 0;
    if (options.hotKeyCount > 0) {
      for (int slot = 0; slot < options.hotKeyCount; ++slot) {
        auto txn = client->Begin(120000);
        const auto keys = SlotKeys(options, slot);
        std::vector<stratakv::Result> values;
        values.reserve(keys.size());
        for (const auto& key : keys) values.push_back(client->Get(txn, key));
        client->Rollback(txn);
        if (values.empty() || std::any_of(values.begin(), values.end(), [](const auto& value) { return !value.ok(); }) ||
            std::any_of(values.begin() + 1, values.end(), [&values](const auto& value) {
              return value.value != values.front().value;
            })) {
          ++atomicityFailures;
        }
      }
    }
    const double seconds = std::max(0.001, elapsedMs / 1000.0);
    const double successRate = static_cast<double>(committed.load()) / options.transactions;
    const double actualConflictRate = attempted.load() == 0
                                          ? 0.0
                                          : static_cast<double>(conflicts.load()) / attempted.load();
    const double attemptsPerCommit = committed.load() == 0
                                         ? 0.0
                                         : static_cast<double>(attempted.load()) / committed.load();
    std::cout << "transactions_total=" << options.transactions << '\n'
              << "transaction_attempts=" << attempted.load() << '\n'
              << "keys_per_region=" << options.keysPerRegion << '\n'
              << "keys_per_transaction=" << options.keysPerRegion * 3 << '\n'
              << "target_contention_share=" << (options.hotKeyCount == 0 ? 0.0 : options.hotPercent / 100.0) << '\n'
              << "actual_conflict_rate=" << actualConflictRate << '\n'
              << "transactions_committed=" << committed.load() << '\n'
              << "transaction_conflicts=" << conflicts.load() << '\n'
              << "unavailable=" << unavailable.load() << '\n'
              << "atomicity_failures=" << atomicityFailures << '\n'
              << "success_rate=" << successRate << '\n'
              << "attempts_per_commit=" << attemptsPerCommit << '\n'
              << "throughput_attempts_per_sec=" << attempted.load() / seconds << '\n'
              << "throughput_commits_per_sec=" << committed.load() / seconds << '\n'
              << "latency_boundary=first_attempt_to_final_result_including_backoff\n"
              << "latency_avg_us=" << Average(latencies) << '\n'
              << "latency_p50_us=" << Percentile(latencies, 0.50) << '\n'
              << "latency_p95_us=" << Percentile(latencies, 0.95) << '\n'
              << "latency_p99_us=" << Percentile(latencies, 0.99) << '\n'
              << "latency_max_us=" << Percentile(latencies, 1.0) << '\n';
    return unavailable.load() == 0 && atomicityFailures == 0 ? 0 : 1;
  } catch (const std::exception& error) {
    std::cerr << "txn contention benchmark: " << error.what() << '\n';
    return 2;
  }
}
