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
    else if (option == "--workload") options.workload = value;
    else throw std::invalid_argument("unknown option " + option);
  }
  if (options.regions.empty() || options.tsoHost.empty() || options.tsoPort <= 0 || options.tsoPort > 65535 ||
      options.workers <= 0 || options.transactions <= 0) {
    throw std::invalid_argument("--regions-config, valid TSO endpoint, positive --workers and --transactions are required");
  }
  if (options.hotPercent > 100) throw std::invalid_argument("--hot-percent must be at most 100");
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
  return {"a" + suffix, "h" + suffix, "p" + suffix};
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
    std::atomic<int> committed{0};
    std::atomic<int> conflicts{0};
    std::atomic<int> unavailable{0};
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
          auto txn = client->Begin(3000);
          const auto keys = Keys(options, index);
          const std::string value = "value-" + options.runId + "-" + std::to_string(index);
          
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
          
          if (result.ok()) committed.fetch_add(1);
          else if (result.status == stratakv::Status::kLockConflict ||
                   result.status == stratakv::Status::kWriteConflict) conflicts.fetch_add(1);
          else unavailable.fetch_add(1);
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
        auto txn = client->Begin(3000);
        const auto keys = SlotKeys(options, slot);
        const auto first = client->Get(txn, keys[0]);
        const auto second = client->Get(txn, keys[1]);
        const auto third = client->Get(txn, keys[2]);
        client->Rollback(txn);
        if (!first.ok() || !second.ok() || !third.ok() || first.value != second.value || first.value != third.value) {
          ++atomicityFailures;
        }
      }
    }
    const double seconds = std::max(0.001, elapsedMs / 1000.0);
    std::cout << "transactions_total=" << options.transactions << '\n'
              << "transactions_committed=" << committed.load() << '\n'
              << "transaction_conflicts=" << conflicts.load() << '\n'
              << "unavailable=" << unavailable.load() << '\n'
              << "atomicity_failures=" << atomicityFailures << '\n'
              << "throughput_attempts_per_sec=" << options.transactions / seconds << '\n'
              << "throughput_commits_per_sec=" << committed.load() / seconds << '\n'
              << "latency_avg_us=" << Average(latencies) << '\n'
              << "latency_p50_us=" << Percentile(latencies, 0.50) << '\n'
              << "latency_p95_us=" << Percentile(latencies, 0.95) << '\n'
              << "latency_p99_us=" << Percentile(latencies, 0.99) << '\n';
    return unavailable.load() == 0 && atomicityFailures == 0 ? 0 : 1;
  } catch (const std::exception& error) {
    std::cerr << "txn contention benchmark: " << error.what() << '\n';
    return 2;
  }
}
