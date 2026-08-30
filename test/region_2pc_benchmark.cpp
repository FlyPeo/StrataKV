#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "distributed_transaction_coordinator.h"
#include "raft_mvcc_storage.h"
#include "region_metadata.h"
#include "remote_timestamp_oracle.h"

namespace {

struct Options {
  std::string regions;
  std::string tsoEndpoints;
  std::string runId = "run";
  int workers = 4;
  int transactions = 20;
  int keysPerTransaction = 3;
  int regionCount = 3;
};

int Positive(const std::string& value) {
  const int parsed = std::stoi(value);
  if (parsed <= 0) throw std::invalid_argument("numeric options must be positive");
  return parsed;
}

Options Parse(int argc, char** argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    if (index + 1 >= argc) throw std::invalid_argument("missing option value");
    const std::string option = argv[index];
    const std::string value = argv[++index];
    if (option == "--regions-config") options.regions = value;
    else if (option == "--tso-endpoints") options.tsoEndpoints = value;
    else if (option == "--run-id") options.runId = value;
    else if (option == "--workers") options.workers = Positive(value);
    else if (option == "--transactions") options.transactions = Positive(value);
    else if (option == "--keys-per-transaction") options.keysPerTransaction = Positive(value);
    else if (option == "--region-count") options.regionCount = Positive(value);
    else throw std::invalid_argument("unknown option " + option);
  }
  if (options.regions.empty() || options.tsoEndpoints.empty()) {
    throw std::invalid_argument("--regions-config and --tso-endpoints are required");
  }
  if (options.regionCount > 3) throw std::invalid_argument("--region-count must be 1, 2, or 3");
  if (options.keysPerTransaction < options.regionCount) {
    throw std::invalid_argument("--keys-per-transaction must be at least --region-count");
  }
  return options;
}

long long Percentile(std::vector<long long> values, double percentile) {
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  return values[static_cast<size_t>(percentile * static_cast<double>(values.size() - 1))];
}

std::shared_ptr<ShardRouter> MakeRouter(const std::string& path) {
  const RegionCatalog catalog = RegionCatalog::LoadFromConfig(path);
  std::vector<ShardRouter::RegionRoute> routes;
  for (const auto& region : catalog.Regions()) {
    std::vector<std::pair<std::string, short>> endpoints;
    for (const auto& peer : region.peers) endpoints.emplace_back(peer.host, peer.port);
    routes.push_back({region, std::make_shared<RaftMvccStorage>(region.regionId, endpoints)});
  }
  return std::make_shared<ShardRouter>(std::move(routes));
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const Options options = Parse(argc, argv);
    auto coordinator = std::make_shared<DistributedTransactionCoordinator>(
        MakeRouter(options.regions), std::make_shared<RemoteTimestampOracle>(options.tsoEndpoints));
    std::atomic<int> next{0};
    std::atomic<int> committed{0};
    std::atomic<int> failed{0};
    std::mutex latencyMutex;
    std::vector<long long> latencies;
    const auto started = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    for (int worker = 0; worker < options.workers; ++worker) {
      workers.emplace_back([&]() {
        while (true) {
          const int transactionIndex = next.fetch_add(1, std::memory_order_relaxed);
          if (transactionIndex >= options.transactions) return;
          Transaction transaction = coordinator->Begin();
          const std::string suffix = ":batch:" + options.runId + ":" +
                                     std::to_string(transactionIndex) + ":";
          static constexpr char kRegionPrefixes[] = {'a', 'h', 'p'};
          for (int keyIndex = 0; keyIndex < options.keysPerTransaction; ++keyIndex) {
            const int regionIndex = keyIndex % options.regionCount;
            const char prefix = kRegionPrefixes[regionIndex];
            transaction.Put(std::string(1, prefix) + suffix +
                                std::to_string(keyIndex / options.regionCount),
                            "value-" + std::to_string(transactionIndex));
          }
          const auto commitStarted = std::chrono::steady_clock::now();
          const TxnStatus status = coordinator->Commit(&transaction);
          const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
                                  std::chrono::steady_clock::now() - commitStarted)
                                  .count();
          if (status == TxnStatus::Ok) committed.fetch_add(1, std::memory_order_relaxed);
          else failed.fetch_add(1, std::memory_order_relaxed);
          std::lock_guard<std::mutex> lock(latencyMutex);
          latencies.push_back(micros);
        }
      });
    }
    for (auto& worker : workers) worker.join();
    const auto elapsedMicros = std::chrono::duration_cast<std::chrono::microseconds>(
                                   std::chrono::steady_clock::now() - started)
                                   .count();
    const double seconds = std::max(0.000001, elapsedMicros / 1000000.0);
    std::cout << "transactions_total=" << options.transactions << '\n'
              << "transactions_committed=" << committed.load() << '\n'
              << "transactions_failed=" << failed.load() << '\n'
              << "region_count=" << options.regionCount << '\n'
              << "keys_per_transaction=" << options.keysPerTransaction << '\n'
              << "throughput_txn_per_sec=" << committed.load() / seconds << '\n'
              << "latency_p50_us=" << Percentile(latencies, 0.50) << '\n'
              << "latency_p95_us=" << Percentile(latencies, 0.95) << '\n'
              << "latency_p99_us=" << Percentile(latencies, 0.99) << '\n';
    return failed.load() == 0 ? 0 : 1;
  } catch (const std::exception& error) {
    std::cerr << "Region 2PC benchmark failed: " << error.what() << std::endl;
    return 2;
  }
}
