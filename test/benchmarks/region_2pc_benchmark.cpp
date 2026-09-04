/*
 * 测试目标：测量事务跨越不同 Region 数量（A3）及本地/跨 Region 混合比例（A2）时，Primary-First 2PC 的吞吐和提交延迟。
 * 测试策略：连接真实 Region 路由和远程 TSO，支持固定 Region 数或混合比例生成器（0/15/100%），收集全局与分桶提交耗时。
 * 测试规模：默认 4 或 8 workers 执行 20~1,000 事务，每事务 3 个键；A2 固定 3 mutations 跨 Region 0/15/100%，workers=8。
 * 验证内容：确认所有事务均成功提交，输出 local/distributed 分桶统计、吞吐及 P50/P95/P99 延迟供可复现比较。
 */
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
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
  int crossPercent = -1;  // -1 indicates using regionCount; 0, 15, 100 for A2 mixed ratio
  uint64_t seed = 20260904;
};

std::string KeyInRegion(const RegionMetadata& region, const std::string& suffix) {
  const std::string key = (region.startKey.empty() ? "0" : region.startKey + ":") + suffix;
  if (!region.Contains(key)) throw std::runtime_error("cannot derive benchmark key inside Region boundary");
  return key;
}

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
    else if (option == "--cross-percent" || option == "--cross-ratio") {
      const int parsed = std::stoi(value);
      if (parsed != 0 && parsed != 15 && parsed != 100) {
        throw std::invalid_argument("--cross-percent must be 0, 15, or 100");
      }
      options.crossPercent = parsed;
    } else if (option == "--seed") {
      options.seed = static_cast<uint64_t>(std::stoull(value));
    } else {
      throw std::invalid_argument("unknown option " + option);
    }
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
    const RegionCatalog catalog = RegionCatalog::LoadFromConfig(options.regions);
    if (catalog.Regions().size() < static_cast<size_t>(options.regionCount)) {
      throw std::invalid_argument("Region catalog has fewer Regions than --region-count");
    }
    auto coordinator = std::make_shared<DistributedTransactionCoordinator>(
        MakeRouter(options.regions), std::make_shared<RemoteTimestampOracle>(options.tsoEndpoints));
    std::atomic<int> next{0};
    std::atomic<int> committed{0};
    std::atomic<int> failed{0};
    std::atomic<int> localAttempted{0};
    std::atomic<int> localCommitted{0};
    std::atomic<int> localFailed{0};
    std::atomic<int> distributedAttempted{0};
    std::atomic<int> distributedCommitted{0};
    std::atomic<int> distributedFailed{0};

    std::mutex latencyMutex;
    std::vector<long long> latencies;
    std::vector<long long> localLatencies;
    std::vector<long long> distributedLatencies;
    const auto started = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;

    for (int worker = 0; worker < options.workers; ++worker) {
      workers.emplace_back([&]() {
        while (true) {
          const int transactionIndex = next.fetch_add(1, std::memory_order_relaxed);
          if (transactionIndex >= options.transactions) return;
          const auto transactionStarted = std::chrono::steady_clock::now();
          Transaction transaction = coordinator->Begin();
          const std::string suffix = ":batch:" + options.runId + ":" +
                                     std::to_string(transactionIndex) + ":";

          bool isDistributed = false;
          if (options.crossPercent >= 0) {
            const uint64_t slot = ((static_cast<uint64_t>(transactionIndex) % 100U) * 21U +
                                   options.seed % 100U) % 100U;
            isDistributed = slot < static_cast<uint64_t>(options.crossPercent);
            if (isDistributed) {
              distributedAttempted.fetch_add(1, std::memory_order_relaxed);
              for (size_t regionIdx = 0; regionIdx < 3; ++regionIdx) {
                transaction.Put(KeyInRegion(catalog.Regions()[regionIdx], suffix + std::to_string(regionIdx)),
                                "value-" + std::to_string(transactionIndex));
              }
            } else {
              localAttempted.fetch_add(1, std::memory_order_relaxed);
              const size_t localRegion = static_cast<size_t>(transactionIndex % 3);
              for (int keyIdx = 0; keyIdx < 3; ++keyIdx) {
                transaction.Put(KeyInRegion(catalog.Regions()[localRegion], suffix + std::to_string(keyIdx)),
                                "value-" + std::to_string(transactionIndex));
              }
            }
          } else {
            for (int keyIndex = 0; keyIndex < options.keysPerTransaction; ++keyIndex) {
              const int regionIndex = keyIndex % options.regionCount;
              transaction.Put(KeyInRegion(catalog.Regions()[static_cast<size_t>(regionIndex)],
                                          suffix + std::to_string(keyIndex / options.regionCount)),
                              "value-" + std::to_string(transactionIndex));
            }
          }

          const TxnStatus status = coordinator->Commit(&transaction);
          const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
                                  std::chrono::steady_clock::now() - transactionStarted)
                                  .count();

          if (status == TxnStatus::Ok) {
            committed.fetch_add(1, std::memory_order_relaxed);
            if (options.crossPercent >= 0) {
              if (isDistributed) distributedCommitted.fetch_add(1, std::memory_order_relaxed);
              else localCommitted.fetch_add(1, std::memory_order_relaxed);
            }
          } else {
            failed.fetch_add(1, std::memory_order_relaxed);
            if (options.crossPercent >= 0) {
              if (isDistributed) distributedFailed.fetch_add(1, std::memory_order_relaxed);
              else localFailed.fetch_add(1, std::memory_order_relaxed);
            }
          }

          std::lock_guard<std::mutex> lock(latencyMutex);
          latencies.push_back(micros);
          if (options.crossPercent >= 0) {
            if (isDistributed) distributedLatencies.push_back(micros);
            else localLatencies.push_back(micros);
          }
        }
      });
    }
    for (auto& worker : workers) worker.join();
    const auto elapsedMicros = std::chrono::duration_cast<std::chrono::microseconds>(
                                   std::chrono::steady_clock::now() - started)
                                   .count();
    const double seconds = std::max(0.000001, elapsedMicros / 1000000.0);
    std::cout << "transactions_total=" << options.transactions << '\n'
              << "transactions_attempted=" << options.transactions << '\n'
              << "transactions_committed=" << committed.load() << '\n'
              << "transactions_failed=" << failed.load() << '\n'
              << "region_count=" << (options.crossPercent >= 0 ? 3 : options.regionCount) << '\n'
              << "keys_per_transaction=" << (options.crossPercent >= 0 ? 3 : options.keysPerTransaction) << '\n'
              << "throughput_attempted_txn_per_sec=" << options.transactions / seconds << '\n'
              << "throughput_committed_txn_per_sec=" << committed.load() / seconds << '\n'
              << "latency_boundary=begin_to_final_commit_result\n"
              << "latency_p50_us=" << Percentile(latencies, 0.50) << '\n'
              << "latency_p95_us=" << Percentile(latencies, 0.95) << '\n'
              << "latency_p99_us=" << Percentile(latencies, 0.99) << '\n'
              << "latency_max_us=" << Percentile(latencies, 1.0) << '\n';

    if (options.crossPercent >= 0) {
      const double actualCross = options.transactions > 0
                                     ? static_cast<double>(distributedAttempted.load()) / options.transactions
                                     : 0.0;
      std::cout << "target_cross_percent=" << options.crossPercent << '\n'
                << "actual_cross_ratio=" << actualCross << '\n'
                << "local_attempted=" << localAttempted.load() << '\n'
                << "local_committed=" << localCommitted.load() << '\n'
                << "local_throughput_committed_txn_per_sec=" << localCommitted.load() / seconds << '\n'
                << "local_latency_p50_us=" << Percentile(localLatencies, 0.50) << '\n'
                << "local_latency_p99_us=" << Percentile(localLatencies, 0.99) << '\n'
                << "distributed_attempted=" << distributedAttempted.load() << '\n'
                << "distributed_committed=" << distributedCommitted.load() << '\n'
                << "distributed_throughput_committed_txn_per_sec=" << distributedCommitted.load() / seconds << '\n'
                << "distributed_latency_p50_us=" << Percentile(distributedLatencies, 0.50) << '\n'
                << "distributed_latency_p99_us=" << Percentile(distributedLatencies, 0.99) << '\n';
    }
    return failed.load() == 0 ? 0 : 1;
  } catch (const std::exception& error) {
    std::cerr << "Region 2PC benchmark failed: " << error.what() << std::endl;
    return 2;
  }
}
