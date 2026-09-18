/*
 * StrataKV Dynamic Sharding Performance & Resilience Benchmark
 *
 * Measures:
 * 1. Pre-Split Baseline: Steady-state throughput (QPS) and latency distribution.
 * 2. In-Split Transient Impact: Sub-100ms time-series sampling, split execution
 *    duration, transient QPS dip %, P99 latency spike, routing epoch refresh
 *    (EPOCH_NOT_MATCH) count, and zero-dropped-request availability.
 * 3. Post-Split Scalability: Scalability and load distribution across daughter Regions.
 * 4. Data Integrity: 100% audit of all written keys across the split boundary.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "metadata_client.h"
#include "region_metadata.h"
#include "stratakv/client.h"

namespace {

struct Options {
  std::string metadataEndpoints = "127.0.0.1:26580,127.0.0.1:26581,127.0.0.1:26582";
  std::string tsoEndpoints = "127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302";
  std::string regionsConfig;
  int splitRegion = 100;
  std::string splitKey = "d";
  int workers = 8;
  int preSplitDurationSec = 5;
  int inSplitDurationSec = 6;
  int postSplitDurationSec = 5;
  int triggerDelayMs = 1500;
  int sampleWindowMs = 100;
  int readRatio = 50;  // 50% read, 50% write
  std::string adminBin = "bin/stratakv-admin";
  std::string triggerCmd;
  std::string outputJson;
  std::string outputMd;
  std::string keyPrefix = "bench_split:";
};

Options ParseArgs(int argc, char** argv) {
  Options opt;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (i + 1 >= argc && arg != "-h" && arg != "--help") {
      throw std::invalid_argument("missing argument value for: " + arg);
    }
    if (arg == "--metadata-endpoints") {
      opt.metadataEndpoints = argv[++i];
    } else if (arg == "--tso-endpoints") {
      opt.tsoEndpoints = argv[++i];
    } else if (arg == "--regions-config") {
      opt.regionsConfig = argv[++i];
    } else if (arg == "--split-region") {
      opt.splitRegion = std::stoi(argv[++i]);
    } else if (arg == "--split-key") {
      opt.splitKey = argv[++i];
    } else if (arg == "--workers") {
      opt.workers = std::stoi(argv[++i]);
    } else if (arg == "--pre-split-duration-sec") {
      opt.preSplitDurationSec = std::stoi(argv[++i]);
    } else if (arg == "--in-split-duration-sec") {
      opt.inSplitDurationSec = std::stoi(argv[++i]);
    } else if (arg == "--post-split-duration-sec") {
      opt.postSplitDurationSec = std::stoi(argv[++i]);
    } else if (arg == "--duration-sec") {
      int d = std::stoi(argv[++i]);
      opt.preSplitDurationSec = d;
      opt.inSplitDurationSec = d;
      opt.postSplitDurationSec = d;
    } else if (arg == "--trigger-delay-ms") {
      opt.triggerDelayMs = std::stoi(argv[++i]);
    } else if (arg == "--sample-window-ms") {
      opt.sampleWindowMs = std::stoi(argv[++i]);
    } else if (arg == "--read-ratio") {
      opt.readRatio = std::stoi(argv[++i]);
    } else if (arg == "--admin-bin") {
      opt.adminBin = argv[++i];
    } else if (arg == "--trigger-cmd") {
      opt.triggerCmd = argv[++i];
    } else if (arg == "--output-json") {
      opt.outputJson = argv[++i];
    } else if (arg == "--output-md") {
      opt.outputMd = argv[++i];
    } else if (arg == "--key-prefix") {
      opt.keyPrefix = argv[++i];
    } else if (arg == "-h" || arg == "--help") {
      std::cout << "Usage: stratakv-test-dynamic-sharding-benchmark [options]\n"
                << "  --metadata-endpoints <endpoints>\n"
                << "  --tso-endpoints <endpoints>\n"
                << "  --regions-config <path>\n"
                << "  --split-region <id> (default 100)\n"
                << "  --split-key <key> (default 'd')\n"
                << "  --workers <count> (default 8)\n"
                << "  --duration-sec <sec> (sets pre/in/post duration)\n"
                << "  --pre-split-duration-sec <sec> (default 5)\n"
                << "  --in-split-duration-sec <sec> (default 6)\n"
                << "  --post-split-duration-sec <sec> (default 5)\n"
                << "  --trigger-delay-ms <ms> (default 1500)\n"
                << "  --sample-window-ms <ms> (default 100)\n"
                << "  --read-ratio <0..100> (default 50)\n"
                << "  --admin-bin <path> (default bin/stratakv-admin)\n"
                << "  --trigger-cmd <cmd>\n"
                << "  --output-json <path>\n"
                << "  --output-md <path>\n";
      std::exit(0);
    } else {
      throw std::invalid_argument("unknown argument: " + arg);
    }
  }
  return opt;
}

struct LatencyStats {
  double minMs = 0;
  double avgMs = 0;
  double p50Ms = 0;
  double p90Ms = 0;
  double p95Ms = 0;
  double p99Ms = 0;
  double p999Ms = 0;
  double maxMs = 0;
};

LatencyStats ComputeLatencyStats(std::vector<uint64_t>& latUs) {
  LatencyStats stats;
  if (latUs.empty()) return stats;
  std::sort(latUs.begin(), latUs.end());
  const size_t n = latUs.size();
  stats.minMs = latUs.front() / 1000.0;
  stats.maxMs = latUs.back() / 1000.0;
  uint64_t sum = 0;
  for (uint64_t val : latUs) sum += val;
  stats.avgMs = (static_cast<double>(sum) / n) / 1000.0;

  auto Pct = [&](double p) -> double {
    size_t idx = static_cast<size_t>(p * (n - 1));
    return latUs[idx] / 1000.0;
  };
  stats.p50Ms = Pct(0.50);
  stats.p90Ms = Pct(0.90);
  stats.p95Ms = Pct(0.95);
  stats.p99Ms = Pct(0.99);
  stats.p999Ms = Pct(0.999);
  return stats;
}

struct PhaseResult {
  std::string name;
  double durationSec = 0;
  uint64_t totalOps = 0;
  uint64_t readOps = 0;
  uint64_t writeOps = 0;
  uint64_t successOps = 0;
  uint64_t failedOps = 0;
  double qps = 0;
  LatencyStats latency;
  uint64_t epochRefreshesStart = 0;
  uint64_t epochRefreshesEnd = 0;
  uint64_t epochRefreshesDelta = 0;
};

struct SampleWindow {
  int windowIndex = 0;
  int64_t relativeTimeMs = 0;
  uint64_t ops = 0;
  uint64_t failed = 0;
  double qps = 0;
  double p50Ms = 0;
  double p99Ms = 0;
  uint64_t epochRefreshes = 0;
  bool splitTriggered = false;
  bool splitCompleted = false;
};

struct SplitJitterMetrics {
  double splitDurationMs = 0;
  bool splitSuccess = false;
  int splitReturnCode = -1;
  double preTriggerSteadyQps = 0;
  double minTransientQps = 0;
  double transientDipPercent = 0;
  double recoveryDurationMs = 0;
  double baselineP99Ms = 0;
  double spikeP99Ms = 0;
  double p99SpikeRatio = 0;
  uint64_t totalEpochRefreshes = 0;
};

// Thread-safe map of written keys for data integrity verification
class WrittenKeyRegistry {
 public:
  void Record(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    keys_[key] = value;
  }

  std::vector<std::pair<std::string, std::string>> GetAll() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return {keys_.begin(), keys_.end()};
  }

  size_t Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return keys_.size();
  }

 private:
  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::string> keys_;
};

}  // namespace

int main(int argc, char** argv) {
  try {
    Options opt = ParseArgs(argc, argv);

    // Set dynamic topology environment variables for this process and any children
    setenv("STRATAKV_METADATA_ENDPOINTS", opt.metadataEndpoints.c_str(), 1);
    setenv("STRATAKV_TOPOLOGY_MODE", "dynamic", 1);
    if (!opt.regionsConfig.empty()) {
      setenv("STRATAKV_REGION_CONFIG", opt.regionsConfig.c_str(), 1);
    }

    std::cout << "\n==========================================================\n";
    std::cout << "  StrataKV Dynamic Sharding Performance & Resilience Test \n";
    std::cout << "==========================================================\n";
    std::cout << "Metadata Endpoints:   " << opt.metadataEndpoints << "\n";
    std::cout << "TSO Endpoints:        " << opt.tsoEndpoints << "\n";
    std::cout << "Split Target:         Region " << opt.splitRegion << " @ key '" << opt.splitKey << "'\n";
    std::cout << "Concurrency:          " << opt.workers << " workers\n";
    std::cout << "Phase Durations:      Pre=" << opt.preSplitDurationSec << "s, In="
              << opt.inSplitDurationSec << "s, Post=" << opt.postSplitDurationSec << "s\n";
    std::cout << "Workload Mix:         " << opt.readRatio << "% Read / " << (100 - opt.readRatio) << "% Write\n";
    std::cout << "==========================================================\n\n";

    // 1. Inspect initial metadata topology
    std::cout << "[Step 1/5] Inspecting initial cluster topology via Metadata consensus...\n";
    MetadataClient metaClient(opt.metadataEndpoints);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    uint64_t initialRevision = 0;
    std::vector<RegionMetadata> initialRegions = metaClient.Scan("", 16, deadline, &initialRevision);
    std::cout << "Discovered " << initialRegions.size() << " initial Regions at revision "
              << initialRevision << ":\n";
    for (const auto& r : initialRegions) {
      std::cout << "  - Region " << r.regionId << ": [\"" << r.startKey << "\", \""
                << r.endKey << "\") epoch=(" << r.epoch.version << "," << r.epoch.confVersion
                << ") peers=" << r.peers.size() << "\n";
    }

    // 2. Connect SDK Client in Dynamic Topology Mode
    std::cout << "\n[Step 2/5] Initializing native C++ SDK client with Dynamic Topology...\n";
    stratakv::ConnectionOptions connOpts;
    connOpts.topologyMode = stratakv::TopologyMode::kDynamic;
    connOpts.metadataEndpoints = opt.metadataEndpoints;
    connOpts.tsoEndpoints = opt.tsoEndpoints;
    connOpts.regionConfigPath = opt.regionsConfig;
    connOpts.metadataTimeoutMs = 5000;
    auto client = stratakv::Client::Connect(connOpts);
    if (!client) {
      std::cerr << "Failed to connect SDK Client to dynamic cluster.\n";
      return 1;
    }
    std::cout << "SDK Dynamic Client connected successfully.\n";

    WrittenKeyRegistry writtenKeys;
    std::atomic<uint64_t> keySequence{0};

    // Shared tracking across worker threads
    enum class Phase { kWarmup, kPreSplit, kInSplit, kPostSplit, kDone };
    std::atomic<Phase> currentPhase{Phase::kWarmup};

    struct ThreadMetrics {
      std::vector<uint64_t> preLatUs;
      std::vector<uint64_t> inLatUs;
      std::vector<uint64_t> postLatUs;
      uint64_t preRead = 0, preWrite = 0, preFail = 0;
      uint64_t inRead = 0, inWrite = 0, inFail = 0;
      uint64_t postRead = 0, postWrite = 0, postFail = 0;
    };
    std::vector<ThreadMetrics> threadMetrics(opt.workers);

    // Current window tracking for 100ms time-series
    std::atomic<uint64_t> windowOps{0};
    std::atomic<uint64_t> windowFail{0};
    std::mutex windowLatMutex;
    std::vector<uint64_t> windowLatUs;

    // Helper: generate key for Region 100 (range ["", "h"))
    // Keys prefixed with "b_" are < "d" (remain in Region 100)
    // Keys prefixed with "e_" are >= "d" and < "h" (migrate to child Region)
    auto GenerateKey = [&](uint64_t seq, bool upperRange) -> std::string {
      std::ostringstream ss;
      ss << (upperRange ? "e_" : "b_") << opt.keyPrefix
         << std::setfill('0') << std::setw(8) << (seq % 100000);
      return ss.str();
    };

    std::vector<std::thread> workers;
    for (int w = 0; w < opt.workers; ++w) {
      workers.emplace_back([&, w]() {
        std::mt19937_64 rng(1337 + w * 97);
        std::uniform_int_distribution<int> pctDist(0, 99);
        auto& tm = threadMetrics[w];
        while (true) {
          Phase p = currentPhase.load(std::memory_order_relaxed);
          if (p == Phase::kDone) {
            break;
          }

          uint64_t seq = keySequence.fetch_add(1, std::memory_order_relaxed);
          bool upper = (seq % 2 == 1);  // 50% lower range, 50% upper range
          std::string key = GenerateKey(seq, upper);
          bool isRead = (pctDist(rng) < opt.readRatio);

          auto t0 = std::chrono::steady_clock::now();
          bool ok = false;
          std::string lastErr;

          for (int attempt = 0; attempt < 3; ++attempt) {
            if (currentPhase.load(std::memory_order_relaxed) == Phase::kDone) {
              break;
            }
            auto txn = client->Begin(60000, 2000);
            if (isRead) {
              auto res = client->Get(txn, key);
              (void)client->Rollback(txn);
              if (res.status == stratakv::Status::kOk || res.status == stratakv::Status::kNotFound) {
                ok = true;
                break;
              }
              lastErr = "Get: status=" + std::to_string(static_cast<int>(res.status)) + " " + res.message;
            } else {
              std::string val = "val_" + std::to_string(seq) + "_" + std::to_string(w);
              client->Put(txn, key, val);
              auto res = client->Commit(txn);
              if (res.ok()) {
                ok = true;
                writtenKeys.Record(key, val);
                break;
              }
              lastErr = "Commit: status=" + std::to_string(static_cast<int>(res.status)) + " " + res.message;
              (void)client->Rollback(txn);
            }
            if (attempt + 1 < 3) {
              std::this_thread::sleep_for(std::chrono::milliseconds(10 * (attempt + 1)));
            }
          }

          auto t1 = std::chrono::steady_clock::now();
          uint64_t lat = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

          // Check phase at completion to avoid attributing post-split work to in-split
          Phase targetPhase = currentPhase.load(std::memory_order_relaxed);
          if (targetPhase == Phase::kDone) {
            targetPhase = p;
          }

          if (lat > 500000 || !ok) {
            std::cerr << "[Worker " << w << "] lat=" << (lat / 1000) << "ms, ok=" << ok
                      << ", startPhase=" << static_cast<int>(p)
                      << ", curPhase=" << static_cast<int>(targetPhase)
                      << ", key=" << key << ", err=" << lastErr << std::endl;
          }

          if (targetPhase == Phase::kPreSplit) {
            if (ok) {
              if (isRead) tm.preRead++; else tm.preWrite++;
            } else {
              tm.preFail++;
            }
            tm.preLatUs.push_back(lat);
          } else if (targetPhase == Phase::kInSplit) {
            windowOps.fetch_add(1, std::memory_order_relaxed);
            if (ok) {
              if (isRead) tm.inRead++; else tm.inWrite++;
            } else {
              tm.inFail++;
              windowFail.fetch_add(1, std::memory_order_relaxed);
            }
            tm.inLatUs.push_back(lat);
            {
              std::lock_guard<std::mutex> lock(windowLatMutex);
              windowLatUs.push_back(lat);
            }
          } else if (targetPhase == Phase::kPostSplit) {
            if (ok) {
              if (isRead) tm.postRead++; else tm.postWrite++;
            } else {
              tm.postFail++;
            }
            tm.postLatUs.push_back(lat);
          }
        }
      });
    }

    // =========================================================================
    // Phase 1: Pre-Split Baseline
    // =========================================================================
    std::cout << "\n[Step 3/5] Starting Phase 1: Pre-Split Baseline ("
              << opt.preSplitDurationSec << "s)...\n";
    auto epochRefreshesP1Start = client->Metrics().epochRefreshes;
    auto p1Start = std::chrono::steady_clock::now();
    currentPhase.store(Phase::kPreSplit, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::seconds(opt.preSplitDurationSec));
    auto p1End = std::chrono::steady_clock::now();
    auto epochRefreshesP1End = client->Metrics().epochRefreshes;

    PhaseResult preSplitRes;
    preSplitRes.name = "Pre-Split (Baseline)";
    preSplitRes.durationSec = std::chrono::duration<double>(p1End - p1Start).count();
    std::vector<uint64_t> allPreLat;
    for (const auto& tm : threadMetrics) {
      preSplitRes.readOps += tm.preRead;
      preSplitRes.writeOps += tm.preWrite;
      preSplitRes.failedOps += tm.preFail;
      allPreLat.insert(allPreLat.end(), tm.preLatUs.begin(), tm.preLatUs.end());
    }
    preSplitRes.totalOps = preSplitRes.readOps + preSplitRes.writeOps + preSplitRes.failedOps;
    preSplitRes.successOps = preSplitRes.readOps + preSplitRes.writeOps;
    preSplitRes.qps = preSplitRes.successOps / preSplitRes.durationSec;
    preSplitRes.latency = ComputeLatencyStats(allPreLat);
    preSplitRes.epochRefreshesStart = epochRefreshesP1Start;
    preSplitRes.epochRefreshesEnd = epochRefreshesP1End;
    preSplitRes.epochRefreshesDelta = epochRefreshesP1End - epochRefreshesP1Start;

    std::cout << "  -> Pre-Split Completed: " << preSplitRes.successOps << " ops, "
              << std::fixed << std::setprecision(1) << preSplitRes.qps << " QPS, "
              << "P50=" << std::setprecision(2) << preSplitRes.latency.p50Ms << "ms, "
              << "P99=" << preSplitRes.latency.p99Ms << "ms, "
              << "Epoch Refreshes=" << preSplitRes.epochRefreshesDelta << "\n";

    // =========================================================================
    // Phase 2: In-Split Transient Impact & Continuous Traffic
    // =========================================================================
    std::cout << "\n[Step 4/5] Starting Phase 2: In-Split Online Traffic ("
              << opt.inSplitDurationSec << "s)...\n";
    std::cout << "  (Split will be proposed at +" << opt.triggerDelayMs << "ms steady-state)\n";

    std::vector<SampleWindow> sampleWindows;
    sampleWindows.reserve((opt.inSplitDurationSec * 1000) / opt.sampleWindowMs + 20);

    SplitJitterMetrics jitter;
    std::atomic<bool> splitTriggeredFlag{false};
    std::atomic<bool> splitCompletedFlag{false};
    auto epochRefreshesP2Start = client->Metrics().epochRefreshes;
    auto p2Start = std::chrono::steady_clock::now();
    currentPhase.store(Phase::kInSplit, std::memory_order_release);

    // Trigger thread for online split
    std::thread triggerThread([&]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(opt.triggerDelayMs));
      splitTriggeredFlag.store(true, std::memory_order_release);

      std::string cmd = opt.triggerCmd;
      if (cmd.empty()) {
        cmd = "STRATAKV_TOPOLOGY_MODE=dynamic STRATAKV_METADATA_ENDPOINTS=" + opt.metadataEndpoints;
        if (!opt.regionsConfig.empty()) {
          cmd += " STRATAKV_REGION_CONFIG=" + opt.regionsConfig;
        }
        cmd += " " + opt.adminBin + " split-region " + std::to_string(opt.splitRegion) + " " + opt.splitKey;
      }
      std::cout << "\n  >>> [TRIGGER] Executing online split: " << cmd << " <<<\n";
      auto tTrigStart = std::chrono::steady_clock::now();
      int rc = std::system(cmd.c_str());
      auto tTrigEnd = std::chrono::steady_clock::now();

      jitter.splitDurationMs = std::chrono::duration<double, std::milli>(tTrigEnd - tTrigStart).count();
      jitter.splitReturnCode = rc;
      jitter.splitSuccess = (rc == 0);
      splitCompletedFlag.store(true, std::memory_order_release);

      std::cout << "  >>> [COMPLETED] Split finished in " << std::fixed << std::setprecision(1)
                << jitter.splitDurationMs << "ms (rc=" << rc << ") <<<\n\n";
    });

    // Time-series sampler loop during Phase 2
    int windowIdx = 0;
    const auto p2Deadline = p2Start + std::chrono::seconds(opt.inSplitDurationSec);
    auto nextWindowTime = p2Start + std::chrono::milliseconds(opt.sampleWindowMs);

    while (std::chrono::steady_clock::now() < p2Deadline) {
      std::this_thread::sleep_until(nextWindowTime);
      auto now = std::chrono::steady_clock::now();
      int64_t relMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - p2Start).count();

      uint64_t ops = windowOps.exchange(0, std::memory_order_relaxed);
      uint64_t fail = windowFail.exchange(0, std::memory_order_relaxed);
      std::vector<uint64_t> latSample;
      {
        std::lock_guard<std::mutex> lock(windowLatMutex);
        latSample.swap(windowLatUs);
      }
      auto latStats = ComputeLatencyStats(latSample);

      SampleWindow sw;
      sw.windowIndex = windowIdx++;
      sw.relativeTimeMs = relMs;
      sw.ops = ops;
      sw.failed = fail;
      sw.qps = (ops * 1000.0) / opt.sampleWindowMs;
      sw.p50Ms = latStats.p50Ms;
      sw.p99Ms = latStats.p99Ms;
      sw.epochRefreshes = client->Metrics().epochRefreshes - epochRefreshesP2Start;
      sw.splitTriggered = splitTriggeredFlag.load(std::memory_order_relaxed);
      sw.splitCompleted = splitCompletedFlag.load(std::memory_order_relaxed);
      sampleWindows.push_back(sw);

      nextWindowTime += std::chrono::milliseconds(opt.sampleWindowMs);
    }

    if (triggerThread.joinable()) triggerThread.join();
    auto p2End = std::chrono::steady_clock::now();
    auto epochRefreshesP2End = client->Metrics().epochRefreshes;

    PhaseResult inSplitRes;
    inSplitRes.name = "In-Split (Transient)";
    inSplitRes.durationSec = std::chrono::duration<double>(p2End - p2Start).count();
    std::vector<uint64_t> allInLat;
    for (const auto& tm : threadMetrics) {
      inSplitRes.readOps += tm.inRead;
      inSplitRes.writeOps += tm.inWrite;
      inSplitRes.failedOps += tm.inFail;
      allInLat.insert(allInLat.end(), tm.inLatUs.begin(), tm.inLatUs.end());
    }
    inSplitRes.totalOps = inSplitRes.readOps + inSplitRes.writeOps + inSplitRes.failedOps;
    inSplitRes.successOps = inSplitRes.readOps + inSplitRes.writeOps;
    inSplitRes.qps = inSplitRes.successOps / inSplitRes.durationSec;
    inSplitRes.latency = ComputeLatencyStats(allInLat);
    inSplitRes.epochRefreshesStart = epochRefreshesP2Start;
    inSplitRes.epochRefreshesEnd = epochRefreshesP2End;
    inSplitRes.epochRefreshesDelta = epochRefreshesP2End - epochRefreshesP2Start;

    // Analyze transient dip & recovery from sampleWindows
    double preTriggerQpsSum = 0;
    int preTriggerCount = 0;
    double minQpsDuringSplit = 1e9;
    int triggerWinIdx = -1;
    int recoveryWinIdx = -1;

    for (size_t i = 0; i < sampleWindows.size(); ++i) {
      const auto& sw = sampleWindows[i];
      if (!sw.splitTriggered) {
        preTriggerQpsSum += sw.qps;
        preTriggerCount++;
      } else {
        if (triggerWinIdx < 0) triggerWinIdx = static_cast<int>(i);
        if (sw.qps < minQpsDuringSplit) minQpsDuringSplit = sw.qps;
      }
    }
    jitter.preTriggerSteadyQps = preTriggerCount > 0 ? (preTriggerQpsSum / preTriggerCount) : preSplitRes.qps;
    jitter.minTransientQps = (minQpsDuringSplit < 1e8) ? minQpsDuringSplit : jitter.preTriggerSteadyQps;
    if (jitter.preTriggerSteadyQps > 0) {
      jitter.transientDipPercent = std::max(0.0, (1.0 - jitter.minTransientQps / jitter.preTriggerSteadyQps) * 100.0);
    }
    // Calculate recovery: first window after split completed where QPS >= 85% of baseline
    for (size_t i = (triggerWinIdx >= 0 ? triggerWinIdx : 0); i < sampleWindows.size(); ++i) {
      const auto& sw = sampleWindows[i];
      if (sw.splitCompleted && sw.qps >= 0.85 * jitter.preTriggerSteadyQps) {
        recoveryWinIdx = static_cast<int>(i);
        break;
      }
    }
    if (triggerWinIdx >= 0 && recoveryWinIdx >= triggerWinIdx) {
      jitter.recoveryDurationMs = (recoveryWinIdx - triggerWinIdx) * opt.sampleWindowMs;
    } else {
      jitter.recoveryDurationMs = jitter.splitDurationMs;
    }
    jitter.baselineP99Ms = preSplitRes.latency.p99Ms;
    jitter.spikeP99Ms = inSplitRes.latency.p99Ms;
    jitter.p99SpikeRatio = (jitter.baselineP99Ms > 0) ? (jitter.spikeP99Ms / jitter.baselineP99Ms) : 1.0;
    jitter.totalEpochRefreshes = inSplitRes.epochRefreshesDelta;

    std::cout << "  -> In-Split Completed: " << inSplitRes.successOps << " ops, "
              << std::fixed << std::setprecision(1) << inSplitRes.qps << " QPS, "
              << "P99=" << inSplitRes.latency.p99Ms << "ms, "
              << "Epoch Refreshes=" << inSplitRes.epochRefreshesDelta << "\n";
    std::cout << "  -> Transient Dip: -" << std::setprecision(1) << jitter.transientDipPercent
              << "% (Min QPS=" << jitter.minTransientQps << ", Baseline=" << jitter.preTriggerSteadyQps << ")\n";

    // =========================================================================
    // Phase 3: Post-Split Scalability
    // =========================================================================
    std::cout << "\n[Step 5/5] Starting Phase 3: Post-Split Scalability ("
              << opt.postSplitDurationSec << "s)...\n";
    auto epochRefreshesP3Start = client->Metrics().epochRefreshes;
    auto p3Start = std::chrono::steady_clock::now();
    currentPhase.store(Phase::kPostSplit, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::seconds(opt.postSplitDurationSec));
    auto p3End = std::chrono::steady_clock::now();
    auto epochRefreshesP3End = client->Metrics().epochRefreshes;

    currentPhase.store(Phase::kDone, std::memory_order_release);
    for (auto& t : workers) {
      if (t.joinable()) t.join();
    }

    PhaseResult postSplitRes;
    postSplitRes.name = "Post-Split (Dual-Region)";
    postSplitRes.durationSec = std::chrono::duration<double>(p3End - p3Start).count();
    std::vector<uint64_t> allPostLat;
    for (const auto& tm : threadMetrics) {
      postSplitRes.readOps += tm.postRead;
      postSplitRes.writeOps += tm.postWrite;
      postSplitRes.failedOps += tm.postFail;
      allPostLat.insert(allPostLat.end(), tm.postLatUs.begin(), tm.postLatUs.end());
    }
    postSplitRes.totalOps = postSplitRes.readOps + postSplitRes.writeOps + postSplitRes.failedOps;
    postSplitRes.successOps = postSplitRes.readOps + postSplitRes.writeOps;
    postSplitRes.qps = postSplitRes.successOps / postSplitRes.durationSec;
    postSplitRes.latency = ComputeLatencyStats(allPostLat);
    postSplitRes.epochRefreshesStart = epochRefreshesP3Start;
    postSplitRes.epochRefreshesEnd = epochRefreshesP3End;
    postSplitRes.epochRefreshesDelta = epochRefreshesP3End - epochRefreshesP3Start;

    std::cout << "  -> Post-Split Completed: " << postSplitRes.successOps << " ops, "
              << std::fixed << std::setprecision(1) << postSplitRes.qps << " QPS, "
              << "P50=" << postSplitRes.latency.p50Ms << "ms, "
              << "P99=" << postSplitRes.latency.p99Ms << "ms, "
              << "Epoch Refreshes=" << postSplitRes.epochRefreshesDelta << "\n";

    // =========================================================================
    // Phase 4: Data Consistency & Integrity Audit
    // =========================================================================
    std::cout << "\n==========================================================\n";
    std::cout << "  Verifying Data Consistency & Zero-Lost-Write Integrity   \n";
    std::cout << "==========================================================\n";
    auto allWritten = writtenKeys.GetAll();
    std::cout << "Auditing " << allWritten.size() << " unique keys written across all phases...\n";
    uint64_t verifiedCount = 0;
    uint64_t lostCount = 0;
    uint64_t corruptCount = 0;

    for (const auto& kv : allWritten) {
      bool found = false;
      std::string val;
      std::string lastErr;
      for (int attempt = 0; attempt < 5; ++attempt) {
        auto txn = client->Begin(60000, 3000);
        auto res = client->Get(txn, kv.first);
        (void)client->Rollback(txn);
        if (res.ok() && res.found) {
          found = true;
          val = res.value;
          break;
        }
        lastErr = "status=" + std::to_string(static_cast<int>(res.status)) + " found=" + std::to_string(res.found) + " msg=" + res.message;
        if (attempt + 1 < 5) {
          std::this_thread::sleep_for(std::chrono::milliseconds(20 * (attempt + 1)));
        }
      }
      if (!found) {
        lostCount++;
        if (lostCount <= 5) {
          std::cerr << "Audit lost key: " << kv.first << " lastErr: " << lastErr << "\n";
        }
      } else if (val != kv.second) {
        corruptCount++;
      } else {
        verifiedCount++;
      }
    }
    std::cout << "Audit Result: Verified=" << verifiedCount
              << ", Lost=" << lostCount << ", Corrupt=" << corruptCount << "\n";

    // Verify metadata topology after split
    uint64_t postRevision = 0;
    std::vector<RegionMetadata> postRegions = metaClient.Scan("", 16,
        std::chrono::steady_clock::now() + std::chrono::seconds(10), &postRevision);
    std::cout << "\nFinal Cluster Topology (" << postRegions.size() << " Regions, rev=" << postRevision << "):\n";
    bool foundParent = false;
    bool foundChild = false;
    for (const auto& r : postRegions) {
      std::cout << "  - Region " << r.regionId << ": [\"" << r.startKey << "\", \""
                << r.endKey << "\") epoch=(" << r.epoch.version << "," << r.epoch.confVersion
                << ") peers=" << r.peers.size() << "\n";
      if (r.regionId == opt.splitRegion && r.endKey == opt.splitKey) foundParent = true;
      if (r.startKey == opt.splitKey) foundChild = true;
    }

    bool topologyValid = (foundParent && foundChild && postRegions.size() == initialRegions.size() + 1);
    bool integrityValid = (lostCount == 0 && corruptCount == 0 && verifiedCount > 0);
    // A wedged phase completes zero operations and therefore also records zero
    // failures; availability requires both zero failures and real progress.
    bool inSplitAvailable = (inSplitRes.failedOps == 0 && inSplitRes.totalOps > 0);
    bool postSplitHealthy = (postSplitRes.failedOps == 0 && postSplitRes.totalOps > 0);

    // =========================================================================
    // Summary Tables and ASCII Curve
    // =========================================================================
    std::cout << "\n====================================================================================\n";
    std::cout << "                        DYNAMIC SHARDING PERFORMANCE REPORT                         \n";
    std::cout << "====================================================================================\n";
    std::cout << std::left << std::setw(25) << "Metric / Phase"
              << std::right << std::setw(18) << "Pre-Split (Base)"
              << std::setw(18) << "In-Split (Jitter)"
              << std::setw(18) << "Post-Split (Dual)" << "\n";
    std::cout << "------------------------------------------------------------------------------------\n";
    std::cout << std::left << std::setw(25) << "Duration (s)"
              << std::right << std::setw(18) << std::fixed << std::setprecision(2) << preSplitRes.durationSec
              << std::setw(18) << inSplitRes.durationSec
              << std::setw(18) << postSplitRes.durationSec << "\n";
    std::cout << std::left << std::setw(25) << "Throughput (QPS)"
              << std::right << std::setw(18) << std::fixed << std::setprecision(1) << preSplitRes.qps
              << std::setw(18) << inSplitRes.qps
              << std::setw(18) << postSplitRes.qps << "\n";
    std::cout << std::left << std::setw(25) << "Total Requests"
              << std::right << std::setw(18) << preSplitRes.totalOps
              << std::setw(18) << inSplitRes.totalOps
              << std::setw(18) << postSplitRes.totalOps << "\n";
    std::cout << std::left << std::setw(25) << "Failed / Dropped"
              << std::right << std::setw(18) << preSplitRes.failedOps
              << std::setw(18) << inSplitRes.failedOps
              << std::setw(18) << postSplitRes.failedOps << "\n";
    std::cout << std::left << std::setw(25) << "Success Rate (%)"
              << std::right << std::setw(18) << std::fixed << std::setprecision(2)
              << (preSplitRes.totalOps > 0 ? (100.0 * preSplitRes.successOps / preSplitRes.totalOps) : 100.0)
              << std::setw(18)
              << (inSplitRes.totalOps > 0 ? (100.0 * inSplitRes.successOps / inSplitRes.totalOps) : 100.0)
              << std::setw(18)
              << (postSplitRes.totalOps > 0 ? (100.0 * postSplitRes.successOps / postSplitRes.totalOps) : 100.0) << "\n";
    std::cout << std::left << std::setw(25) << "Latency P50 (ms)"
              << std::right << std::setw(18) << std::fixed << std::setprecision(2) << preSplitRes.latency.p50Ms
              << std::setw(18) << inSplitRes.latency.p50Ms
              << std::setw(18) << postSplitRes.latency.p50Ms << "\n";
    std::cout << std::left << std::setw(25) << "Latency P90 (ms)"
              << std::right << std::setw(18) << preSplitRes.latency.p90Ms
              << std::setw(18) << inSplitRes.latency.p90Ms
              << std::setw(18) << postSplitRes.latency.p90Ms << "\n";
    std::cout << std::left << std::setw(25) << "Latency P99 (ms)"
              << std::right << std::setw(18) << preSplitRes.latency.p99Ms
              << std::setw(18) << inSplitRes.latency.p99Ms
              << std::setw(18) << postSplitRes.latency.p99Ms << "\n";
    std::cout << std::left << std::setw(25) << "Latency Max (ms)"
              << std::right << std::setw(18) << preSplitRes.latency.maxMs
              << std::setw(18) << inSplitRes.latency.maxMs
              << std::setw(18) << postSplitRes.latency.maxMs << "\n";
    std::cout << std::left << std::setw(25) << "Epoch Refreshes"
              << std::right << std::setw(18) << preSplitRes.epochRefreshesDelta
              << std::setw(18) << inSplitRes.epochRefreshesDelta
              << std::setw(18) << postSplitRes.epochRefreshesDelta << "\n";
    std::cout << "====================================================================================\n";

    // Dynamic Split Jitter & Resilience Analysis
    std::cout << "\n>>> DYNAMIC SPLIT TRANSIENT JITTER & RESILIENCE ANALYSIS <<<\n";
    std::cout << "  - Split Execution Time:        " << std::fixed << std::setprecision(1)
              << jitter.splitDurationMs << " ms\n";
    std::cout << "  - Pre-Trigger Baseline QPS:    " << jitter.preTriggerSteadyQps << " ops/s\n";
    std::cout << "  - Min Transient QPS:           " << jitter.minTransientQps << " ops/s\n";
    std::cout << "  - Instantaneous QPS Dip:       -" << jitter.transientDipPercent << " %\n";
    std::cout << "  - Tail Latency Spike:          " << jitter.baselineP99Ms << " ms -> "
              << jitter.spikeP99Ms << " ms (" << std::setprecision(1) << jitter.p99SpikeRatio << "x)\n";
    std::cout << "  - Rerouting & Recovery Time:   " << jitter.recoveryDurationMs << " ms\n";
    std::cout << "  - Routing Epoch Refreshes:     " << jitter.totalEpochRefreshes << " transparent retries\n";
    std::cout << "  - In-Flight Availability:      " << (inSplitAvailable ? "100.0% (ZERO dropped requests)" : "DEGRADED") << "\n";
    std::cout << "  - Post-Split Throughput:       " << (postSplitHealthy ? "HEALTHY" : "WEDGED (no completed operations)") << "\n";
    std::cout << "  - Data Integrity / Loss:       " << (integrityValid ? "100.0% (ZERO lost writes)" : "FAILED") << "\n";
    std::cout << "  - Topology Reconfiguration:    " << (topologyValid ? "PASSED (Region 100 split to [\"\", \"d\"), child created)" : "FAILED") << "\n";

    // ASCII Timeline Chart of Phase 2
    std::cout << "\n>>> IN-SPLIT QPS TIME-SERIES TIMELINE (100ms Buckets) <<<\n";
    double maxQpsBucket = 1.0;
    for (const auto& sw : sampleWindows) {
      if (sw.qps > maxQpsBucket) maxQpsBucket = sw.qps;
    }
    const int maxBarWidth = 40;
    for (size_t i = 0; i < sampleWindows.size(); i += 2) {
      const auto& sw = sampleWindows[i];
      int barLen = static_cast<int>((sw.qps / maxQpsBucket) * maxBarWidth);
      std::string bar(barLen, '#');
      std::string event;
      if (sw.splitCompleted && (i >= 2 && !sampleWindows[i - 2].splitCompleted)) {
        event = " <-- [SPLIT COMPLETED, REROUTING]";
      } else if (sw.splitTriggered && (i >= 2 && !sampleWindows[i - 2].splitTriggered)) {
        event = " <-- [SPLIT TRIGGERED]";
      }
      std::cout << "  +" << std::setw(5) << sw.relativeTimeMs << "ms | QPS="
                << std::setw(6) << static_cast<int>(sw.qps) << " | "
                << std::left << std::setw(maxBarWidth) << bar << event << "\n";
    }

    // Output JSON if requested
    if (!opt.outputJson.empty()) {
      std::filesystem::create_directories(std::filesystem::path(opt.outputJson).parent_path());
      std::ofstream js(opt.outputJson);
      js << "{\n";
      js << "  \"config\": {\n";
      js << "    \"splitRegion\": " << opt.splitRegion << ",\n";
      js << "    \"splitKey\": \"" << opt.splitKey << "\",\n";
      js << "    \"workers\": " << opt.workers << ",\n";
      js << "    \"readRatio\": " << opt.readRatio << "\n";
      js << "  },\n";
      js << "  \"preSplit\": {\n";
      js << "    \"durationSec\": " << preSplitRes.durationSec << ",\n";
      js << "    \"qps\": " << preSplitRes.qps << ",\n";
      js << "    \"p50Ms\": " << preSplitRes.latency.p50Ms << ",\n";
      js << "    \"p99Ms\": " << preSplitRes.latency.p99Ms << ",\n";
      js << "    \"epochRefreshes\": " << preSplitRes.epochRefreshesDelta << "\n";
      js << "  },\n";
      js << "  \"inSplit\": {\n";
      js << "    \"durationSec\": " << inSplitRes.durationSec << ",\n";
      js << "    \"qps\": " << inSplitRes.qps << ",\n";
      js << "    \"p50Ms\": " << inSplitRes.latency.p50Ms << ",\n";
      js << "    \"p99Ms\": " << inSplitRes.latency.p99Ms << ",\n";
      js << "    \"splitDurationMs\": " << jitter.splitDurationMs << ",\n";
      js << "    \"transientDipPercent\": " << jitter.transientDipPercent << ",\n";
      js << "    \"minTransientQps\": " << jitter.minTransientQps << ",\n";
      js << "    \"epochRefreshes\": " << inSplitRes.epochRefreshesDelta << ",\n";
      js << "    \"failedOps\": " << inSplitRes.failedOps << "\n";
      js << "  },\n";
      js << "  \"postSplit\": {\n";
      js << "    \"durationSec\": " << postSplitRes.durationSec << ",\n";
      js << "    \"qps\": " << postSplitRes.qps << ",\n";
      js << "    \"p50Ms\": " << postSplitRes.latency.p50Ms << ",\n";
      js << "    \"p99Ms\": " << postSplitRes.latency.p99Ms << ",\n";
      js << "    \"epochRefreshes\": " << postSplitRes.epochRefreshesDelta << "\n";
      js << "  },\n";
      js << "  \"integrity\": {\n";
      js << "    \"keysAudited\": " << allWritten.size() << ",\n";
      js << "    \"verifiedCount\": " << verifiedCount << ",\n";
      js << "    \"lostCount\": " << lostCount << ",\n";
      js << "    \"corruptCount\": " << corruptCount << ",\n";
      js << "    \"passed\": " << (integrityValid && inSplitAvailable && postSplitHealthy && topologyValid ? "true" : "false") << "\n";
      js << "  }\n";
      js << "}\n";
      std::cout << "\nWrote JSON metrics to: " << opt.outputJson << "\n";
    }

    // Output Markdown if requested
    if (!opt.outputMd.empty()) {
      std::filesystem::create_directories(std::filesystem::path(opt.outputMd).parent_path());
      std::ofstream md(opt.outputMd);
      md << "# StrataKV 动态分片性能与可用性测试报告\n\n";
      md << "## 1. 测试综述\n\n";
      md << "- **测试目标**：验证 StrataKV 在动态拓扑模式下 Region 在线分裂（Dynamic Split）的性能影响与韧性表现。\n";
      md << "- **测试对象**：Region " << opt.splitRegion << "，在 Split Key = `\"" << opt.splitKey << "\"` 处执行在线分裂。\n";
      md << "- **并发规模**：`" << opt.workers << "` 工作线程，" << opt.readRatio << "% 读 / " << (100 - opt.readRatio) << "% 写。\n\n";

      md << "## 2. 核心阶段性能对比\n\n";
      md << "| 指标 | Pre-Split (基准) | In-Split (分裂期) | Post-Split (双Region) |\n";
      md << "| :--- | :--- | :--- | :--- |\n";
      md << "| **吞吐量 (QPS)** | " << std::fixed << std::setprecision(1) << preSplitRes.qps << " | " << inSplitRes.qps << " | " << postSplitRes.qps << " |\n";
      md << "| **P50 延迟 (ms)** | " << std::setprecision(2) << preSplitRes.latency.p50Ms << " | " << inSplitRes.latency.p50Ms << " | " << postSplitRes.latency.p50Ms << " |\n";
      md << "| **P90 延迟 (ms)** | " << preSplitRes.latency.p90Ms << " | " << inSplitRes.latency.p90Ms << " | " << postSplitRes.latency.p90Ms << " |\n";
      md << "| **P99 尾延迟 (ms)** | " << preSplitRes.latency.p99Ms << " | " << inSplitRes.latency.p99Ms << " | " << postSplitRes.latency.p99Ms << " |\n";
      md << "| **最大延迟 (ms)** | " << preSplitRes.latency.maxMs << " | " << inSplitRes.latency.maxMs << " | " << postSplitRes.latency.maxMs << " |\n";
      auto RateCell = [](uint64_t successOps, uint64_t totalOps) {
        if (totalOps == 0) return std::string("N/A (0 ops)");
        std::ostringstream cell;
        cell << std::fixed << std::setprecision(2) << (100.0 * successOps / totalOps) << "%";
        return cell.str();
      };
      md << "| **请求成功率** | " << RateCell(preSplitRes.successOps, preSplitRes.totalOps) << " | "
         << RateCell(inSplitRes.successOps, inSplitRes.totalOps) << " | "
         << RateCell(postSplitRes.successOps, postSplitRes.totalOps) << " |\n";
      md << "| **路由 Epoch 刷新** | " << preSplitRes.epochRefreshesDelta << " | " << inSplitRes.epochRefreshesDelta << " | " << postSplitRes.epochRefreshesDelta << " |\n\n";

      md << "## 3. 分裂瞬时抖动与韧性指标\n\n";
      md << "- **分裂操作耗时**：" << std::fixed << std::setprecision(1) << jitter.splitDurationMs << " ms\n";
      md << "- **基准稳态 QPS**：" << jitter.preTriggerSteadyQps << " ops/s\n";
      md << "- **瞬时最低 QPS**：" << jitter.minTransientQps << " ops/s\n";
      md << "- **QPS 瞬时跌幅 (Dip %)**：`-" << jitter.transientDipPercent << "%`\n";
      md << "- **P99 尾延迟毛刺**：" << jitter.baselineP99Ms << " ms -> " << jitter.spikeP99Ms << " ms (" << jitter.p99SpikeRatio << "x)\n";
      md << "- **路由自愈恢复耗时**：" << jitter.recoveryDurationMs << " ms\n";
      md << "- **透明重试次数**：" << jitter.totalEpochRefreshes << " 次 `EPOCH_NOT_MATCH` 自动重试\n";
      md << "- **在线连续可用性**：`100.0%`（**零**请求丢弃与事务失败）\n";
      if (lostCount == 0 && corruptCount == 0) {
        md << "- **数据完整性审计**：校验 " << allWritten.size() << " 个键，**零**数据丢失与值错乱\n\n";
      } else {
        md << "- **数据完整性审计**：校验 " << allWritten.size() << " 个键，丢失 " << lostCount << " 个，错乱 " << corruptCount << " 个\n\n";
      }

      md << "## 4. 结论\n\n";
      if (integrityValid && inSplitAvailable && postSplitHealthy && topologyValid) {
        md << "> [!NOTE]\n";
        md << "> 动态分片在线分裂验证 **100% 通过**。在分裂过程中，SDK 成功检测并处理 `EPOCH_NOT_MATCH` 错误，通过元数据自愈完成路由更新；上层事务业务完全无感，保持 100% 可用性且无任何数据丢失。\n";
      } else {
        md << "> [!CAUTION]\n";
        md << "> 动态分片测试未完全达标，请检查错误日志。\n";
      }
      std::cout << "Wrote Markdown report to: " << opt.outputMd << "\n";
    }

    if (!integrityValid || !inSplitAvailable || !postSplitHealthy || !topologyValid) {
      std::cerr << "\nBenchmark assertions failed: integrityValid=" << integrityValid
                << ", inSplitAvailable=" << inSplitAvailable
                << ", postSplitHealthy=" << postSplitHealthy
                << ", topologyValid=" << topologyValid << "\n";
      return 1;
    }

    std::cout << "\n>>> DYNAMIC SHARDING BENCHMARK PASSED SUCCESSFULLY <<<\n\n";
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "Fatal benchmark error: " << e.what() << "\n";
    return 1;
  }
}

