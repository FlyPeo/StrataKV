/*
 * 测试目标：验证真实多节点集群在并发跨 Region 事务和故障恢复后的原子性、可用性与持久性。
 * 测试策略：通过原生 C++ SDK 并发写入三键跨 Region 事务，记录每次提交确认与延迟，再并发读取校验；
 *           支持 verify-only 对故障注入或集群重启后的同一批数据再次验证。
 * 测试规模：默认 8 个 worker 执行 1,000 个事务，即写入并校验 3,000 个键；每次事务或
 *           读取最多重试 20 次，规模与重试预算均可通过参数调整。
 * 验证内容：确认已确认提交的事务三键全部存在且值一致，绝不出现只存在 1～2 个键的
 *           部分提交，并统计 availability、safety 和 verification failure。
 */
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "stratakv/client.h"

namespace {

struct Options {
  std::string regionsConfig;
  std::string tsoEndpoints = "127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302";
  std::string metadataEndpoints;
  std::string topologyMode = "static";
  std::string runId;
  std::string history = "test-results/reliability/standalone-history.jsonl";
  std::string verifyFrom;
  int workers = 8;
  int transactions = 1000;
  int retries = 20;
  int retryDelayMs = 100;
  int timeoutMs = 180000;
  uint64_t lockTtlMs = 2500;
  bool verifyOnly = false;
};

struct Record {
  bool commitAcknowledged = false;
  int attempts = 0;
  long long latencyMs = 0;
  std::string error;
  int presentKeys = 0;
  bool valuesCorrect = false;
  std::string verifyError;
};

void Usage(const char* program) {
  std::cout
      << "Usage: " << program << " [options]\n"
      << "  --regions-config PATH   Region configuration catalog\n"
      << "  --tso-endpoints CSV     TSO cluster endpoints\n"
      << "  --metadata-endpoints CSV Meta cluster endpoints for dynamic mode\n"
      << "  --topology-mode MODE    Topology mode: static or dynamic (default: static)\n"
      << "  --workers N             Concurrent workers (default 8)\n"
      << "  --transactions N        Cross-Region transactions (default 1000)\n"
      << "  --retries N             Attempts per transaction/read (default 20)\n"
      << "  --retry-delay-ms N      Base retry delay (default 100)\n"
      << "  --timeout-ms N          Per-request timeout (default 180000)\n"
      << "  --lock-ttl-ms N         Transaction lock TTL (default 4000)\n"
      << "  --run-id ID             Stable key namespace; required by --verify-only\n"
      << "  --history PATH          JSON Lines result file\n"
      << "  --verify-from PATH      Earlier history file with ground truth for --verify-only\n"
      << "  --verify-only           Do not write; verify an earlier run\n"
      << "  --help                  Show this help\n";
}

int ParsePositive(const std::string& text, const char* name) {
  try {
    size_t consumed = 0;
    const long value = std::stol(text, &consumed);
    if (consumed != text.size() || value <= 0 || value > 100000000) throw std::invalid_argument("range");
    return static_cast<int>(value);
  } catch (const std::exception&) {
    throw std::invalid_argument(std::string(name) + " must be a positive integer");
  }
}

Options ParseOptions(int argc, char** argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    if (argument == "--help") {
      Usage(argv[0]);
      std::exit(0);
    }
    if (argument == "--verify-only") {
      options.verifyOnly = true;
      continue;
    }
    if (index + 1 >= argc) throw std::invalid_argument("missing value for " + argument);
    const std::string value = argv[++index];
    if (argument == "--regions-config") options.regionsConfig = value;
    else if (argument == "--tso-endpoints") options.tsoEndpoints = value;
    else if (argument == "--metadata-endpoints") options.metadataEndpoints = value;
    else if (argument == "--topology-mode") options.topologyMode = value;
    else if (argument == "--workers") options.workers = ParsePositive(value, "--workers");
    else if (argument == "--transactions") options.transactions = ParsePositive(value, "--transactions");
    else if (argument == "--retries") options.retries = ParsePositive(value, "--retries");
    else if (argument == "--retry-delay-ms") options.retryDelayMs = ParsePositive(value, "--retry-delay-ms");
    else if (argument == "--timeout-ms") options.timeoutMs = ParsePositive(value, "--timeout-ms");
    else if (argument == "--lock-ttl-ms") options.lockTtlMs = static_cast<uint64_t>(ParsePositive(value, "--lock-ttl-ms"));
    else if (argument == "--run-id") options.runId = value;
    else if (argument == "--history") options.history = value;
    else if (argument == "--verify-from") options.verifyFrom = value;
    else throw std::invalid_argument("unknown option: " + argument);
  }

  if (options.runId.empty()) {
    if (options.verifyOnly) throw std::invalid_argument("--run-id is required with --verify-only");
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    options.runId = std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
  }
  if (!std::all_of(options.runId.begin(), options.runId.end(),
                   [](unsigned char c) { return std::isalnum(c) || c == '-' || c == '_'; })) {
    throw std::invalid_argument("--run-id may contain only letters, digits, '-' and '_'");
  }
  return options;
}

std::string JsonEscape(const std::string& value) {
  std::string output;
  for (const unsigned char c : value) {
    switch (c) {
      case '"': output += "\\\""; break;
      case '\\': output += "\\\\"; break;
      case '\n': output += "\\n"; break;
      case '\r': output += "\\r"; break;
      case '\t': output += "\\t"; break;
      default:
        if (c < 0x20) output += '?';
        else output += static_cast<char>(c);
    }
  }
  return output;
}

std::vector<std::string> Keys(const Options& options, int index) {
  const std::string suffix = ":reliability:" + options.runId + ':' + std::to_string(index);
  return {"a" + suffix, "h" + suffix, "p" + suffix};
}

std::string Value(const Options& options, int index) {
  return "value-" + options.runId + '-' + std::to_string(index);
}

void Backoff(const Options& options, int attempt, bool isLockConflict = false) {
  const int multiplier = std::min(attempt, 10);
  const int base = isLockConflict ? std::max(options.retryDelayMs, 300) : options.retryDelayMs;
  std::this_thread::sleep_for(std::chrono::milliseconds(base * multiplier));
}

void RunTransaction(const std::shared_ptr<stratakv::Client>& client, const Options& options,
                    int index, Record* record) {
  const auto started = std::chrono::steady_clock::now();
  const std::vector<std::string> keys = Keys(options, index);
  const std::string value = Value(options, index);

  for (int attempt = 1; attempt <= options.retries; ++attempt) {
    record->attempts = attempt;
    std::shared_ptr<stratakv::Transaction> txn;
    try {
      txn = client->Begin(options.lockTtlMs);
      for (const auto& key : keys) {
        auto res = client->Put(txn, key, value);
        if (!res.ok()) {
          throw std::runtime_error("Put failed for " + key + ": " + res.message);
        }
      }
      auto commitRes = client->Commit(txn);
      if (!commitRes.ok()) {
        throw std::runtime_error("Commit failed: " + commitRes.message);
      }
      record->commitAcknowledged = true;
      record->error.clear();
      break;
    } catch (const std::exception& error) {
      record->error = error.what();
      if (txn != nullptr && !txn->Finished()) {
        try {
          client->Rollback(txn);
        } catch (const std::exception&) {
        }
      }
      const bool isLockConflict = record->error.find("LOCK_CONFLICT") != std::string::npos;
      if (attempt < options.retries) Backoff(options, attempt, isLockConflict);
    }
  }
  record->latencyMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - started)
                          .count();
}

void VerifyTransaction(const std::shared_ptr<stratakv::Client>& client, const Options& options,
                       int index, Record* record) {
  const std::vector<std::string> keys = Keys(options, index);
  const std::string expected = Value(options, index);
  for (int attempt = 1; attempt <= options.retries; ++attempt) {
    std::shared_ptr<stratakv::Transaction> txn;
    try {
      txn = client->Begin(options.lockTtlMs);
      int present = 0;
      bool correct = true;
      for (const auto& key : keys) {
        auto res = client->Get(txn, key);
        if (res.ok() && res.found) {
          ++present;
          if (res.value != expected) correct = false;
        } else if (!res.ok() && res.status != stratakv::Status::kNotFound) {
          throw std::runtime_error("Get failed for " + key + ": " + res.message);
        }
      }
      client->Rollback(txn);
      record->presentKeys = present;
      record->valuesCorrect = correct && present == 3;
      record->verifyError.clear();
      return;
    } catch (const std::exception& error) {
      record->verifyError = error.what();
      if (txn != nullptr && !txn->Finished()) {
        try {
          client->Rollback(txn);
        } catch (const std::exception&) {
        }
      }
      const bool isLockConflict = record->verifyError.find("LOCK_CONFLICT") != std::string::npos;
      if (attempt < options.retries) Backoff(options, attempt, isLockConflict);
    }
  }
}

long long Percentile(std::vector<long long> values, double percentile) {
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  const size_t index = static_cast<size_t>(percentile * static_cast<double>(values.size() - 1));
  return values[index];
}

void WriteHistory(const Options& options, const std::vector<Record>& records, bool passed,
                  int safetyViolations, int verificationFailures) {
  const std::filesystem::path historyPath(options.history);
  if (historyPath.has_parent_path()) {
    std::filesystem::create_directories(historyPath.parent_path());
  }
  std::ofstream output(historyPath, std::ios::trunc);
  if (!output) throw std::runtime_error("cannot write history file " + options.history);
  for (int index = 0; index < options.transactions; ++index) {
    const Record& record = records[index];
    output << "{\"type\":\"transaction\",\"run_id\":\"" << JsonEscape(options.runId)
           << "\",\"index\":" << index
           << ",\"commit_acknowledged\":" << (record.commitAcknowledged ? "true" : "false")
           << ",\"attempts\":" << record.attempts
           << ",\"latency_ms\":" << record.latencyMs
           << ",\"present_keys\":" << record.presentKeys
           << ",\"values_correct\":" << (record.valuesCorrect ? "true" : "false")
           << ",\"error\":\"" << JsonEscape(record.error)
           << "\",\"verify_error\":\"" << JsonEscape(record.verifyError) << "\"}\n";
  }
  output << "{\"type\":\"summary\",\"run_id\":\"" << JsonEscape(options.runId)
         << "\",\"transactions\":" << options.transactions
         << ",\"safety_violations\":" << safetyViolations
         << ",\"verification_failures\":" << verificationFailures
         << ",\"passed\":" << (passed ? "true" : "false") << "}\n";
}

void LoadExpectedHistory(const std::string& path, std::vector<Record>* records) {
  std::ifstream input(path);
  if (!input) return;
  std::string line;
  while (std::getline(input, line)) {
    if (line.find("\"type\":\"transaction\"") == std::string::npos) continue;
    auto idxPos = line.find("\"index\":");
    auto ackPos = line.find("\"commit_acknowledged\":");
    if (idxPos != std::string::npos && ackPos != std::string::npos) {
      try {
        const int index = std::stoi(line.substr(idxPos + 8));
        const bool ack = line.substr(ackPos + 22, 4) == "true";
        if (index >= 0 && static_cast<size_t>(index) < records->size()) {
          (*records)[index].commitAcknowledged = ack;
        }
      } catch (const std::exception&) {
      }
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const Options options = ParseOptions(argc, argv);
    stratakv::ConnectionOptions connOpts;
    if (!options.metadataEndpoints.empty() || options.topologyMode == "dynamic") {
      connOpts.topologyMode = stratakv::TopologyMode::kDynamic;
      connOpts.metadataEndpoints = options.metadataEndpoints;
      connOpts.metadataTimeoutMs = 5000;
    } else {
      connOpts.topologyMode = stratakv::TopologyMode::kStatic;
      connOpts.regionConfigPath = options.regionsConfig;
    }
    connOpts.tsoEndpoints = options.tsoEndpoints;

    auto client = stratakv::Client::Connect(connOpts);
    std::vector<Record> records(static_cast<size_t>(options.transactions));

    const auto workloadStart = std::chrono::steady_clock::now();
    if (!options.verifyOnly) {
      std::atomic<int> next{0};
      std::vector<std::thread> workers;
      for (int worker = 0; worker < options.workers; ++worker) {
        workers.emplace_back([&]() {
          while (true) {
            const int index = next.fetch_add(1);
            if (index >= options.transactions) return;
            RunTransaction(client, options, index, &records[index]);
          }
        });
      }
      for (auto& worker : workers) worker.join();
    } else {
      if (!options.verifyFrom.empty()) {
        LoadExpectedHistory(options.verifyFrom, &records);
      }
    }
    const long long workloadMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::steady_clock::now() - workloadStart)
                                     .count();

    std::atomic<int> verifyNext{0};
    std::vector<std::thread> verifiers;
    for (int worker = 0; worker < options.workers; ++worker) {
      verifiers.emplace_back([&]() {
        while (true) {
          const int index = verifyNext.fetch_add(1);
          if (index >= options.transactions) return;
          VerifyTransaction(client, options, index, &records[index]);
        }
      });
    }
    for (auto& verifier : verifiers) verifier.join();

    int committed = 0;
    int safetyViolations = 0;
    int verificationFailures = 0;
    std::vector<long long> latencies;
    for (const Record& record : records) {
      if (record.commitAcknowledged) {
        ++committed;
        if (!options.verifyOnly) latencies.push_back(record.latencyMs);
      }
      if (!record.verifyError.empty()) {
        ++verificationFailures;
        continue;
      }
      if (record.presentKeys == 1 || record.presentKeys == 2) {
        ++safetyViolations;
      } else if (record.presentKeys == 3 && !record.valuesCorrect) {
        ++safetyViolations;
      } else if (record.commitAcknowledged && record.presentKeys != 3) {
        ++safetyViolations;
      }
    }

    const bool passed = safetyViolations == 0 && verificationFailures == 0 &&
                        (options.verifyOnly ? (!options.verifyFrom.empty() ? (committed > 0) : true)
                                           : (committed == options.transactions));
    WriteHistory(options, records, passed, safetyViolations, verificationFailures);

    const double seconds = std::max(0.001, static_cast<double>(workloadMs) / 1000.0);
    std::cout << "run_id=" << options.runId << '\n'
              << "transactions_total=" << options.transactions << '\n'
              << "transactions_committed=" << committed << '\n'
              << "availability_failures=" << (options.transactions - committed) << '\n'
              << "safety_violations=" << safetyViolations << '\n'
              << "verification_failures=" << verificationFailures << '\n';
    if (!options.verifyOnly) {
      std::cout << "throughput_txn_per_sec=" << (static_cast<double>(options.transactions) / seconds) << '\n'
                << "latency_p50_ms=" << Percentile(latencies, 0.50) << '\n'
                << "latency_p95_ms=" << Percentile(latencies, 0.95) << '\n'
                << "latency_p99_ms=" << Percentile(latencies, 0.99) << '\n';
    }
    std::cout << "history=" << options.history << '\n'
              << (passed ? "RELIABILITY PASS" : "RELIABILITY FAIL") << '\n';
    return passed ? 0 : 1;
  } catch (const std::exception& error) {
    std::cerr << "stratakv-test-reliability: " << error.what() << '\n';
    return 2;
  }
}
