/*
 * 测试目标：让 Direct SDK 与 Gateway 使用完全相同的 YCSB-compatible 操作流和统计边界。
 * 测试策略：worker 从全局序号领取任务，在独占 adapter 上执行单键事务并聚合线程局部指标。
 * 测试规模：smoke 为 3,000-record Load 与每点 10,000 operations，最大并发 8；full 可到 32。
 * 验证内容：Read/Update/RMW 的事务边界正确，失败不计成功，重试与退避包含在端到端延迟。
 */
#include "support/performance/workload_runner.h"

#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <exception>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <utility>

#include "region_metadata.h"
#include "stratakv/client.h"

namespace stratakv::test::performance {
namespace {

struct Endpoint {
  std::string host;
  std::string port;
};

struct HttpResponse {
  int status = 0;
  std::string body;
};

std::optional<std::string> JsonString(const std::string& json, const std::string& key) {
  const std::string marker = "\"" + key + "\"";
  size_t position = json.find(marker);
  if (position == std::string::npos) return std::nullopt;
  position = json.find(':', position + marker.size());
  if (position == std::string::npos) return std::nullopt;
  position = json.find('"', position + 1);
  if (position == std::string::npos) return std::nullopt;
  ++position;
  std::string value;
  bool escaped = false;
  for (; position < json.size(); ++position) {
    const char character = json[position];
    if (escaped) {
      switch (character) {
        case 'n': value += '\n'; break;
        case 'r': value += '\r'; break;
        case 't': value += '\t'; break;
        default: value += character; break;
      }
      escaped = false;
    } else if (character == '\\') {
      escaped = true;
    } else if (character == '"') {
      return value;
    } else {
      value += character;
    }
  }
  return std::nullopt;
}

std::string UrlEncode(const std::string& value) {
  static constexpr char hex[] = "0123456789ABCDEF";
  std::string result;
  for (const unsigned char character : value) {
    if (std::isalnum(character) || character == '-' || character == '_' || character == '.' || character == '~') {
      result += static_cast<char>(character);
    } else {
      result += '%';
      result += hex[(character >> 4U) & 0x0fU];
      result += hex[character & 0x0fU];
    }
  }
  return result;
}

Endpoint ParseEndpoint(std::string value) {
  constexpr const char* prefix = "http://";
  if (value.rfind(prefix, 0) != 0) throw std::invalid_argument("Gateway URL must start with http://");
  value.erase(0, std::char_traits<char>::length(prefix));
  if (value.find('/') != std::string::npos) throw std::invalid_argument("Gateway URL must not contain a path");
  const size_t separator = value.rfind(':');
  if (separator == std::string::npos || separator == 0 || separator + 1 == value.size()) {
    throw std::invalid_argument("Gateway URL must contain host:port");
  }
  return {value.substr(0, separator), value.substr(separator + 1)};
}

int Connect(const Endpoint& endpoint, int timeoutMs) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* addresses = nullptr;
  if (getaddrinfo(endpoint.host.c_str(), endpoint.port.c_str(), &hints, &addresses) != 0) {
    throw std::runtime_error("cannot resolve Gateway host");
  }
  int socketFd = -1;
  for (addrinfo* address = addresses; address != nullptr; address = address->ai_next) {
    socketFd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (socketFd < 0) continue;
    const timeval timeout{timeoutMs / 1000, (timeoutMs % 1000) * 1000};
    setsockopt(socketFd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(socketFd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    if (connect(socketFd, address->ai_addr, address->ai_addrlen) == 0) break;
    close(socketFd);
    socketFd = -1;
  }
  freeaddrinfo(addresses);
  if (socketFd < 0) throw std::runtime_error("cannot connect to Gateway");
  return socketFd;
}

HttpResponse Request(const Endpoint& endpoint, int timeoutMs, const std::string& method,
                     const std::string& path, const std::string& body = {}) {
  const int socketFd = Connect(endpoint, timeoutMs);
  std::ostringstream request;
  request << method << ' ' << path << " HTTP/1.1\r\n"
          << "Host: " << endpoint.host << ':' << endpoint.port << "\r\n"
          << "Connection: close\r\n";
  if (!body.empty()) request << "Content-Type: application/json\r\n";
  request << "Content-Length: " << body.size() << "\r\n\r\n" << body;
  const std::string encoded = request.str();
  size_t sent = 0;
  while (sent < encoded.size()) {
    const ssize_t written = send(socketFd, encoded.data() + sent, encoded.size() - sent, MSG_NOSIGNAL);
    if (written <= 0) {
      close(socketFd);
      throw std::runtime_error("failed to send Gateway request");
    }
    sent += static_cast<size_t>(written);
  }

  std::string response;
  char buffer[8192];
  while (true) {
    const ssize_t received = recv(socketFd, buffer, sizeof(buffer), 0);
    if (received == 0) break;
    if (received < 0) {
      close(socketFd);
      throw std::runtime_error("failed or timed out reading Gateway response");
    }
    response.append(buffer, static_cast<size_t>(received));
    if (response.size() > 4U * 1024U * 1024U) {
      close(socketFd);
      throw std::runtime_error("Gateway response is too large");
    }
  }
  close(socketFd);
  const size_t lineEnd = response.find("\r\n");
  const size_t bodyStart = response.find("\r\n\r\n");
  if (lineEnd == std::string::npos || bodyStart == std::string::npos) {
    throw std::runtime_error("invalid Gateway HTTP response");
  }
  std::istringstream statusLine(response.substr(0, lineEnd));
  std::string version;
  HttpResponse parsed;
  if (!(statusLine >> version >> parsed.status)) throw std::runtime_error("invalid Gateway status line");
  parsed.body = response.substr(bodyStart + 4);
  return parsed;
}

AdapterStatus StatusFromName(const std::string& name) {
  if (name == "OK" || name == "ALREADY_COMMITTED") return AdapterStatus::kOk;
  if (name == "NOT_FOUND") return AdapterStatus::kNotFound;
  if (name == "LOCK_CONFLICT" || name == "WRITE_CONFLICT") return AdapterStatus::kConflict;
  if (name == "TIMEOUT") return AdapterStatus::kTimeout;
  if (name == "CLEANUP_PENDING") return AdapterStatus::kCleanupPending;
  if (name == "RESULT_UNKNOWN") return AdapterStatus::kResultUnknown;
  if (name == "INVALID_TRANSACTION" || name == "ABORT_ONLY") return AdapterStatus::kInvalid;
  return AdapterStatus::kUnavailable;
}

bool Retryable(AdapterStatus status) {
  return status == AdapterStatus::kConflict || status == AdapterStatus::kTimeout ||
         status == AdapterStatus::kUnavailable;
}

AdapterResult GatewayResult(const HttpResponse& response) {
  const std::string statusName = JsonString(response.body, "status").value_or("UNAVAILABLE");
  AdapterResult result;
  result.status = StatusFromName(statusName);
  result.value = JsonString(response.body, "value").value_or("");
  result.found = result.status == AdapterStatus::kOk && response.status == 200 &&
                 response.body.find("\"value\"") != std::string::npos;
  result.retryable = Retryable(result.status);
  result.message = JsonString(response.body, "message").value_or(statusName);
  return result;
}

class GatewayTransaction final : public TransactionHandle {
 public:
  explicit GatewayTransaction(std::string id) : id(std::move(id)) {}
  std::string id;
};

class GatewayAdapter final : public ClientAdapter {
 public:
  GatewayAdapter(Endpoint endpoint, int timeoutMs) : endpoint_(std::move(endpoint)), timeoutMs_(timeoutMs) {}

  std::unique_ptr<TransactionHandle> Begin(uint64_t lockTtlMs) override {
    const HttpResponse response = Request(endpoint_, timeoutMs_, "POST", "/v1/transactions",
                                          "{\"lockTtlMs\":" + std::to_string(lockTtlMs) + "}");
    const auto id = JsonString(response.body, "id");
    if (response.status != 201 || !id || id->empty()) {
      throw std::runtime_error("Gateway Begin failed: " + JsonString(response.body, "message").value_or("HTTP error"));
    }
    return std::make_unique<GatewayTransaction>(*id);
  }

  AdapterResult Get(TransactionHandle* transaction, const std::string& key) override {
    return GatewayResult(Request(endpoint_, timeoutMs_, "GET", Path(transaction) + "/keys/" + UrlEncode(key)));
  }

  AdapterResult Put(TransactionHandle* transaction, const std::string& key,
                    const std::string& value) override {
    return GatewayResult(Request(endpoint_, timeoutMs_, "POST", Path(transaction) + "/mutations",
                                 "{\"key\":\"" + JsonEscape(key) + "\",\"value\":\"" +
                                     JsonEscape(value) + "\"}"));
  }

  AdapterResult Commit(TransactionHandle* transaction) override {
    return GatewayResult(Request(endpoint_, timeoutMs_, "POST", Path(transaction) + "/commit"));
  }

  AdapterResult Rollback(TransactionHandle* transaction) override {
    const HttpResponse response = Request(endpoint_, timeoutMs_, "POST", Path(transaction) + "/rollback");
    if (response.status == 404) return {AdapterStatus::kOk};
    return GatewayResult(response);
  }

 private:
  std::string Path(TransactionHandle* transaction) const {
    auto* gateway = dynamic_cast<GatewayTransaction*>(transaction);
    if (gateway == nullptr) throw std::invalid_argument("invalid Gateway transaction handle");
    return "/v1/transactions/" + gateway->id;
  }

  Endpoint endpoint_;
  int timeoutMs_;
};

AdapterStatus FromSdkStatus(stratakv::Status status) {
  switch (status) {
    case stratakv::Status::kOk:
    case stratakv::Status::kAlreadyCommitted: return AdapterStatus::kOk;
    case stratakv::Status::kNotFound: return AdapterStatus::kNotFound;
    case stratakv::Status::kLockConflict:
    case stratakv::Status::kWriteConflict: return AdapterStatus::kConflict;
    case stratakv::Status::kTimeout: return AdapterStatus::kTimeout;
    case stratakv::Status::kCleanupPending: return AdapterStatus::kCleanupPending;
    case stratakv::Status::kResultUnknown: return AdapterStatus::kResultUnknown;
    case stratakv::Status::kAbortOnly:
    case stratakv::Status::kInvalidTransaction: return AdapterStatus::kInvalid;
    case stratakv::Status::kUnavailable: return AdapterStatus::kUnavailable;
  }
  return AdapterStatus::kUnavailable;
}

AdapterResult FromSdkResult(const stratakv::Result& source) {
  AdapterResult result;
  result.status = FromSdkStatus(source.status);
  result.value = source.value;
  result.found = source.found;
  result.retryable = source.retryable;
  result.message = source.message;
  return result;
}

class DirectTransaction final : public TransactionHandle {
 public:
  explicit DirectTransaction(std::shared_ptr<stratakv::Transaction> transaction)
      : transaction(std::move(transaction)) {}
  std::shared_ptr<stratakv::Transaction> transaction;
};

class DirectAdapter final : public ClientAdapter {
 public:
  explicit DirectAdapter(std::shared_ptr<stratakv::Client> client) : client_(std::move(client)) {}

  std::unique_ptr<TransactionHandle> Begin(uint64_t lockTtlMs) override {
    return std::make_unique<DirectTransaction>(client_->Begin(lockTtlMs));
  }
  AdapterResult Get(TransactionHandle* transaction, const std::string& key) override {
    return FromSdkResult(client_->Get(Handle(transaction), key));
  }
  AdapterResult Put(TransactionHandle* transaction, const std::string& key,
                    const std::string& value) override {
    return FromSdkResult(client_->Put(Handle(transaction), key, value));
  }
  AdapterResult Commit(TransactionHandle* transaction) override {
    return FromSdkResult(client_->Commit(Handle(transaction)));
  }
  AdapterResult Rollback(TransactionHandle* transaction) override {
    return FromSdkResult(client_->Rollback(Handle(transaction)));
  }

 private:
  std::shared_ptr<stratakv::Transaction> Handle(TransactionHandle* transaction) const {
    auto* direct = dynamic_cast<DirectTransaction*>(transaction);
    if (direct == nullptr) throw std::invalid_argument("invalid Direct transaction handle");
    return direct->transaction;
  }
  std::shared_ptr<stratakv::Client> client_;
};

struct WorkerSummary {
  RunSummary summary;
};

AdapterResult ExecuteOnce(ClientAdapter* adapter, OperationKind kind, const std::string& key,
                          const std::string& value) {
  auto transaction = adapter->Begin(120000);
  AdapterResult result;
  if (kind == OperationKind::kRead) {
    result = adapter->Get(transaction.get(), key);
    const AdapterResult rollback = adapter->Rollback(transaction.get());
    if (result.ok() && !rollback.ok()) result = rollback;
    return result;
  }
  if (kind == OperationKind::kReadModifyWrite) {
    result = adapter->Get(transaction.get(), key);
    if (!result.ok()) {
      (void)adapter->Rollback(transaction.get());
      return result;
    }
  }
  result = adapter->Put(transaction.get(), key, value);
  if (!result.ok()) {
    (void)adapter->Rollback(transaction.get());
    return result;
  }
  return adapter->Commit(transaction.get());
}

void CountFailure(AdapterStatus status, RunSummary* summary) {
  switch (status) {
    case AdapterStatus::kConflict: ++summary->conflicts; break;
    case AdapterStatus::kTimeout: ++summary->timeouts; break;
    case AdapterStatus::kCleanupPending: ++summary->cleanupPending; break;
    case AdapterStatus::kResultUnknown: ++summary->resultUnknown; break;
    case AdapterStatus::kOk: break;
    case AdapterStatus::kNotFound:
    case AdapterStatus::kUnavailable:
    case AdapterStatus::kInvalid: ++summary->unavailable; break;
  }
}

RunSummary Run(const WorkloadSpec& spec, const RegionKeyCodec& keys, const AdapterFactory& factory,
               const std::string& caseId, bool load) {
  spec.Validate();
  const uint64_t total = load ? spec.recordCount : spec.operationCount;
  OperationGenerator generator(spec);
  std::atomic<uint64_t> next{0};
  std::vector<WorkerSummary> local(static_cast<size_t>(spec.workers));
  std::vector<std::thread> workers;
  std::exception_ptr workerError;
  std::mutex errorMutex;
  const auto runStarted = std::chrono::steady_clock::now();
  for (int workerIndex = 0; workerIndex < spec.workers; ++workerIndex) {
    workers.emplace_back([&, workerIndex]() {
      try {
        auto adapter = factory();
        RunSummary& summary = local[static_cast<size_t>(workerIndex)].summary;
        while (true) {
          const uint64_t sequence = next.fetch_add(1, std::memory_order_relaxed);
          if (sequence >= total) return;
          const Operation operation = load ? Operation{sequence, sequence, OperationKind::kUpdate}
                                           : generator.At(sequence);
          ++summary.attempted;
          summary.reads += operation.kind == OperationKind::kRead;
          summary.updates += operation.kind == OperationKind::kUpdate;
          summary.readModifyWrites += operation.kind == OperationKind::kReadModifyWrite;
          const std::string key = keys.Key(operation.recordId);
          const std::string value = StableValue(operation.sequence, spec.valueSize);
          const auto started = std::chrono::steady_clock::now();
          AdapterResult result;
          for (int attempt = 1; attempt <= spec.maxAttempts; ++attempt) {
            try {
              result = ExecuteOnce(adapter.get(), operation.kind, key, value);
            } catch (const std::exception& error) {
              result = {AdapterStatus::kUnavailable, {}, false, true, error.what()};
            }
            if (result.ok() || !result.retryable || attempt == spec.maxAttempts) break;
            ++summary.retries;
            std::this_thread::sleep_for(std::chrono::milliseconds(spec.retryDelayMs * attempt));
          }
          const uint64_t latency = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                             std::chrono::steady_clock::now() - started)
                                                             .count());
          summary.latency.Record(latency);
          if (result.ok()) ++summary.successful;
          else CountFailure(result.status, &summary);
        }
      } catch (...) {
        std::lock_guard<std::mutex> lock(errorMutex);
        if (workerError == nullptr) workerError = std::current_exception();
      }
    });
  }
  for (auto& worker : workers) worker.join();
  if (workerError != nullptr) std::rethrow_exception(workerError);

  RunSummary combined;
  combined.caseId = caseId;
  combined.path = spec.path;
  combined.workload = load ? "load" : WorkloadName(spec.workload);
  combined.distribution = DistributionName(spec.distribution);
  combined.workers = spec.workers;
  combined.elapsedSeconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - runStarted).count();
  for (const auto& worker : local) {
    const RunSummary& source = worker.summary;
    combined.attempted += source.attempted;
    combined.successful += source.successful;
    combined.reads += source.reads;
    combined.updates += source.updates;
    combined.readModifyWrites += source.readModifyWrites;
    combined.conflicts += source.conflicts;
    combined.timeouts += source.timeouts;
    combined.unavailable += source.unavailable;
    combined.cleanupPending += source.cleanupPending;
    combined.resultUnknown += source.resultUnknown;
    combined.retries += source.retries;
    combined.latency.Merge(source.latency);
  }
  return combined;
}

}  // namespace

AdapterFactory DirectAdapterFactory(const std::string& regionsConfig, const std::string& tsoEndpoints) {
  return [regionsConfig, tsoEndpoints]() {
    return std::make_unique<DirectAdapter>(stratakv::Client::Connect(regionsConfig, tsoEndpoints));
  };
}

AdapterFactory GatewayAdapterFactory(const std::string& gateway, int timeoutMs) {
  const Endpoint endpoint = ParseEndpoint(gateway);
  return [endpoint, timeoutMs]() { return std::make_unique<GatewayAdapter>(endpoint, timeoutMs); };
}

std::vector<RegionRange> LoadRegionRanges(const std::string& regionsConfig) {
  const RegionCatalog catalog = RegionCatalog::LoadFromConfig(regionsConfig);
  std::vector<RegionRange> ranges;
  ranges.reserve(catalog.Regions().size());
  for (const auto& region : catalog.Regions()) {
    ranges.push_back({region.regionId, region.startKey, region.endKey});
  }
  return ranges;
}

RunSummary LoadRecords(const WorkloadSpec& spec, const RegionKeyCodec& keys,
                       const AdapterFactory& factory, const std::string& caseId) {
  return Run(spec, keys, factory, caseId, true);
}

RunSummary RunRecords(const WorkloadSpec& spec, const RegionKeyCodec& keys,
                      const AdapterFactory& factory, const std::string& caseId) {
  return Run(spec, keys, factory, caseId, false);
}

A2Summary RunA2(const A2TransactionGenerator& generator, const AdapterFactory& factory,
                const std::string& caseId, uint64_t transactionCount, int workers,
                int maxAttempts, int retryDelayMs, int timeoutMs) {
  struct A2WorkerState {
    uint64_t localAttempted = 0;
    uint64_t localCommitted = 0;
    uint64_t localFailed = 0;

    uint64_t distributedAttempted = 0;
    uint64_t distributedCommitted = 0;
    uint64_t distributedFailed = 0;

    Histogram totalLatency;
    Histogram localLatency;
    Histogram distributedLatency;

    std::map<int, uint64_t> localRegionTxnCount;
    std::map<int, uint64_t> distributedRegionMutationCount;
  };

  std::atomic<uint64_t> next{0};
  std::vector<A2WorkerState> localStates(static_cast<size_t>(workers));
  std::vector<std::thread> threadPool;
  std::exception_ptr workerError;
  std::mutex errorMutex;
  const auto runStarted = std::chrono::steady_clock::now();

  for (int workerIndex = 0; workerIndex < workers; ++workerIndex) {
    threadPool.emplace_back([&, workerIndex]() {
      try {
        auto adapter = factory();
        A2WorkerState& state = localStates[static_cast<size_t>(workerIndex)];
        while (true) {
          const uint64_t seq = next.fetch_add(1, std::memory_order_relaxed);
          if (seq >= transactionCount) return;

          const A2Transaction txn = generator.At(seq);
          if (txn.isDistributed) {
            ++state.distributedAttempted;
            for (const auto& mut : txn.mutations) {
              ++state.distributedRegionMutationCount[mut.regionId];
            }
          } else {
            ++state.localAttempted;
            ++state.localRegionTxnCount[txn.targetRegionId];
          }

          const auto started = std::chrono::steady_clock::now();
          bool committed = false;
          for (int attempt = 1; attempt <= maxAttempts; ++attempt) {
            auto handle = adapter->Begin(timeoutMs);
            bool putOk = true;
            for (const auto& mut : txn.mutations) {
              const AdapterResult res = adapter->Put(handle.get(), mut.key, mut.value);
              if (!res.ok()) {
                putOk = false;
                break;
              }
            }
            if (putOk) {
              const AdapterResult commitRes = adapter->Commit(handle.get());
              if (commitRes.ok()) {
                committed = true;
                break;
              }
            }
            (void)adapter->Rollback(handle.get());
            if (attempt == maxAttempts) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(retryDelayMs * attempt));
          }

          const uint64_t latencyUs = static_cast<uint64_t>(
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - started).count());

          state.totalLatency.Record(latencyUs);
          if (txn.isDistributed) {
            state.distributedLatency.Record(latencyUs);
            if (committed) ++state.distributedCommitted;
            else ++state.distributedFailed;
          } else {
            state.localLatency.Record(latencyUs);
            if (committed) ++state.localCommitted;
            else ++state.localFailed;
          }
        }
      } catch (...) {
        std::lock_guard<std::mutex> lock(errorMutex);
        if (workerError == nullptr) workerError = std::current_exception();
      }
    });
  }

  for (auto& t : threadPool) t.join();
  if (workerError != nullptr) std::rethrow_exception(workerError);

  A2Summary summary;
  summary.caseId = caseId;
  summary.targetCrossPercent = generator.CrossPercent();
  summary.workers = workers;
  summary.elapsedSeconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - runStarted).count();

  for (const auto& s : localStates) {
    summary.localAttempted += s.localAttempted;
    summary.localCommitted += s.localCommitted;
    summary.localFailed += s.localFailed;

    summary.distributedAttempted += s.distributedAttempted;
    summary.distributedCommitted += s.distributedCommitted;
    summary.distributedFailed += s.distributedFailed;

    summary.totalLatency.Merge(s.totalLatency);
    summary.localLatency.Merge(s.localLatency);
    summary.distributedLatency.Merge(s.distributedLatency);

    for (const auto& pair : s.localRegionTxnCount) {
      summary.localRegionTxnCount[pair.first] += pair.second;
    }
    for (const auto& pair : s.distributedRegionMutationCount) {
      summary.distributedRegionMutationCount[pair.first] += pair.second;
    }
  }

  summary.totalAttempted = summary.localAttempted + summary.distributedAttempted;
  summary.totalCommitted = summary.localCommitted + summary.distributedCommitted;
  summary.totalFailed = summary.localFailed + summary.distributedFailed;

  return summary;
}

A3Summary RunA3(const A3TransactionGenerator& generator, const AdapterFactory& factory,
                const std::string& caseId, uint64_t transactionCount, int workers,
                int maxAttempts, int retryDelayMs, int timeoutMs) {
  struct A3WorkerState {
    uint64_t attempted = 0;
    uint64_t committed = 0;
    uint64_t failed = 0;
    Histogram latency;
    std::map<int, uint64_t> regionMutationCounts;
  };

  std::atomic<uint64_t> next{0};
  std::vector<A3WorkerState> localStates(static_cast<size_t>(workers));
  std::vector<std::thread> threadPool;
  std::exception_ptr workerError;
  std::mutex errorMutex;
  const auto runStarted = std::chrono::steady_clock::now();

  for (int workerIndex = 0; workerIndex < workers; ++workerIndex) {
    threadPool.emplace_back([&, workerIndex]() {
      try {
        auto adapter = factory();
        A3WorkerState& state = localStates[static_cast<size_t>(workerIndex)];
        while (true) {
          const uint64_t seq = next.fetch_add(1, std::memory_order_relaxed);
          if (seq >= transactionCount) return;

          const A3Transaction txn = generator.At(seq);
          ++state.attempted;
          for (const auto& mut : txn.mutations) {
            ++state.regionMutationCounts[mut.regionId];
          }

          const auto started = std::chrono::steady_clock::now();
          bool committed = false;
          for (int attempt = 1; attempt <= maxAttempts; ++attempt) {
            auto handle = adapter->Begin(timeoutMs);
            bool putOk = true;
            for (const auto& mut : txn.mutations) {
              const AdapterResult res = adapter->Put(handle.get(), mut.key, mut.value);
              if (!res.ok()) {
                putOk = false;
                break;
              }
            }
            if (putOk) {
              const AdapterResult commitRes = adapter->Commit(handle.get());
              if (commitRes.ok()) {
                committed = true;
                break;
              }
            }
            (void)adapter->Rollback(handle.get());
            if (attempt == maxAttempts) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(retryDelayMs * attempt));
          }

          const uint64_t latencyUs = static_cast<uint64_t>(
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - started).count());

          state.latency.Record(latencyUs);
          if (committed) ++state.committed;
          else ++state.failed;
        }
      } catch (...) {
        std::lock_guard<std::mutex> lock(errorMutex);
        if (workerError == nullptr) workerError = std::current_exception();
      }
    });
  }

  for (auto& t : threadPool) t.join();
  if (workerError != nullptr) std::rethrow_exception(workerError);

  A3Summary summary;
  summary.caseId = caseId;
  summary.fanout = generator.Fanout();
  summary.workers = workers;
  summary.elapsedSeconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - runStarted).count();

  for (const auto& s : localStates) {
    summary.totalAttempted += s.attempted;
    summary.totalCommitted += s.committed;
    summary.totalFailed += s.failed;
    summary.latency.Merge(s.latency);
    for (const auto& pair : s.regionMutationCounts) {
      summary.regionMutationCounts[pair.first] += pair.second;
    }
  }

  return summary;
}

}  // namespace stratakv::test::performance
