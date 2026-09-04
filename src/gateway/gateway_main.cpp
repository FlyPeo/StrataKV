// Public HTTP/JSON gateway entrypoint.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>

#include <pulsar/fd_manager.hpp>
#include <pulsar/iomanager.hpp>
#include <pulsar/stack_allocator.hpp>
#include <pulsar/sync.hpp>

#include "bounded_thread_pool.h"
#include "stratakv/client.h"

namespace {

struct HttpRequest {
  std::string method;
  std::string path;
  std::string body;
};

struct HttpResponse {
  int status = 200;
  std::string body;
  std::string contentType = "application/json; charset=utf-8";
};

std::string JsonEscape(const std::string& value) {
  std::string result;
  result.reserve(value.size() + 8);
  for (unsigned char c : value) {
    switch (c) {
      case '"': result += "\\\""; break;
      case '\\': result += "\\\\"; break;
      case '\n': result += "\\n"; break;
      case '\r': result += "\\r"; break;
      case '\t': result += "\\t"; break;
      default:
        if (c < 0x20) {
          const char* hex = "0123456789abcdef";
          result += "\\u00";
          result += hex[(c >> 4) & 0x0f];
          result += hex[c & 0x0f];
        } else {
          result += static_cast<char>(c);
        }
    }
  }
  return result;
}

std::optional<std::string> JsonString(const std::string& body, const std::string& field) {
  const std::string name = "\"" + field + "\"";
  const size_t fieldPos = body.find(name);
  if (fieldPos == std::string::npos) return std::nullopt;
  size_t cursor = body.find(':', fieldPos + name.size());
  if (cursor == std::string::npos) return std::nullopt;
  ++cursor;
  while (cursor < body.size() && std::isspace(static_cast<unsigned char>(body[cursor]))) ++cursor;
  if (cursor >= body.size() || body[cursor] != '"') return std::nullopt;
  ++cursor;

  std::string value;
  while (cursor < body.size()) {
    const char c = body[cursor++];
    if (c == '"') return value;
    if (c != '\\') {
      value += c;
      continue;
    }
    if (cursor >= body.size()) return std::nullopt;
    const char escaped = body[cursor++];
    switch (escaped) {
      case '"': value += '"'; break;
      case '\\': value += '\\'; break;
      case '/': value += '/'; break;
      case 'b': value += '\b'; break;
      case 'f': value += '\f'; break;
      case 'n': value += '\n'; break;
      case 'r': value += '\r'; break;
      case 't': value += '\t'; break;
      default: return std::nullopt;  // Unicode escapes are intentionally rejected in v1.
    }
  }
  return std::nullopt;
}

bool JsonBool(const std::string& body, const std::string& field, bool defaultValue) {
  const std::string name = "\"" + field + "\"";
  const size_t fieldPos = body.find(name);
  if (fieldPos == std::string::npos) return defaultValue;
  size_t cursor = body.find(':', fieldPos + name.size());
  if (cursor == std::string::npos) return defaultValue;
  ++cursor;
  while (cursor < body.size() && std::isspace(static_cast<unsigned char>(body[cursor]))) ++cursor;
  if (cursor + 4 <= body.size() && body.compare(cursor, 4, "true") == 0) return true;
  if (cursor + 5 <= body.size() && body.compare(cursor, 5, "false") == 0) return false;
  return defaultValue;
}

std::optional<uint64_t> JsonUint64(const std::string& body, const std::string& field) {
  const std::string name = "\"" + field + "\"";
  const size_t fieldPos = body.find(name);
  if (fieldPos == std::string::npos) return std::nullopt;
  size_t cursor = body.find(':', fieldPos + name.size());
  if (cursor == std::string::npos) return std::nullopt;
  ++cursor;
  while (cursor < body.size() && std::isspace(static_cast<unsigned char>(body[cursor]))) ++cursor;
  
  uint64_t value = 0;
  bool found = false;
  while (cursor < body.size() && std::isdigit(static_cast<unsigned char>(body[cursor]))) {
    value = value * 10 + (body[cursor] - '0');
    found = true;
    ++cursor;
  }
  if (!found) return std::nullopt;
  return value;
}

std::vector<std::string> JsonStringArray(const std::string& body, const std::string& field) {
  std::vector<std::string> result;
  const std::string name = "\"" + field + "\"";
  const size_t fieldPos = body.find(name);
  if (fieldPos == std::string::npos) return result;
  size_t cursor = body.find(':', fieldPos + name.size());
  if (cursor == std::string::npos) return result;
  ++cursor;
  while (cursor < body.size() && std::isspace(static_cast<unsigned char>(body[cursor]))) ++cursor;
  if (cursor >= body.size() || body[cursor] != '[') return result;
  ++cursor;
  
  while (cursor < body.size() && body[cursor] != ']') {
    while (cursor < body.size() && std::isspace(static_cast<unsigned char>(body[cursor]))) ++cursor;
    if (cursor < body.size() && body[cursor] == '"') {
      ++cursor;
      std::string value;
      while (cursor < body.size() && body[cursor] != '"') {
        if (body[cursor] == '\\' && cursor + 1 < body.size()) {
          ++cursor;
          value += body[cursor++];
        } else {
          value += body[cursor++];
        }
      }
      if (cursor < body.size() && body[cursor] == '"') ++cursor;
      result.push_back(value);
    }
    while (cursor < body.size() && body[cursor] != ',' && body[cursor] != ']') ++cursor;
    if (cursor < body.size() && body[cursor] == ',') ++cursor;
  }
  return result;
}

std::string PercentDecode(const std::string& value) {
  std::string result;
  result.reserve(value.size());
  auto hex = [](char c) -> int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + c - 'a';
    if (c >= 'A' && c <= 'F') return 10 + c - 'A';
    return -1;
  };
  for (size_t index = 0; index < value.size(); ++index) {
    if (value[index] == '%' && index + 2 < value.size()) {
      const int high = hex(value[index + 1]);
      const int low = hex(value[index + 2]);
      if (high >= 0 && low >= 0) {
        result += static_cast<char>((high << 4) | low);
        index += 2;
        continue;
      }
    }
    result += value[index] == '+' ? ' ' : value[index];
  }
  return result;
}

int HttpStatus(stratakv::Status status) {
  switch (status) {
    case stratakv::Status::kOk:
    case stratakv::Status::kAlreadyCommitted: return 200;
    case stratakv::Status::kNotFound: return 404;
    case stratakv::Status::kLockConflict:
    case stratakv::Status::kWriteConflict: return 409;
    case stratakv::Status::kUnavailable: return 503;
    case stratakv::Status::kInvalidTransaction: return 400;
  }
  return 503;
}

std::string ResultJson(const stratakv::Result& result) {
  std::ostringstream output;
  output << "{\"status\":\"" << stratakv::StatusName(result.status) << "\"";
  if (!result.value.empty()) output << ",\"value\":\"" << JsonEscape(result.value) << "\"";
  if (!result.message.empty()) output << ",\"message\":\"" << JsonEscape(result.message) << "\"";
  output << '}';
  return output.str();
}

std::string BatchResultJson(const stratakv::BatchResult& result) {
  std::ostringstream output;
  output << "{\"status\":\"" << stratakv::StatusName(result.status) << "\"";
  if (!result.message.empty()) output << ",\"message\":\"" << JsonEscape(result.message) << "\"";
  output << ",\"values\":[";
  for (size_t i = 0; i < result.values.size(); ++i) {
    if (i > 0) output << ",";
    output << "{\"key\":\"" << JsonEscape(result.values[i].first) << "\"";
    if (!result.values[i].second.value.empty()) {
      output << ",\"value\":\"" << JsonEscape(result.values[i].second.value) << "\"";
    }
    output << "}";
  }
  output << "]}";
  return output.str();
}

std::string TransactionStatusResultJson(const stratakv::TransactionStatusResult& result) {
  std::ostringstream output;
  output << "{\"status\":\"" << stratakv::StatusName(result.status) << "\"";
  if (!result.message.empty()) output << ",\"message\":\"" << JsonEscape(result.message) << "\"";
  std::string stateName = "NOT_FOUND";
  switch (result.state) {
    case stratakv::TransactionRecordState::kLocked: stateName = "LOCKED"; break;
    case stratakv::TransactionRecordState::kCommitted: stateName = "COMMITTED"; break;
    case stratakv::TransactionRecordState::kRolledBack: stateName = "ROLLED_BACK"; break;
    case stratakv::TransactionRecordState::kNotFound: stateName = "NOT_FOUND"; break;
  }
  output << ",\"state\":\"" << stateName << "\"";
  output << ",\"commitTs\":" << result.commitTimestamp;
  output << '}';
  return output.str();
}

HttpResponse Error(int status, const std::string& code, const std::string& message) {
  return {status, "{\"status\":\"" + code + "\",\"message\":\"" + JsonEscape(message) + "\"}"};
}

class Gateway {
 public:
  Gateway(std::function<std::shared_ptr<stratakv::Client>()> clientFactory,
          std::string runtimeMode, size_t runtimeWorkers,
          BoundedThreadPool* requestExecutor)
      : clientFactory_(std::move(clientFactory)),
        runtimeMode_(std::move(runtimeMode)),
        runtimeWorkers_(runtimeWorkers),
        requestExecutor_(requestExecutor) {}

  Gateway(std::shared_ptr<stratakv::Client> client, std::string runtimeMode, size_t runtimeWorkers,
          BoundedThreadPool* requestExecutor)
      : client_(std::move(client)),
        runtimeMode_(std::move(runtimeMode)),
        runtimeWorkers_(runtimeWorkers),
        requestExecutor_(requestExecutor) {}

  std::shared_ptr<stratakv::Client> Client() {
    std::lock_guard<std::mutex> lock(clientMutex_);
    if (!client_ && clientFactory_) {
      client_ = clientFactory_();
    }
    if (!client_) {
      throw std::runtime_error("stratakv client is unavailable");
    }
    return client_;
  }

  void ConnectionOpened() {
    acceptedConnections_.fetch_add(1, std::memory_order_relaxed);
    const uint64_t active = activeConnections_.fetch_add(1, std::memory_order_relaxed) + 1;
    uint64_t peak = peakConnections_.load(std::memory_order_relaxed);
    while (active > peak &&
           !peakConnections_.compare_exchange_weak(peak, active, std::memory_order_relaxed)) {
    }
  }

  void ConnectionClosed() { activeConnections_.fetch_sub(1, std::memory_order_relaxed); }

  HttpResponse Handle(const HttpRequest& request) {
    requests_.fetch_add(1, std::memory_order_relaxed);
    if (request.method == "GET" && request.path == "/healthz") {
      return {200, "{\"status\":\"ok\"}"};
    }
    if (request.method == "GET" && request.path == "/metrics") {
      return {200, Metrics(), "text/plain; version=0.0.4; charset=utf-8"};
    }
    if (request.method == "POST" && request.path == "/v1/transactions") {
      const auto lockTtlMs = JsonUint64(request.body, "lockTtlMs");
      if (!lockTtlMs) return Error(400, "INVALID_ARGUMENT", "JSON field lockTtlMs is required");
      const auto transaction = Client()->Begin(*lockTtlMs);
      const std::string id = NewId();
      {
        std::lock_guard<std::mutex> lock(transactionsMutex_);
        transactions_.emplace(id, transaction);
      }
      started_.fetch_add(1, std::memory_order_relaxed);
      return {201, "{\"id\":\"" + id + "\",\"startTs\":" + std::to_string(transaction->StartTimestamp()) + "}"};
    }

    static const std::string prefix = "/v1/transactions/";
    if (request.path.rfind(prefix, 0) != 0) return Error(404, "NOT_FOUND", "route does not exist");
    const std::string remainder = request.path.substr(prefix.size());
    const size_t separator = remainder.find('/');
    if (separator == std::string::npos) return Error(404, "NOT_FOUND", "route does not exist");
    const std::string id = remainder.substr(0, separator);
    const std::string action = remainder.substr(separator);
    const auto transaction = Find(id);
    if (transaction == nullptr) return Error(404, "NOT_FOUND", "transaction does not exist or has completed");

    if (request.method == "POST" && action == "/mutations") {
      const auto key = JsonString(request.body, "key");
      if (!key || key->empty()) return Error(400, "INVALID_ARGUMENT", "JSON string field key is required");
      stratakv::Result result;
      if (JsonBool(request.body, "delete", false)) {
        result = Client()->Delete(transaction, *key);
      } else {
        const auto value = JsonString(request.body, "value");
        if (!value) return Error(400, "INVALID_ARGUMENT", "JSON string field value is required for Put");
        result = Client()->Put(transaction, *key, *value);
      }
      return ResultResponse(result);
    }

    if (request.method == "GET" && action.rfind("/keys/", 0) == 0) {
      const std::string key = PercentDecode(action.substr(std::string("/keys/").size()));
      if (key.empty()) return Error(400, "INVALID_ARGUMENT", "key is required");
      return ResultResponse(Client()->Get(transaction, key));
    }

    if (request.method == "POST" && action == "/commit") {
      const stratakv::Result result = Client()->Commit(transaction);
      Remove(id);
      if (result.ok()) committed_.fetch_add(1, std::memory_order_relaxed);
      return ResultResponse(result);
    }

    if (request.method == "POST" && action == "/rollback") {
      const stratakv::Result result = Client()->Rollback(transaction);
      Remove(id);
      rolledBack_.fetch_add(1, std::memory_order_relaxed);
      return ResultResponse(result);
    }
    
    if (request.method == "GET" && action.empty()) {
      const stratakv::TransactionStatusResult result = Client()->QueryTransactionStatus(transaction);
      return {HttpStatus(result.status), TransactionStatusResultJson(result)};
    }
    
    if (request.method == "POST" && action.rfind("/keys/", 0) == 0 && action.rfind("/lock") == action.size() - 5) {
      const std::string key = PercentDecode(action.substr(6, action.size() - 11)); // /keys/<key>/lock
      if (key.empty()) return Error(400, "INVALID_ARGUMENT", "key is required");
      return ResultResponse(Client()->GetForUpdate(transaction, key));
    }
    
    if (request.method == "POST" && action == "/locks") {
      const auto keys = JsonStringArray(request.body, "keys");
      if (keys.empty()) return Error(400, "INVALID_ARGUMENT", "JSON array field keys is required");
      if (JsonBool(request.body, "read", false)) {
        const stratakv::BatchResult result = Client()->BatchGetForUpdate(transaction, keys);
        return {HttpStatus(result.status), BatchResultJson(result)};
      } else {
        return ResultResponse(Client()->LockKeys(transaction, keys));
      }
    }

    return Error(404, "NOT_FOUND", "route does not exist");
  }

 private:
  std::string NewId() {
    const uint64_t sequence = nextId_.fetch_add(1, std::memory_order_relaxed);
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return "txn-" + std::to_string(now) + "-" + std::to_string(sequence);
  }

  std::shared_ptr<stratakv::Transaction> Find(const std::string& id) {
    std::lock_guard<std::mutex> lock(transactionsMutex_);
    const auto found = transactions_.find(id);
    return found == transactions_.end() ? nullptr : found->second;
  }

  void Remove(const std::string& id) {
    std::lock_guard<std::mutex> lock(transactionsMutex_);
    transactions_.erase(id);
  }

  HttpResponse ResultResponse(const stratakv::Result& result) {
    if (!result.ok()) failed_.fetch_add(1, std::memory_order_relaxed);
    return {HttpStatus(result.status), ResultJson(result)};
  }

  std::string Metrics() {
    size_t openTransactions = 0;
    {
      std::lock_guard<std::mutex> lock(transactionsMutex_);
      openTransactions = transactions_.size();
    }
    std::ostringstream output;
    stratakv::ClientMetrics clientMetrics{};
    {
      std::lock_guard<std::mutex> lock(clientMutex_);
      if (!client_ && clientFactory_) {
        try {
          client_ = clientFactory_();
        } catch (...) {
        }
      }
      if (client_) {
        try {
          clientMetrics = client_->Metrics();
        } catch (...) {
        }
      }
    }
    output << "# HELP stratakv_gateway_requests_total Total HTTP requests handled by the StrataKV gateway.\n"
           << "# TYPE stratakv_gateway_requests_total counter\n"
           << "stratakv_gateway_requests_total " << requests_.load() << "\n"
           << "# HELP stratakv_gateway_transactions_started_total Transactions created by the gateway.\n"
           << "# TYPE stratakv_gateway_transactions_started_total counter\n"
           << "stratakv_gateway_transactions_started_total " << started_.load() << "\n"
           << "# HELP stratakv_gateway_transactions_committed_total Transactions successfully committed.\n"
           << "# TYPE stratakv_gateway_transactions_committed_total counter\n"
           << "stratakv_gateway_transactions_committed_total " << committed_.load() << "\n"
           << "# HELP stratakv_gateway_transactions_rolled_back_total Transactions explicitly rolled back.\n"
           << "# TYPE stratakv_gateway_transactions_rolled_back_total counter\n"
           << "stratakv_gateway_transactions_rolled_back_total " << rolledBack_.load() << "\n"
           << "# HELP stratakv_gateway_failures_total Gateway operations returning a non-success status.\n"
           << "# TYPE stratakv_gateway_failures_total counter\n"
           << "stratakv_gateway_failures_total " << failed_.load() << "\n"
           << "# HELP stratakv_gateway_open_transactions Number of active in-memory transaction sessions.\n"
           << "# TYPE stratakv_gateway_open_transactions gauge\n"
           << "stratakv_gateway_open_transactions " << openTransactions << "\n"
           << "# HELP stratakv_txn_rollback_region_total Region rollback groups issued by the 2PC coordinator.\n"
           << "# TYPE stratakv_txn_rollback_region_total counter\n"
           << "stratakv_txn_rollback_region_total " << clientMetrics.rollbackRegionCount << "\n"
           << "# HELP stratakv_gateway_runtime_info Active Gateway concurrency runtime.\n"
           << "# TYPE stratakv_gateway_runtime_info gauge\n"
           << "stratakv_gateway_runtime_info{mode=\"" << runtimeMode_ << "\",io_workers=\"" << runtimeWorkers_
           << "\",request_workers=\"" << (requestExecutor_ == nullptr ? 0 : requestExecutor_->workers())
           << "\"} 1\n"
           << "# HELP stratakv_gateway_connections_total Accepted HTTP connections.\n"
           << "# TYPE stratakv_gateway_connections_total counter\n"
           << "stratakv_gateway_connections_total " << acceptedConnections_.load() << "\n"
           << "# HELP stratakv_gateway_active_connections Current HTTP connection handlers.\n"
           << "# TYPE stratakv_gateway_active_connections gauge\n"
           << "stratakv_gateway_active_connections " << activeConnections_.load() << "\n"
           << "# HELP stratakv_gateway_peak_connections Maximum concurrent HTTP connection handlers.\n"
           << "# TYPE stratakv_gateway_peak_connections gauge\n"
           << "stratakv_gateway_peak_connections " << peakConnections_.load() << "\n";
    if (requestExecutor_ != nullptr) {
      output << "# HELP stratakv_gateway_request_executor_queue_depth Blocking requests waiting for a native worker.\n"
             << "# TYPE stratakv_gateway_request_executor_queue_depth gauge\n"
             << "stratakv_gateway_request_executor_queue_depth " << requestExecutor_->queued() << "\n"
             << "# HELP stratakv_gateway_request_executor_active_workers Native workers executing blocking requests.\n"
             << "# TYPE stratakv_gateway_request_executor_active_workers gauge\n"
             << "stratakv_gateway_request_executor_active_workers " << requestExecutor_->active() << "\n"
             << "# HELP stratakv_gateway_request_executor_queue_capacity Maximum queued blocking requests.\n"
             << "# TYPE stratakv_gateway_request_executor_queue_capacity gauge\n"
             << "stratakv_gateway_request_executor_queue_capacity " << requestExecutor_->queueCapacity() << "\n"
             << "# HELP stratakv_gateway_request_executor_rejected_total Requests rejected by executor backpressure.\n"
             << "# TYPE stratakv_gateway_request_executor_rejected_total counter\n"
             << "stratakv_gateway_request_executor_rejected_total " << requestExecutor_->rejected() << "\n";
    }
    return output.str();
  }

  std::function<std::shared_ptr<stratakv::Client>()> clientFactory_;
  std::mutex clientMutex_;
  std::shared_ptr<stratakv::Client> client_;
  const std::string runtimeMode_;
  const size_t runtimeWorkers_;
  BoundedThreadPool* const requestExecutor_;
  std::mutex transactionsMutex_;
  std::unordered_map<std::string, std::shared_ptr<stratakv::Transaction>> transactions_;
  std::atomic<uint64_t> nextId_{1};
  std::atomic<uint64_t> requests_{0};
  std::atomic<uint64_t> started_{0};
  std::atomic<uint64_t> committed_{0};
  std::atomic<uint64_t> rolledBack_{0};
  std::atomic<uint64_t> failed_{0};
  std::atomic<uint64_t> acceptedConnections_{0};
  std::atomic<uint64_t> activeConnections_{0};
  std::atomic<uint64_t> peakConnections_{0};
};

bool ReadRequest(int fd, HttpRequest* request) {
  std::string data;
  char buffer[4096];
  size_t headerEnd = std::string::npos;
  while ((headerEnd = data.find("\r\n\r\n")) == std::string::npos) {
    const ssize_t received = recv(fd, buffer, sizeof(buffer), 0);
    if (received <= 0 || data.size() + static_cast<size_t>(received) > 65536) return false;
    data.append(buffer, static_cast<size_t>(received));
  }
  const std::string header = data.substr(0, headerEnd);
  std::istringstream headerStream(header);
  std::string version;
  if (!(headerStream >> request->method >> request->path >> version)) return false;
  const size_t query = request->path.find('?');
  if (query != std::string::npos) request->path.erase(query);

  size_t contentLength = 0;
  std::string line;
  std::getline(headerStream, line);  // Consume the first-line remainder.
  while (std::getline(headerStream, line)) {
    if (!line.empty() && line.back() == '\r') line.pop_back();
    const std::string key = "Content-Length:";
    if (line.rfind(key, 0) == 0) {
      try {
        contentLength = static_cast<size_t>(std::stoull(line.substr(key.size())));
      } catch (const std::exception&) {
        return false;
      }
    }
  }
  if (contentLength > 65536) return false;
  request->body = data.substr(headerEnd + 4);
  while (request->body.size() < contentLength) {
    const ssize_t received = recv(fd, buffer, sizeof(buffer), 0);
    if (received <= 0) return false;
    request->body.append(buffer, static_cast<size_t>(received));
  }
  request->body.resize(contentLength);
  return true;
}

const char* ReasonPhrase(int status) {
  switch (status) {
    case 200: return "OK";
    case 201: return "Created";
    case 400: return "Bad Request";
    case 404: return "Not Found";
    case 409: return "Conflict";
    case 503: return "Service Unavailable";
    default: return "Internal Server Error";
  }
}

void SendResponse(int fd, const HttpResponse& response) {
  std::ostringstream output;
  output << "HTTP/1.1 " << response.status << ' ' << ReasonPhrase(response.status) << "\r\n"
         << "Content-Type: " << response.contentType << "\r\n"
         << "Content-Length: " << response.body.size() << "\r\n"
         << "Connection: close\r\n\r\n"
         << response.body;
  const std::string encoded = output.str();
  size_t sent = 0;
  while (sent < encoded.size()) {
    const ssize_t written = send(fd, encoded.data() + sent, encoded.size() - sent, 0);
    if (written <= 0) return;
    sent += static_cast<size_t>(written);
  }
}

std::string RequestOperation(const HttpRequest& request) {
  if (request.method == "POST" && request.path == "/v1/transactions") return "begin";
  if (request.method == "GET" && request.path == "/healthz") return "health";
  if (request.method == "GET" && request.path == "/metrics") return "metrics";
  if (request.path.find("/mutations") != std::string::npos) {
    return JsonBool(request.body, "delete", false) ? "delete" : "put";
  }
  if (request.path.find("/keys/") != std::string::npos) return "get";
  if (request.path.find("/commit") != std::string::npos) return "commit";
  if (request.path.find("/rollback") != std::string::npos) return "rollback";
  return "unknown";
}

std::optional<std::string> RequestKey(const HttpRequest& request) {
  if (request.path.find("/mutations") != std::string::npos) return JsonString(request.body, "key");
  const std::string keyPrefix = "/keys/";
  const size_t keyPos = request.path.find(keyPrefix);
  if (keyPos == std::string::npos) return std::nullopt;
  return PercentDecode(request.path.substr(keyPos + keyPrefix.size()));
}

void LogRequest(const HttpRequest& request, const HttpResponse& response, long long latencyMs) {
  const std::string operation = RequestOperation(request);
  // Health checks and Prometheus scrapes are periodic background work.
  // Keep failures visible, but do not let successful probes obscure user I/O.
  if ((operation == "health" || operation == "metrics") && response.status < 400) return;

  const auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  std::cout << "{\"ts_unix_ms\":" << timestamp << ",\"level\":\"info\",\"component\":\"gateway\",\"event\":\"request\",\"method\":\""
            << JsonEscape(request.method) << "\",\"path\":\"" << JsonEscape(request.path)
            << "\",\"operation\":\"" << operation << "\"";
  if (const auto key = RequestKey(request); key && !key->empty()) {
    // Deliberately record the key but never the value: values may be sensitive.
    std::cout << ",\"key\":\"" << JsonEscape(*key) << "\"";
  }
  std::cout << ",\"http_status\":" << response.status << ",\"latency_ms\":" << latencyMs << "}" << std::endl;
}

struct RequestCompletion {
  pulsar::FiberSemaphore completed;
  HttpResponse response;
};

void HandleConnection(int fd, Gateway* gateway, BoundedThreadPool* requestExecutor) {
  gateway->ConnectionOpened();
  struct ConnectionGuard {
    Gateway* gateway;
    ~ConnectionGuard() { gateway->ConnectionClosed(); }
  } guard{gateway};

  HttpRequest request;
  if (!ReadRequest(fd, &request)) {
    SendResponse(fd, Error(400, "INVALID_REQUEST", "unable to parse HTTP request"));
    close(fd);
    return;
  }
  const auto start = std::chrono::steady_clock::now();
  HttpResponse response;
  if (requestExecutor == nullptr) {
    try {
      response = gateway->Handle(request);
    } catch (const std::exception& error) {
      response = Error(503, "UNAVAILABLE", error.what());
    } catch (...) {
      response = Error(503, "UNAVAILABLE", "request execution failed");
    }
  } else {
    auto completion = std::make_shared<RequestCompletion>();
    const bool accepted = requestExecutor->TrySchedule([gateway, request, completion]() {
      try {
        completion->response = gateway->Handle(request);
      } catch (const std::exception& error) {
        completion->response = Error(503, "UNAVAILABLE", error.what());
      } catch (...) {
        completion->response = Error(503, "UNAVAILABLE", "request execution failed");
      }
      completion->completed.signal();
    });
    if (!accepted) {
      response = Error(503, "GATEWAY_BUSY", "request executor queue is full");
    } else if (completion->completed.wait() != 0) {
      response = Error(503, "UNAVAILABLE", "request execution was interrupted");
    } else {
      response = std::move(completion->response);
    }
  }
  const long long latencyMs = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start).count();
  SendResponse(fd, response);
  LogRequest(request, response, latencyMs);
  close(fd);
}

void PrintUsage(const char* program) {
  std::cerr << "Usage: " << program
            << " --regions-config <path> [--host 0.0.0.0] [--port 8080]"
               " [--tso-host 127.0.0.1] [--tso-port 26300]"
               " [--tso-endpoints host:port,host:port,...]"
               " [--runtime thread|fiber] [--workers 4] [--request-workers 16]"
               " [--fiber-stack-cache-mib 0] [--fiber-cache-per-worker 1]\n";
}

}  // namespace

int main(int argc, char** argv) {
  std::string regionConfigPath;
  std::string host = "0.0.0.0";
  std::string tsoHost = "127.0.0.1";
  std::string tsoEndpoints = "127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302";
  // Keep the established pthread path as the latency-oriented default. Fiber
  // mode is an explicit bounded-thread option for high connection fan-in; the
  // benchmark record documents its resource win and tail-latency trade-off.
  std::string runtimeMode = "thread";
  int port = 8080;
  int tsoPort = 26300;
  int runtimeWorkers = 4;
  int requestWorkers = 16;
  int fiberStackCacheMiB = 0;
  int fiberCachePerWorker = 1;
  for (int index = 1; index < argc; index += 2) {
    if (index + 1 >= argc) {
      PrintUsage(argv[0]);
      return 2;
    }
    const std::string option = argv[index];
    const std::string value = argv[index + 1];
    if (option == "--regions-config") regionConfigPath = value;
    else if (option == "--host") host = value;
    else if (option == "--tso-host") { tsoHost = value; tsoEndpoints.clear(); }
    else if (option == "--tso-endpoints") tsoEndpoints = value;
    else if (option == "--runtime") runtimeMode = value;
    else if (option == "--port") {
      try { port = std::stoi(value); } catch (const std::exception&) { port = 0; }
    } else if (option == "--tso-port") {
      try { tsoPort = std::stoi(value); } catch (const std::exception&) { tsoPort = 0; }
      tsoEndpoints.clear();
    } else if (option == "--workers") {
      try { runtimeWorkers = std::stoi(value); } catch (const std::exception&) { runtimeWorkers = 0; }
    } else if (option == "--request-workers") {
      try { requestWorkers = std::stoi(value); } catch (const std::exception&) { requestWorkers = 0; }
    } else if (option == "--fiber-stack-cache-mib") {
      try { fiberStackCacheMiB = std::stoi(value); } catch (const std::exception&) { fiberStackCacheMiB = -1; }
    } else if (option == "--fiber-cache-per-worker") {
      try { fiberCachePerWorker = std::stoi(value); } catch (const std::exception&) { fiberCachePerWorker = -1; }
    } else {
      PrintUsage(argv[0]);
      return 2;
    }
  }
  if (regionConfigPath.empty() || tsoHost.empty() || port <= 0 || port > 65535 || tsoPort <= 0 || tsoPort > 65535 ||
      runtimeWorkers <= 0 || runtimeWorkers > 256 ||
      requestWorkers <= 0 || requestWorkers > 256 ||
      fiberStackCacheMiB < 0 || fiberStackCacheMiB > 4096 ||
      fiberCachePerWorker < 0 || fiberCachePerWorker > 4096 ||
      (runtimeMode == "thread" && (fiberStackCacheMiB != 0 || fiberCachePerWorker != 1)) ||
      (runtimeMode != "fiber" && runtimeMode != "thread")) {
    PrintUsage(argv[0]);
    return 2;
  }

  try {
    std::unique_ptr<BoundedThreadPool> requestExecutor;
    if (runtimeMode == "fiber") {
      requestExecutor = std::make_unique<BoundedThreadPool>(
          static_cast<size_t>(requestWorkers), static_cast<size_t>(requestWorkers) * 16);
    }
    if (tsoEndpoints.empty()) tsoEndpoints = tsoHost + ":" + std::to_string(tsoPort);
    auto clientFactory = [regionConfigPath, tsoEndpoints]() {
      return stratakv::Client::Connect(regionConfigPath, tsoEndpoints);
    };
    Gateway gateway(std::move(clientFactory), runtimeMode,
                    runtimeMode == "fiber" ? static_cast<size_t>(runtimeWorkers) : 0,
                    requestExecutor.get());
    const int listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) throw std::runtime_error("cannot create listener socket");
    int reuse = 1;
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1 || bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 ||
        listen(listener, 128) < 0) {
      close(listener);
      throw std::runtime_error("cannot bind gateway listener on " + host + ':' + std::to_string(port));
    }
    std::cout << "{\"level\":\"info\",\"component\":\"gateway\",\"event\":\"started\",\"host\":\""
              << JsonEscape(host) << "\",\"port\":" << port << ",\"runtime\":\"" << runtimeMode
              << "\",\"io_workers\":" << (runtimeMode == "fiber" ? runtimeWorkers : 0)
              << ",\"request_workers\":" << (runtimeMode == "fiber" ? requestWorkers : 0)
              << ",\"fiber_stack_cache_mib\":" << (runtimeMode == "fiber" ? fiberStackCacheMiB : 0)
              << ",\"fiber_cache_per_worker\":" << (runtimeMode == "fiber" ? fiberCachePerWorker : 0)
              << "}" << std::endl;

    if (runtimeMode == "thread") {
      while (true) {
        const int client = accept(listener, nullptr, nullptr);
        if (client < 0) {
          if (errno == EINTR) continue;
          continue;
        }
        std::thread(HandleConnection, client, &gateway, nullptr).detach();
      }
    }

    // The listener was created on the main thread before Hook was enabled.
    // Register it explicitly so accept() becomes an epoll-backed Fiber wait.
    if (!pulsar::FdMgr::GetInstance()->get(listener, true)) {
      close(listener);
      throw std::runtime_error("cannot register gateway listener with Pulsar");
    }
    pulsar::SchedulerReuseOptions reuseOptions;
    reuseOptions.callbackFiberCachePerWorker = static_cast<size_t>(fiberCachePerWorker);
    if (fiberStackCacheMiB > 0) {
      pulsar::StackPoolOptions stackPoolOptions;
      stackPoolOptions.maxCachedBytes = static_cast<size_t>(fiberStackCacheMiB) * 1024 * 1024;
      reuseOptions.stackAllocator = pulsar::MakePooledStackAllocator(stackPoolOptions);
    }
    pulsar::IOManager runtime(static_cast<size_t>(runtimeWorkers), false, "stratakv-gateway",
                              std::move(reuseOptions));
    runtime.scheduler([listener, &gateway, &runtime, requestPool = requestExecutor.get()]() {
      while (true) {
        const int client = accept(listener, nullptr, nullptr);
        if (client < 0) {
          if (errno == EINTR) continue;
          usleep(1000);
          continue;
        }
        runtime.scheduler([client, &gateway, requestPool]() {
          HandleConnection(client, &gateway, requestPool);
        });
      }
    });

    // The runtime owns all request workers. The process is service-style and
    // terminates through the normal SIGTERM/SIGINT process lifecycle.
    while (true) pause();
  } catch (const std::exception& error) {
    std::cerr << "{\"level\":\"error\",\"component\":\"gateway\",\"message\":\"" << JsonEscape(error.what()) << "\"}" << std::endl;
    return 1;
  }
}
