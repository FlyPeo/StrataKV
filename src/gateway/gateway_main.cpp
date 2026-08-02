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
  return body.compare(cursor, 4, "true") == 0;
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

HttpResponse Error(int status, const std::string& code, const std::string& message) {
  return {status, "{\"status\":\"" + code + "\",\"message\":\"" + JsonEscape(message) + "\"}"};
}

class Gateway {
 public:
  explicit Gateway(std::shared_ptr<stratakv::Client> client) : client_(std::move(client)) {}

  HttpResponse Handle(const HttpRequest& request) {
    requests_.fetch_add(1, std::memory_order_relaxed);
    if (request.method == "GET" && request.path == "/healthz") {
      return {200, "{\"status\":\"ok\"}"};
    }
    if (request.method == "GET" && request.path == "/metrics") {
      return {200, Metrics(), "text/plain; version=0.0.4; charset=utf-8"};
    }
    if (request.method == "POST" && request.path == "/v1/transactions") {
      const auto transaction = client_->Begin();
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
        result = client_->Delete(transaction, *key);
      } else {
        const auto value = JsonString(request.body, "value");
        if (!value) return Error(400, "INVALID_ARGUMENT", "JSON string field value is required for Put");
        result = client_->Put(transaction, *key, *value);
      }
      return ResultResponse(result);
    }

    if (request.method == "GET" && action.rfind("/keys/", 0) == 0) {
      const std::string key = PercentDecode(action.substr(std::string("/keys/").size()));
      if (key.empty()) return Error(400, "INVALID_ARGUMENT", "key is required");
      return ResultResponse(client_->Get(transaction, key));
    }

    if (request.method == "POST" && action == "/commit") {
      const stratakv::Result result = client_->Commit(transaction);
      Remove(id);
      if (result.ok()) committed_.fetch_add(1, std::memory_order_relaxed);
      return ResultResponse(result);
    }

    if (request.method == "POST" && action == "/rollback") {
      const stratakv::Result result = client_->Rollback(transaction);
      Remove(id);
      rolledBack_.fetch_add(1, std::memory_order_relaxed);
      return ResultResponse(result);
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
           << "stratakv_gateway_open_transactions " << openTransactions << "\n";
    return output.str();
  }

  std::shared_ptr<stratakv::Client> client_;
  std::mutex transactionsMutex_;
  std::unordered_map<std::string, std::shared_ptr<stratakv::Transaction>> transactions_;
  std::atomic<uint64_t> nextId_{1};
  std::atomic<uint64_t> requests_{0};
  std::atomic<uint64_t> started_{0};
  std::atomic<uint64_t> committed_{0};
  std::atomic<uint64_t> rolledBack_{0};
  std::atomic<uint64_t> failed_{0};
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

void HandleConnection(int fd, Gateway* gateway) {
  HttpRequest request;
  if (!ReadRequest(fd, &request)) {
    SendResponse(fd, Error(400, "INVALID_REQUEST", "unable to parse HTTP request"));
    close(fd);
    return;
  }
  const auto start = std::chrono::steady_clock::now();
  HttpResponse response;
  try {
    response = gateway->Handle(request);
  } catch (const std::exception& error) {
    response = Error(503, "UNAVAILABLE", error.what());
  }
  const long long latencyMs = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start).count();
  SendResponse(fd, response);
  LogRequest(request, response, latencyMs);
  close(fd);
}

void PrintUsage(const char* program) {
  std::cerr << "Usage: " << program << " --regions-config <path> [--host 0.0.0.0] [--port 8080]\n";
}

}  // namespace

int main(int argc, char** argv) {
  std::string regionConfigPath;
  std::string host = "0.0.0.0";
  int port = 8080;
  for (int index = 1; index < argc; index += 2) {
    if (index + 1 >= argc) {
      PrintUsage(argv[0]);
      return 2;
    }
    const std::string option = argv[index];
    const std::string value = argv[index + 1];
    if (option == "--regions-config") regionConfigPath = value;
    else if (option == "--host") host = value;
    else if (option == "--port") {
      try { port = std::stoi(value); } catch (const std::exception&) { port = 0; }
    } else {
      PrintUsage(argv[0]);
      return 2;
    }
  }
  if (regionConfigPath.empty() || port <= 0 || port > 65535) {
    PrintUsage(argv[0]);
    return 2;
  }

  try {
    Gateway gateway(stratakv::Client::Connect(regionConfigPath));
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
              << JsonEscape(host) << "\",\"port\":" << port << "}" << std::endl;
    while (true) {
      const int client = accept(listener, nullptr, nullptr);
      if (client < 0) {
        if (errno == EINTR) continue;
        continue;
      }
      std::thread(HandleConnection, client, &gateway).detach();
    }
  } catch (const std::exception& error) {
    std::cerr << "{\"level\":\"error\",\"component\":\"gateway\",\"message\":\"" << JsonEscape(error.what()) << "\"}" << std::endl;
    return 1;
  }
}
