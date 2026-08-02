#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

struct Options {
  std::string gateway = "http://127.0.0.1:8080";
  std::string runId;
  std::string history = "stratakv-reliability-history.jsonl";
  int workers = 8;
  int transactions = 1000;
  int retries = 12;
  int retryDelayMs = 200;
  int timeoutMs = 180000;
  bool verifyOnly = false;
};

struct Endpoint {
  std::string host;
  std::string port;
};

struct HttpResponse {
  int status = 0;
  std::string body;
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
      << "  --gateway URL          Gateway endpoint (default http://127.0.0.1:8080)\n"
      << "  --workers N            Concurrent workers (default 8)\n"
      << "  --transactions N       Cross-Region transactions (default 1000)\n"
      << "  --retries N            Attempts per transaction/read (default 12)\n"
      << "  --retry-delay-ms N      Base retry delay (default 200)\n"
      << "  --timeout-ms N          Per-request socket timeout (default 180000)\n"
      << "  --run-id ID             Stable key namespace; required by --verify-only\n"
      << "  --history PATH          JSON Lines result file\n"
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
    if (argument == "--gateway") options.gateway = value;
    else if (argument == "--workers") options.workers = ParsePositive(value, "--workers");
    else if (argument == "--transactions") options.transactions = ParsePositive(value, "--transactions");
    else if (argument == "--retries") options.retries = ParsePositive(value, "--retries");
    else if (argument == "--retry-delay-ms") options.retryDelayMs = ParsePositive(value, "--retry-delay-ms");
    else if (argument == "--timeout-ms") options.timeoutMs = ParsePositive(value, "--timeout-ms");
    else if (argument == "--run-id") options.runId = value;
    else if (argument == "--history") options.history = value;
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

Endpoint ParseEndpoint(std::string value) {
  constexpr const char* prefix = "http://";
  if (value.rfind(prefix, 0) != 0) throw std::invalid_argument("--gateway must start with http://");
  value.erase(0, std::char_traits<char>::length(prefix));
  if (value.find('/') != std::string::npos) throw std::invalid_argument("--gateway must not contain a path");
  const size_t separator = value.rfind(':');
  if (separator == std::string::npos || separator == 0 || separator + 1 == value.size()) {
    throw std::invalid_argument("--gateway must contain host:port");
  }
  return {value.substr(0, separator), value.substr(separator + 1)};
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
    const char c = json[position];
    if (escaped) {
      switch (c) {
        case 'n': value += '\n'; break;
        case 'r': value += '\r'; break;
        case 't': value += '\t'; break;
        default: value += c; break;
      }
      escaped = false;
    } else if (c == '\\') {
      escaped = true;
    } else if (c == '"') {
      return value;
    } else {
      value += c;
    }
  }
  return std::nullopt;
}

std::string UrlEncode(const std::string& value) {
  static constexpr char hex[] = "0123456789ABCDEF";
  std::string result;
  for (const unsigned char c : value) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      result += static_cast<char>(c);
    } else {
      result += '%';
      result += hex[(c >> 4) & 0x0f];
      result += hex[c & 0x0f];
    }
  }
  return result;
}

int Connect(const Endpoint& endpoint, int timeoutMs) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* addresses = nullptr;
  if (getaddrinfo(endpoint.host.c_str(), endpoint.port.c_str(), &hints, &addresses) != 0) {
    throw std::runtime_error("cannot resolve Gateway host " + endpoint.host);
  }

  int fd = -1;
  for (addrinfo* address = addresses; address != nullptr; address = address->ai_next) {
    fd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (fd < 0) continue;
    const timeval timeout{timeoutMs / 1000, (timeoutMs % 1000) * 1000};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    if (connect(fd, address->ai_addr, address->ai_addrlen) == 0) break;
    close(fd);
    fd = -1;
  }
  freeaddrinfo(addresses);
  if (fd < 0) throw std::runtime_error("cannot connect to Gateway");
  return fd;
}

HttpResponse Request(const Endpoint& endpoint, int timeoutMs, const std::string& method,
                     const std::string& path, const std::string& body = {}) {
  const int fd = Connect(endpoint, timeoutMs);
  std::ostringstream request;
  request << method << ' ' << path << " HTTP/1.1\r\n"
          << "Host: " << endpoint.host << ':' << endpoint.port << "\r\n"
          << "Connection: close\r\n";
  if (!body.empty()) request << "Content-Type: application/json\r\n";
  request << "Content-Length: " << body.size() << "\r\n\r\n" << body;
  const std::string encoded = request.str();

  size_t sent = 0;
  while (sent < encoded.size()) {
    const ssize_t written = send(fd, encoded.data() + sent, encoded.size() - sent, MSG_NOSIGNAL);
    if (written <= 0) {
      close(fd);
      throw std::runtime_error("failed to send Gateway request");
    }
    sent += static_cast<size_t>(written);
  }

  std::string response;
  char buffer[8192];
  while (true) {
    const ssize_t received = recv(fd, buffer, sizeof(buffer), 0);
    if (received == 0) break;
    if (received < 0) {
      close(fd);
      throw std::runtime_error("failed or timed out while reading Gateway response");
    }
    response.append(buffer, static_cast<size_t>(received));
    if (response.size() > 4 * 1024 * 1024) {
      close(fd);
      throw std::runtime_error("Gateway response is too large");
    }
  }
  close(fd);

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

std::string ResponseError(const HttpResponse& response) {
  const auto status = JsonString(response.body, "status");
  const auto message = JsonString(response.body, "message");
  std::string result = "HTTP " + std::to_string(response.status);
  if (status) result += " " + *status;
  if (message && (!status || *message != *status)) result += ": " + *message;
  return result;
}

std::string Begin(const Endpoint& endpoint, const Options& options) {
  const HttpResponse response = Request(endpoint, options.timeoutMs, "POST", "/v1/transactions");
  const auto id = JsonString(response.body, "id");
  if (response.status != 201 || !id || id->empty()) throw std::runtime_error(ResponseError(response));
  return *id;
}

void Put(const Endpoint& endpoint, const Options& options, const std::string& transaction,
         const std::string& key, const std::string& value) {
  const std::string body = "{\"key\":\"" + JsonEscape(key) + "\",\"value\":\"" + JsonEscape(value) + "\"}";
  const HttpResponse response =
      Request(endpoint, options.timeoutMs, "POST", "/v1/transactions/" + transaction + "/mutations", body);
  if (response.status != 200) throw std::runtime_error(ResponseError(response));
}

void Commit(const Endpoint& endpoint, const Options& options, const std::string& transaction) {
  const HttpResponse response =
      Request(endpoint, options.timeoutMs, "POST", "/v1/transactions/" + transaction + "/commit");
  const auto status = JsonString(response.body, "status");
  if (response.status != 200 || !status || (*status != "OK" && *status != "ALREADY_COMMITTED")) {
    throw std::runtime_error(ResponseError(response));
  }
}

void Rollback(const Endpoint& endpoint, const Options& options, const std::string& transaction) {
  const HttpResponse response =
      Request(endpoint, options.timeoutMs, "POST", "/v1/transactions/" + transaction + "/rollback");
  if (response.status != 200 && response.status != 404) throw std::runtime_error(ResponseError(response));
}

std::optional<std::string> Get(const Endpoint& endpoint, const Options& options,
                               const std::string& transaction, const std::string& key) {
  const HttpResponse response = Request(endpoint, options.timeoutMs, "GET",
                                        "/v1/transactions/" + transaction + "/keys/" + UrlEncode(key));
  if (response.status == 404 && JsonString(response.body, "status") == std::optional<std::string>("NOT_FOUND")) {
    return std::nullopt;
  }
  const auto value = JsonString(response.body, "value");
  if (response.status != 200 || !value) throw std::runtime_error(ResponseError(response));
  return value;
}

std::vector<std::string> Keys(const Options& options, int index) {
  const std::string suffix = ":reliability:" + options.runId + ':' + std::to_string(index);
  return {"a" + suffix, "h" + suffix, "p" + suffix};
}

std::string Value(const Options& options, int index) {
  return "value-" + options.runId + '-' + std::to_string(index);
}

void Backoff(const Options& options, int attempt) {
  const int multiplier = std::min(attempt, 10);
  std::this_thread::sleep_for(std::chrono::milliseconds(options.retryDelayMs * multiplier));
}

void RunTransaction(const Endpoint& endpoint, const Options& options, int index, Record* record) {
  const auto started = std::chrono::steady_clock::now();
  const std::vector<std::string> keys = Keys(options, index);
  const std::string value = Value(options, index);

  for (int attempt = 1; attempt <= options.retries; ++attempt) {
    record->attempts = attempt;
    std::string transaction;
    try {
      transaction = Begin(endpoint, options);
      for (const auto& key : keys) Put(endpoint, options, transaction, key, value);
      Commit(endpoint, options, transaction);
      record->commitAcknowledged = true;
      record->error.clear();
      break;
    } catch (const std::exception& error) {
      record->error = error.what();
      if (!transaction.empty()) {
        try {
          Rollback(endpoint, options, transaction);
        } catch (const std::exception&) {
        }
      }
      if (attempt < options.retries) Backoff(options, attempt);
    }
  }
  record->latencyMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - started)
                          .count();
}

void VerifyTransaction(const Endpoint& endpoint, const Options& options, int index, Record* record) {
  const std::vector<std::string> keys = Keys(options, index);
  const std::string expected = Value(options, index);
  for (int attempt = 1; attempt <= options.retries; ++attempt) {
    std::string transaction;
    try {
      transaction = Begin(endpoint, options);
      int present = 0;
      bool correct = true;
      for (const auto& key : keys) {
        const auto value = Get(endpoint, options, transaction, key);
        if (value) {
          ++present;
          if (*value != expected) correct = false;
        }
      }
      Rollback(endpoint, options, transaction);
      record->presentKeys = present;
      record->valuesCorrect = correct && present == 3;
      record->verifyError.clear();
      return;
    } catch (const std::exception& error) {
      record->verifyError = error.what();
      if (!transaction.empty()) {
        try {
          Rollback(endpoint, options, transaction);
        } catch (const std::exception&) {
        }
      }
      if (attempt < options.retries) Backoff(options, attempt);
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
  std::ofstream output(options.history, std::ios::trunc);
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

}  // namespace

int main(int argc, char** argv) {
  try {
    const Options options = ParseOptions(argc, argv);
    const Endpoint endpoint = ParseEndpoint(options.gateway);
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
            RunTransaction(endpoint, options, index, &records[index]);
          }
        });
      }
      for (auto& worker : workers) worker.join();
    } else {
      for (Record& record : records) record.commitAcknowledged = true;
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
          VerifyTransaction(endpoint, options, index, &records[index]);
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

    const bool passed = committed == options.transactions && safetyViolations == 0 && verificationFailures == 0;
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
