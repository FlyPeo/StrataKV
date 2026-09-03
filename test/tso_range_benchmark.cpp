#include <arpa/inet.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "mprpc_channel.h"
#include "mprpc_controller.h"
#include "remote_timestamp_oracle.h"
#include "tso_rpc.pb.h"

namespace {

struct Options {
  uint64_t requests = 2048;
  uint64_t clients = 8;
  uint64_t baselineRange = 1;
  uint64_t optimizedRange = 4096;
  uint64_t warmupRequests = 128;
};

struct Result {
  std::string mode;
  uint64_t rangeSize = 0;
  double throughput = 0;
  double p50Us = 0;
  double p95Us = 0;
  double p99Us = 0;
  uint64_t proposals = 0;
  uint64_t reservations = 0;
  uint64_t allocations = 0;
  double allocationsPerReservation = 0;
};

class TsoProcess {
 public:
  ~TsoProcess() { Stop(); }

  void Start(const std::string& executable, int nodeId, const std::string& peers,
             const std::filesystem::path& dataDirectory, uint64_t rangeSize) {
    std::filesystem::create_directories(dataDirectory);
    const std::string nodeIdText = std::to_string(nodeId);
    const std::string statePath = (dataDirectory / "tso.state").string();
    const std::string rangeText = std::to_string(rangeSize);
    pid_ = fork();
    if (pid_ < 0) throw std::runtime_error("fork failed");
    if (pid_ == 0) {
      if (chdir(dataDirectory.c_str()) != 0) _exit(126);
      const int logFd = open("tso.log", O_CREAT | O_TRUNC | O_WRONLY, 0644);
      if (logFd >= 0) {
        dup2(logFd, STDOUT_FILENO);
        dup2(logFd, STDERR_FILENO);
        close(logFd);
      }
      execl(executable.c_str(), executable.c_str(), "--node-id", nodeIdText.c_str(),
            "--peers", peers.c_str(), "--state-file", statePath.c_str(), "--range-size",
            rangeText.c_str(), nullptr);
      _exit(127);
    }
  }

  void Stop() {
    if (pid_ <= 0) return;
    kill(pid_, SIGKILL);
    int status = 0;
    while (waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
    }
    pid_ = -1;
  }

 private:
  pid_t pid_ = -1;
};

uint64_t ParsePositive(const std::string& text, const char* option) {
  size_t consumed = 0;
  uint64_t value = 0;
  try {
    value = std::stoull(text, &consumed);
  } catch (const std::exception&) {
    throw std::invalid_argument(std::string("invalid ") + option);
  }
  if (consumed != text.size() || value == 0) {
    throw std::invalid_argument(std::string("invalid ") + option);
  }
  return value;
}

Options ParseOptions(int argc, char** argv) {
  Options options;
  for (int index = 1; index < argc; index += 2) {
    if (index + 1 >= argc) throw std::invalid_argument("benchmark options require values");
    const std::string option = argv[index];
    const uint64_t value = ParsePositive(argv[index + 1], option.c_str());
    if (option == "--requests") {
      options.requests = value;
    } else if (option == "--clients") {
      options.clients = value;
    } else if (option == "--baseline-range") {
      options.baselineRange = value;
    } else if (option == "--optimized-range") {
      options.optimizedRange = value;
    } else if (option == "--warmup") {
      options.warmupRequests = value;
    } else {
      throw std::invalid_argument("unknown benchmark option: " + option);
    }
  }
  if (options.clients > options.requests) options.clients = options.requests;
  return options;
}

uint16_t UnusedPort() {
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) throw std::runtime_error("socket failed");
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    close(fd);
    throw std::runtime_error("bind ephemeral port failed");
  }
  socklen_t length = sizeof(address);
  if (getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
    close(fd);
    throw std::runtime_error("getsockname failed");
  }
  const uint16_t port = ntohs(address.sin_port);
  close(fd);
  return port;
}

std::string SiblingExecutable(const std::string& name) {
  std::vector<char> path(4096);
  const ssize_t length = readlink("/proc/self/exe", path.data(), path.size() - 1);
  if (length <= 0) throw std::runtime_error("cannot locate benchmark executable");
  path[static_cast<size_t>(length)] = '\0';
  return (std::filesystem::path(path.data()).parent_path() / name).string();
}

std::string EndpointText(const std::vector<TsoEndpoint>& endpoints) {
  std::string text;
  for (const auto& endpoint : endpoints) {
    if (!text.empty()) text += ',';
    text += endpoint.host + ':' + std::to_string(endpoint.port);
  }
  return text;
}

std::optional<tsoRpcProtocol::TimestampStatusReply> ReadStatus(const TsoEndpoint& endpoint) {
  MprpcChannel channel(endpoint.host, static_cast<short>(endpoint.port), false);
  tsoRpcProtocol::timestampOracleRpc_Stub stub(&channel);
  tsoRpcProtocol::TimestampStatusRequest request;
  tsoRpcProtocol::TimestampStatusReply response;
  MprpcController controller;
  stub.Status(&controller, &request, &response, nullptr);
  if (controller.Failed()) return std::nullopt;
  return response;
}

int WaitForLeader(const std::vector<TsoEndpoint>& endpoints) {
  for (int attempt = 0; attempt < 160; ++attempt) {
    for (size_t index = 0; index < endpoints.size(); ++index) {
      const auto status = ReadStatus(endpoints[index]);
      if (status.has_value() && status->isleader()) return static_cast<int>(index);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  throw std::runtime_error("benchmark TSO cluster did not elect a leader");
}

double Percentile(const std::vector<double>& sorted, double percentile) {
  if (sorted.empty()) return 0;
  const size_t rank = static_cast<size_t>(std::ceil(percentile * sorted.size()));
  return sorted[std::max<size_t>(1, rank) - 1];
}

Result RunCase(const Options& options, const std::string& mode, uint64_t rangeSize) {
  char directoryTemplate[] = "/tmp/stratakv-tso-benchmark-XXXXXX";
  const char* created = mkdtemp(directoryTemplate);
  if (created == nullptr) throw std::runtime_error("mkdtemp failed");
  const std::filesystem::path directory(created);
  std::vector<std::unique_ptr<TsoProcess>> processes;

  try {
    std::vector<TsoEndpoint> endpoints;
    while (endpoints.size() < 3) {
      const uint16_t port = UnusedPort();
      if (std::none_of(endpoints.begin(), endpoints.end(),
                       [port](const TsoEndpoint& endpoint) { return endpoint.port == port; })) {
        endpoints.push_back(TsoEndpoint{"127.0.0.1", port});
      }
    }
    const std::string peers = EndpointText(endpoints);
    const std::string executable = SiblingExecutable("stratakv-tso");
    for (int nodeId = 0; nodeId < 3; ++nodeId) {
      processes.push_back(std::make_unique<TsoProcess>());
      processes.back()->Start(executable, nodeId, peers,
                              directory / ("node-" + std::to_string(nodeId)), rangeSize);
    }
    const int leader = WaitForLeader(endpoints);
    RemoteTimestampOracle controlClient(endpoints);
    for (uint64_t index = 0; index < options.warmupRequests; ++index) {
      (void)controlClient.Next();
    }

    // Start both cases at an empty range so proposal and amortization deltas
    // describe only the measured window, not a warm-up remainder.
    const auto warmStatus = ReadStatus(endpoints[static_cast<size_t>(leader)]);
    if (!warmStatus.has_value() || !warmStatus->isleader()) {
      throw std::runtime_error("leader changed during benchmark warm-up");
    }
    controlClient.Observe(warmStatus->activerangehighwater());
    const auto before = ReadStatus(endpoints[static_cast<size_t>(leader)]);
    if (!before.has_value() || !before->isleader()) {
      throw std::runtime_error("leader changed before benchmark window");
    }

    std::atomic<uint64_t> ready{0};
    std::atomic<bool> go{false};
    std::mutex errorMutex;
    std::string workerError;
    std::vector<std::vector<double>> clientLatencies(options.clients);
    std::vector<std::vector<uint64_t>> clientTimestamps(options.clients);
    std::vector<std::thread> workers;
    workers.reserve(options.clients);
    for (uint64_t clientId = 0; clientId < options.clients; ++clientId) {
      const uint64_t count = options.requests / options.clients +
                             (clientId < options.requests % options.clients ? 1 : 0);
      workers.emplace_back([&, clientId, count]() {
        try {
          RemoteTimestampOracle client(endpoints);
          auto& latencies = clientLatencies[clientId];
          auto& timestamps = clientTimestamps[clientId];
          latencies.reserve(count);
          timestamps.reserve(count);
          ready.fetch_add(1, std::memory_order_release);
          while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
          for (uint64_t index = 0; index < count; ++index) {
            const auto started = std::chrono::steady_clock::now();
            timestamps.push_back(client.Next());
            const auto elapsed = std::chrono::steady_clock::now() - started;
            latencies.push_back(
                std::chrono::duration<double, std::micro>(elapsed).count());
          }
        } catch (const std::exception& error) {
          std::lock_guard<std::mutex> lock(errorMutex);
          if (workerError.empty()) workerError = error.what();
        }
      });
    }
    while (ready.load(std::memory_order_acquire) != options.clients) std::this_thread::yield();
    const auto started = std::chrono::steady_clock::now();
    go.store(true, std::memory_order_release);
    for (auto& worker : workers) worker.join();
    const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    if (!workerError.empty()) throw std::runtime_error("benchmark client failed: " + workerError);

    std::vector<double> latencies;
    std::vector<uint64_t> timestamps;
    latencies.reserve(options.requests);
    timestamps.reserve(options.requests);
    for (size_t index = 0; index < clientLatencies.size(); ++index) {
      latencies.insert(latencies.end(), clientLatencies[index].begin(), clientLatencies[index].end());
      timestamps.insert(timestamps.end(), clientTimestamps[index].begin(), clientTimestamps[index].end());
    }
    std::sort(latencies.begin(), latencies.end());
    std::sort(timestamps.begin(), timestamps.end());
    if (timestamps.size() != options.requests ||
        std::adjacent_find(timestamps.begin(), timestamps.end()) != timestamps.end()) {
      throw std::runtime_error("benchmark observed missing or duplicate timestamps");
    }

    const auto after = ReadStatus(endpoints[static_cast<size_t>(leader)]);
    if (!after.has_value() || !after->isleader()) {
      throw std::runtime_error("leader changed during benchmark window");
    }
    Result result;
    result.mode = mode;
    result.rangeSize = rangeSize;
    result.throughput = static_cast<double>(options.requests) / seconds;
    result.p50Us = Percentile(latencies, 0.50);
    result.p95Us = Percentile(latencies, 0.95);
    result.p99Us = Percentile(latencies, 0.99);
    result.proposals = after->rangeproposalcount() - before->rangeproposalcount();
    result.reservations = after->rangereservationcount() - before->rangereservationcount();
    result.allocations = after->timestampallocationcount() - before->timestampallocationcount();
    result.allocationsPerReservation =
        result.reservations == 0
            ? 0
            : static_cast<double>(result.allocations) / static_cast<double>(result.reservations);

    for (auto& process : processes) process->Stop();
    std::filesystem::remove_all(directory);
    return result;
  } catch (...) {
    for (auto& process : processes) process->Stop();
    std::filesystem::remove_all(directory);
    throw;
  }
}

void PrintResult(const Result& result) {
  std::cout << "| " << result.mode << " | " << result.rangeSize << " | " << std::fixed
            << std::setprecision(1) << result.throughput << " | " << result.p50Us << " | "
            << result.p95Us << " | " << result.p99Us << " | " << result.proposals << " | "
            << result.allocationsPerReservation << " |\n";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const Options options = ParseOptions(argc, argv);
    const Result baseline = RunCase(options, "per-timestamp baseline", options.baselineRange);
    const Result optimized = RunCase(options, "range reservation", options.optimizedRange);

    std::cout << "TSO benchmark: same binary/host, 3 Raft members, " << options.clients
              << " clients, " << options.requests << " measured requests, "
              << options.warmupRequests << " warm-up requests per case\n\n";
    std::cout << "| mode | range | throughput (ops/s) | P50 (us) | P95 (us) | P99 (us) | Raft proposals | allocations/reservation |\n";
    std::cout << "|---|---:|---:|---:|---:|---:|---:|---:|\n";
    PrintResult(baseline);
    PrintResult(optimized);
    std::cout << "\nThroughput speedup: " << std::fixed << std::setprecision(2)
              << optimized.throughput / baseline.throughput << "x; proposal reduction: "
              << (baseline.proposals == 0
                      ? 0.0
                      : 100.0 * (1.0 - static_cast<double>(optimized.proposals) /
                                           static_cast<double>(baseline.proposals)))
              << "%\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "TSO benchmark failed: " << error.what() << '\n';
    return 1;
  }
}
