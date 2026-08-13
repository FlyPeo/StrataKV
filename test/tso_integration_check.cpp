#include <arpa/inet.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
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

class TsoProcess {
 public:
  ~TsoProcess() { Stop(SIGKILL); }

  void Start(const std::string& executable, int nodeId, const std::string& peers,
             const std::filesystem::path& dataDirectory) {
    if (pid_ > 0) throw std::logic_error("TSO process is already running");
    std::filesystem::create_directories(dataDirectory);
    const std::string nodeIdText = std::to_string(nodeId);
    const std::string statePath = (dataDirectory / "tso.state").string();
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
            "--peers", peers.c_str(), "--state-file", statePath.c_str(),
            "--segment-size", "64", nullptr);
      _exit(127);
    }
  }

  void Stop(int signal) {
    if (pid_ <= 0) return;
    kill(pid_, signal);
    int status = 0;
    while (waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
    }
    pid_ = -1;
  }

 private:
  pid_t pid_ = -1;
};

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
  if (length <= 0) throw std::runtime_error("cannot locate test executable");
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

int WaitForLeader(const std::vector<TsoEndpoint>& endpoints, int excluded = -1) {
  for (int attempt = 0; attempt < 160; ++attempt) {
    for (size_t index = 0; index < endpoints.size(); ++index) {
      if (static_cast<int>(index) == excluded) continue;
      const auto status = ReadStatus(endpoints[index]);
      if (status.has_value() && status->isleader()) return static_cast<int>(index);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  throw std::runtime_error("TSO cluster did not elect a leader");
}

bool DirectNextSucceeds(const TsoEndpoint& endpoint) {
  MprpcChannel channel(endpoint.host, static_cast<short>(endpoint.port), false);
  tsoRpcProtocol::timestampOracleRpc_Stub stub(&channel);
  tsoRpcProtocol::TimestampRequest request;
  tsoRpcProtocol::TimestampReply response;
  MprpcController controller;
  stub.Next(&controller, &request, &response, nullptr);
  return !controller.Failed() && response.err().empty() && !response.notleader() && response.timestamp() != 0;
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-tso-consensus-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) {
    std::cerr << "mkdtemp failed: " << std::strerror(errno) << '\n';
    return 1;
  }

  const std::filesystem::path testDirectory(temporaryDirectory);
  std::vector<std::unique_ptr<TsoProcess>> processes;
  try {
    const std::string executable = SiblingExecutable("stratakv-tso");
    std::vector<TsoEndpoint> endpoints;
    while (endpoints.size() < 3) {
      const uint16_t port = UnusedPort();
      if (std::none_of(endpoints.begin(), endpoints.end(),
                       [port](const TsoEndpoint& endpoint) { return endpoint.port == port; })) {
        endpoints.push_back(TsoEndpoint{"127.0.0.1", port});
      }
    }
    const std::string peers = EndpointText(endpoints);
    for (int index = 0; index < 3; ++index) {
      processes.push_back(std::make_unique<TsoProcess>());
      processes.back()->Start(executable, index, peers, testDirectory / ("node-" + std::to_string(index)));
    }

    const int firstLeader = WaitForLeader(endpoints);
    RemoteTimestampOracle failoverClient(endpoints);
    std::vector<uint64_t> timestamps;
    timestamps.push_back(failoverClient.Next());

    constexpr int kClients = 8;
    constexpr int kTimestampsPerClient = 100;
    std::vector<std::vector<uint64_t>> clientTimestamps(kClients);
    std::vector<std::thread> workers;
    std::mutex errorMutex;
    std::string threadError;
    for (int clientIndex = 0; clientIndex < kClients; ++clientIndex) {
      workers.emplace_back([&, clientIndex]() {
        try {
          RemoteTimestampOracle client(endpoints);
          auto& values = clientTimestamps[clientIndex];
          values.reserve(kTimestampsPerClient);
          for (int index = 0; index < kTimestampsPerClient; ++index) values.push_back(client.Next());
        } catch (const std::exception& error) {
          std::lock_guard<std::mutex> lock(errorMutex);
          if (threadError.empty()) threadError = error.what();
        }
      });
    }
    for (auto& worker : workers) worker.join();
    if (!threadError.empty()) throw std::runtime_error("concurrent TSO client failed: " + threadError);

    for (const auto& values : clientTimestamps) {
      timestamps.insert(timestamps.end(), values.begin(), values.end());
    }
    std::sort(timestamps.begin(), timestamps.end());
    if (std::adjacent_find(timestamps.begin(), timestamps.end()) != timestamps.end()) {
      throw std::runtime_error("duplicate timestamp allocated across clients");
    }
    uint64_t maximum = timestamps.back();

    // Kill -9 the active leader. A new timestamp must be committed by the
    // remaining majority without changing the endpoint list used by clients.
    processes[static_cast<size_t>(firstLeader)]->Stop(SIGKILL);
    const int secondLeader = WaitForLeader(endpoints, firstLeader);
    const uint64_t afterFailover = failoverClient.Next();
    if (afterFailover <= maximum) throw std::runtime_error("timestamp regressed after leader failover");
    maximum = afterFailover;

    int stoppedFollower = -1;
    for (int index = 0; index < 3; ++index) {
      if (index != firstLeader && index != secondLeader) stoppedFollower = index;
    }
    processes[static_cast<size_t>(stoppedFollower)]->Stop(SIGKILL);
    if (DirectNextSucceeds(endpoints[static_cast<size_t>(secondLeader)])) {
      throw std::runtime_error("single TSO member allocated without a majority");
    }
    processes[static_cast<size_t>(stoppedFollower)]->Start(
        executable, stoppedFollower, peers, testDirectory / ("node-" + std::to_string(stoppedFollower)));
    const uint64_t afterQuorumRestore = failoverClient.Next();
    if (afterQuorumRestore <= maximum) throw std::runtime_error("timestamp regressed after quorum restore");
    maximum = afterQuorumRestore;

    // Rejoin the old leader, then crash-restart the entire control plane from
    // its three independent persisted Raft and watermark directories.
    processes[static_cast<size_t>(firstLeader)]->Start(
        executable, firstLeader, peers, testDirectory / ("node-" + std::to_string(firstLeader)));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    (void)secondLeader;
    for (auto& process : processes) process->Stop(SIGKILL);
    for (int index = 0; index < 3; ++index) {
      processes[static_cast<size_t>(index)]->Start(
          executable, index, peers, testDirectory / ("node-" + std::to_string(index)));
    }
    WaitForLeader(endpoints);
    const uint64_t afterRestart = failoverClient.Next();
    if (afterRestart <= maximum) throw std::runtime_error("timestamp regressed after cluster restart");

    for (auto& process : processes) process->Stop(SIGTERM);
    std::filesystem::remove_all(testDirectory);
    std::cout << "TSO consensus check passed: " << timestamps.size()
              << " concurrent allocations were unique; leader failover advanced to " << afterFailover
              << ", minority allocation was rejected, and full restart advanced to " << afterRestart << '\n';
    return 0;
  } catch (const std::exception& error) {
    for (auto& process : processes) process->Stop(SIGKILL);
    std::filesystem::remove_all(testDirectory);
    std::cerr << error.what() << '\n';
    return 1;
  }
}
