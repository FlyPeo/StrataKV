/*
 * Test a real three-member metadata cluster across bootstrap, failover,
 * quorum loss, snapshot compaction, and a complete process restart.
 */
#include <arpa/inet.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "metadata_client.h"
#include "metadata_rpc.pb.h"
#include "mprpc_channel.h"
#include "mprpc_controller.h"

namespace {

using Endpoint = std::pair<std::string, short>;

class MetadataProcess {
 public:
  ~MetadataProcess() { Stop(SIGKILL); }

  void Start(const std::string& executable, int nodeId, const std::string& peers,
             const std::filesystem::path& dataDirectory,
             const std::filesystem::path& regionConfig) {
    if (pid_ > 0) throw std::logic_error("metadata process is already running");
    std::filesystem::create_directories(dataDirectory);
    const std::string nodeIdText = std::to_string(nodeId);
    pid_ = fork();
    if (pid_ < 0) throw std::runtime_error("fork failed");
    if (pid_ == 0) {
      if (chdir(dataDirectory.c_str()) != 0) _exit(126);
      const int logFd = open("metadata.log", O_CREAT | O_TRUNC | O_WRONLY, 0644);
      if (logFd >= 0) {
        dup2(logFd, STDOUT_FILENO);
        dup2(logFd, STDERR_FILENO);
        close(logFd);
      }
      execl(executable.c_str(), executable.c_str(), "--node-id", nodeIdText.c_str(),
            "--peers", peers.c_str(), "--data-dir", dataDirectory.c_str(),
            "--bootstrap-regions", regionConfig.c_str(), "--snapshot-threshold", "2",
            nullptr);
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

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

uint16_t UnusedLowPort(const std::vector<Endpoint>& used) {
  const unsigned start = 24000U + static_cast<unsigned>(getpid()) % 7000U;
  for (unsigned offset = 0; offset < 7000; ++offset) {
    const uint16_t port = static_cast<uint16_t>(24000U + (start - 24000U + offset) % 7000U);
    if (std::any_of(used.begin(), used.end(), [port](const Endpoint& endpoint) {
          return static_cast<uint16_t>(endpoint.second) == port;
        })) {
      continue;
    }
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket failed");
    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    const bool available = bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0;
    close(fd);
    if (available) return port;
  }
  throw std::runtime_error("unable to reserve metadata test ports");
}

std::string SiblingExecutable(const std::string& name) {
  std::vector<char> path(4096);
  const ssize_t length = readlink("/proc/self/exe", path.data(), path.size() - 1);
  if (length <= 0) throw std::runtime_error("cannot locate test executable");
  path[static_cast<size_t>(length)] = '\0';
  return (std::filesystem::path(path.data()).parent_path() / name).string();
}

std::string EndpointText(const std::vector<Endpoint>& endpoints) {
  std::string text;
  for (const auto& endpoint : endpoints) {
    if (!text.empty()) text += ',';
    text += endpoint.first + ':' +
            std::to_string(static_cast<uint16_t>(endpoint.second));
  }
  return text;
}

void WriteRegionConfig(const std::filesystem::path& path) {
  std::ofstream output(path);
  if (!output) throw std::runtime_error("cannot create metadata bootstrap config");
  output << R"(region.count=3
region.0.id=100
region.0.start_key=
region.0.end_key=h
region.0.peer.count=3
region.0.peer.0.node_id=0
region.0.peer.0.host=127.0.0.1
region.0.peer.0.port=23000
region.0.peer.1.node_id=1
region.0.peer.1.host=127.0.0.1
region.0.peer.1.port=23001
region.0.peer.2.node_id=2
region.0.peer.2.host=127.0.0.1
region.0.peer.2.port=23002
region.1.id=101
region.1.start_key=h
region.1.end_key=p
region.1.peer.count=3
region.1.peer.0.node_id=0
region.1.peer.0.host=127.0.0.1
region.1.peer.0.port=23000
region.1.peer.1.node_id=1
region.1.peer.1.host=127.0.0.1
region.1.peer.1.port=23001
region.1.peer.2.node_id=2
region.1.peer.2.host=127.0.0.1
region.1.peer.2.port=23002
region.2.id=102
region.2.start_key=p
region.2.end_key=
region.2.peer.count=3
region.2.peer.0.node_id=0
region.2.peer.0.host=127.0.0.1
region.2.peer.0.port=23000
region.2.peer.1.node_id=1
region.2.peer.1.host=127.0.0.1
region.2.peer.1.port=23001
region.2.peer.2.node_id=2
region.2.peer.2.host=127.0.0.1
region.2.peer.2.port=23002
)";
  output.flush();
  if (!output.good()) throw std::runtime_error("cannot flush metadata bootstrap config");
}

std::optional<metadataRpcProtocol::MetadataStatusReply> ReadStatus(const Endpoint& endpoint) {
  MprpcChannel channel(endpoint.first, endpoint.second, false);
  metadataRpcProtocol::metadataRpc_Stub stub(&channel);
  metadataRpcProtocol::MetadataStatusRequest request;
  metadataRpcProtocol::MetadataStatusReply reply;
  MprpcController controller;
  stub.Status(&controller, &request, &reply, nullptr);
  if (controller.Failed()) return std::nullopt;
  return reply;
}

int WaitForLeader(const std::vector<Endpoint>& endpoints,
                  const std::vector<bool>& running) {
  for (int attempt = 0; attempt < 240; ++attempt) {
    for (size_t index = 0; index < endpoints.size(); ++index) {
      if (!running[index]) continue;
      const auto status = ReadStatus(endpoints[index]);
      if (status.has_value() && status->isleader()) return static_cast<int>(index);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  throw std::runtime_error("metadata cluster did not elect a leader");
}

void WaitForBootstrap(const std::vector<Endpoint>& endpoints,
                      const std::vector<bool>& running) {
  for (int attempt = 0; attempt < 240; ++attempt) {
    for (size_t index = 0; index < endpoints.size(); ++index) {
      if (!running[index]) continue;
      const auto status = ReadStatus(endpoints[index]);
      if (status.has_value() && status->isleader() && status->revision() >= 1) return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  throw std::runtime_error("metadata cluster did not finish bootstrap");
}

metadataRpcProtocol::MetadataCommand Allocation(std::string mutationId,
                                                uint64_t revision,
                                                uint64_t count) {
  metadataRpcProtocol::MetadataCommand command;
  command.set_mutationid(std::move(mutationId));
  command.set_expectedrevision(revision);
  command.mutable_allocateids()->set_namespace_(metadataRpcProtocol::ID_NAMESPACE_REGION);
  command.mutable_allocateids()->set_count(count);
  return command;
}

bool HasSnapshot(const std::filesystem::path& testDirectory) {
  for (int nodeId = 0; nodeId < 3; ++nodeId) {
    const auto snapshot = testDirectory / ("node-" + std::to_string(nodeId)) /
                          "metadata-v1" / ("node-" + std::to_string(nodeId)) /
                          ("snapshotPersist_metadata_node" + std::to_string(nodeId) + ".txt");
    std::error_code error;
    if (std::filesystem::file_size(snapshot, error) > 0 && !error) return true;
  }
  return false;
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-metadata-consensus-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) {
    std::cerr << "mkdtemp failed: " << std::strerror(errno) << '\n';
    return 1;
  }

  const std::filesystem::path testDirectory(temporaryDirectory);
  std::vector<std::unique_ptr<MetadataProcess>> processes;
  std::vector<bool> running(3, false);
  try {
    const std::string executable = SiblingExecutable("stratakv-meta");
    std::vector<Endpoint> endpoints;
    for (int index = 0; index < 3; ++index) {
      const uint16_t port = UnusedLowPort(endpoints);
      endpoints.emplace_back("127.0.0.1", static_cast<short>(port));
    }
    const std::string peers = EndpointText(endpoints);
    const auto regionConfig = testDirectory / "regions.conf";
    WriteRegionConfig(regionConfig);

    for (int index = 0; index < 3; ++index) {
      processes.push_back(std::make_unique<MetadataProcess>());
      processes.back()->Start(executable, index, peers,
                              testDirectory / ("node-" + std::to_string(index)),
                              regionConfig);
      running[static_cast<size_t>(index)] = true;
    }

    const int firstLeader = WaitForLeader(endpoints, running);
    WaitForBootstrap(endpoints, running);
    MetadataClient client(peers);
    uint64_t revision = 0;
    const auto left = client.LookupKey("a", std::chrono::steady_clock::now() +
                                               std::chrono::seconds(5), &revision);
    Require(left.regionId == 100 && revision == 1,
            "bootstrap did not publish exactly one initial revision");
    const auto middle = client.LookupKey("h", std::chrono::steady_clock::now() +
                                                  std::chrono::seconds(5));
    Require(middle.regionId == 101, "half-open boundary did not route to the right Region");
    const auto scan = client.Scan("", 10, std::chrono::steady_clock::now() +
                                            std::chrono::seconds(5), &revision);
    Require(scan.size() == 3 && scan[0].regionId == 100 && scan[1].regionId == 101 &&
                scan[2].regionId == 102 && revision == 1,
            "metadata range scan did not return one coherent revision");

    const auto firstAllocation = Allocation("integration-allocation-1", revision, 4);
    const auto allocated = client.Mutate(firstAllocation,
                                         std::chrono::steady_clock::now() +
                                             std::chrono::seconds(5));
    Require(allocated.error() == metadataRpcProtocol::METADATA_OK &&
                allocated.revision() == 2 && allocated.firstallocatedid() > 102 &&
                allocated.allocatedcount() == 4,
            "metadata ID allocation failed");
    const auto duplicate = client.Mutate(firstAllocation,
                                         std::chrono::steady_clock::now() +
                                             std::chrono::seconds(5));
    Require(duplicate.SerializeAsString() == allocated.SerializeAsString(),
            "duplicate metadata mutation changed its result");

    for (int attempt = 0; attempt < 120 && !HasSnapshot(testDirectory); ++attempt) {
      std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    Require(HasSnapshot(testDirectory), "metadata snapshot was not created");

    processes[static_cast<size_t>(firstLeader)]->Stop(SIGKILL);
    running[static_cast<size_t>(firstLeader)] = false;
    const int secondLeader = WaitForLeader(endpoints, running);
    client.LookupKey("z", std::chrono::steady_clock::now() + std::chrono::seconds(5),
                     &revision);
    auto afterFailoverCommand = Allocation("integration-allocation-2", revision, 2);
    const auto afterFailover = client.Mutate(afterFailoverCommand,
                                             std::chrono::steady_clock::now() +
                                                 std::chrono::seconds(5));
    Require(afterFailover.error() == metadataRpcProtocol::METADATA_OK &&
                afterFailover.firstallocatedid() > allocated.firstallocatedid(),
            "metadata mutation did not recover after leader failure");

    int remainingFollower = -1;
    for (int index = 0; index < 3; ++index) {
      if (running[static_cast<size_t>(index)] && index != secondLeader) {
        remainingFollower = index;
        break;
      }
    }
    Require(remainingFollower >= 0, "cannot locate follower for quorum-loss test");
    processes[static_cast<size_t>(remainingFollower)]->Stop(SIGKILL);
    running[static_cast<size_t>(remainingFollower)] = false;
    const auto withoutQuorum = client.Mutate(
        Allocation("integration-no-quorum", afterFailover.revision(), 1),
        std::chrono::steady_clock::now() + std::chrono::milliseconds(600));
    Require(withoutQuorum.error() == metadataRpcProtocol::METADATA_TIMEOUT,
            "isolated metadata member accepted a mutation without quorum");

    processes[static_cast<size_t>(remainingFollower)]->Start(
        executable, remainingFollower, peers,
        testDirectory / ("node-" + std::to_string(remainingFollower)), regionConfig);
    running[static_cast<size_t>(remainingFollower)] = true;
    WaitForLeader(endpoints, running);
    metadataRpcProtocol::MetadataCommandResult afterQuorum;
    for (int attempt = 0; attempt < 4; ++attempt) {
      client.LookupKey("z", std::chrono::steady_clock::now() + std::chrono::seconds(5),
                       &revision);
      afterQuorum = client.Mutate(
          Allocation("integration-after-quorum-" + std::to_string(attempt), revision, 1),
          std::chrono::steady_clock::now() + std::chrono::seconds(5));
      if (afterQuorum.error() != metadataRpcProtocol::METADATA_REVISION_MISMATCH) break;
    }
    Require(afterQuorum.error() == metadataRpcProtocol::METADATA_OK,
            "metadata cluster did not accept mutations after quorum recovery: error=" +
                std::to_string(afterQuorum.error()) + " revision=" +
                std::to_string(afterQuorum.revision()) + " message=" +
                afterQuorum.message());

    for (int index = 0; index < 3; ++index) {
      if (!running[static_cast<size_t>(index)]) {
        processes[static_cast<size_t>(index)]->Start(
            executable, index, peers,
            testDirectory / ("node-" + std::to_string(index)), regionConfig);
        running[static_cast<size_t>(index)] = true;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (int index = 0; index < 3; ++index) {
      processes[static_cast<size_t>(index)]->Stop(SIGKILL);
      running[static_cast<size_t>(index)] = false;
    }
    for (int index = 0; index < 3; ++index) {
      processes[static_cast<size_t>(index)]->Start(
          executable, index, peers,
          testDirectory / ("node-" + std::to_string(index)), regionConfig);
      running[static_cast<size_t>(index)] = true;
    }
    WaitForLeader(endpoints, running);
    const auto restored = client.LookupKey(
        "h", std::chrono::steady_clock::now() + std::chrono::seconds(8), &revision);
    Require(restored.regionId == 101 && revision >= afterQuorum.revision(),
            "metadata revision or topology regressed after complete restart");
    const auto dedupAfterRestart = client.Mutate(
        firstAllocation, std::chrono::steady_clock::now() + std::chrono::seconds(5));
    Require(dedupAfterRestart.SerializeAsString() == allocated.SerializeAsString(),
            "snapshot/replay lost metadata mutation deduplication");

    for (auto& process : processes) process->Stop(SIGTERM);
    std::filesystem::remove_all(testDirectory);
    const auto metrics = client.Metrics();
    Require(metrics.retries > 0, "fault test did not exercise metadata client retries");
    std::cout << "Metadata consensus check passed: bootstrap revision 1, coherent scan, "
                 "deduplicated allocation, leader failover, quorum timeout, and snapshot-backed "
                 "restart were verified"
              << std::endl;
    return 0;
  } catch (const std::exception& error) {
    for (auto& process : processes) process->Stop(SIGKILL);
    std::cerr << error.what() << " (artifacts: " << testDirectory << ")\n";
    return 1;
  }
}
