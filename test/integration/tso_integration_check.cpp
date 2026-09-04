/*
 * 测试目标：验证三成员 TSO 在正常运行、Leader 故障、失去多数派和快照重启下的安全性。
 * 测试策略：在临时目录和随机端口启动真实 TSO 子进程，并发取号后依次模拟暂停 Leader、
 *           SIGKILL、少数派运行、恢复 quorum、日志压缩以及全量重启。
 * 测试规模：固定 3 个 TSO 进程、range size 64；先串行取号 131 次，再由 8 个客户端
 *           各取 100 次，并额外取号 2,500 次以触发日志压缩和快照。
 * 验证内容：确认时间戳单调且唯一、range 预留被摊销、旧 Leader 被 fence、未提交区间
 *           不推进 high-water、少数派拒绝发号，且快照恢复后时间戳继续前进。
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
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "mprpc_channel.h"
#include "mprpc_controller.h"
#include "raft_rpc.pb.h"
#include "remote_timestamp_oracle.h"
#include "tso_rpc.pb.h"

namespace {

class TsoProcess {
 public:
  ~TsoProcess() { Stop(SIGKILL); }

  void Start(const std::string& executable, int nodeId, const std::string& peers,
             const std::filesystem::path& dataDirectory, uint64_t rangeSize) {
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
      const std::string rangeSizeText = std::to_string(rangeSize);
      execl(executable.c_str(), executable.c_str(), "--node-id", nodeIdText.c_str(),
            "--peers", peers.c_str(), "--state-file", statePath.c_str(),
            "--range-size", rangeSizeText.c_str(), nullptr);
      _exit(127);
    }
  }

  void Signal(int signal) {
    if (pid_ <= 0 || kill(pid_, signal) != 0) throw std::runtime_error("signal TSO process failed");
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

raftRpcProctoc::RequestVoteReply DirectRequestVote(const TsoEndpoint& endpoint, int term) {
  MprpcChannel channel(endpoint.host, static_cast<short>(endpoint.port), false);
  raftRpcProctoc::raftRpc_Stub stub(&channel);
  raftRpcProctoc::RequestVoteArgs request;
  request.set_term(term);
  request.set_candidateid(99);
  request.set_lastlogindex(std::numeric_limits<int32_t>::max());
  request.set_lastlogterm(std::numeric_limits<int32_t>::max());
  raftRpcProctoc::RequestVoteReply response;
  MprpcController controller;
  stub.RequestVote(&controller, &request, &response, nullptr);
  if (controller.Failed()) throw std::runtime_error("direct RequestVote RPC failed");
  return response;
}

void WaitUntilFollower(const TsoEndpoint& endpoint) {
  for (int attempt = 0; attempt < 160; ++attempt) {
    const auto status = ReadStatus(endpoint);
    if (status.has_value() && !status->isleader()) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  throw std::runtime_error("resumed stale TSO leader did not step down");
}

bool HasSnapshot(const std::filesystem::path& testDirectory) {
  for (int nodeId = 0; nodeId < 3; ++nodeId) {
    const auto snapshot = testDirectory / ("node-" + std::to_string(nodeId)) / "run_data" /
                          ("snapshotPersist_tso_control_node" + std::to_string(nodeId) + ".txt");
    std::error_code error;
    if (std::filesystem::file_size(snapshot, error) > 0 && !error) return true;
  }
  return false;
}

}  // namespace

int main() {
  constexpr uint64_t kRangeSize = 64;
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
      processes.back()->Start(executable, index, peers,
                              testDirectory / ("node-" + std::to_string(index)), kRangeSize);
    }

    const int firstLeader = WaitForLeader(endpoints);
    const auto firstStatus = ReadStatus(endpoints[static_cast<size_t>(firstLeader)]);
    if (!firstStatus.has_value() || firstStatus->protocolversion() < 3 ||
        !firstStatus->hybridlogical() || firstStatus->rangesize() != kRangeSize) {
      throw std::runtime_error("TSO leader did not advertise range-reservation protocol v3");
    }
    RemoteTimestampOracle failoverClient(endpoints);
    std::vector<uint64_t> timestamps;
    for (int index = 0; index < 2 * static_cast<int>(kRangeSize) + 3; ++index) {
      const uint64_t timestamp = failoverClient.Next();
      if (!timestamps.empty() && timestamp <= timestamps.back()) {
        throw std::runtime_error("single-thread TSO allocation was not increasing");
      }
      timestamps.push_back(timestamp);
    }
    if (HlcTimestamp::PhysicalMs(timestamps.back()) == 0) {
      throw std::runtime_error("TSO did not return a physical-plus-logical timestamp");
    }

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

    const auto rangeStatus = ReadStatus(endpoints[static_cast<size_t>(firstLeader)]);
    if (!rangeStatus.has_value() || rangeStatus->rangereservationcount() < 2 ||
        rangeStatus->rangeproposalcount() >= timestamps.size() / 4 ||
        rangeStatus->averagetimestampsperreservation() <= 4.0) {
      throw std::runtime_error("TSO did not amortize allocations across committed ranges");
    }

    // A heartbeat-acknowledging follower must not grant a competing
    // higher-term vote inside the 300 ms minimum election interval. This is
    // the Raft-side half of the TSO's shorter 150 ms majority fence proof.
    std::this_thread::sleep_for(std::chrono::milliseconds(160));
    (void)failoverClient.Peek();
    const auto fencedStatus = ReadStatus(endpoints[static_cast<size_t>(firstLeader)]);
    if (!fencedStatus.has_value() || !fencedStatus->isleader()) {
      throw std::runtime_error("cannot establish TSO fence for vote suppression test");
    }
    for (size_t index = 0; index < endpoints.size(); ++index) {
      if (static_cast<int>(index) == firstLeader) continue;
      const auto vote = DirectRequestVote(endpoints[index], fencedStatus->term() + 1);
      if (vote.votegranted() || vote.term() != fencedStatus->term()) {
        throw std::runtime_error("recent leader contact did not suppress a competing vote");
      }
    }

    const uint64_t observed = std::max(
        maximum + 1000, HlcTimestamp::Compose(HlcTimestamp::WallClockMs() + 10, 0));
    failoverClient.Observe(observed);
    const uint64_t afterObserve = failoverClient.Next();
    if (afterObserve <= observed) throw std::runtime_error("Observe did not advance TSO");
    maximum = afterObserve;

    // Pause the active member to model a partitioned/stale leader. The new
    // leader must skip the old member's entire committed-but-unused range.
    const auto beforePartition = ReadStatus(endpoints[static_cast<size_t>(firstLeader)]);
    if (!beforePartition.has_value() ||
        beforePartition->activerangehighwater() < maximum) {
      throw std::runtime_error("leader did not expose an active committed range");
    }
    const uint64_t partitionedRangeHighWater = beforePartition->activerangehighwater();
    processes[static_cast<size_t>(firstLeader)]->Signal(SIGSTOP);
    const int partitionLeader = WaitForLeader(endpoints, firstLeader);
    const uint64_t afterPartition = failoverClient.Next();
    if (afterPartition <= partitionedRangeHighWater) {
      throw std::runtime_error("new leader reused the stale leader's committed range");
    }
    maximum = afterPartition;
    processes[static_cast<size_t>(firstLeader)]->Signal(SIGCONT);
    WaitUntilFollower(endpoints[static_cast<size_t>(firstLeader)]);
    if (DirectNextSucceeds(endpoints[static_cast<size_t>(firstLeader)])) {
      throw std::runtime_error("stale TSO leader allocated after rejoining");
    }

    // Kill -9 the active leader. A new timestamp must be committed by the
    // remaining majority and skip its committed-but-unused range.
    const auto beforeCrash = ReadStatus(endpoints[static_cast<size_t>(partitionLeader)]);
    if (!beforeCrash.has_value()) throw std::runtime_error("cannot read leader before crash");
    const uint64_t crashedRangeHighWater = beforeCrash->activerangehighwater();
    processes[static_cast<size_t>(partitionLeader)]->Stop(SIGKILL);
    const int secondLeader = WaitForLeader(endpoints, partitionLeader);
    const uint64_t afterFailover = failoverClient.Next();
    if (afterFailover <= maximum || afterFailover <= crashedRangeHighWater) {
      throw std::runtime_error("timestamp did not skip the crashed leader's committed range");
    }
    maximum = afterFailover;

    // Restore a full quorum before exercising an accepted but uncommitted
    // reservation. Observe(activeHighWater) empties the local range without a
    // proposal; immediately removing both followers leaves the cached fence
    // valid long enough for Start() to accept a proposal that cannot commit.
    processes[static_cast<size_t>(partitionLeader)]->Start(
        executable, partitionLeader, peers,
        testDirectory / ("node-" + std::to_string(partitionLeader)), kRangeSize);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::this_thread::sleep_for(std::chrono::milliseconds(180));
    (void)failoverClient.Peek();
    const auto beforeMinority = ReadStatus(endpoints[static_cast<size_t>(secondLeader)]);
    if (!beforeMinority.has_value() || !beforeMinority->isleader()) {
      throw std::runtime_error("cannot prepare minority reservation test");
    }
    failoverClient.Observe(beforeMinority->activerangehighwater());
    std::vector<int> stoppedFollowers;
    for (int index = 0; index < 3; ++index) {
      if (index != secondLeader) {
        processes[static_cast<size_t>(index)]->Stop(SIGKILL);
        stoppedFollowers.push_back(index);
      }
    }
    if (DirectNextSucceeds(endpoints[static_cast<size_t>(secondLeader)])) {
      throw std::runtime_error("single TSO member allocated without a majority");
    }
    const auto afterRejectedReservation = ReadStatus(endpoints[static_cast<size_t>(secondLeader)]);
    if (!afterRejectedReservation.has_value() ||
        afterRejectedReservation->highwater() != beforeMinority->highwater() ||
        afterRejectedReservation->rangeproposalcount() <= beforeMinority->rangeproposalcount()) {
      throw std::runtime_error("uncommitted range changed the committed high-water");
    }
    // The cached majority fence is now certainly expired. Keeping one active
    // range would still not authorize this isolated member to allocate.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    if (DirectNextSucceeds(endpoints[static_cast<size_t>(secondLeader)])) {
      throw std::runtime_error("expired TSO fence allowed minority allocation");
    }

    const int restoredFollower = stoppedFollowers.front();
    processes[static_cast<size_t>(restoredFollower)]->Start(
        executable, restoredFollower, peers,
        testDirectory / ("node-" + std::to_string(restoredFollower)), kRangeSize);
    const uint64_t afterQuorumRestore = failoverClient.Next();
    if (afterQuorumRestore <= maximum) throw std::runtime_error("timestamp regressed after quorum restore");
    maximum = afterQuorumRestore;

    const int otherFollower = stoppedFollowers.back();
    processes[static_cast<size_t>(otherFollower)]->Start(
        executable, otherFollower, peers,
        testDirectory / ("node-" + std::to_string(otherFollower)), kRangeSize);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Generate enough range entries to compact the TSO Raft log, then restart
    // from the independent Raft/snapshot files of all three members.
    for (int index = 0; index < 2500; ++index) maximum = failoverClient.Next();
    for (int attempt = 0; attempt < 80 && !HasSnapshot(testDirectory); ++attempt) {
      std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    if (!HasSnapshot(testDirectory)) throw std::runtime_error("TSO snapshot was not created");
    const int leaderBeforeRestart = WaitForLeader(endpoints);
    const auto restartStatus = ReadStatus(endpoints[static_cast<size_t>(leaderBeforeRestart)]);
    if (!restartStatus.has_value()) throw std::runtime_error("cannot read TSO before restart");
    const uint64_t committedBeforeRestart = restartStatus->highwater();
    for (auto& process : processes) process->Stop(SIGKILL);
    for (int index = 0; index < 3; ++index) {
      processes[static_cast<size_t>(index)]->Start(
          executable, index, peers, testDirectory / ("node-" + std::to_string(index)), kRangeSize);
    }
    WaitForLeader(endpoints);
    const uint64_t afterRestart = failoverClient.Next();
    if (afterRestart <= maximum || afterRestart <= committedBeforeRestart) {
      throw std::runtime_error("timestamp regressed after snapshot-backed cluster restart");
    }

    for (auto& process : processes) process->Stop(SIGTERM);
    std::filesystem::remove_all(testDirectory);
    std::cout << "TSO consensus check passed: " << timestamps.size()
              << " allocations were unique across multiple ranges; Observe advanced to "
              << afterObserve << ", stale/crashed leaders were fenced, minority reservations were"
                 " rejected, and snapshot restart advanced to "
              << afterRestart << '\n';
    return 0;
  } catch (const std::exception& error) {
    for (auto& process : processes) process->Stop(SIGKILL);
    std::filesystem::remove_all(testDirectory);
    std::cerr << error.what() << '\n';
    return 1;
  }
}
