/*
 * End-to-end dynamic-topology cluster test.
 *
 * It boots a real control plane (three metadata members, three TSO members)
 * and three storage nodes in dynamic mode, then drives the SDK through
 * single-key and cross-Region transactions. The fault phases are the point of
 * the test: killing a storage node forces stale routes (leader/epoch changes)
 * that the client must refresh within its own deadline, and killing the whole
 * metadata cluster must leave a warm cache serving traffic instead of silently
 * falling back to regions.conf.
 */
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
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

#include "kv_server_rpc.pb.h"
#include "metadata_client.h"
#include "mvcc_storage.h"
#include "metadata_rpc.pb.h"
#include "mprpc_channel.h"
#include "mprpc_controller.h"
#include "stratakv/client.h"
#include "tso_rpc.pb.h"

namespace {

using Endpoint = std::pair<std::string, uint16_t>;

// A child process running one cluster role. Logs are redirected into the
// process's data directory so a failure can be diagnosed after the fact.
class ClusterProcess {
 public:
  ~ClusterProcess() { Stop(SIGKILL); }

  // reservedFds are listening sockets held by the parent so the ports cannot be
  // reused between allocation and exec; the child closes them before exec.
  void Start(const std::string& executable, const std::string& logName,
             const std::filesystem::path& dataDirectory,
             const std::vector<std::string>& arguments,
             const std::vector<int>& reservedFds = {}) {
    if (pid_ > 0) throw std::logic_error("cluster process is already running");
    std::filesystem::create_directories(dataDirectory);
    const std::string directory = dataDirectory.string();
    pid_ = fork();
    if (pid_ < 0) throw std::runtime_error("fork failed");
    if (pid_ == 0) {
      for (const int fd : reservedFds) close(fd);
      if (chdir(directory.c_str()) != 0) _exit(126);
      const int logFd = open(logName.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
      if (logFd >= 0) {
        dup2(logFd, STDOUT_FILENO);
        dup2(logFd, STDERR_FILENO);
        close(logFd);
      }
      std::vector<char*> argv;
      argv.push_back(const_cast<char*>(executable.c_str()));
      for (const auto& argument : arguments) argv.push_back(const_cast<char*>(argument.c_str()));
      argv.push_back(nullptr);
      execv(executable.c_str(), argv.data());
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

  bool Running() const { return pid_ > 0; }

 private:
  pid_t pid_ = -1;
};

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
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

bool PortAccepts(uint16_t port) {
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return false;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  const bool connected = connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0;
  close(fd);
  return connected;
}

bool CanBind(uint16_t port) {
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return false;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  const bool bound = bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0;
  close(fd);
  return bound;
}

// Ports are taken from a band below the kernel's ephemeral range, so nothing
// else on the machine is likely to grab one between allocation and bind.
std::vector<uint16_t> UnusedPorts(size_t count) {
  std::vector<uint16_t> ports;
  for (uint16_t candidate = 25000; candidate < 32000 && ports.size() < count; ++candidate) {
    if (PortAccepts(candidate)) continue;
    if (!CanBind(candidate)) continue;
    ports.push_back(candidate);
  }
  if (ports.size() != count) throw std::runtime_error("cannot allocate test ports");
  return ports;
}

std::string EndpointText(const std::vector<uint16_t>& ports) {
  std::string text;
  for (const uint16_t port : ports) {
    if (!text.empty()) text += ',';
    text += "127.0.0.1:" + std::to_string(port);
  }
  return text;
}

std::string SiblingExecutable(const std::string& name) {
  std::vector<char> path(4096);
  const ssize_t length = readlink("/proc/self/exe", path.data(), path.size() - 1);
  if (length <= 0) throw std::runtime_error("cannot locate test executable");
  path[static_cast<size_t>(length)] = '\0';
  return (std::filesystem::path(path.data()).parent_path() / name).string();
}

// One config feeds both the metadata bootstrap and the static catalog that
// tells each node which ports it owns.
void WriteRegionConfig(const std::filesystem::path& path,
                       const std::vector<uint16_t>& nodePorts) {
  std::ofstream output(path);
  if (!output) throw std::runtime_error("cannot create region config");
  const std::vector<std::pair<std::string, std::string>> ranges = {
      {"", "h"}, {"h", "p"}, {"p", ""}};
  output << "region.count=" << ranges.size() << '\n';
  for (size_t index = 0; index < ranges.size(); ++index) {
    output << "region." << index << ".id=" << (100 + index) << '\n';
    output << "region." << index << ".start_key=" << ranges[index].first << '\n';
    output << "region." << index << ".end_key=" << ranges[index].second << '\n';
    output << "region." << index << ".peer.count=" << nodePorts.size() << '\n';
    for (size_t peer = 0; peer < nodePorts.size(); ++peer) {
      output << "region." << index << ".peer." << peer << ".node_id=" << peer << '\n';
      output << "region." << index << ".peer." << peer << ".host=127.0.0.1" << '\n';
      output << "region." << index << ".peer." << peer << ".port=" << nodePorts[peer] << '\n';
    }
  }
  output.flush();
  if (!output.good()) throw std::runtime_error("cannot flush region config");
}

void WaitForPorts(const std::vector<uint16_t>& ports, const std::string& role) {
  for (int attempt = 0; attempt < 600; ++attempt) {
    bool ready = true;
    for (const uint16_t port : ports) {
      if (!PortAccepts(port)) {
        ready = false;
        break;
      }
    }
    if (ready) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  throw std::runtime_error(role + " did not start listening in time");
}

std::optional<metadataRpcProtocol::MetadataStatusReply> MetadataStatus(uint16_t port) {
  MprpcChannel channel("127.0.0.1", static_cast<short>(port), false);
  metadataRpcProtocol::metadataRpc_Stub stub(&channel);
  metadataRpcProtocol::MetadataStatusRequest request;
  metadataRpcProtocol::MetadataStatusReply reply;
  MprpcController controller;
  stub.Status(&controller, &request, &reply, nullptr);
  if (controller.Failed()) return std::nullopt;
  return reply;
}

void WaitForMetadataBootstrap(const std::vector<uint16_t>& ports,
                              const std::filesystem::path& testDirectory) {
  for (int attempt = 0; attempt < 600; ++attempt) {
    for (const uint16_t port : ports) {
      const auto status = MetadataStatus(port);
      if (status.has_value() && status->isleader() && status->revision() >= 1) return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  std::string message = "metadata cluster did not finish bootstrap; last log lines:\n";
  for (size_t index = 0; index < ports.size(); ++index) {
    std::ifstream log(testDirectory / ("meta-" + std::to_string(index)) / "metadata.log");
    std::string line;
    std::string last;
    while (std::getline(log, line)) {
      if (!line.empty()) last = line;
    }
    message += "meta-" + std::to_string(index) + ": " + last + "\n";
  }
  throw std::runtime_error(message);
}

bool TsoLeaderElected(const std::vector<uint16_t>& ports) {
  for (const uint16_t port : ports) {
    MprpcChannel channel("127.0.0.1", static_cast<short>(port), false);
    tsoRpcProtocol::timestampOracleRpc_Stub stub(&channel);
    tsoRpcProtocol::TimestampStatusRequest request;
    tsoRpcProtocol::TimestampStatusReply reply;
    MprpcController controller;
    stub.Status(&controller, &request, &reply, nullptr);
    if (!controller.Failed() && reply.isleader()) return true;
  }
  return false;
}

void WaitForTsoLeader(const std::vector<uint16_t>& ports) {
  for (int attempt = 0; attempt < 600; ++attempt) {
    if (TsoLeaderElected(ports)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  throw std::runtime_error("TSO cluster did not elect a leader");
}

// The node prints this line only after its Region Raft groups started and the
// complete registry was atomically published, so traffic before it would only
// exercise the empty-registry rejection path.
void WaitForNodesPublished(const std::filesystem::path& testDirectory) {
  const std::string readyLine = "listening on shared RPC port";
  for (int attempt = 0; attempt < 600; ++attempt) {
    bool allPublished = true;
    for (size_t index = 0; index < 3; ++index) {
      std::ifstream log(testDirectory / ("node-" + std::to_string(index)) / "node.log");
      std::string line;
      bool found = false;
      while (std::getline(log, line)) {
        if (line.find(readyLine) != std::string::npos) found = true;
      }
      if (!found) allPublished = false;
    }
    if (allPublished) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  throw std::runtime_error("storage nodes did not publish their initial registries");
}

// Drives one manual split end to end: metadata prepare + commit, AdminSplit
// proposal rotated across the parent's peers, and a wait until every replica
// reports the complete local phase.
void DriveSplit(const std::string& metadataPeers, const std::vector<uint16_t>& nodePorts,
                int regionId, const std::string& splitKey, uint64_t expectedVersion) {
  MetadataClient metadata(metadataPeers);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  uint64_t revision = 0;
  const RegionMetadata region = metadata.LookupRegion(regionId, deadline, &revision);
  Require(region.epoch.version == expectedVersion, "split preflight epoch must match");

  metadataRpcProtocol::MetadataCommand prepare;
  prepare.set_mutationid("split-prepare:" + std::to_string(regionId) + ":" + splitKey);
  prepare.set_expectedrevision(revision);
  prepare.mutable_preparesplit()->set_regionid(static_cast<uint64_t>(regionId));
  prepare.mutable_preparesplit()->set_splitkey(splitKey);
  prepare.mutable_preparesplit()->mutable_expectedepoch()->set_version(region.epoch.version);
  prepare.mutable_preparesplit()->mutable_expectedepoch()->set_confversion(
      region.epoch.confVersion);
  const auto prepared = metadata.Mutate(prepare, deadline);
  Require(prepared.error() == metadataRpcProtocol::METADATA_OK,
          "split prepare must commit");

  metadataRpcProtocol::MetadataCommand commit;
  commit.set_mutationid("split-commit:" + std::to_string(regionId) + ":" + splitKey);
  commit.set_expectedrevision(prepared.revision());
  commit.mutable_commitsplit()->set_regionid(static_cast<uint64_t>(regionId));
  const auto committed = metadata.Mutate(commit, deadline);
  Require(committed.error() == metadataRpcProtocol::METADATA_OK, "split commit must commit");
  RegionMetadata parentAfter;
  for (const auto& descriptor : committed.regions()) {
    if (static_cast<int>(descriptor.regionid()) == regionId) {
      parentAfter = FromProtoRegion(descriptor);
    }
  }
  Require(parentAfter.regionId == regionId, "commit must return the shrunken parent");

  stratakv::region::AdminSplitCommand split;
  split.set_splitkey(splitKey);
  split.set_metadatarevision(committed.revision());
  // The parent descriptor must be the PRE-split one: replicas validate the
  // command against their current (unshrunk) descriptor.
  *split.mutable_parent() = ToProtoRegion(region);
  bool childFound = false;
  for (const auto& descriptor : committed.regions()) {
    if (static_cast<int>(descriptor.regionid()) != regionId) {
      *split.mutable_child() = descriptor;
      childFound = true;
    }
  }
  Require(childFound, "commit must return the child descriptor");

  bool proposed = false;
  for (const auto& peer : parentAfter.peers) {
    MprpcChannel channel(peer.host, peer.port, false);
    raftKVRpcProctoc::kvServerRpc_Stub stub(&channel);
    raftKVRpcProctoc::ProposeAdminSplitArgs args;
    args.set_regionid(regionId);
    auto* header = args.mutable_header();
    header->set_regionid(static_cast<uint64_t>(regionId));
    header->set_peerid(peer.peerId);
    // The node still holds the pre-split descriptor until it applies this very
    // entry, so the proposal header must carry the pre-split epoch.
    header->mutable_epoch()->set_version(region.epoch.version);
    header->mutable_epoch()->set_confversion(region.epoch.confVersion);
    header->set_clientid("split-driver");
    header->set_requestid(1);
    *args.mutable_split() = split;
    raftKVRpcProctoc::ProposeAdminSplitReply reply;
    MprpcController controller;
    stub.ProposeAdminSplit(&controller, &args, &reply, nullptr);
    if (!controller.Failed() &&
        reply.err() == std::to_string(static_cast<int>(TxnStatus::Ok))) {
      proposed = true;
      break;
    }
  }
  Require(proposed, "the AdminSplit proposal must reach the parent leader");

  // Every replica must reach the complete local phase.
  for (int attempt = 0; attempt < 120; ++attempt) {
    bool allComplete = true;
    for (const auto& peer : parentAfter.peers) {
      MprpcChannel channel(peer.host, peer.port, false);
      raftKVRpcProctoc::kvServerRpc_Stub stub(&channel);
      raftKVRpcProctoc::RegionSplitStatusArgs args;
      args.set_regionid(regionId);
      raftKVRpcProctoc::RegionSplitStatusReply reply;
      MprpcController controller;
      stub.RegionSplitStatus(&controller, &args, &reply, nullptr);
      if (controller.Failed() || reply.phase() < 4) allComplete = false;
    }
    if (allComplete) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  throw std::runtime_error("split did not complete on all replicas");
}

// Runs one transaction, retrying the whole thing so a leader change or a
// refresh inside the client is allowed to cost attempts but not correctness.
stratakv::Result PutAndCommit(const std::shared_ptr<stratakv::Client>& client,
                              const std::vector<std::pair<std::string, std::string>>& writes,
                              int attempts, const std::string& context) {
  stratakv::Result last;
  for (int attempt = 0; attempt < attempts; ++attempt) {
    auto transaction = client->Begin(120000);
    bool failed = false;
    for (const auto& write : writes) {
      const auto put = client->Put(transaction, write.first, write.second);
      if (!put.ok()) {
        failed = true;
        last = put;
        break;
      }
    }
    if (failed) {
      client->Rollback(transaction);
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      continue;
    }
    last = client->Commit(transaction);
    if (last.ok()) return last;
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  throw std::runtime_error(context + " did not commit: " + stratakv::StatusName(last.status) +
                           " " + last.message);
}

std::string ReadValue(const std::shared_ptr<stratakv::Client>& client, const std::string& key,
                      int attempts, const std::string& context) {
  stratakv::Result last;
  for (int attempt = 0; attempt < attempts; ++attempt) {
    auto transaction = client->Begin(120000);
    last = client->Get(transaction, key);
    client->Commit(transaction);
    if (last.ok()) return last.value;
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  throw std::runtime_error(context + " could not read " + key + ": " +
                           stratakv::StatusName(last.status) + " " + last.message);
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-dynamic-cluster-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) {
    std::cerr << "mkdtemp failed: " << std::strerror(errno) << '\n';
    return 1;
  }
  const std::filesystem::path testDirectory(temporaryDirectory);
  std::vector<std::unique_ptr<ClusterProcess>> processes;
  try {
    const std::string tsoExecutable = SiblingExecutable("stratakv-tso");
    const std::string metadataExecutable = SiblingExecutable("stratakv-meta");
    const std::string nodeExecutable = SiblingExecutable("stratakv-node");

    // One allocation for every role: the kernel hands the same ephemeral port
    // back out once a probe socket is closed, so per-role batches could collide
    // and a metadata member would end up talking to a TSO peer.
    const std::vector<uint16_t> allPorts = UnusedPorts(9);
    const std::vector<uint16_t> tsoPorts(allPorts.begin(), allPorts.begin() + 3);
    const std::vector<uint16_t> metadataPorts(allPorts.begin() + 3, allPorts.begin() + 6);
    const std::vector<uint16_t> nodePorts(allPorts.begin() + 6, allPorts.end());
    const std::string tsoPeers = EndpointText(tsoPorts);
    const std::string metadataPeers = EndpointText(metadataPorts);
    const auto regionConfig = testDirectory / "regions.conf";
    WriteRegionConfig(regionConfig, nodePorts);

    for (size_t index = 0; index < 3; ++index) {
      const std::string id = std::to_string(index);
      auto process = std::make_unique<ClusterProcess>();
      process->Start(tsoExecutable, "tso.log", testDirectory / ("tso-" + id),
                     {"--node-id", id, "--peers", tsoPeers, "--state-file",
                      (testDirectory / ("tso-" + id) / "tso.state").string(), "--range-size",
                      "64"});
      processes.push_back(std::move(process));
    }
    WaitForTsoLeader(tsoPorts);

    for (size_t index = 0; index < 3; ++index) {
      const std::string id = std::to_string(index);
      auto process = std::make_unique<ClusterProcess>();
      process->Start(metadataExecutable, "metadata.log", testDirectory / ("meta-" + id),
                     {"--node-id", id, "--peers", metadataPeers, "--data-dir",
                      (testDirectory / ("meta-" + id)).string(), "--bootstrap-regions",
                      regionConfig.string(), "--snapshot-threshold", "2"});
      processes.push_back(std::move(process));
    }
    WaitForMetadataBootstrap(metadataPorts, testDirectory);

    // Dynamic nodes never serve a guessed topology: they load one metadata
    // revision before any Region peer exists.
    for (size_t index = 0; index < 3; ++index) {
      const std::string id = std::to_string(index);
      auto process = std::make_unique<ClusterProcess>();
      process->Start(nodeExecutable, "node.log", testDirectory / ("node-" + id),
                     {"--node-id", id, "--regions-config", regionConfig.string(),
                      "--tso-endpoints", tsoPeers, "--topology-mode", "dynamic",
                      "--metadata-endpoints", metadataPeers, "--metadata-timeout-ms", "5000"});
      processes.push_back(std::move(process));
    }
    WaitForPorts(nodePorts, "storage nodes");
    WaitForNodesPublished(testDirectory);
    // Registry is published; give the Region Raft groups a moment to elect.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    stratakv::ConnectionOptions options;
    options.topologyMode = stratakv::TopologyMode::kDynamic;
    options.metadataEndpoints = metadataPeers;
    options.tsoEndpoints = tsoPeers;
    options.metadataTimeoutMs = 5000;
    // No regionConfigPath on purpose: dynamic mode must not be able to fall
    // back to the static catalog if metadata is unavailable.
    auto client = stratakv::Client::Connect(options);
    Require(client != nullptr, "dynamic client must connect");

    // Single-key phase.
    PutAndCommit(client, {{"alpha", "one"}}, 12, "single-key write");
    Require(ReadValue(client, "alpha", 12, "single-key read") == "one",
            "single-key value must survive a round trip");

    // Batched cross-Region phase: one transaction spanning all three Regions.
    PutAndCommit(client, {{"alpha", "two"}, {"mike", "two"}, {"zulu", "two"}}, 12,
                 "cross-Region batch");
    Require(ReadValue(client, "alpha", 12, "batch read") == "two" &&
                ReadValue(client, "mike", 12, "batch read") == "two" &&
                ReadValue(client, "zulu", 12, "batch read") == "two",
            "every Region of a batched transaction must be committed atomically");

    // Manual split of the first Region at "b" while traffic continues: the
    // client refreshes stale routes and the new child serves [b, h).
    DriveSplit(metadataPeers, nodePorts, 100, "b", 1);
    // Give the shrunken parent's ReadIndex barrier time to stabilize before
    // the boundary-crossing write (distinguishes transient vs persistent).
    std::this_thread::sleep_for(std::chrono::seconds(10));
    PutAndCommit(client, {{"alpha", "pre-split"}, {"delta", "child"}, {"mike", "split"}}, 12,
                 "write across the new boundary");
    Require(ReadValue(client, "delta", 12, "child read") == "child",
            "the child Region must serve its range after the split");
    Require(ReadValue(client, "alpha", 12, "parent read") == "pre-split",
            "the shrunken parent must keep serving its range");
    Require(ReadValue(client, "mike", 12, "sibling read") == "split",
            "unrelated Regions must be unaffected");

    // Stale-route injection: killing a storage node invalidates the cached
    // leaders, so the next phase must refresh and keep serving.
    processes[6]->Stop(SIGKILL);
    PutAndCommit(client, {{"alpha", "three"}, {"mike", "three"}, {"zulu", "three"}}, 40,
                 "write while a storage node is down");
    Require(ReadValue(client, "mike", 40, "read while a storage node is down") == "three",
            "the client must refresh its route and keep serving after a leader change");

    // Metadata outage with a warm cache: data traffic keeps working because
    // routing is already cached.
    for (size_t index = 3; index < 6; ++index) processes[index]->Stop(SIGKILL);
    PutAndCommit(client, {{"alpha", "four"}, {"zulu", "four"}}, 20,
                 "write during a metadata outage with a warm cache");
    Require(ReadValue(client, "alpha", 20, "read during a metadata outage") == "four",
            "a warm cache must serve traffic while metadata is down");

    // A fresh client has no cache and no metadata: it must fail rather than
    // silently serve a stale static topology.
    bool coldClientRejected = false;
    try {
      stratakv::ConnectionOptions cold = options;
      cold.metadataTimeoutMs = 1500;
      stratakv::Client::Connect(cold);
    } catch (const std::exception&) {
      coldClientRejected = true;
    }
    Require(coldClientRejected, "dynamic mode must not start without metadata");

    // Operator rollback drill: metadata is already down and the dynamic nodes
    // are stopped, so the unchanged cluster returns to static compatibility
    // mode. Region RocksDB and TSO state are untouched; previously committed
    // data must remain readable and the cluster must accept new writes.
    for (size_t index = 6; index < processes.size(); ++index) processes[index]->Stop(SIGKILL);
    std::vector<uint16_t> staticNodePorts = UnusedPorts(3);
    WriteRegionConfig(testDirectory / "regions-static.conf", staticNodePorts);
    std::vector<std::unique_ptr<ClusterProcess>> staticNodes;
    for (size_t index = 0; index < 3; ++index) {
      auto process = std::make_unique<ClusterProcess>();
      // A real rollback reuses the same node working directories: Region
      // RocksDB, Raft persisters and TSO state stay untouched.
      process->Start(nodeExecutable, "static-node.log",
                     testDirectory / ("node-" + std::to_string(index)),
                     {"--node-id", std::to_string(index),
                      "--regions-config", (testDirectory / "regions-static.conf").string(),
                      "--tso-endpoints", tsoPeers});
      staticNodes.push_back(std::move(process));
    }
    WaitForPorts(staticNodePorts, "static rollback nodes");
    std::this_thread::sleep_for(std::chrono::seconds(7));
    {
      stratakv::ConnectionOptions rollback = options;
      rollback.topologyMode = stratakv::TopologyMode::kStatic;
      rollback.metadataEndpoints.clear();
      rollback.regionConfigPath = (testDirectory / "regions-static.conf").string();
      auto rollbackClient = stratakv::Client::Connect(rollback);
      Require(rollbackClient != nullptr, "static rollback client must connect");
      Require(ReadValue(rollbackClient, "alpha", 20, "rollback read") == "four",
              "committed data must survive the dynamic-to-static rollback");
      PutAndCommit(rollbackClient, {{"alpha", "five"}}, 12, "write after rollback");
      Require(ReadValue(rollbackClient, "alpha", 12, "post-rollback read") == "five",
              "the rolled-back cluster must keep accepting writes");
    }
    for (auto& process : staticNodes) process->Stop(SIGKILL);

    for (auto& process : processes) process->Stop(SIGKILL);
    std::filesystem::remove_all(testDirectory);
    std::cout << "Dynamic cluster integration checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    for (auto& process : processes) process->Stop(SIGKILL);
    std::cerr << "Dynamic cluster integration checks failed: " << error.what() << std::endl;
    std::cerr << "artifacts kept at " << testDirectory << std::endl;
    return 1;
  }
}
