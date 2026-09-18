#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "metadata_consensus.h"
#include "metadata_service.h"
#include "region_metadata.h"
#include "rpc_provider.h"
#include "topology_config.h"

namespace {

void Usage(const char* program) {
  std::cerr << "Usage: " << program
            << " --node-id <0..2> --peers <host:port,host:port,host:port>"
               " --data-dir <path> [--bootstrap-regions <regions.conf>]"
               " [--snapshot-threshold <entries>]\n";
}

uint64_t ParseUnsigned(const std::string& value, const std::string& option) {
  try {
    size_t consumed = 0;
    const uint64_t parsed = std::stoull(value, &consumed);
    if (consumed != value.size()) throw std::invalid_argument("trailing characters");
    return parsed;
  } catch (const std::exception&) {
    throw std::invalid_argument("invalid " + option + ": " + value);
  }
}

metadataRpcProtocol::MetadataCommand MakeBootstrap(const RegionCatalog& catalog) {
  metadataRpcProtocol::MetadataCommand command;
  command.set_mutationid("bootstrap:" + catalog.Digest());
  command.set_expectedrevision(0);
  command.mutable_bootstrap()->set_configdigest(catalog.Digest());
  for (const auto& descriptor : catalog.ToProto()) {
    *command.mutable_bootstrap()->add_regions() = descriptor;
  }
  std::unordered_map<uint64_t, stratakv::region::StoreDescriptor> stores;
  for (const auto& region : catalog.Regions()) {
    for (const auto& peer : region.peers) {
      auto& store = stores[peer.storeId];
      store.set_storeid(peer.storeId);
      store.set_host(peer.host);
      store.set_port(static_cast<uint32_t>(peer.port));
    }
  }
  std::vector<uint64_t> storeIds;
  for (const auto& item : stores) storeIds.push_back(item.first);
  std::sort(storeIds.begin(), storeIds.end());
  for (uint64_t storeId : storeIds) {
    *command.mutable_bootstrap()->add_stores() = stores.at(storeId);
  }
  return command;
}

void BootstrapWhenLeader(const std::shared_ptr<MetadataConsensusNode>& node,
                         metadataRpcProtocol::MetadataCommand command) {
  const auto overallDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
  while (std::chrono::steady_clock::now() < overallDeadline) {
    try {
      const auto result =
          node->Mutate(command, std::chrono::steady_clock::now() + std::chrono::seconds(3));
      if (result.error() == metadataRpcProtocol::METADATA_OK ||
          result.error() == metadataRpcProtocol::METADATA_ALREADY_BOOTSTRAPPED) {
        std::cout << "{\"level\":\"info\",\"component\":\"metadata\","
                     "\"event\":\"bootstrap_complete\",\"revision\":"
                  << node->LocalView()->revision << "}" << std::endl;
        return;
      }
      if (result.error() != metadataRpcProtocol::METADATA_TIMEOUT &&
          result.error() != metadataRpcProtocol::METADATA_NOT_LEADER) {
        std::cerr << "metadata bootstrap rejected: " << result.message() << std::endl;
        return;
      }
    } catch (const MetadataNotLeaderError&) {
    } catch (const std::exception& error) {
      std::cerr << "metadata bootstrap retry: " << error.what() << std::endl;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  std::cerr << "metadata bootstrap did not complete within 60 seconds" << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
  // Block termination signals synchronously: the main thread waits in sigwait
  // while every spawned thread inherits the blocked mask, so only sigwait
  // observes SIGTERM/SIGINT and shutdown runs on a single well-defined path.
  sigset_t terminationSignals;
  sigemptyset(&terminationSignals);
  sigaddset(&terminationSignals, SIGTERM);
  sigaddset(&terminationSignals, SIGINT);
  pthread_sigmask(SIG_BLOCK, &terminationSignals, nullptr);
  if (argc == 2 && std::string(argv[1]) == "--help") {
    Usage(argv[0]);
    return EXIT_SUCCESS;
  }
  int nodeId = -1;
  int snapshotThreshold = 64;
  std::string peersText;
  std::string dataDirectory;
  std::string bootstrapRegions;
  try {
    for (int index = 1; index < argc; index += 2) {
      if (index + 1 >= argc) throw std::invalid_argument("missing option value");
      const std::string option = argv[index];
      const std::string value = argv[index + 1];
      if (option == "--node-id") {
        const uint64_t parsed = ParseUnsigned(value, option);
        if (parsed > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
          throw std::invalid_argument("metadata node ID is too large");
        }
        nodeId = static_cast<int>(parsed);
      } else if (option == "--peers") {
        peersText = value;
      } else if (option == "--data-dir") {
        dataDirectory = value;
      } else if (option == "--bootstrap-regions") {
        bootstrapRegions = value;
      } else if (option == "--snapshot-threshold") {
        const uint64_t parsed = ParseUnsigned(value, option);
        if (parsed == 0 || parsed > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
          throw std::invalid_argument("snapshot threshold is outside the valid range");
        }
        snapshotThreshold = static_cast<int>(parsed);
      } else {
        throw std::invalid_argument("unknown option: " + option);
      }
    }
    if (nodeId < 0 || peersText.empty() || dataDirectory.empty()) {
      Usage(argv[0]);
      return EXIT_FAILURE;
    }
    const auto peers = ParseServiceEndpoints(peersText, "metadata peers");
    if (peers.size() != 3 || nodeId >= static_cast<int>(peers.size())) {
      throw std::invalid_argument("metadata service requires exactly three peers and node ID 0..2");
    }
    const std::filesystem::path versioned =
        std::filesystem::path(dataDirectory) / "metadata-v1" /
        ("node-" + std::to_string(nodeId));
    auto node = std::make_shared<MetadataConsensusNode>(
        nodeId, peers, versioned.string(), snapshotThreshold);
    MetadataService service(node);
    node->Start();
    if (!bootstrapRegions.empty()) {
      const RegionCatalog catalog = RegionCatalog::LoadFromConfig(bootstrapRegions);
      std::thread(BootstrapWhenLeader, node, MakeBootstrap(catalog)).detach();
    }
    std::cout << "{\"level\":\"info\",\"component\":\"metadata\","
                 "\"event\":\"started\",\"node_id\":"
              << nodeId << ",\"port\":" << peers[static_cast<size_t>(nodeId)].second
              << ",\"data_dir\":\"" << versioned.string() << "\"}" << std::endl;
    const short listenPort = peers[static_cast<size_t>(nodeId)].second;
    // The RpcProvider's muduo EventLoop must be constructed, run, and destroyed
    // on one thread, so the provider lives on the RPC thread stack and the main
    // thread only borrows a pointer to request its exit.
    RpcProvider* providerHandle = nullptr;
    std::mutex providerMutex;
    std::condition_variable providerReady;
    bool providerPublished = false;
    std::thread rpcThread([&]() {
      RpcProvider provider;
      provider.NotifyService(&service);
      provider.NotifyService(node->RaftService());
      {
        std::lock_guard<std::mutex> lock(providerMutex);
        providerHandle = &provider;
        providerPublished = true;
      }
      providerReady.notify_all();
      provider.Run(node->NodeId(), listenPort);
    });
    {
      std::unique_lock<std::mutex> lock(providerMutex);
      providerReady.wait_for(lock, std::chrono::seconds(5),
                             [&] { return providerPublished; });
    }
    int signal = 0;
    sigwait(&terminationSignals, &signal);
    std::cout << "{\"level\":\"info\",\"component\":\"metadata\","
                 "\"event\":\"shutdown_begin\",\"signal\":" << signal << "}" << std::endl;
    // Bounded shutdown of the consensus node's apply thread and waiters, then
    // the RPC listener. Raft workers follow the process-lifetime model.
    node->Stop();
    {
      std::lock_guard<std::mutex> lock(providerMutex);
      if (providerHandle != nullptr) providerHandle->Stop();
    }
    rpcThread.join();
    std::cout << "{\"level\":\"info\",\"component\":\"metadata\","
                 "\"event\":\"shutdown_complete\"}" << std::endl;
    // Raft's ticker/replication threads are detached by design and never
    // observe shutdown, so normal exit() would tear down objects they still
    // use and can deadlock in static destruction. Persister writes are
    // flushed per call, so terminating the process here loses nothing.
    std::_Exit(EXIT_SUCCESS);
  } catch (const std::exception& error) {
    std::cerr << "unable to start stratakv-meta: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
