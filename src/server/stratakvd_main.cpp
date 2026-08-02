#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "kvServer.h"
#include "region_metadata.h"

namespace {

void PrintUsage(const char* program) {
  std::cerr << "Usage: " << program
            << " --node-id <id> (--port <port> --config <cluster.conf> | --regions-config <regions.conf>)"
               " [--max-raft-state <bytes>]\n";
}

bool ParseInt(const std::string& text, int* value) {
  try {
    size_t consumed = 0;
    const long parsed = std::stol(text, &consumed);
    if (consumed != text.size() || parsed < 0 || parsed > std::numeric_limits<int>::max()) {
      return false;
    }
    *value = static_cast<int>(parsed);
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc == 2 && std::string(argv[1]) == "--help") {
    PrintUsage(argv[0]);
    return EXIT_SUCCESS;
  }

  int nodeId = -1;
  int port = -1;
  // Persist rewrites the complete Raft state. Keeping the default at 64 MiB
  // causes severe O(log-size) write amplification before the first snapshot.
  // A 1 MiB cap snapshots at 90% in KvServer and keeps long local runs bounded.
  int maxRaftState = 1024 * 1024;
  std::string configPath;
  std::string regionsConfigPath;

  for (int index = 1; index < argc; index += 2) {
    if (index + 1 >= argc) {
      PrintUsage(argv[0]);
      return EXIT_FAILURE;
    }

    const std::string option = argv[index];
    const std::string value = argv[index + 1];
    if (option == "--node-id") {
      if (!ParseInt(value, &nodeId)) {
        std::cerr << "invalid --node-id: " << value << '\n';
        return EXIT_FAILURE;
      }
    } else if (option == "--port") {
      if (!ParseInt(value, &port) || port == 0 || port > 65535) {
        std::cerr << "invalid --port: " << value << '\n';
        return EXIT_FAILURE;
      }
    } else if (option == "--config") {
      configPath = value;
    } else if (option == "--regions-config") {
      regionsConfigPath = value;
    } else if (option == "--max-raft-state") {
      if (!ParseInt(value, &maxRaftState) || maxRaftState == 0) {
        std::cerr << "invalid --max-raft-state: " << value << '\n';
        return EXIT_FAILURE;
      }
    } else {
      std::cerr << "unknown option: " << option << '\n';
      PrintUsage(argv[0]);
      return EXIT_FAILURE;
    }
  }

  if (nodeId < 0 || (!regionsConfigPath.empty() && (!configPath.empty() || port >= 0)) ||
      (regionsConfigPath.empty() && (port < 0 || configPath.empty()))) {
    PrintUsage(argv[0]);
    return EXIT_FAILURE;
  }

  if (!regionsConfigPath.empty()) {
    try {
      const RegionCatalog catalog = RegionCatalog::LoadFromConfig(regionsConfigPath);
      const auto assignments = catalog.PeersOnNode(nodeId);
      if (assignments.empty()) {
        std::cerr << "node " << nodeId << " has no Region peers in " << regionsConfigPath << '\n';
        return EXIT_FAILURE;
      }

      std::cout << "Starting StrataKV physical node " << nodeId << " with " << assignments.size()
                << " Region peers using " << regionsConfigPath << '\n';
      for (const auto& assignment : assignments) {
        std::vector<std::pair<std::string, short>> peerAddresses;
        peerAddresses.reserve(assignment.region.peers.size());
        for (const auto& peer : assignment.region.peers) {
          peerAddresses.emplace_back(peer.host, peer.port);
        }
        const short localPort = assignment.region.peers[assignment.peerIndex].port;
        const int regionId = assignment.region.regionId;
        const int localPeerId = static_cast<int>(assignment.peerIndex);
        std::thread([nodeId, regionId, localPeerId, maxRaftState, peerAddresses = std::move(peerAddresses), localPort]() {
          try {
            // KvServer owns a long-running RPC server and apply loop.  Each Region peer needs
            // its own instance because it owns an independent Raft state machine and storage.
            new KvServer(nodeId, regionId, localPeerId, maxRaftState, peerAddresses, localPort);
          } catch (const std::exception& error) {
            std::cerr << "failed to start region " << regionId << " peer on node " << nodeId << ": "
                      << error.what() << '\n';
          }
        }).detach();
      }

      // The Region peer constructors run their own apply loops on detached threads.
      // Keep the physical-node process alive for the lifetime of those peers.
      while (true) {
        std::this_thread::sleep_for(std::chrono::hours(24));
      }
    } catch (const std::exception& error) {
      std::cerr << "unable to load Region configuration: " << error.what() << '\n';
      return EXIT_FAILURE;
    }
  }

  std::cout << "Starting StrataKV node " << nodeId << " on port " << port << " using " << configPath << '\n';
  KvServer server(nodeId, maxRaftState, configPath, static_cast<short>(port));
  return EXIT_SUCCESS;
}
