#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "node_server.h"
#include "region_metadata.h"

namespace {

void PrintUsage(const char* program) {
  std::cerr << "Usage: " << program
            << " --node-id <id> --regions-config <regions.conf>"
               " [--max-raft-state <bytes>] [--tso-endpoints <endpoints>]\n";
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
  // Persist rewrites the complete Raft state. Keeping the default at 64 MiB
  // causes severe O(log-size) write amplification before the first snapshot.
  // A 1 MiB cap snapshots at 90% in RegionPeer and keeps long local runs bounded.
  int maxRaftState = 1024 * 1024;
  std::string regionsConfigPath;
  std::string tsoEndpoints;

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
    } else if (option == "--regions-config") {
      regionsConfigPath = value;
    } else if (option == "--max-raft-state") {
      if (!ParseInt(value, &maxRaftState) || maxRaftState == 0) {
        std::cerr << "invalid --max-raft-state: " << value << '\n';
        return EXIT_FAILURE;
      }
    } else if (option == "--tso-endpoints") {
      tsoEndpoints = value;
    } else {
      std::cerr << "unknown option: " << option << '\n';
      PrintUsage(argv[0]);
      return EXIT_FAILURE;
    }
  }

  if (nodeId < 0 || regionsConfigPath.empty()) {
    PrintUsage(argv[0]);
    return EXIT_FAILURE;
  }

  try {
    const RegionCatalog catalog = RegionCatalog::LoadFromConfig(regionsConfigPath);
    NodeServer server(nodeId, maxRaftState, catalog, tsoEndpoints);
    server.Start();
    while (true) {
      std::this_thread::sleep_for(std::chrono::hours(24));
    }
  } catch (const std::exception& error) {
    std::cerr << "unable to start NodeServer: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
