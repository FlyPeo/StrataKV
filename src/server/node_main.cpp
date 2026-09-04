#include <algorithm>
#include <chrono>
#include <cstdint>
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
               " [--raft-log-gc-threshold <count>]"
               " [--raft-log-gc-count-limit <count>]"
               " [--raft-log-gc-size-limit <bytes>]"
               " [--raft-log-gc-tick-interval-ms <milliseconds>]"
               " [--tso-endpoints <endpoints>]\n";
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

bool ParseUint64(const std::string& text, uint64_t* value) {
  if (text.empty() ||
      std::any_of(text.begin(), text.end(), [](char ch) { return ch < '0' || ch > '9'; })) {
    return false;
  }
  try {
    size_t consumed = 0;
    const unsigned long long parsed = std::stoull(text, &consumed);
    if (consumed != text.size()) return false;
    *value = static_cast<uint64_t>(parsed);
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
  RaftLogGcConfig raftLogGcConfig;
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
    } else if (option == "--raft-log-gc-threshold") {
      if (!ParseUint64(value, &raftLogGcConfig.threshold) || raftLogGcConfig.threshold == 0) {
        std::cerr << "invalid --raft-log-gc-threshold: " << value << '\n';
        return EXIT_FAILURE;
      }
    } else if (option == "--raft-log-gc-count-limit") {
      if (!ParseUint64(value, &raftLogGcConfig.countLimit) || raftLogGcConfig.countLimit == 0) {
        std::cerr << "invalid --raft-log-gc-count-limit: " << value << '\n';
        return EXIT_FAILURE;
      }
    } else if (option == "--raft-log-gc-size-limit" || option == "--max-raft-state") {
      if (!ParseUint64(value, &raftLogGcConfig.sizeLimitBytes) ||
          raftLogGcConfig.sizeLimitBytes == 0) {
        std::cerr << "invalid " << option << ": " << value << '\n';
        return EXIT_FAILURE;
      }
    } else if (option == "--raft-log-gc-tick-interval-ms") {
      uint64_t intervalMs = 0;
      if (!ParseUint64(value, &intervalMs) || intervalMs == 0 ||
          intervalMs > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        std::cerr << "invalid --raft-log-gc-tick-interval-ms: " << value << '\n';
        return EXIT_FAILURE;
      }
      raftLogGcConfig.tickInterval = std::chrono::milliseconds(intervalMs);
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
    NodeServer server(nodeId, raftLogGcConfig, catalog, tsoEndpoints);
    server.Start();
    while (true) {
      std::this_thread::sleep_for(std::chrono::hours(24));
    }
  } catch (const std::exception& error) {
    std::cerr << "unable to start NodeServer: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
