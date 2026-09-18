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

#include "metadata_client.h"
#include "node_server.h"
#include "region_metadata.h"
#include "topology_config.h"

namespace {

void PrintUsage(const char* program) {
  std::cerr << "Usage: " << program
            << " --node-id <id> --regions-config <regions.conf>"
               " [--raft-log-gc-threshold <count>]"
               " [--raft-log-gc-count-limit <count>]"
               " [--raft-log-gc-size-limit <bytes>]"
               " [--raft-log-gc-tick-interval-ms <milliseconds>]"
               " [--tso-endpoints <endpoints>]"
               " [--topology-mode static|dynamic]"
               " [--metadata-endpoints <host:port,...>]"
               " [--metadata-timeout-ms <milliseconds>]\n";
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
  TopologyConfig topology;

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
    } else if (option == "--topology-mode") {
      try {
        topology.mode = ParseTopologyMode(value);
      } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return EXIT_FAILURE;
      }
    } else if (option == "--metadata-endpoints") {
      topology.metadataEndpoints = value;
    } else if (option == "--metadata-timeout-ms") {
      uint64_t timeoutMs = 0;
      if (!ParseUint64(value, &timeoutMs) || timeoutMs == 0 ||
          timeoutMs > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        std::cerr << "invalid --metadata-timeout-ms: " << value << '\n';
        return EXIT_FAILURE;
      }
      topology.metadataTimeout = std::chrono::milliseconds(timeoutMs);
    } else {
      std::cerr << "unknown option: " << option << '\n';
      PrintUsage(argv[0]);
      return EXIT_FAILURE;
    }
  }

  topology.regionConfigPath = regionsConfigPath;
  if (nodeId < 0) {
    PrintUsage(argv[0]);
    return EXIT_FAILURE;
  }

  try {
    topology.Validate(true);
    const RegionCatalog catalog = RegionCatalog::LoadFromConfig(regionsConfigPath);
    if (topology.mode == TopologyMode::Dynamic) {
      // Dynamic bootstrap loads one complete metadata revision before any
      // Region peer exists, so the node never serves a guessed topology.
      MetadataClient metadata(topology.metadataEndpoints);
      uint64_t revision = 0;
      const auto deadline =
          std::chrono::steady_clock::now() + topology.metadataTimeout;
      NodeDynamicBootstrap bootstrap;
      bootstrap.regions = metadata.Scan("", 4096, deadline, &revision);
      bootstrap.revision = revision;
      if (bootstrap.regions.empty() || revision == 0) {
        std::cerr << "unable to start NodeServer: metadata returned no Region view\n";
        return EXIT_FAILURE;
      }
      std::cout << "[node bootstrap] metadata_revision=" << revision
                << " regions=" << bootstrap.regions.size() << std::endl;
      NodeServer server(nodeId, raftLogGcConfig, catalog, tsoEndpoints, bootstrap);
      server.Start();
      while (true) {
        std::this_thread::sleep_for(std::chrono::hours(24));
      }
    }
    NodeServer server(nodeId, raftLogGcConfig, catalog, tsoEndpoints, false);
    server.Start();
    while (true) {
      std::this_thread::sleep_for(std::chrono::hours(24));
    }
  } catch (const std::exception& error) {
    std::cerr << "unable to start NodeServer: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
