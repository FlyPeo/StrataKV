#include <cstdlib>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "rpc_provider.h"
#include "tso_consensus.h"
#include "tso_service.h"

namespace {

void PrintUsage(const char* program) {
  std::cerr << "Usage: " << program
            << " --node-id <index> --peers <host:port,...> --state-file <path>"
               " [--segment-size 4096]\n";
}

bool ParseUnsigned(const std::string& value, uint64_t* parsed) {
  try {
    size_t consumed = 0;
    *parsed = std::stoull(value, &consumed);
    return consumed == value.size();
  } catch (const std::exception&) {
    return false;
  }
}

std::vector<TsoConsensusNode::Endpoint> ParsePeers(const std::string& value) {
  std::vector<TsoConsensusNode::Endpoint> peers;
  size_t begin = 0;
  while (begin <= value.size()) {
    const size_t end = value.find(',', begin);
    const std::string endpoint = value.substr(begin, end == std::string::npos ? end : end - begin);
    const size_t colon = endpoint.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 == endpoint.size()) {
      throw std::invalid_argument("invalid TSO peer endpoint: " + endpoint);
    }
    uint64_t port = 0;
    if (!ParseUnsigned(endpoint.substr(colon + 1), &port) || port == 0 || port > 65535) {
      throw std::invalid_argument("invalid TSO peer port: " + endpoint);
    }
    peers.emplace_back(endpoint.substr(0, colon), static_cast<short>(port));
    if (end == std::string::npos) break;
    begin = end + 1;
  }
  return peers;
}

}  // namespace

int main(int argc, char** argv) {
  uint64_t nodeId = std::numeric_limits<uint64_t>::max();
  uint64_t segmentSize = 4096;
  std::string peersText;
  std::string stateFile;

  for (int index = 1; index < argc; index += 2) {
    if (index + 1 >= argc) {
      PrintUsage(argv[0]);
      return EXIT_FAILURE;
    }
    const std::string option = argv[index];
    const std::string value = argv[index + 1];
    if (option == "--node-id") {
      if (!ParseUnsigned(value, &nodeId)) nodeId = std::numeric_limits<uint64_t>::max();
    } else if (option == "--peers") {
      peersText = value;
    } else if (option == "--state-file") {
      stateFile = value;
    } else if (option == "--segment-size") {
      if (!ParseUnsigned(value, &segmentSize)) segmentSize = 0;
    } else {
      PrintUsage(argv[0]);
      return EXIT_FAILURE;
    }
  }

  if (peersText.empty() || stateFile.empty() || segmentSize == 0) {
    PrintUsage(argv[0]);
    return EXIT_FAILURE;
  }

  try {
    const auto peers = ParsePeers(peersText);
    if (nodeId >= peers.size()) throw std::invalid_argument("invalid TSO node id");

    auto oracle = std::make_shared<TsoConsensusNode>(static_cast<int>(nodeId), peers, stateFile, segmentSize);
    TsoService service(oracle);
    RpcProvider provider;
    provider.NotifyService(&service);
    provider.NotifyService(oracle->RaftService());
    oracle->Start();
    provider.Run(static_cast<int>(nodeId), peers[static_cast<size_t>(nodeId)].second);
  } catch (const std::exception& error) {
    std::cerr << "unable to start stratakv-tso: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
