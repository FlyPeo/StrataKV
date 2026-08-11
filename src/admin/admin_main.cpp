#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "kv_server_rpc.pb.h"
#include "mprpc_channel.h"
#include "mprpc_controller.h"
#include "region_metadata.h"

namespace {

constexpr const char* kDefaultEndpoints = "node-0:26100,node-1:26101,node-2:26102";

struct Endpoint {
  std::string host;
  short port;
};

void PrintUsage(const char* program) {
  std::cerr << "Usage: " << program << " <put|get|list|local-list> <key-or-prefix> [value]\n"
            << "Set STRATAKV_ENDPOINTS to a comma-separated host:port peer list, or\n"
            << "STRATAKV_REGION_CONFIG to a Region metadata file for key-range routing.\n"
            << "Raw shared NodeServer endpoints also require STRATAKV_REGION_ID.\n";
}

bool ParseEndpoint(const std::string& text, Endpoint* endpoint) {
  const size_t separator = text.rfind(':');
  if (separator == std::string::npos || separator == 0 || separator == text.size() - 1) {
    return false;
  }
  try {
    size_t consumed = 0;
    const int port = std::stoi(text.substr(separator + 1), &consumed);
    if (consumed != text.size() - separator - 1 || port <= 0 || port > 65535) {
      return false;
    }
    endpoint->host = text.substr(0, separator);
    endpoint->port = static_cast<short>(port);
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

std::vector<Endpoint> LoadEndpoints() {
  const char* configured = std::getenv("STRATAKV_ENDPOINTS");
  const std::string endpoints = configured == nullptr ? kDefaultEndpoints : configured;
  std::vector<Endpoint> result;
  size_t start = 0;
  while (start < endpoints.size()) {
    const size_t end = endpoints.find(',', start);
    const std::string item = endpoints.substr(start, end == std::string::npos ? std::string::npos : end - start);
    Endpoint endpoint;
    if (!ParseEndpoint(item, &endpoint)) {
      throw std::invalid_argument("invalid endpoint: " + item);
    }
    result.push_back(std::move(endpoint));
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
  }
  return result;
}

std::vector<Endpoint> EndpointsForRegion(const RegionMetadata& region) {
  std::vector<Endpoint> result;
  result.reserve(region.peers.size());
  for (const auto& peer : region.peers) {
    result.push_back({peer.host, peer.port});
  }
  return result;
}

std::string NewClientId() {
  const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  return "stratakv-cli-" + std::to_string(getpid()) + "-" + std::to_string(now);
}

int Put(const std::vector<Endpoint>& endpoints, const std::string& key, const std::string& value, int regionId) {
  const std::string clientId = NewClientId();
  for (const Endpoint& endpoint : endpoints) {
    MprpcChannel channel(endpoint.host, endpoint.port, true);
    raftKVRpcProctoc::kvServerRpc_Stub stub(&channel);
    raftKVRpcProctoc::PutAppendArgs request;
    raftKVRpcProctoc::PutAppendReply reply;
    MprpcController controller;
    request.set_key(key);
    request.set_value(value);
    request.set_op("Put");
    request.set_clientid(clientId);
    request.set_requestid(1);
    request.set_regionid(regionId);
    stub.PutAppend(&controller, &request, &reply, nullptr);
    if (!controller.Failed() && reply.err() == "OK") {
      std::cout << "OK endpoint=" << endpoint.host << ':' << endpoint.port;
      if (regionId >= 0) std::cout << " region=" << regionId;
      std::cout << " key=" << key << '\n';
      return EXIT_SUCCESS;
    }
    std::cerr << "retry endpoint=" << endpoint.host << ':' << endpoint.port << " reason="
              << (controller.Failed() ? controller.ErrorText() : reply.err()) << '\n';
  }
  return EXIT_FAILURE;
}

int Get(const std::vector<Endpoint>& endpoints, const std::string& key, int regionId) {
  const std::string clientId = NewClientId();
  for (const Endpoint& endpoint : endpoints) {
    MprpcChannel channel(endpoint.host, endpoint.port, true);
    raftKVRpcProctoc::kvServerRpc_Stub stub(&channel);
    raftKVRpcProctoc::GetArgs request;
    raftKVRpcProctoc::GetReply reply;
    MprpcController controller;
    request.set_key(key);
    request.set_clientid(clientId);
    request.set_requestid(1);
    request.set_regionid(regionId);
    stub.Get(&controller, &request, &reply, nullptr);
    if (!controller.Failed() && reply.err() == "OK") {
      std::cout << reply.value() << '\n';
      return EXIT_SUCCESS;
    }
    if (!controller.Failed() && reply.err() == "ErrNoKey") {
      std::cerr << "NOT_FOUND key=" << key << '\n';
      return 2;
    }
    std::cerr << "retry endpoint=" << endpoint.host << ':' << endpoint.port << " reason="
              << (controller.Failed() ? controller.ErrorText() : reply.err()) << '\n';
  }
  return EXIT_FAILURE;
}

int List(const std::vector<Endpoint>& endpoints, const std::string& prefix, int regionId) {
  for (const Endpoint& endpoint : endpoints) {
    MprpcChannel channel(endpoint.host, endpoint.port, true);
    raftKVRpcProctoc::kvServerRpc_Stub stub(&channel);
    raftKVRpcProctoc::ListArgs request;
    raftKVRpcProctoc::ListReply reply;
    MprpcController controller;
    request.set_prefix(prefix);
    request.set_limit(100);
    request.set_regionid(regionId);
    stub.List(&controller, &request, &reply, nullptr);
    if (!controller.Failed() && reply.err() == "OK") {
      for (const auto& entry : reply.entries()) {
        std::cout << entry.key() << '\t' << entry.value() << '\n';
      }
      return EXIT_SUCCESS;
    }
    std::cerr << "retry endpoint=" << endpoint.host << ':' << endpoint.port << " reason="
              << (controller.Failed() ? controller.ErrorText() : reply.err()) << '\n';
  }
  return EXIT_FAILURE;
}

// This deliberately does not retry another peer: operators use it to inspect
// the exact local state of the endpoint supplied in STRATAKV_ENDPOINTS.
int LocalList(const std::vector<Endpoint>& endpoints, const std::string& prefix) {
  if (endpoints.size() != 1) {
    std::cerr << "local-list requires exactly one endpoint in STRATAKV_ENDPOINTS\n";
    return EXIT_FAILURE;
  }
  const Endpoint& endpoint = endpoints.front();
  MprpcChannel channel(endpoint.host, endpoint.port, true);
  raftKVRpcProctoc::kvServerRpc_Stub stub(&channel);
  raftKVRpcProctoc::ListArgs request;
  raftKVRpcProctoc::ListReply reply;
  MprpcController controller;
  const char* regionIdText = std::getenv("STRATAKV_REGION_ID");
  if (regionIdText == nullptr) {
    std::cerr << "local-list requires STRATAKV_REGION_ID with a shared NodeServer endpoint\n";
    return EXIT_FAILURE;
  }
  request.set_prefix(prefix);
  request.set_limit(1000);
  request.set_allowfollowerread(true);
  request.set_regionid(std::stoi(regionIdText));
  stub.List(&controller, &request, &reply, nullptr);
  if (controller.Failed() || reply.err() != "OK") {
    std::cerr << "local endpoint=" << endpoint.host << ':' << endpoint.port << " reason="
              << (controller.Failed() ? controller.ErrorText() : reply.err()) << '\n';
    return EXIT_FAILURE;
  }
  for (const auto& entry : reply.entries()) {
    std::cout << entry.key() << '\t' << entry.value() << '\n';
  }
  return EXIT_SUCCESS;
}

std::optional<RegionCatalog> LoadRegionCatalog() {
  const char* configPath = std::getenv("STRATAKV_REGION_CONFIG");
  if (configPath == nullptr || std::string(configPath).empty()) return std::nullopt;
  return RegionCatalog::LoadFromConfig(configPath);
}

int ListAllRegions(const RegionCatalog& catalog, const std::string& prefix) {
  int result = EXIT_SUCCESS;
  for (const auto& region : catalog.Regions()) {
    const int status = List(EndpointsForRegion(region), prefix, region.regionId);
    if (status != EXIT_SUCCESS) result = status;
  }
  return result;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc == 2 && std::string(argv[1]) == "--help") {
    PrintUsage(argv[0]);
    return EXIT_SUCCESS;
  }
  if (argc < 2 || argc > 4) {
    PrintUsage(argv[0]);
    return EXIT_FAILURE;
  }

  try {
    const std::string operation = argv[1];
    if (operation == "local-list" && (argc == 2 || argc == 3)) {
      return LocalList(LoadEndpoints(), argc == 3 ? argv[2] : "");
    }
    const std::optional<RegionCatalog> catalog = LoadRegionCatalog();
    const std::vector<Endpoint> endpoints = catalog ? std::vector<Endpoint>() : LoadEndpoints();
    if (operation == "put" && argc == 4) {
      if (catalog) {
        const auto& region = catalog->FindByKey(argv[2]);
        return Put(EndpointsForRegion(region), argv[2], argv[3], region.regionId);
      }
        const char* regionId = std::getenv("STRATAKV_REGION_ID");
        return Put(endpoints, argv[2], argv[3], regionId == nullptr ? -1 : std::stoi(regionId));
    }
    if (operation == "get" && argc == 3) {
      if (catalog) {
        const auto& region = catalog->FindByKey(argv[2]);
        return Get(EndpointsForRegion(region), argv[2], region.regionId);
      }
      const char* regionId = std::getenv("STRATAKV_REGION_ID");
      return Get(endpoints, argv[2], regionId == nullptr ? -1 : std::stoi(regionId));
    }
    if (operation == "list" && (argc == 2 || argc == 3)) {
      if (catalog) return ListAllRegions(*catalog, argc == 3 ? argv[2] : "");
      const char* regionId = std::getenv("STRATAKV_REGION_ID");
      return List(endpoints, argc == 3 ? argv[2] : "", regionId == nullptr ? -1 : std::stoi(regionId));
    }
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  PrintUsage(argv[0]);
  return EXIT_FAILURE;
}
