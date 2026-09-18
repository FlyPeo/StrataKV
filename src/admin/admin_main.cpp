#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <thread>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "auto_balancer_planner.h"
#include "metadata_client.h"
#include "mvcc_storage.h"
#include "kv_server_rpc.pb.h"
#include "mprpc_channel.h"
#include "mprpc_controller.h"
#include "region_metadata.h"
#include "topology_config.h"

namespace {

constexpr const char* kDefaultEndpoints = "node-0:26100,node-1:26101,node-2:26102";

struct Endpoint {
  std::string host;
  short port;
};

void PrintUsage(const char* program) {
  std::cerr << "Usage: " << program
            << " <put|get|list|local-list|split-region|move-peer|"
            << "balancer-status|balancer-enable|balancer-disable|"
            << "balancer-pause|balancer-resume|balancer-cancel|balancer-dry-run> [arguments]\n"
            << "  split-region requires: split-region <region_id> <split_key>\n"
            << "  move-peer requires: move-peer <region_id> <from_store_id> <to_store_id>\n"
            << "  balancer-cancel requires: balancer-cancel <operator_id>\n"
            << "Set STRATAKV_ENDPOINTS to a comma-separated host:port peer list, or\n"
            << "STRATAKV_REGION_CONFIG to a Region metadata file for key-range routing.\n"
            << "Raw shared NodeServer endpoints also require STRATAKV_REGION_ID.\n"
            << "Topology mode: STRATAKV_TOPOLOGY_MODE=static|dynamic,\n"
            << "STRATAKV_METADATA_ENDPOINTS=host:port,... and\n"
            << "STRATAKV_METADATA_TIMEOUT_MS=<milliseconds> for dynamic mode.\n";
}

// Parses and validates the deployment topology selection shared by every
// StrataKV entry point. An incomplete dynamic configuration must fail with an
// actionable message instead of silently falling back to static routing.
TopologyConfig LoadTopologyConfig() {
  TopologyConfig topology;
  if (const char* mode = std::getenv("STRATAKV_TOPOLOGY_MODE")) {
    topology.mode = ParseTopologyMode(mode);
  }
  if (const char* endpoints = std::getenv("STRATAKV_METADATA_ENDPOINTS")) {
    topology.metadataEndpoints = endpoints;
  }
  if (const char* timeoutMs = std::getenv("STRATAKV_METADATA_TIMEOUT_MS")) {
    topology.metadataTimeout =
        std::chrono::milliseconds(std::stoull(timeoutMs));
  }
  topology.regionConfigPath = std::getenv("STRATAKV_REGION_CONFIG") != nullptr
                                  ? std::string(std::getenv("STRATAKV_REGION_CONFIG"))
                                  : std::string();
  if (topology.mode == TopologyMode::Static && topology.regionConfigPath.empty()) {
    // Legacy raw-endpoint mode without a Region catalog. Dynamic-only options
    // must still be rejected instead of being silently ignored.
    if (!topology.metadataEndpoints.empty()) {
      throw std::invalid_argument("static topology mode does not accept metadata endpoints");
    }
    if (topology.metadataTimeout.count() <= 0) {
      throw std::invalid_argument("metadata timeout must be positive");
    }
    return topology;
  }
  topology.Validate(false);
  return topology;
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

// --- Manual Region split (dynamic mode only) ---

std::unique_ptr<metadataRpcProtocol::metadataRpc_Stub> MetadataStub(const std::string& host,
                                                                    short port) {
  auto channel = std::make_unique<MprpcChannel>(host, port, false);
  auto stub = std::make_unique<metadataRpcProtocol::metadataRpc_Stub>(channel.get());
  // The channel must outlive the stub; keep both alive via a leak-isolated pair.
  return stub;
}

// Mutate with one retry on revision mismatch: the caller re-reads the current
// revision from the result and re-submits with a fresh mutation id.
metadataRpcProtocol::MetadataCommandResult SplitMutate(
    MetadataClient& metadata, const metadataRpcProtocol::MetadataCommand& command,
    std::chrono::steady_clock::time_point deadline) {
  return metadata.Mutate(command, deadline);
}

int SplitRegion(const std::string& regionIdText, const std::string& splitKey) {
  const int regionId = std::stoi(regionIdText);
  const char* endpoints = std::getenv("STRATAKV_METADATA_ENDPOINTS");
  if (endpoints == nullptr || std::string(endpoints).empty()) {
    std::cerr << "split-region requires dynamic topology mode "
                 "(STRATAKV_METADATA_ENDPOINTS)\n";
    return EXIT_FAILURE;
  }
  MetadataClient metadata(endpoints);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);

  uint64_t revision = 0;
  const RegionMetadata region =
      metadata.LookupRegion(static_cast<uint64_t>(regionId), deadline, &revision);
  if (region.splitPending) {
    std::cerr << "Region " << regionId << " already has a split in progress\n";
    return EXIT_FAILURE;
  }

  // Phase 1: prepare (allocates child identifiers, marks the parent).
  metadataRpcProtocol::MetadataCommand prepare;
  prepare.set_mutationid("split-prepare:" + std::to_string(regionId) + ":" + splitKey + ":" +
                         std::to_string(revision));
  prepare.set_expectedrevision(revision);
  prepare.mutable_preparesplit()->set_regionid(static_cast<uint64_t>(regionId));
  prepare.mutable_preparesplit()->set_splitkey(splitKey);
  prepare.mutable_preparesplit()->mutable_expectedepoch()->set_version(region.epoch.version);
  prepare.mutable_preparesplit()->mutable_expectedepoch()->set_confversion(
      region.epoch.confVersion);
  auto prepared = metadata.Mutate(prepare, deadline);
  if (prepared.error() != metadataRpcProtocol::METADATA_OK) {
    std::cerr << "split prepare rejected: " << prepared.message() << '\n';
    return EXIT_FAILURE;
  }
  std::cout << "split prepared at revision " << prepared.revision() << "\n";

  // Phase 2: commit (publishes the shrunken parent and the child).
  metadataRpcProtocol::MetadataCommand commit;
  commit.set_mutationid("split-commit:" + std::to_string(regionId) + ":" + splitKey + ":" +
                        std::to_string(revision));
  commit.set_expectedrevision(prepared.revision());
  commit.mutable_commitsplit()->set_regionid(static_cast<uint64_t>(regionId));
  auto committed = metadata.Mutate(commit, deadline);
  if (committed.error() != metadataRpcProtocol::METADATA_OK) {
    std::cerr << "split commit rejected: " << committed.message() << '\n';
    return EXIT_FAILURE;
  }
  std::cout << "split committed at revision " << committed.revision() << "; child Regions:";
  for (const auto& region : committed.regions()) {
    std::cout << " " << region.regionid();
  }
  std::cout << "\n";

  // Trigger: propose AdminSplit through the parent's Raft (rotate peers until
  // the leader accepts).
  RegionMetadata parentAfter;
  for (const auto& region : committed.regions()) {
    if (static_cast<int>(region.regionid()) == regionId) parentAfter = FromProtoRegion(region);
  }
  if (parentAfter.regionId != regionId) {
    std::cerr << "split result did not include the parent descriptor\n";
    return EXIT_FAILURE;
  }

  // Re-fetch the prepared marker (carries the child descriptor) through the
  // parent descriptor in the commit result? The commit cleared the marker, so
  // use the prepare result's child descriptor for the AdminSplit payload.
  stratakv::region::RegionDescriptor childDescriptor;
  const RegionMetadata childFromPrepare = [&]() {
    // Re-derive from metadata scan: the child is now published.
    const auto scan = metadata.Scan(splitKey, 8, deadline, nullptr);
    for (const auto& candidate : scan) {
      if (candidate.startKey == splitKey) return candidate;
    }
    throw std::runtime_error("child Region not found in metadata after commit");
  }();
  childDescriptor = ToProtoRegion(childFromPrepare);

  stratakv::region::AdminSplitCommand split;
  split.set_splitkey(splitKey);
  split.set_metadatarevision(committed.revision());
  // Replicas validate against their current (unshrunk) descriptor.
  *split.mutable_parent() = ToProtoRegion(region);
  *split.mutable_child() = childDescriptor;

  raftKVRpcProctoc::ProposeAdminSplitArgs proposeArgs;
  proposeArgs.set_regionid(regionId);
  *proposeArgs.mutable_split() = split;
  bool proposed = false;
  for (const auto& peer : parentAfter.peers) {
    MprpcChannel channel(peer.host, peer.port, false);
    raftKVRpcProctoc::kvServerRpc_Stub stub(&channel);
    raftKVRpcProctoc::ProposeAdminSplitArgs proposeArgs;
    proposeArgs.set_regionid(regionId);
    auto* header = proposeArgs.mutable_header();
    header->set_regionid(static_cast<uint64_t>(regionId));
    header->set_peerid(peer.peerId);
    header->mutable_epoch()->set_version(region.epoch.version);
    header->mutable_epoch()->set_confversion(region.epoch.confVersion);
    header->set_clientid("admin");
    header->set_requestid(1);
    *proposeArgs.mutable_split() = split;
    raftKVRpcProctoc::ProposeAdminSplitReply reply;
    MprpcController controller;
    stub.ProposeAdminSplit(&controller, &proposeArgs, &reply, nullptr);
    if (controller.Failed()) {
      std::cerr << "propose to " << peer.host << ':' << peer.port
                << " failed: " << controller.ErrorText() << '\n';
      continue;
    }
    if (reply.err() == std::to_string(static_cast<int>(TxnStatus::Ok))) {
      std::cout << "AdminSplit proposed via " << peer.host << ':' << peer.port << '\n';
      proposed = true;
      break;
    }
  }
  if (!proposed) {
    std::cerr << "no parent peer accepted the AdminSplit proposal\n";
    return EXIT_FAILURE;
  }

  // Progress: poll every replica until all report the complete phase.
  for (int attempt = 0; attempt < 120; ++attempt) {
    bool allComplete = true;
    for (const auto& peer : parentAfter.peers) {
      MprpcChannel channel(peer.host, peer.port, false);
      raftKVRpcProctoc::kvServerRpc_Stub stub(&channel);
      raftKVRpcProctoc::RegionSplitStatusArgs statusArgs;
      statusArgs.set_regionid(regionId);
      raftKVRpcProctoc::RegionSplitStatusReply statusReply;
      MprpcController controller;
      stub.RegionSplitStatus(&controller, &statusArgs, &statusReply, nullptr);
      if (controller.Failed() || statusReply.phase() < 4) allComplete = false;
    }
    if (allComplete) {
      std::cout << "split complete on all replicas\n";
      return EXIT_SUCCESS;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  std::cerr << "split did not complete on all replicas within the wait window\n";
  return EXIT_FAILURE;
}

void FillRegionHeader(stratakv::region::RegionRequestHeader* header,
                      const RegionMetadata& region, const RegionPeerLocation& peer,
                      uint64_t requestId) {
  header->set_regionid(static_cast<uint64_t>(region.regionId));
  header->set_peerid(peer.peerId);
  header->mutable_epoch()->set_version(region.epoch.version);
  header->mutable_epoch()->set_confversion(region.epoch.confVersion);
  header->set_clientid("stratakv-admin-move-peer");
  header->set_requestid(requestId);
}

bool ProposeMembershipChange(const RegionMetadata& region,
                             stratakv::region::ConfChangeType type,
                             const RegionPeerLocation& target,
                             uint64_t metadataRevision) {
  for (const auto& endpoint : region.peers) {
    MprpcChannel channel(endpoint.host, endpoint.port, false);
    raftKVRpcProctoc::kvServerRpc_Stub stub(&channel);
    raftKVRpcProctoc::ProposeConfChangeArgs request;
    request.set_regionid(region.regionId);
    FillRegionHeader(request.mutable_header(), region, endpoint,
                     static_cast<uint64_t>(type) + 1);
    auto* command = request.mutable_confchange();
    command->set_changetype(type);
    auto* peer = command->mutable_peer();
    peer->set_peerid(target.peerId);
    peer->set_storeid(target.storeId);
    peer->set_host(target.host);
    peer->set_port(target.port);
    peer->set_islearner(type == stratakv::region::CONF_CHANGE_ADD_LEARNER);
    command->mutable_expectedepoch()->set_version(region.epoch.version);
    command->mutable_expectedepoch()->set_confversion(region.epoch.confVersion);
    command->set_metadatarevision(metadataRevision);
    raftKVRpcProctoc::ProposeConfChangeReply reply;
    MprpcController controller;
    stub.ProposeConfChange(&controller, &request, &reply, nullptr);
    if (!controller.Failed() &&
        reply.err() == std::to_string(static_cast<int>(TxnStatus::Ok))) {
      return true;
    }
  }
  return false;
}

std::optional<RegionMetadata> PollMigrationStatus(
    const std::vector<RegionPeerLocation>& endpoints, int regionId, uint64_t targetPeerId,
    std::chrono::steady_clock::time_point deadline,
    const std::function<bool(const RegionMetadata&, const stratakv::region::ReplicaMigrationStatus&)>&
        ready) {
  while (std::chrono::steady_clock::now() < deadline) {
    for (const auto& endpoint : endpoints) {
      MprpcChannel channel(endpoint.host, endpoint.port, false);
      raftKVRpcProctoc::kvServerRpc_Stub stub(&channel);
      raftKVRpcProctoc::RegionMigrationStatusArgs request;
      request.set_regionid(regionId);
      request.set_targetpeerid(targetPeerId);
      raftKVRpcProctoc::RegionMigrationStatusReply reply;
      MprpcController controller;
      stub.RegionMigrationStatus(&controller, &request, &reply, nullptr);
      if (controller.Failed() ||
          reply.err() != std::to_string(static_cast<int>(TxnStatus::Ok)) ||
          !reply.has_region()) {
        continue;
      }
      RegionMetadata current = FromProtoRegion(reply.region());
      std::cout << "phase="
                << stratakv::region::ReplicaMigrationPhase_Name(reply.status().phase())
                << " region_id=" << regionId << " peer_id=" << targetPeerId
                << " store_id=" << reply.status().targetpeer().storeid()
                << " epoch=" << current.epoch.version << '.' << current.epoch.confVersion
                << " log_lag=" << reply.status().loglag()
                << " transferred_bytes=" << reply.status().transferredbytes() << '\n';
      if (ready(current, reply.status())) return current;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  return std::nullopt;
}

int MovePeer(const std::string& regionIdText, const std::string& fromStoreText,
             const std::string& toStoreText) {
  const uint64_t regionId = std::stoull(regionIdText);
  const uint64_t fromStoreId = std::stoull(fromStoreText);
  const uint64_t toStoreId = std::stoull(toStoreText);
  const char* endpoints = std::getenv("STRATAKV_METADATA_ENDPOINTS");
  if (endpoints == nullptr || std::string(endpoints).empty()) {
    throw std::invalid_argument("move-peer requires STRATAKV_METADATA_ENDPOINTS");
  }
  uint64_t timeoutMs = 120000;
  if (const char* configured = std::getenv("STRATAKV_MIGRATION_TIMEOUT_MS")) {
    timeoutMs = std::stoull(configured);
  }
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeoutMs);
  MetadataClient metadata(endpoints);
  uint64_t revision = 0;
  const RegionMetadata original = metadata.LookupRegion(regionId, deadline, &revision);
  const auto source = std::find_if(original.peers.begin(), original.peers.end(),
                                   [&](const RegionPeerLocation& peer) {
                                     return peer.storeId == fromStoreId;
                                   });
  if (source == original.peers.end()) {
    throw std::invalid_argument("source Store does not host the Region");
  }
  if (std::any_of(original.peers.begin(), original.peers.end(),
                  [&](const RegionPeerLocation& peer) { return peer.storeId == toStoreId; })) {
    throw std::invalid_argument("target Store already hosts the Region");
  }

  metadataRpcProtocol::MetadataCommand prepare;
  prepare.set_mutationid("move-peer:prepare:" + std::to_string(regionId) + ":" +
                         std::to_string(fromStoreId) + ":" + std::to_string(toStoreId) + ":" +
                         std::to_string(original.epoch.confVersion));
  prepare.set_expectedrevision(revision);
  auto* move = prepare.mutable_preparemovepeer();
  move->set_regionid(regionId);
  move->set_fromstoreid(fromStoreId);
  move->set_tostoreid(toStoreId);
  move->mutable_expectedepoch()->set_version(original.epoch.version);
  move->mutable_expectedepoch()->set_confversion(original.epoch.confVersion);
  const auto prepared = metadata.Mutate(prepare, deadline);
  if (prepared.error() != metadataRpcProtocol::METADATA_OK ||
      !prepared.has_migration() || prepared.regions_size() != 1) {
    std::cerr << "move-peer preflight rejected: " << prepared.message() << '\n';
    return EXIT_FAILURE;
  }
  RegionMetadata mountedDescriptor = FromProtoRegion(prepared.regions(0));
  const auto& targetProto = prepared.migration().targetpeer();
  const RegionPeerLocation target{static_cast<int>(targetProto.storeid() - 1),
                                  targetProto.host(), static_cast<short>(targetProto.port()),
                                  targetProto.storeid(), targetProto.peerid(), true};
  std::cout << "phase=PreparingTarget region_id=" << regionId
            << " peer_id=" << target.peerId << " store_id=" << target.storeId << '\n';

  auto cancelPrepared = [&](bool removeLearner) {
    if (removeLearner) {
      (void)ProposeMembershipChange(mountedDescriptor,
                                    stratakv::region::CONF_CHANGE_REMOVE_PEER,
                                    target, prepared.revision());
    }
    metadataRpcProtocol::MetadataCommand cancel;
    cancel.set_mutationid("move-peer:cancel:" + std::to_string(regionId) + ":" +
                          std::to_string(target.peerId));
    cancel.set_expectedrevision(prepared.revision());
    cancel.mutable_cancelmovepeer()->set_regionid(regionId);
    cancel.mutable_cancelmovepeer()->set_newpeerid(target.peerId);
    const auto cancelled = metadata.Mutate(cancel, deadline);

    MprpcChannel channel(target.host, target.port, false);
    raftKVRpcProctoc::kvServerRpc_Stub stub(&channel);
    raftKVRpcProctoc::RetireMigrationSourceArgs request;
    request.set_regionid(static_cast<int>(regionId));
    request.set_peerid(target.peerId);
    request.set_metadatarevision(cancelled.error() == metadataRpcProtocol::METADATA_OK
                                     ? cancelled.revision()
                                     : prepared.revision());
    request.set_timeoutms(5000);
    raftKVRpcProctoc::RetireMigrationSourceReply reply;
    MprpcController controller;
    stub.RetireMigrationSource(&controller, &request, &reply, nullptr);
  };

  {
    MprpcChannel channel(target.host, target.port, false);
    raftKVRpcProctoc::kvServerRpc_Stub stub(&channel);
    raftKVRpcProctoc::PrepareMigrationTargetArgs request;
    *request.mutable_region() = ToProtoRegion(mountedDescriptor);
    request.set_targetpeerid(target.peerId);
    raftKVRpcProctoc::PrepareMigrationTargetReply reply;
    MprpcController controller;
    stub.PrepareMigrationTarget(&controller, &request, &reply, nullptr);
    if (controller.Failed() ||
        reply.err() != std::to_string(static_cast<int>(TxnStatus::Ok))) {
      std::cerr << "target preparation failed: "
                << (controller.Failed() ? controller.ErrorText() : reply.err()) << '\n';
      cancelPrepared(false);
      return EXIT_FAILURE;
    }
  }

  RegionMetadata addEpoch = original;
  if (!ProposeMembershipChange(addEpoch, stratakv::region::CONF_CHANGE_ADD_LEARNER,
                               target, prepared.revision())) {
    std::cerr << "no Region leader accepted AddLearner\n";
    cancelPrepared(false);
    return EXIT_FAILURE;
  }
  std::vector<RegionPeerLocation> statusEndpoints = original.peers;
  statusEndpoints.push_back(target);
  auto caughtUp = PollMigrationStatus(
      statusEndpoints, static_cast<int>(regionId), target.peerId, deadline,
      [](const RegionMetadata& current, const auto& status) {
        const auto* peer = current.FindPeer(status.targetpeer().peerid());
        return peer != nullptr && peer->isLearner && status.loglag() <= 100;
      });
  if (!caughtUp) {
    std::cerr << "learner catch-up timed out\n";
    cancelPrepared(true);
    return EXIT_FAILURE;
  }

  if (!ProposeMembershipChange(*caughtUp,
                               stratakv::region::CONF_CHANGE_PROMOTE_LEARNER,
                               target, prepared.revision())) {
    std::cerr << "no Region leader accepted PromoteLearner\n";
    return EXIT_FAILURE;
  }
  auto promoted = PollMigrationStatus(
      statusEndpoints, static_cast<int>(regionId), target.peerId, deadline,
      [](const RegionMetadata& current, const auto& status) {
        const auto* peer = current.FindPeer(status.targetpeer().peerid());
        return peer != nullptr && !peer->isLearner;
      });
  if (!promoted) {
    std::cerr << "learner promotion timed out\n";
    return EXIT_FAILURE;
  }

  if (!ProposeMembershipChange(*promoted, stratakv::region::CONF_CHANGE_REMOVE_PEER,
                               *source, prepared.revision())) {
    std::cerr << "no Region leader accepted RemovePeer\n";
    return EXIT_FAILURE;
  }
  auto removed = PollMigrationStatus(
      statusEndpoints, static_cast<int>(regionId), target.peerId, deadline,
      [sourcePeerId = source->peerId](const RegionMetadata& current, const auto&) {
        return current.FindPeer(sourcePeerId) == nullptr;
      });
  if (!removed) {
    std::cerr << "source retirement timed out\n";
    return EXIT_FAILURE;
  }

  metadataRpcProtocol::MetadataCommand commit;
  commit.set_mutationid("move-peer:commit:" + std::to_string(regionId) + ":" +
                        std::to_string(target.peerId));
  commit.set_expectedrevision(prepared.revision());
  commit.mutable_commitmovepeer()->set_regionid(regionId);
  commit.mutable_commitmovepeer()->set_newpeerid(target.peerId);
  commit.mutable_commitmovepeer()->mutable_expectedepoch()->set_version(original.epoch.version);
  commit.mutable_commitmovepeer()->mutable_expectedepoch()->set_confversion(
      original.epoch.confVersion);
  const auto committed = metadata.Mutate(commit, deadline);
  if (committed.error() != metadataRpcProtocol::METADATA_OK) {
    std::cerr << "metadata commit failed: " << committed.message() << '\n';
    return EXIT_FAILURE;
  }
  std::cout << "phase=Complete region_id=" << regionId << " peer_id=" << target.peerId
            << " store_id=" << target.storeId << " revision=" << committed.revision() << '\n';
  return EXIT_SUCCESS;
}

// --- Auto-Balancer Administration ---

int BalancerStatus(const std::string& endpoints) {
  MetadataClient metadata(endpoints);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  const auto reply = metadata.BalancerStatus(deadline);
  const auto nowMs = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());

  std::cout << "--- Auto-Balancer Status ---\n";
  std::cout << "revision: " << reply.revision() << " leader_id: " << reply.leaderid() << '\n';
  const auto& cfg = reply.config();
  std::cout << "config:\n"
            << "  enabled: " << (cfg.enabled() ? "true" : "false") << '\n'
            << "  paused: " << (cfg.paused() ? "true" : "false") << '\n'
            << "  version: " << cfg.version() << '\n'
            << "  evaluation_interval_ms: " << cfg.evaluationintervalms() << '\n'
            << "  heartbeat_timeout_ms: " << cfg.heartbeattimeoutms() << '\n'
            << "  minimum_free_bytes: " << cfg.minimumfreebytes() << '\n'
            << "  maximum_disk_used_ratio: " << cfg.maximumdiskusedratio() << '\n'
            << "  replication_factor: " << cfg.replicationfactor() << '\n'
            << "  imbalance_threshold: " << cfg.imbalancethreshold() << '\n'
            << "  split_size_bytes: " << cfg.splitsizebytes() << '\n'
            << "  split_request_threshold: " << cfg.splitrequestthreshold() << '\n'
            << "  split_consecutive_windows: " << cfg.splitconsecutivewindows() << '\n'
            << "  region_cooldown_ms: " << cfg.regioncooldownms() << '\n'
            << "  maximum_active_operators: " << cfg.maximumactiveoperators() << '\n'
            << "  maximum_active_per_store: " << cfg.maximumactiveperstore() << '\n';

  std::cout << "stores (" << reply.storeheartbeats_size() << "):\n";
  for (const auto& hb : reply.storeheartbeats()) {
    const bool alive = hb.expiresatms() > nowMs;
    const double diskUsedRatio = hb.capacitybytes() == 0 ? 0.0 :
        static_cast<double>(hb.capacitybytes() - hb.availablebytes()) / static_cast<double>(hb.capacitybytes());
    std::cout << "  store_id=" << hb.storeid()
              << " status=" << (alive ? "healthy" : "stale")
              << " seq=" << hb.sequence()
              << " capacity=" << hb.capacitybytes()
              << " available=" << hb.availablebytes()
              << " disk_used_ratio=" << std::fixed << std::setprecision(2) << diskUsedRatio
              << " requests=" << hb.requestcount()
              << " reported_regions=" << hb.regions_size() << '\n';
  }

  std::cout << "active_operators (" << reply.operators_size() << "):\n";
  for (const auto& op : reply.operators()) {
    std::string typeStr = (op.type() == metadataRpcProtocol::SCHEDULING_OPERATOR_MOVE_PEER ? "MOVE_PEER" : "SPLIT_REGION");
    std::string phaseStr;
    switch (op.phase()) {
      case metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING: phaseStr = "PENDING"; break;
      case metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING: phaseStr = "DISPATCHING"; break;
      case metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING: phaseStr = "WAITING"; break;
      case metadataRpcProtocol::SCHEDULING_OPERATOR_SUCCEEDED: phaseStr = "SUCCEEDED"; break;
      case metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLING: phaseStr = "CANCELLING"; break;
      case metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED: phaseStr = "CANCELLED"; break;
      case metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED: phaseStr = "FAILED"; break;
      default: phaseStr = "UNKNOWN"; break;
    }
    std::cout << "  operator_id=" << op.operatorid()
              << " type=" << typeStr
              << " region_id=" << op.regionid()
              << " source_store=" << op.sourcestoreid()
              << " target_store=" << op.targetstoreid()
              << " phase=" << phaseStr
              << " attempts=" << op.attempts()
              << " irreversible=" << (op.irreversible() ? "true" : "false")
              << " deadline_ms=" << op.deadlinems();
    if (!op.lasterror().empty()) {
      std::cout << " last_error=\"" << op.lasterror() << "\"";
    }
    std::cout << '\n';
  }
  return EXIT_SUCCESS;
}

int UpdateBalancerConfig(const std::string& endpoints,
                         std::function<void(metadataRpcProtocol::AutoBalancerConfig&)> updateFn,
                         const std::string& actionDescription) {
  MetadataClient metadata(endpoints);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  const auto current = metadata.BalancerStatus(deadline);

  metadataRpcProtocol::AutoBalancerConfig newConfig = current.config();
  updateFn(newConfig);
  newConfig.set_version(current.config().version() + 1);

  metadataRpcProtocol::MetadataCommand cmd;
  cmd.set_mutationid("balancer-" + actionDescription + "-" +
                     std::to_string(std::chrono::system_clock::now().time_since_epoch().count()));
  cmd.set_expectedrevision(current.revision());
  *cmd.mutable_updateautobalancerconfig()->mutable_config() = newConfig;

  const auto result = metadata.Mutate(cmd, deadline);
  if (result.error() != metadataRpcProtocol::METADATA_OK) {
    std::cerr << "failed to " << actionDescription << " Auto-Balancer: " << result.message() << '\n';
    return EXIT_FAILURE;
  }
  std::cout << "Auto-Balancer " << actionDescription << " succeeded at revision " << result.revision() << '\n';
  return EXIT_SUCCESS;
}

int BalancerEnable(const std::string& endpoints) {
  return UpdateBalancerConfig(endpoints, [](auto& cfg) { cfg.set_enabled(true); }, "enable");
}

int BalancerDisable(const std::string& endpoints) {
  return UpdateBalancerConfig(endpoints, [](auto& cfg) { cfg.set_enabled(false); }, "disable");
}

int BalancerPause(const std::string& endpoints) {
  return UpdateBalancerConfig(endpoints, [](auto& cfg) { cfg.set_paused(true); }, "pause");
}

int BalancerResume(const std::string& endpoints) {
  return UpdateBalancerConfig(endpoints, [](auto& cfg) { cfg.set_paused(false); }, "resume");
}

int BalancerCancel(const std::string& endpoints, const std::string& operatorId) {
  MetadataClient metadata(endpoints);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);

  metadataRpcProtocol::MetadataCommand cmd;
  cmd.set_mutationid("balancer-cancel-" + operatorId + "-" +
                     std::to_string(std::chrono::system_clock::now().time_since_epoch().count()));
  auto* cancel = cmd.mutable_cancelschedulingoperator();
  cancel->set_operatorid(operatorId);
  const auto nowMs = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  cancel->set_updatedatms(nowMs);

  const auto result = metadata.Mutate(cmd, deadline);
  if (result.error() != metadataRpcProtocol::METADATA_OK) {
    std::cerr << "cancellation rejected for operator " << operatorId
              << ": " << result.message() << '\n';
    return EXIT_FAILURE;
  }
  std::cout << "operator " << operatorId << " cancellation accepted at revision "
            << result.revision() << '\n';
  return EXIT_SUCCESS;
}

int BalancerDryRun(const std::string& endpoints) {
  MetadataClient metadata(endpoints);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  const auto status = metadata.BalancerStatus(deadline);
  uint64_t revision = 0;
  const auto regions = metadata.Scan("", 10000, deadline, &revision);

  auto view = std::make_shared<MetadataView>();
  view->revision = status.revision();
  view->balancerConfig = status.config();
  view->catalog = std::make_shared<const RegionCatalog>(regions);
  for (const auto& region : regions) {
    for (const auto& peer : region.peers) {
      stratakv::region::StoreDescriptor sd;
      sd.set_storeid(peer.storeId);
      sd.set_host(peer.host);
      sd.set_port(static_cast<uint32_t>(peer.port));
      view->stores[peer.storeId] = sd;
    }
  }
  for (const auto& hb : status.storeheartbeats()) {
    view->storeHeartbeats[hb.storeid()] = hb;
    if (view->stores.find(hb.storeid()) == view->stores.end()) {
      stratakv::region::StoreDescriptor sd;
      sd.set_storeid(hb.storeid());
      view->stores[hb.storeid()] = sd;
    }
  }
  for (const auto& op : status.operators()) {
    view->schedulingOperators[op.operatorid()] = op;
    if (op.phase() != metadataRpcProtocol::SCHEDULING_OPERATOR_SUCCEEDED &&
        op.phase() != metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED &&
        op.phase() != metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED) {
      view->activeOperatorByRegion[op.regionid()] = op.operatorid();
    }
  }

  ClusterSchedulingSnapshot snapshot;
  snapshot.metadata = view;
  snapshot.nowMs = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());

  AutoBalancerPlanner planner;
  const auto planResult = planner.Plan(snapshot);

  std::cout << "--- Auto-Balancer Dry-Run (revision " << view->revision << ") ---\n";
  std::cout << "ranked_plans (" << planResult.plans.size() << "):\n";
  for (size_t i = 0; i < planResult.plans.size(); ++i) {
    const auto& plan = planResult.plans[i];
    std::string reasonStr;
    switch (plan.reason) {
      case SchedulingPlanReason::SafetyRepair: reasonStr = "SafetyRepair"; break;
      case SchedulingPlanReason::AutomaticSplit: reasonStr = "AutomaticSplit"; break;
      case SchedulingPlanReason::Balance: reasonStr = "Balance"; break;
    }
    const auto& op = plan.operation;
    std::string typeStr = (op.type() == metadataRpcProtocol::SCHEDULING_OPERATOR_MOVE_PEER ? "MOVE_PEER" : "SPLIT_REGION");
    std::cout << "  #" << (i + 1)
              << " reason=" << reasonStr
              << " type=" << typeStr
              << " region_id=" << op.regionid();
    if (op.type() == metadataRpcProtocol::SCHEDULING_OPERATOR_MOVE_PEER) {
      std::cout << " source_store=" << op.sourcestoreid()
                << " target_store=" << op.targetstoreid()
                << " score_benefit=" << std::fixed << std::setprecision(4) << plan.scoreBenefit;
    }
    std::cout << " explanation=\"" << plan.explanation << "\"\n";
  }

  std::cout << "rejections (" << planResult.rejections.size() << "):\n";
  for (const auto& rej : planResult.rejections) {
    std::cout << "  region_id=" << rej.regionId << " reason=\"" << rej.reason << "\"\n";
  }
  return EXIT_SUCCESS;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc == 2 && std::string(argv[1]) == "--help") {
    PrintUsage(argv[0]);
    return EXIT_SUCCESS;
  }
  if (argc < 2 || argc > 5) {
    PrintUsage(argv[0]);
    return EXIT_FAILURE;
  }

  try {
    const TopologyConfig topology = LoadTopologyConfig();
    const std::string operation = argv[1];

    const auto requireDynamic = [&]() -> const std::string& {
      if (topology.mode != TopologyMode::Dynamic || topology.metadataEndpoints.empty()) {
        std::cerr << "balancer commands require dynamic topology mode "
                     "(STRATAKV_TOPOLOGY_MODE=dynamic and STRATAKV_METADATA_ENDPOINTS)\n";
        std::exit(EXIT_FAILURE);
      }
      return topology.metadataEndpoints;
    };

    if (operation == "balancer-status" || (operation == "balancer" && argc >= 3 && std::string(argv[2]) == "status")) {
      return BalancerStatus(requireDynamic());
    }
    if (operation == "balancer-enable" || (operation == "balancer" && argc >= 3 && std::string(argv[2]) == "enable")) {
      return BalancerEnable(requireDynamic());
    }
    if (operation == "balancer-disable" || (operation == "balancer" && argc >= 3 && std::string(argv[2]) == "disable")) {
      return BalancerDisable(requireDynamic());
    }
    if (operation == "balancer-pause" || (operation == "balancer" && argc >= 3 && std::string(argv[2]) == "pause")) {
      return BalancerPause(requireDynamic());
    }
    if (operation == "balancer-resume" || (operation == "balancer" && argc >= 3 && std::string(argv[2]) == "resume")) {
      return BalancerResume(requireDynamic());
    }
    if (operation == "balancer-dry-run" || (operation == "balancer" && argc >= 3 && std::string(argv[2]) == "dry-run")) {
      return BalancerDryRun(requireDynamic());
    }
    if (operation == "balancer-cancel" || (operation == "balancer" && argc >= 3 && std::string(argv[2]) == "cancel")) {
      const std::string opId = (operation == "balancer-cancel") ? (argc >= 3 ? argv[2] : "") : (argc >= 4 ? argv[3] : "");
      if (opId.empty()) {
        std::cerr << "balancer-cancel requires: balancer-cancel <operator_id>\n";
        return EXIT_FAILURE;
      }
      return BalancerCancel(requireDynamic(), opId);
    }
    if (operation == "split-region") {
      if (argc != 4) {
        std::cerr << "split-region requires: split-region <region_id> <split_key>\n";
        return EXIT_FAILURE;
      }
      if (topology.mode != TopologyMode::Dynamic) {
        std::cerr << "split-region requires dynamic topology mode "
                     "(STRATAKV_TOPOLOGY_MODE=dynamic)\n";
        return EXIT_FAILURE;
      }
      return SplitRegion(argv[2], argv[3]);
    }
    if (operation == "move-peer") {
      if (argc != 5) {
        std::cerr << "move-peer requires: move-peer <region_id> <from_store_id> <to_store_id>\n";
        return EXIT_FAILURE;
      }
      if (topology.mode != TopologyMode::Dynamic) {
        std::cerr << "move-peer requires dynamic topology mode\n";
        return EXIT_FAILURE;
      }
      return MovePeer(argv[2], argv[3], argv[4]);
    }
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
