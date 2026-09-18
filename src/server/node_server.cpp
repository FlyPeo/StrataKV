#include "node_server.h"

#include "metadata_client.h"
#include "node_bootstrap.h"
#include "region_request_validator.h"
#include "structured_region_log.h"
#include "raft_mvcc_storage.h"
#include "txn_recovery_manager.h"
#include "remote_timestamp_oracle.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace {

template <typename Reply>
void RegionUnavailable(Reply* response, google::protobuf::Closure* done) {
  RegionValidationResult unavailable;
  unavailable.code = stratakv::region::REGION_ERROR_REGION_NOT_FOUND;
  unavailable.message = "request names a Region this node does not serve";
  PopulateRegionError(unavailable, nullptr, response->mutable_header());
  response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
  done->Run();
}

// Routing keys carried by each KV request. Prefix scans and Region-scoped
// maintenance commands name no point keys, so only their headers are validated.
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::PutAppendArgs& request) {
  return {request.key()};
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::GetArgs& request) {
  return {request.key()};
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::ListArgs&) { return {}; }
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnGetArgs& request) {
  return {request.key()};
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnPrewriteArgs& request) {
  return {request.key()};
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnBatchPrewriteArgs& request) {
  std::vector<std::string> keys;
  keys.reserve(request.mutations().size());
  for (const auto& mutation : request.mutations()) keys.push_back(mutation.key());
  return keys;
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnCommitArgs& request) {
  return {request.key()};
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnBatchCommitArgs& request) {
  return {request.keys().begin(), request.keys().end()};
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnRollbackArgs& request) {
  return {request.key()};
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnBatchRollbackArgs& request) {
  return {request.keys().begin(), request.keys().end()};
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnGetLockArgs& request) {
  return {request.key()};
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnAcquirePessimisticLockArgs& request) {
  return {request.key()};
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnCheckStatusArgs& request) {
  return {request.primarykey()};
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnResolveLockArgs& request) {
  return {request.key()};
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnFindCommitTsArgs& request) {
  return {request.key()};
}
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnExpiredLocksArgs&) { return {}; }
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnGarbageCollectArgs&) { return {}; }
std::vector<std::string> RequestKeys(const raftKVRpcProctoc::TxnMaxObservedTsArgs&) { return {}; }

template <typename Reply>
void RejectRegionRequest(const RegionMetadata& descriptor, Reply* response,
                         google::protobuf::Closure* done,
                         const RegionValidationResult& validation) {
  PopulateRegionError(validation, &descriptor, response->mutable_header());
  response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
  done->Run();
}

}  // namespace

NodeServer::NodeServer(int nodeId, RaftLogGcConfig raftLogGcConfig,
                       const RegionCatalog& catalog, const std::string& tsoEndpoints,
                       bool requireRegionHeader)
    : nodeId_(nodeId),
      requireRegionHeader_(requireRegionHeader),
      raftLogGcConfig_(raftLogGcConfig),
      txnScheduler_(std::make_shared<NodeTxnScheduler>()),
      kvService_(this),
      raftService_(this) {
  const auto assignments = catalog.PeersOnNode(nodeId);
  if (assignments.empty()) {
    throw std::invalid_argument("physical node has no Region peers");
  }

  for (const auto& assignment : assignments) {
    const auto& local = assignment.region.peers[assignment.peerIndex];
    if (port_ == 0) {
      port_ = local.port;
    } else if (port_ != local.port) {
      throw std::invalid_argument("all Region peers on one physical node must share one RPC port");
    }

    auto regionPeer = std::make_shared<RegionPeer>(
        nodeId_, assignment.region, static_cast<int>(assignment.peerIndex), raftLogGcConfig,
        txnScheduler_);
    peers_.push_back(std::move(regionPeer));
  }

  if (!tsoEndpoints.empty()) {
    auto tsoClient = std::make_shared<RemoteTimestampOracle>(tsoEndpoints);
    recoveryManager_ = std::make_unique<TxnRecoveryManager>(
        txnScheduler_, std::move(tsoClient),
        [this]() { return BuildRecoveryRoutes(); },
        [this]() { return registry_.Metrics().revision; });
  }
  WireSplitCallbacks(raftLogGcConfig);
  for (const auto& peer : peers_) WireMigrationCallback(peer);
}

NodeServer::NodeServer(int nodeId, RaftLogGcConfig raftLogGcConfig,
                       const RegionCatalog& catalog, const std::string& tsoEndpoints,
                       const NodeDynamicBootstrap& bootstrap)
    : nodeId_(nodeId),
      requireRegionHeader_(true),
      raftLogGcConfig_(raftLogGcConfig),
      txnScheduler_(std::make_shared<NodeTxnScheduler>()),
      kvService_(this),
      raftService_(this) {
  // Dynamic mode never guesses topology: every local peer must come from the
  // same validated metadata revision, and one failure leaves the registry
  // unpublished so no partial Region set can serve traffic.
  const auto report = BootstrapNodeRegions(
      nodeId, catalog, bootstrap.regions, bootstrap.revision,
      [this, raftLogGcConfig](const RegionMetadata& descriptor, size_t peerIndex) {
        return std::make_shared<RegionPeer>(nodeId_, descriptor, static_cast<int>(peerIndex),
                                            raftLogGcConfig, txnScheduler_);
      });
  if (!report.ok()) {
    for (const auto& failure : report.failures) {
      EmitRegionLog(std::cerr,
                    {"node_bootstrap_failed", failure.regionId, 0, 0, 0, 0, report.revision, "",
                     failure.component.c_str(), failure.message});
    }
    throw std::runtime_error("dynamic bootstrap failed: " + report.Summary());
  }
  peers_ = report.peers;
  port_ = peers_.front()->LocalPeerDescriptor().port;
  EmitRegionLog(std::cout, {"node_bootstrap_published", peers_.front()->RegionId(), 0, 0, 0, 0,
                            report.revision, "", "registry", report.Summary()});

  if (!tsoEndpoints.empty()) {
    auto tsoClient = std::make_shared<RemoteTimestampOracle>(tsoEndpoints);
    recoveryManager_ = std::make_unique<TxnRecoveryManager>(
        txnScheduler_, std::move(tsoClient),
        [this]() { return BuildRecoveryRoutes(); },
        [this]() { return registry_.Metrics().revision; });
  }
  WireSplitCallbacks(raftLogGcConfig);
  for (const auto& peer : peers_) WireMigrationCallback(peer);

  StoreHeartbeatReporterConfig reporterConfig;
  reporterConfig.storeId = peers_.empty() ? static_cast<uint64_t>(nodeId_ + 1)
                                          : peers_.front()->LocalPeerDescriptor().storeId;
  reporterConfig.metadataEndpoints = splitAckEndpoints_;
  heartbeatReporter_ = std::make_unique<StoreHeartbeatReporter>(
      reporterConfig, &registry_,
      splitAckEndpoints_.empty() ? nullptr : std::make_shared<MetadataClient>(splitAckEndpoints_));
}

void NodeServer::RecordValidationReject(const RegionValidationResult& validation,
                                       const RegionMetadata& descriptor) {
  registry_.RecordValidationReject(validation.code);
  EmitRegionLog(std::cerr, {"region_request_rejected", descriptor.regionId, 0, 0,
                            descriptor.epoch.version, descriptor.epoch.confVersion,
                            descriptor.metadataRevision,
                            stratakv::region::RegionErrorCode_Name(validation.code).c_str(),
                            "dispatch", validation.message});
}

std::vector<ShardRouter::RegionRoute> NodeServer::BuildRecoveryRoutes() const {
  std::vector<ShardRouter::RegionRoute> routes;
  const auto peers = registry_.Peers();
  routes.reserve(peers.size());
  for (const auto& peer : peers) {
    const RegionMetadata& descriptor = peer->Descriptor();
    std::vector<std::pair<std::string, short>> endpoints;
    endpoints.reserve(descriptor.peers.size());
    for (const auto& location : descriptor.peers) {
      endpoints.emplace_back(location.host, location.port);
    }
    routes.push_back(
        {descriptor, std::make_shared<RaftMvccStorage>(descriptor.regionId, endpoints)});
  }
  return routes;
}

NodeServer::~NodeServer() {
  if (heartbeatReporter_) {
    heartbeatReporter_->Stop();
  }
}

void NodeServer::SetMetadataEndpoints(const std::string& endpoints) {
  splitAckEndpoints_ = endpoints;
  StoreHeartbeatReporterConfig config;
  config.storeId = peers_.empty() ? static_cast<uint64_t>(nodeId_ + 1)
                                  : peers_.front()->LocalPeerDescriptor().storeId;
  config.metadataEndpoints = endpoints;
  heartbeatReporter_ = std::make_unique<StoreHeartbeatReporter>(
      config, &registry_,
      endpoints.empty() ? nullptr : std::make_shared<MetadataClient>(endpoints));
}

void NodeServer::Start() {
  std::thread rpcThread([this]() {
    RpcProvider provider;
    provider.NotifyService(&kvService_);
    provider.NotifyService(&raftService_);
    provider.Run(nodeId_, port_);
  });
  rpcThread.detach();

  // Deployment starts all physical-node processes together. Give every shared
  // listener time to enter its event loop before Region Raft groups connect.
  std::this_thread::sleep_for(std::chrono::seconds(6));
  for (const auto& peer : peers_) {
    peer->Start();
  }
  for (const auto& peer : peers_) txnScheduler_->RegisterRegion(peer);
  registry_.PublishInitial(peers_, peers_.front()->Descriptor().metadataRevision);

  if (recoveryManager_) {
    recoveryManager_->Start();
  }

  if (heartbeatReporter_ && !heartbeatReporter_->IsRunning()) {
    heartbeatReporter_->Start();
  }

  std::cout << "NodeServer " << nodeId_ << " listening on shared RPC port " << port_
            << " with " << peers_.size() << " Region peers" << std::endl;
}

// The callback fires on the parent's apply thread; heavy child creation runs
// on a detached helper so the apply loop is never blocked by RocksDB open or
// registry publication.
void NodeServer::WireSplitCallbacks(RaftLogGcConfig raftLogGcConfig) {
  for (size_t index = 0; index < peers_.size(); ++index) {
    peers_[index]->SetSplitCompletedCallback(
        [this, index, raftLogGcConfig](const RegionMetadata& shrunken,
                                       const RegionMetadata& child) {
          std::thread([this, index, raftLogGcConfig, shrunken, child]() {
            try {
              MaterializeSplitChild(index, shrunken, child);
            } catch (const std::exception& error) {
              std::cerr << "{\"level\":\"error\",\"component\":\"node\","
                           "\"event\":\"split_materialize_failed\",\"error\":\""
                        << error.what() << "\"}" << std::endl;
            }
          }).detach();
        });
  }
}

void NodeServer::MaterializeSplitChild(size_t parentPeerIndex, const RegionMetadata& shrunken,
                                       const RegionMetadata& child) {
  {
    std::lock_guard<std::mutex> lock(materializeMutex_);
    if (!materializedChildren_.insert(child.regionId).second) {
      // Idempotent retry (lost ack, callback replay): the child is already
      // materialized and published on this node.
      return;
    }
  }
  // The child mirrors the parent's placement: the local peer sits at the same
  // index (fallback: match the parent's advertised endpoint).
  size_t childIndex = parentPeerIndex;
  if (childIndex >= child.peers.size() ||
      child.peers[childIndex].nodeId != nodeId_) {
    const RegionPeerLocation& local = peers_[parentPeerIndex]->LocalPeerDescriptor();
    bool matched = false;
    for (size_t index = 0; index < child.peers.size(); ++index) {
      if (child.peers[index].nodeId == nodeId_ && child.peers[index].port == local.port) {
        childIndex = index;
        matched = true;
        break;
      }
    }
    if (!matched && childIndex >= child.peers.size()) {
      throw std::runtime_error("child descriptor assigns no peer to this node");
    }
  }

  const bool parentWasLeader = peers_[parentPeerIndex]->IsRaftLeader();
  auto childPeer = std::make_shared<RegionPeer>(nodeId_, child, static_cast<int>(childIndex),
                                                RaftLogGcConfig{}, txnScheduler_);
  WireMigrationCallback(childPeer);
  txnScheduler_->RegisterRegion(childPeer);
  childPeer->Start();
  registry_.Register(childPeer, child.metadataRevision);
  if (parentWasLeader) {
    std::thread([childPeer]() {
      for (int attempt = 0; attempt < 30; ++attempt) {
        if (childPeer->IsRaftLeader()) break;
        childPeer->Campaign();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
    }).detach();
  }

  std::cout << "{\"level\":\"info\",\"component\":\"node\",\"event\":\"split_child_published\","
               "\"parent_region_id\":" << shrunken.regionId << ",\"child_region_id\":"
            << child.regionId << ",\"metadata_revision\":" << child.metadataRevision << "}"
            << std::endl;
  AckSplitToMetadata(static_cast<uint64_t>(shrunken.regionId), child);
}

void NodeServer::WireMigrationCallback(const std::shared_ptr<RegionPeer>& peer) {
  peer->SetRemovedCallback([this](int regionId, uint64_t peerId, uint64_t revision) {
    std::thread([this, regionId, peerId, revision]() {
      try {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        const auto result = RetireMigrationPeer(regionId, peerId, revision, deadline);
        if (!result.drained) {
          std::cerr << "{\"level\":\"warn\",\"component\":\"node\","
                       "\"event\":\"migration_retire_timeout\",\"region_id\":"
                    << regionId << ",\"peer_id\":" << peerId
                    << ",\"remaining_requests\":" << result.remainingRequests << "}"
                    << std::endl;
        }
      } catch (const std::exception& error) {
        std::cerr << "{\"level\":\"error\",\"component\":\"node\","
                     "\"event\":\"migration_retire_failed\",\"region_id\":"
                  << regionId << ",\"peer_id\":" << peerId << ",\"error\":\""
                  << error.what() << "\"}" << std::endl;
      }
    }).detach();
  });
}

std::shared_ptr<RegionPeer> NodeServer::MountMigrationPeer(
    const RegionMetadata& descriptor, uint64_t targetPeerId, bool startWorkers) {
  if (!requireRegionHeader_) {
    throw std::logic_error("online replica migration requires dynamic topology mode");
  }
  size_t localIndex = descriptor.peers.size();
  for (size_t index = 0; index < descriptor.peers.size(); ++index) {
    if (descriptor.peers[index].peerId == targetPeerId &&
        descriptor.peers[index].nodeId == nodeId_ && descriptor.peers[index].isLearner) {
      localIndex = index;
      break;
    }
  }
  if (localIndex == descriptor.peers.size()) {
    throw std::invalid_argument("migration descriptor does not assign the target learner to this node");
  }

  {
    std::lock_guard<std::mutex> lock(peersMutex_);
    for (const auto& existing : peers_) {
      if (existing->RegionId() == descriptor.regionId) {
        if (existing->LocalPeerDescriptor().peerId == targetPeerId) return existing;
        throw std::logic_error("node already hosts a different peer for the migration Region");
      }
    }
  }

  auto peer = std::make_shared<RegionPeer>(nodeId_, descriptor,
                                           static_cast<int>(localIndex),
                                           raftLogGcConfig_, txnScheduler_);
  WireMigrationCallback(peer);
  if (startWorkers) {
    peer->Start();
  } else {
    peer->MarkServing();
  }
  txnScheduler_->RegisterRegion(peer);
  registry_.Register(peer, descriptor.metadataRevision);
  {
    std::lock_guard<std::mutex> lock(peersMutex_);
    peers_.push_back(peer);
  }
  return peer;
}

RegionRetireResult NodeServer::RetireMigrationPeer(
    int regionId, uint64_t peerId, uint64_t revision,
    std::chrono::steady_clock::time_point deadline) {
  std::shared_ptr<RegionPeer> peer;
  {
    std::lock_guard<std::mutex> lock(peersMutex_);
    const auto found = std::find_if(peers_.begin(), peers_.end(), [&](const auto& candidate) {
      return candidate->RegionId() == regionId &&
             candidate->LocalPeerDescriptor().peerId == peerId;
    });
    if (found == peers_.end()) return {};
    peer = *found;
  }

  RegionRetireResult result = registry_.Unregister(regionId, revision, deadline);
  if (!result.found || !result.drained) return result;
  txnScheduler_->OnRegionRemoved(regionId);
  {
    std::lock_guard<std::mutex> lock(peersMutex_);
    peers_.erase(std::remove(peers_.begin(), peers_.end(), peer), peers_.end());
  }
  std::thread([peer = std::move(peer)]() {
    if (!peer->DeleteStorageData()) {
      std::cerr << "{\"level\":\"error\",\"component\":\"node\","
                   "\"event\":\"migration_storage_cleanup_failed\",\"region_id\":"
                << peer->RegionId() << "}" << std::endl;
    }
  }).detach();
  return result;
}

void NodeServer::AckSplitToMetadata(uint64_t parentRegionId, const RegionMetadata& child) {
  if (splitAckEndpoints_.empty()) return;
  try {
    MetadataClient metadata(splitAckEndpoints_);
    metadataRpcProtocol::MetadataCommand ack;
    ack.set_mutationid("split-ack:" + std::to_string(child.regionId) + ":node" +
                       std::to_string(nodeId_));
    ack.set_expectedrevision(child.metadataRevision);
    ack.mutable_acksplit()->set_parentregionid(parentRegionId);
    ack.mutable_acksplit()->add_childregionids(static_cast<uint64_t>(child.regionId));
    metadata.Mutate(ack, std::chrono::steady_clock::now() + std::chrono::seconds(5));
  } catch (const std::exception& error) {
    // Idempotent retry happens on the next materialization attempt; a lost ack
    // never blocks serving.
    std::cerr << "{\"level\":\"warn\",\"component\":\"node\",\"event\":\"split_ack_failed\","
                 "\"error\":\"" << error.what() << "\"}" << std::endl;
  }
}

RegionPeerHandle NodeServer::FindPeer(int regionId) const { return registry_.Lookup(regionId); }

namespace {

class RetainedRegionDone final : public google::protobuf::Closure {
 public:
  RetainedRegionDone(RegionPeerHandle peer, google::protobuf::Closure* done)
      : peer_(std::move(peer)), done_(done) {}
  void Run() override {
    done_->Run();
    delete this;
  }

 private:
  RegionPeerHandle peer_;
  google::protobuf::Closure* done_;
};

}  // namespace

#define DISPATCH_KV_EX(Method, RequestType, ReplyType, RequireHeader)                   \
  void KvServiceDispatcher::Method(google::protobuf::RpcController* controller,          \
                                   const raftKVRpcProctoc::RequestType* request,          \
                                   raftKVRpcProctoc::ReplyType* response,                 \
                                   google::protobuf::Closure* done) {                     \
    RegionPeerHandle peer = server_->FindPeer(request->regionid());                       \
    if (!peer) {                                                                          \
      RegionUnavailable(response, done);                                                  \
      return;                                                                             \
    }                                                                                     \
    RegionPeer* target = peer.operator->();                                               \
    const bool enforceHeader = (RequireHeader) && server_->requireRegionHeader_;          \
    if (enforceHeader || request->has_header()) {                                         \
      const auto validation = ValidateRegionRequest(                                      \
          target->Descriptor(), target->LocalPeerDescriptor().peerId,                     \
          request->regionid(),                                                            \
          request->has_header() ? &request->header() : nullptr,                           \
          RequestKeys(*request), enforceHeader);                                          \
      if (!validation.ok()) {                                                             \
        server_->RecordValidationReject(validation, target->Descriptor());                \
        RejectRegionRequest(target->Descriptor(), response, done, validation);            \
        return;                                                                           \
      }                                                                                   \
    }                                                                                     \
    auto* retainedDone = new RetainedRegionDone(std::move(peer), done);                   \
    target->Method(controller, request, response, retainedDone);                         \
  }

#define DISPATCH_KV(Method, RequestType, ReplyType) \
  DISPATCH_KV_EX(Method, RequestType, ReplyType, true)

#define DISPATCH_KV_INTERNAL(Method, RequestType, ReplyType) \
  DISPATCH_KV_EX(Method, RequestType, ReplyType, false)

DISPATCH_KV(PutAppend, PutAppendArgs, PutAppendReply)
DISPATCH_KV(Get, GetArgs, GetReply)
DISPATCH_KV(List, ListArgs, ListReply)
DISPATCH_KV(TxnGet, TxnGetArgs, TxnGetReply)
DISPATCH_KV(TxnPrewrite, TxnPrewriteArgs, TxnPrewriteReply)
DISPATCH_KV(TxnBatchPrewrite, TxnBatchPrewriteArgs, TxnBatchPrewriteReply)
DISPATCH_KV(TxnCommit, TxnCommitArgs, TxnCommitReply)
DISPATCH_KV(TxnBatchCommit, TxnBatchCommitArgs, TxnBatchCommitReply)
DISPATCH_KV(TxnRollback, TxnRollbackArgs, TxnRollbackReply)
DISPATCH_KV(TxnBatchRollback, TxnBatchRollbackArgs, TxnBatchRollbackReply)
DISPATCH_KV(TxnGetLock, TxnGetLockArgs, TxnGetLockReply)
DISPATCH_KV(TxnAcquirePessimisticLock, TxnAcquirePessimisticLockArgs, TxnAcquirePessimisticLockReply)
DISPATCH_KV_INTERNAL(TxnCheckStatus, TxnCheckStatusArgs, TxnCheckStatusReply)
DISPATCH_KV_INTERNAL(TxnResolveLock, TxnResolveLockArgs, TxnResolveLockReply)
DISPATCH_KV_INTERNAL(TxnFindCommitTs, TxnFindCommitTsArgs, TxnFindCommitTsReply)
DISPATCH_KV_INTERNAL(TxnExpiredLocks, TxnExpiredLocksArgs, TxnExpiredLocksReply)
DISPATCH_KV_INTERNAL(TxnGarbageCollect, TxnGarbageCollectArgs, TxnGarbageCollectReply)
DISPATCH_KV_INTERNAL(TxnMaxObservedTs, TxnMaxObservedTsArgs, TxnMaxObservedTsReply)

#undef DISPATCH_KV_INTERNAL
#undef DISPATCH_KV
#undef DISPATCH_KV_EX

void KvServiceDispatcher::ProposeAdminSplit(google::protobuf::RpcController* controller,
                                            const raftKVRpcProctoc::ProposeAdminSplitArgs* request,
                                            raftKVRpcProctoc::ProposeAdminSplitReply* response,
                                            google::protobuf::Closure* done) {
  RegionPeerHandle peer = server_->FindPeer(request->regionid());
  if (!peer) {
    RegionUnavailable(response, done);
    return;
  }
  RegionPeer* target = peer.operator->();
  if (server_->requireRegionHeader_ || request->has_header()) {
    const auto validation = ValidateRegionRequest(
        target->Descriptor(), target->LocalPeerDescriptor().peerId, request->regionid(),
        request->has_header() ? &request->header() : nullptr, {}, server_->requireRegionHeader_);
    if (!validation.ok()) {
      server_->RecordValidationReject(validation, target->Descriptor());
      RejectRegionRequest(target->Descriptor(), response, done, validation);
      return;
    }
  }
  auto* retainedDone = new RetainedRegionDone(std::move(peer), done);
  target->ProposeAdminSplit(controller, request, response, retainedDone);
}

void KvServiceDispatcher::ProposeConfChange(google::protobuf::RpcController* controller,
                                            const raftKVRpcProctoc::ProposeConfChangeArgs* request,
                                            raftKVRpcProctoc::ProposeConfChangeReply* response,
                                            google::protobuf::Closure* done) {
  RegionPeerHandle peer = server_->FindPeer(request->regionid());
  if (!peer) {
    RegionUnavailable(response, done);
    return;
  }
  RegionPeer* target = peer.operator->();
  if (server_->requireRegionHeader_ || request->has_header()) {
    const auto validation = ValidateRegionRequest(
        target->Descriptor(), target->LocalPeerDescriptor().peerId, request->regionid(),
        request->has_header() ? &request->header() : nullptr, {}, server_->requireRegionHeader_);
    if (!validation.ok()) {
      server_->RecordValidationReject(validation, target->Descriptor());
      RejectRegionRequest(target->Descriptor(), response, done, validation);
      return;
    }
  }
  auto* retainedDone = new RetainedRegionDone(std::move(peer), done);
  target->ProposeConfChange(controller, request, response, retainedDone);
}

void KvServiceDispatcher::PrepareMigrationTarget(
    google::protobuf::RpcController*,
    const raftKVRpcProctoc::PrepareMigrationTargetArgs* request,
    raftKVRpcProctoc::PrepareMigrationTargetReply* response,
    google::protobuf::Closure* done) {
  try {
    RegionMetadata descriptor = FromProtoRegion(request->region());
    auto peer = server_->MountMigrationPeer(descriptor, request->targetpeerid(), true);
    response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
    auto* status = response->mutable_status();
    status->set_regionid(static_cast<uint64_t>(descriptor.regionId));
    status->set_phase(stratakv::region::REPLICA_MIGRATION_CATCHING_UP);
    const auto local = peer->LocalPeerDescriptor();
    status->mutable_targetpeer()->set_peerid(local.peerId);
    status->mutable_targetpeer()->set_storeid(local.storeId);
    status->mutable_targetpeer()->set_host(local.host);
    status->mutable_targetpeer()->set_port(local.port);
    status->mutable_targetpeer()->set_islearner(true);
  } catch (const std::exception& error) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    auto* regionError = response->mutable_header()->mutable_error();
    regionError->set_code(stratakv::region::REGION_ERROR_STORAGE);
    regionError->set_message(error.what());
  }
  done->Run();
}

void KvServiceDispatcher::RetireMigrationSource(
    google::protobuf::RpcController*,
    const raftKVRpcProctoc::RetireMigrationSourceArgs* request,
    raftKVRpcProctoc::RetireMigrationSourceReply* response,
    google::protobuf::Closure* done) {
  try {
    const auto timeout = std::chrono::milliseconds(
        request->timeoutms() == 0 ? 5000 : request->timeoutms());
    const auto result = server_->RetireMigrationPeer(
        request->regionid(), request->peerid(), request->metadatarevision(),
        std::chrono::steady_clock::now() + timeout);
    response->set_drained(result.drained);
    response->set_remainingrequests(result.remainingRequests);
    response->set_err(result.found && result.drained
                          ? std::to_string(static_cast<int>(TxnStatus::Ok))
                          : std::to_string(static_cast<int>(TxnStatus::StorageError)));
  } catch (const std::exception& error) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    auto* regionError = response->mutable_header()->mutable_error();
    regionError->set_code(stratakv::region::REGION_ERROR_STORAGE);
    regionError->set_message(error.what());
  }
  done->Run();
}

void KvServiceDispatcher::RegionMigrationStatus(
    google::protobuf::RpcController*,
    const raftKVRpcProctoc::RegionMigrationStatusArgs* request,
    raftKVRpcProctoc::RegionMigrationStatusReply* response,
    google::protobuf::Closure* done) {
  RegionPeerHandle peer = server_->FindPeer(request->regionid());
  if (!peer) {
    RegionUnavailable(response, done);
    return;
  }
  const RegionMetadata descriptor = peer->Descriptor();
  *response->mutable_region() = ToProtoRegion(descriptor);
  auto* status = response->mutable_status();
  status->set_regionid(static_cast<uint64_t>(descriptor.regionId));
  const RegionPeerLocation* target = descriptor.FindPeer(request->targetpeerid());
  if (target == nullptr) {
    status->set_phase(stratakv::region::REPLICA_MIGRATION_NOT_STARTED);
    status->set_loglag(0);
  } else {
    auto* encoded = status->mutable_targetpeer();
    encoded->set_peerid(target->peerId);
    encoded->set_storeid(target->storeId);
    encoded->set_host(target->host);
    encoded->set_port(target->port);
    encoded->set_islearner(target->isLearner);
    const int lag = peer->GetPeerLag(target->peerId);
    status->set_loglag(lag < 0 ? 0 : static_cast<uint64_t>(lag));
    status->set_phase(target->isLearner
                          ? stratakv::region::REPLICA_MIGRATION_CATCHING_UP
                          : stratakv::region::REPLICA_MIGRATION_PROMOTED);
  }
  response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
  done->Run();
}

void KvServiceDispatcher::RegionSplitStatus(google::protobuf::RpcController*,
                                            const raftKVRpcProctoc::RegionSplitStatusArgs* request,
                                            raftKVRpcProctoc::RegionSplitStatusReply* response,
                                            google::protobuf::Closure* done) {
  RegionPeerHandle peer = server_->FindPeer(request->regionid());
  if (!peer) {
    RegionUnavailable(response, done);
    return;
  }
  // Operator observability: read-only phase query, validated when a header is
  // present but never blocked by dynamic-mode header enforcement, so progress
  // stays pollable across the parent's own epoch transition.
  RegionPeer* target = peer.operator->();
  if (request->has_header()) {
    const auto validation = ValidateRegionRequest(
        target->Descriptor(), target->LocalPeerDescriptor().peerId, request->regionid(),
        &request->header(), {}, false);
    if (!validation.ok()) {
      server_->RecordValidationReject(validation, target->Descriptor());
      RejectRegionRequest(target->Descriptor(), response, done, validation);
      return;
    }
  }
  response->set_phase(static_cast<int>(target->SplitPhase()));
  response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
  done->Run();
}

void KvServiceDispatcher::TxnProtocolCapabilities(
    google::protobuf::RpcController*,
    const raftKVRpcProctoc::TxnProtocolCapabilitiesArgs*,
    raftKVRpcProctoc::TxnProtocolCapabilitiesReply* response,
    google::protobuf::Closure* done) {
  response->set_protocolversion(kTxnProtocolVersion);
  response->set_preparedcommandversion(PreparedMvccWrite::kCommandVersion);
  response->set_lockformatversion(kMvccLockFormatVersion);
  response->set_hlcexpiry(true);
  done->Run();
}

void RaftServiceDispatcher::AppendEntries(google::protobuf::RpcController* controller,
                                          const raftRpcProctoc::AppendEntriesArgs* request,
                                          raftRpcProctoc::AppendEntriesReply* response,
                                          google::protobuf::Closure* done) {
  RegionPeerHandle peer = server_->FindPeer(request->regionid());
  if (!peer) {
    response->set_term(0);
    response->set_success(false);
    response->set_updatenextindex(1);
    response->set_appstate(Disconnected);
    done->Run();
    return;
  }
  RegionPeer* target = peer.operator->();
  // Dynamic mode fenced Raft transport: identity and epoch are enforced, so a
  // stale member cannot replicate into a Region whose descriptor has moved on.
  if (server_->requireRegionHeader_ || request->has_header()) {
    const auto validation = ValidateRaftPeerRequest(
        target->Descriptor(), target->LocalPeerDescriptor().peerId, request->regionid(),
        request->has_header() ? &request->header() : nullptr, server_->requireRegionHeader_);
    if (!validation.ok()) {
      const RegionMetadata currentDescriptor = target->Descriptor();
      server_->RecordValidationReject(validation, currentDescriptor);
      PopulateRegionError(validation, &currentDescriptor, response->mutable_header());
      done->Run();
      return;
    }
  }
  auto* retainedDone = new RetainedRegionDone(std::move(peer), done);
  target->RaftNode()->AppendEntries(controller, request, response, retainedDone);
}

void RaftServiceDispatcher::InstallSnapshot(google::protobuf::RpcController* controller,
                                            const raftRpcProctoc::InstallSnapshotRequest* request,
                                            raftRpcProctoc::InstallSnapshotResponse* response,
                                            google::protobuf::Closure* done) {
  RegionPeerHandle peer = server_->FindPeer(request->regionid());
  if (!peer) {
    response->set_term(0);
    done->Run();
    return;
  }
  RegionPeer* target = peer.operator->();
  // Dynamic mode fenced Raft transport: identity and epoch are enforced, so a
  // stale member cannot replicate into a Region whose descriptor has moved on.
  if (server_->requireRegionHeader_ || request->has_header()) {
    const auto validation = ValidateRaftPeerRequest(
        target->Descriptor(), target->LocalPeerDescriptor().peerId, request->regionid(),
        request->has_header() ? &request->header() : nullptr, server_->requireRegionHeader_);
    if (!validation.ok()) {
      const RegionMetadata currentDescriptor = target->Descriptor();
      server_->RecordValidationReject(validation, currentDescriptor);
      PopulateRegionError(validation, &currentDescriptor, response->mutable_header());
      done->Run();
      return;
    }
  }
  auto* retainedDone = new RetainedRegionDone(std::move(peer), done);
  target->RaftNode()->InstallSnapshot(controller, request, response, retainedDone);
}

void RaftServiceDispatcher::RequestVote(google::protobuf::RpcController* controller,
                                        const raftRpcProctoc::RequestVoteArgs* request,
                                        raftRpcProctoc::RequestVoteReply* response,
                                        google::protobuf::Closure* done) {
  RegionPeerHandle peer = server_->FindPeer(request->regionid());
  if (!peer) {
    response->set_term(0);
    response->set_votegranted(false);
    response->set_votestate(Expire);
    done->Run();
    return;
  }
  RegionPeer* target = peer.operator->();
  // Dynamic mode fenced Raft transport: identity and epoch are enforced, so a
  // stale member cannot replicate into a Region whose descriptor has moved on.
  if (server_->requireRegionHeader_ || request->has_header()) {
    const auto validation = ValidateRaftPeerRequest(
        target->Descriptor(), target->LocalPeerDescriptor().peerId, request->regionid(),
        request->has_header() ? &request->header() : nullptr, server_->requireRegionHeader_);
    if (!validation.ok()) {
      const RegionMetadata currentDescriptor = target->Descriptor();
      server_->RecordValidationReject(validation, currentDescriptor);
      PopulateRegionError(validation, &currentDescriptor, response->mutable_header());
      done->Run();
      return;
    }
  }
  auto* retainedDone = new RetainedRegionDone(std::move(peer), done);
  target->RaftNode()->RequestVote(controller, request, response, retainedDone);
}
