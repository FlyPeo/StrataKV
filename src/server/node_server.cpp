#include "node_server.h"

#include "txn_recovery_manager.h"
#include "remote_timestamp_oracle.h"
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace {

template <typename Reply>
void RegionUnavailable(Reply* response, google::protobuf::Closure* done) {
  response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
  done->Run();
}

}  // namespace

NodeServer::NodeServer(int nodeId, RaftLogGcConfig raftLogGcConfig,
                       const RegionCatalog& catalog, const std::string& tsoEndpoints)
    : nodeId_(nodeId),
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

    std::vector<std::pair<std::string, short>> addresses;
    addresses.reserve(assignment.region.peers.size());
    for (const auto& peer : assignment.region.peers) {
      addresses.emplace_back(peer.host, peer.port);
    }

    auto regionPeer = std::make_shared<RegionPeer>(
        nodeId_, assignment.region.regionId, static_cast<int>(assignment.peerIndex), raftLogGcConfig,
        assignment.region.startKey, assignment.region.endKey,
        std::move(addresses), txnScheduler_);
    txnScheduler_->RegisterRegion(regionPeer);
    peersByRegion_.emplace(assignment.region.regionId, regionPeer);
    peers_.push_back(std::move(regionPeer));
  }

  if (!tsoEndpoints.empty()) {
    auto tsoClient = std::make_shared<RemoteTimestampOracle>(tsoEndpoints);
    recoveryManager_ = std::make_unique<TxnRecoveryManager>(txnScheduler_, std::move(tsoClient), catalog);
  }
}

NodeServer::~NodeServer() = default;

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

  if (recoveryManager_) {
    recoveryManager_->Start();
  }

  std::cout << "NodeServer " << nodeId_ << " listening on shared RPC port " << port_
            << " with " << peers_.size() << " Region peers" << std::endl;
}

RegionPeer* NodeServer::FindPeer(int regionId) const {
  const auto found = peersByRegion_.find(regionId);
  return found == peersByRegion_.end() ? nullptr : found->second.get();
}

#define DISPATCH_KV(Method, RequestType, ReplyType)                                      \
  void KvServiceDispatcher::Method(google::protobuf::RpcController* controller,          \
                                   const raftKVRpcProctoc::RequestType* request,          \
                                   raftKVRpcProctoc::ReplyType* response,                 \
                                   google::protobuf::Closure* done) {                     \
    RegionPeer* peer = server_->FindPeer(request->regionid());                            \
    if (peer == nullptr) {                                                                \
      RegionUnavailable(response, done);                                                  \
      return;                                                                             \
    }                                                                                     \
    peer->Method(controller, request, response, done);                                    \
  }

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
DISPATCH_KV(TxnCheckStatus, TxnCheckStatusArgs, TxnCheckStatusReply)
DISPATCH_KV(TxnResolveLock, TxnResolveLockArgs, TxnResolveLockReply)
DISPATCH_KV(TxnFindCommitTs, TxnFindCommitTsArgs, TxnFindCommitTsReply)
DISPATCH_KV(TxnExpiredLocks, TxnExpiredLocksArgs, TxnExpiredLocksReply)
DISPATCH_KV(TxnGarbageCollect, TxnGarbageCollectArgs, TxnGarbageCollectReply)
DISPATCH_KV(TxnMaxObservedTs, TxnMaxObservedTsArgs, TxnMaxObservedTsReply)

#undef DISPATCH_KV

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
  RegionPeer* peer = server_->FindPeer(request->regionid());
  if (peer == nullptr) {
    response->set_term(0);
    response->set_success(false);
    response->set_updatenextindex(1);
    response->set_appstate(Disconnected);
    done->Run();
    return;
  }
  peer->RaftNode()->AppendEntries(controller, request, response, done);
}

void RaftServiceDispatcher::InstallSnapshot(google::protobuf::RpcController* controller,
                                            const raftRpcProctoc::InstallSnapshotRequest* request,
                                            raftRpcProctoc::InstallSnapshotResponse* response,
                                            google::protobuf::Closure* done) {
  RegionPeer* peer = server_->FindPeer(request->regionid());
  if (peer == nullptr) {
    response->set_term(0);
    done->Run();
    return;
  }
  peer->RaftNode()->InstallSnapshot(controller, request, response, done);
}

void RaftServiceDispatcher::RequestVote(google::protobuf::RpcController* controller,
                                        const raftRpcProctoc::RequestVoteArgs* request,
                                        raftRpcProctoc::RequestVoteReply* response,
                                        google::protobuf::Closure* done) {
  RegionPeer* peer = server_->FindPeer(request->regionid());
  if (peer == nullptr) {
    response->set_term(0);
    response->set_votegranted(false);
    response->set_votestate(Expire);
    done->Run();
    return;
  }
  peer->RaftNode()->RequestVote(controller, request, response, done);
}
