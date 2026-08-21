#ifndef STRATAKV_SERVER_NODE_SERVER_H
#define STRATAKV_SERVER_NODE_SERVER_H

#include <memory>
#include <unordered_map>
#include <vector>

#include "region_peer.h"
#include "region_metadata.h"
#include "rpc_provider.h"

class NodeServer;

// One protobuf service per physical node. Requests carry RegionId and are
// forwarded to lightweight RegionPeer objects owned by NodeServer.
class NodeTxnScheduler;
class TxnRecoveryManager;
class RemoteTimestampOracle;

class KvServiceDispatcher final : public raftKVRpcProctoc::kvServerRpc {
 public:
  explicit KvServiceDispatcher(NodeServer* server) : server_(server) {}

  void PutAppend(google::protobuf::RpcController*, const raftKVRpcProctoc::PutAppendArgs*,
                 raftKVRpcProctoc::PutAppendReply*, google::protobuf::Closure*) override;
  void Get(google::protobuf::RpcController*, const raftKVRpcProctoc::GetArgs*,
           raftKVRpcProctoc::GetReply*, google::protobuf::Closure*) override;
  void List(google::protobuf::RpcController*, const raftKVRpcProctoc::ListArgs*,
            raftKVRpcProctoc::ListReply*, google::protobuf::Closure*) override;
  void TxnGet(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnGetArgs*,
              raftKVRpcProctoc::TxnGetReply*, google::protobuf::Closure*) override;
  void TxnPrewrite(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnPrewriteArgs*,
                   raftKVRpcProctoc::TxnPrewriteReply*, google::protobuf::Closure*) override;
  void TxnCommit(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnCommitArgs*,
                 raftKVRpcProctoc::TxnCommitReply*, google::protobuf::Closure*) override;
  void TxnRollback(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnRollbackArgs*,
                   raftKVRpcProctoc::TxnRollbackReply*, google::protobuf::Closure*) override;
  void TxnGetLock(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnGetLockArgs*,
                  raftKVRpcProctoc::TxnGetLockReply*, google::protobuf::Closure*) override;
  void TxnAcquirePessimisticLock(google::protobuf::RpcController*,
                                 const raftKVRpcProctoc::TxnAcquirePessimisticLockArgs*,
                                 raftKVRpcProctoc::TxnAcquirePessimisticLockReply*,
                                 google::protobuf::Closure*) override;
  void TxnCheckStatus(google::protobuf::RpcController*,
                      const raftKVRpcProctoc::TxnCheckStatusArgs*,
                      raftKVRpcProctoc::TxnCheckStatusReply*,
                      google::protobuf::Closure*) override;
  void TxnResolveLock(google::protobuf::RpcController*,
                      const raftKVRpcProctoc::TxnResolveLockArgs*,
                      raftKVRpcProctoc::TxnResolveLockReply*,
                      google::protobuf::Closure*) override;
  void TxnProtocolCapabilities(google::protobuf::RpcController*,
                               const raftKVRpcProctoc::TxnProtocolCapabilitiesArgs*,
                               raftKVRpcProctoc::TxnProtocolCapabilitiesReply*,
                               google::protobuf::Closure*) override;
  void TxnFindCommitTs(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnFindCommitTsArgs*,
                       raftKVRpcProctoc::TxnFindCommitTsReply*, google::protobuf::Closure*) override;
  void TxnExpiredLocks(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnExpiredLocksArgs*,
                       raftKVRpcProctoc::TxnExpiredLocksReply*, google::protobuf::Closure*) override;
  void TxnGarbageCollect(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnGarbageCollectArgs*,
                         raftKVRpcProctoc::TxnGarbageCollectReply*, google::protobuf::Closure*) override;
  void TxnMaxObservedTs(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnMaxObservedTsArgs*,
                        raftKVRpcProctoc::TxnMaxObservedTsReply*, google::protobuf::Closure*) override;

 private:
  NodeServer* server_;
};

class RaftServiceDispatcher final : public raftRpcProctoc::raftRpc {
 public:
  explicit RaftServiceDispatcher(NodeServer* server) : server_(server) {}

  void AppendEntries(google::protobuf::RpcController*, const raftRpcProctoc::AppendEntriesArgs*,
                     raftRpcProctoc::AppendEntriesReply*, google::protobuf::Closure*) override;
  void InstallSnapshot(google::protobuf::RpcController*, const raftRpcProctoc::InstallSnapshotRequest*,
                       raftRpcProctoc::InstallSnapshotResponse*, google::protobuf::Closure*) override;
  void RequestVote(google::protobuf::RpcController*, const raftRpcProctoc::RequestVoteArgs*,
                   raftRpcProctoc::RequestVoteReply*, google::protobuf::Closure*) override;

 private:
  NodeServer* server_;
};

class NodeServer {
 public:
  NodeServer(int nodeId, int maxRaftState, const RegionCatalog& catalog, const std::string& tsoEndpoints = "");
  ~NodeServer();
  void Start();
  NodeTxnScheduler* TxnSchedulerForTest() const { return txnScheduler_.get(); }
  const std::vector<std::shared_ptr<RegionPeer>>& PeersForTest() const { return peers_; }

 private:
  friend class KvServiceDispatcher;
  friend class RaftServiceDispatcher;

  RegionPeer* FindPeer(int regionId) const;

  int nodeId_;
  short port_ = 0;
  // Declared before peers so it is destroyed after them. There is exactly one
  // transaction scheduler and one latch table per physical NodeServer.
  std::shared_ptr<NodeTxnScheduler> txnScheduler_;
  std::unique_ptr<TxnRecoveryManager> recoveryManager_;
  std::vector<std::shared_ptr<RegionPeer>> peers_;
  std::unordered_map<int, std::shared_ptr<RegionPeer>> peersByRegion_;
  KvServiceDispatcher kvService_;
  RaftServiceDispatcher raftService_;
};

#endif  // STRATAKV_SERVER_NODE_SERVER_H
