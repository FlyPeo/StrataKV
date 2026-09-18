#ifndef STRATAKV_SERVER_NODE_SERVER_H
#define STRATAKV_SERVER_NODE_SERVER_H

#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "region_peer.h"
#include "region_metadata.h"
#include "region_registry.h"
#include "region_request_validator.h"
#include "rpc_provider.h"
#include "shard_router.h"
#include "store_heartbeat_reporter.h"

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
  void TxnBatchPrewrite(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnBatchPrewriteArgs*,
                        raftKVRpcProctoc::TxnBatchPrewriteReply*, google::protobuf::Closure*) override;
  void TxnCommit(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnCommitArgs*,
                 raftKVRpcProctoc::TxnCommitReply*, google::protobuf::Closure*) override;
  void TxnBatchCommit(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnBatchCommitArgs*,
                      raftKVRpcProctoc::TxnBatchCommitReply*, google::protobuf::Closure*) override;
  void TxnRollback(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnRollbackArgs*,
                   raftKVRpcProctoc::TxnRollbackReply*, google::protobuf::Closure*) override;
  void TxnBatchRollback(google::protobuf::RpcController*, const raftKVRpcProctoc::TxnBatchRollbackArgs*,
                        raftKVRpcProctoc::TxnBatchRollbackReply*, google::protobuf::Closure*) override;
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
  void ProposeAdminSplit(google::protobuf::RpcController*,
                         const raftKVRpcProctoc::ProposeAdminSplitArgs*,
                         raftKVRpcProctoc::ProposeAdminSplitReply*,
                         google::protobuf::Closure*) override;
  void ProposeConfChange(google::protobuf::RpcController*,
                         const raftKVRpcProctoc::ProposeConfChangeArgs*,
                         raftKVRpcProctoc::ProposeConfChangeReply*,
                         google::protobuf::Closure*) override;
  void PrepareMigrationTarget(google::protobuf::RpcController*,
                              const raftKVRpcProctoc::PrepareMigrationTargetArgs*,
                              raftKVRpcProctoc::PrepareMigrationTargetReply*,
                              google::protobuf::Closure*) override;
  void RetireMigrationSource(google::protobuf::RpcController*,
                             const raftKVRpcProctoc::RetireMigrationSourceArgs*,
                             raftKVRpcProctoc::RetireMigrationSourceReply*,
                             google::protobuf::Closure*) override;
  void RegionMigrationStatus(google::protobuf::RpcController*,
                             const raftKVRpcProctoc::RegionMigrationStatusArgs*,
                             raftKVRpcProctoc::RegionMigrationStatusReply*,
                             google::protobuf::Closure*) override;
  void RegionSplitStatus(google::protobuf::RpcController*,
                         const raftKVRpcProctoc::RegionSplitStatusArgs*,
                         raftKVRpcProctoc::RegionSplitStatusReply*,
                         google::protobuf::Closure*) override;

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

// The complete Region view of one validated metadata revision. A storage node
// bootstraps all of its assigned peers from exactly one of these views.
struct NodeDynamicBootstrap {
  std::vector<RegionMetadata> regions;
  uint64_t revision = 0;
};

class NodeServer {
 public:
  // requireRegionHeader enforces the typed Region header in dynamic topology
  // mode; static compatibility mode still accepts legacy headerless requests.
  NodeServer(int nodeId, RaftLogGcConfig raftLogGcConfig, const RegionCatalog& catalog,
             const std::string& tsoEndpoints = "", bool requireRegionHeader = false);
  // Dynamic bootstrap: every locally assigned peer is built from one metadata
  // revision and the registry is published only when all of them initialize.
  NodeServer(int nodeId, RaftLogGcConfig raftLogGcConfig, const RegionCatalog& catalog,
             const std::string& tsoEndpoints, const NodeDynamicBootstrap& bootstrap);
  ~NodeServer();
  // Dynamic-mode split acks go to the metadata members after a node publishes
  // its materialized child. No-op when unset (static mode / tests).
  void SetSplitAckEndpoints(const std::string& endpoints) { SetMetadataEndpoints(endpoints); }
  void SetMetadataEndpoints(const std::string& endpoints);
  StoreHeartbeatReporter* HeartbeatReporterForTest() { return heartbeatReporter_.get(); }
  RegionRegistry& RegistryForTest() { return registry_; }
  void WireSplitCallbacks(RaftLogGcConfig raftLogGcConfig);
  void MaterializeSplitChild(size_t parentPeerIndex, const RegionMetadata& shrunken,
                             const RegionMetadata& child);
  void AckSplitToMetadata(uint64_t parentRegionId, const RegionMetadata& child);
  void Start();
  NodeTxnScheduler* TxnSchedulerForTest() const { return txnScheduler_.get(); }
  const std::vector<std::shared_ptr<RegionPeer>>& PeersForTest() const { return peers_; }
  // Publishes the startup registry without binding the RPC listener or
  // starting Region Raft groups, so dispatcher validation is unit-testable.
  void PublishInitialRegistryForTest() {
    registry_.PublishInitial(peers_, peers_.front()->Descriptor().metadataRevision);
  }
  RegionRegistryMetrics RegistryMetrics() const { return registry_.Metrics(); }
  std::shared_ptr<RegionPeer> MountMigrationPeer(const RegionMetadata& descriptor,
                                                 uint64_t targetPeerId,
                                                 bool startWorkers = true);
  RegionRetireResult RetireMigrationPeer(
      int regionId, uint64_t peerId, uint64_t revision,
      std::chrono::steady_clock::time_point deadline);

 private:
  friend class KvServiceDispatcher;
  friend class RaftServiceDispatcher;

  RegionPeerHandle FindPeer(int regionId) const;
  // Builds recovery routes from the registry's current published view, so the
  // TxnRecoveryManager tracks dynamic membership instead of the startup catalog.
  std::vector<ShardRouter::RegionRoute> BuildRecoveryRoutes() const;
  void RecordValidationReject(const RegionValidationResult& validation,
                             const RegionMetadata& descriptor);
  void WireMigrationCallback(const std::shared_ptr<RegionPeer>& peer);

  int nodeId_;
  std::string splitAckEndpoints_;
  std::mutex materializeMutex_;
  std::unordered_set<int> materializedChildren_;
  short port_ = 0;
  bool requireRegionHeader_ = false;
  RaftLogGcConfig raftLogGcConfig_;
  mutable std::mutex peersMutex_;
  // Declared before peers so it is destroyed after them. There is exactly one
  // transaction scheduler and one latch table per physical NodeServer.
  std::shared_ptr<NodeTxnScheduler> txnScheduler_;
  std::unique_ptr<TxnRecoveryManager> recoveryManager_;
  std::vector<std::shared_ptr<RegionPeer>> peers_;
  RegionRegistry registry_;
  KvServiceDispatcher kvService_;
  RaftServiceDispatcher raftService_;
  std::unique_ptr<StoreHeartbeatReporter> heartbeatReporter_;
};

#endif  // STRATAKV_SERVER_NODE_SERVER_H
