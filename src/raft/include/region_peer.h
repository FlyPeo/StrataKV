#ifndef STRATAKV_RAFT_REGION_PEER_H
#define STRATAKV_RAFT_REGION_PEER_H

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include "kv_engine.h"
#include "kv_engine_factory.h"
#include "kv_server_rpc.pb.h"
#include "raft.h"
#include "mvcc_storage.h"
#include "region_metadata.h"
#include "txn_scheduler.h"

#include <functional>
#include <shared_mutex>

enum class RegionPeerState { Initializing, Serving, Retiring, Stopped };

// A lightweight replicated state-machine peer for one Region. Networking is
// owned by the physical-node NodeServer; this object owns only Region-local
// Raft, MVCC, persistence and apply state.
class RegionPeer : public TxnRegionExecutor {
 private:
  std::mutex m_mtx;
  int m_me;
  int m_physicalNodeId = -1;
  int m_regionId = -1;
  std::string m_regionStartKey;
  std::string m_regionEndKey;
  RegionMetadata m_descriptor;
  std::atomic<RegionPeerState> m_lifecycle{RegionPeerState::Initializing};
  std::atomic<uint64_t> m_inFlightRequests{0};
  std::atomic<int> m_adminSplitRequestId{1};
  std::atomic<int> m_adminConfChangeRequestId{1};
  std::atomic<int> m_splitPhase{0};
  std::atomic<bool> m_splitMaterializing{false};
  std::atomic<bool> m_stopRequested{false};
  std::atomic<bool> m_started{false};
  std::atomic<int> m_testApplyIndex_{0};
  mutable std::shared_mutex m_descriptorMutex;
  std::mutex m_splitMutex;
  std::mutex m_confChangeMutex;
  std::string m_splitStatePath;
  std::string m_dbPath;
  std::function<void(const RegionMetadata& shrunken, const RegionMetadata& child)>
      m_splitCompletedCallback;
  std::function<void(int regionId, uint64_t peerId, uint64_t revision)>
      m_removedCallback;

  void WriteSplitStateLocked(int phase, uint64_t generation,
                             const stratakv::region::AdminSplitCommand& command) const;
  mutable std::mutex m_lifecycleMutex;
  std::condition_variable m_lifecycleCondition;
  std::mutex m_stopMutex;
  std::condition_variable m_stopCondition;
  std::thread m_statusThread;
  std::thread m_applyThread;
  std::thread m_raftLogGcThread;
  std::shared_ptr<Raft> m_raftNode;
  std::shared_ptr<Persister> m_persister;
  std::shared_ptr<LockQueue<ApplyMsg> > applyChan;  // kvServer和raft节点的通信管道
  std::vector<std::pair<std::string, short>> m_peerAddresses;
  RaftLogGcConfig m_raftLogGcConfig;

  // Serialized state used while producing and restoring Region snapshots.
  std::string m_serializedKVData;
  std::unique_ptr<IKVEngine> m_kvEngine;
  std::shared_ptr<MvccStorage> m_mvccStorage;
  std::weak_ptr<NodeTxnScheduler> m_nodeTxnScheduler;

  using WaitApplyQueue = std::shared_ptr<LockQueue<Op>>;
  std::unordered_map<std::string, WaitApplyQueue> waitApplyCh;
  // Maps a request key to the queue that receives its apply result.

  std::unordered_map<std::string, int> m_lastRequestId;  // clientid -> requestID  //一个kV服务器可能连接多个client

  std::atomic<uint64_t> m_prewriteApplyConflicts{0};
  std::atomic<uint64_t> m_txnRaftApplies{0};
  std::atomic<uint64_t> m_raftLogGcRuns{0};
  std::atomic<uint64_t> m_raftLogGcSoftRuns{0};
  std::atomic<uint64_t> m_raftLogGcForcedRuns{0};
  std::atomic<uint64_t> m_raftLogGcReclaimedEntries{0};
  std::atomic<uint64_t> m_raftLogGcLastDurationMicros{0};
  std::atomic<uint64_t> m_raftLogGcTotalDurationMicros{0};
  std::atomic<uint64_t> m_raftLogGcMaxDurationMicros{0};
  // A new leader is not allowed to serve MVCC prepare/status decisions until
  // a current-term ReadIndex has reached the local state machine.
  std::atomic<int> m_txnReadyTerm{-1};

  std::atomic<uint64_t> m_approximateBytes{0};
  std::atomic<uint64_t> m_totalRequests{0};
  std::atomic<uint64_t> m_splitCandidateGeneration{0};
  std::function<std::string(uint64_t)> m_splitCandidateResolver;

  // The apply loop and the GC snapshotter share one state-machine boundary.
  // This prevents a snapshot labelled N from observing part of command N+1.
  std::mutex m_stateMachineExecutionMutex;

  // Raft::lastApplied means "delivered to applyChan". Linearizable reads must
  // instead wait for this Region state machine (including RocksDB) to finish.
  mutable std::mutex m_applyProgressMutex;
  std::condition_variable m_applyProgressCv;
  int m_stateMachineAppliedIndex = 0;
  bool m_stateMachineHealthy = true;

  // last SnapShot point , raftIndex
  int m_lastSnapShotRaftLogIndex;

 public:
  RegionPeer() = delete;

  RegionPeer(int physicalNodeId, RegionMetadata descriptor, int localPeerId,
             RaftLogGcConfig raftLogGcConfig,
             const std::shared_ptr<NodeTxnScheduler>& nodeTxnScheduler);
  ~RegionPeer() override;

  // Start Region-local Raft and apply workers after the NodeServer listener is
  // accepting RPCs. The call is non-blocking after initialization completes.
  void Start();
  void Stop();
  void Campaign();
  bool IsRaftLeader() const;
  bool DeleteStorageData();
  const std::string& StoragePath() const { return m_dbPath; }
  int RegionId() const { return m_regionId; }
  // Thread-safe copies: the descriptor shrinks in place when an AdminSplit
  // applies, while RPC threads validate against it concurrently.
  RegionMetadata Descriptor() const;
  RegionPeerLocation LocalPeerDescriptor() const;
  // In-place shrink on AdminSplit apply. Rejects a non-advancing epoch.
  void UpdateDescriptorShrunk(const RegionMetadata& shrunken);
  // Invoked on the apply thread after a split fully materializes locally.
  // Applies one committed AdminSplit entry. Public: the Raft apply loop calls
  // it, and split tests use it to drive the phase state machine directly.
  TxnStatus ApplyAdminSplit(const stratakv::region::AdminSplitCommand& command);
  void MaterializeSplitChildAsync(const stratakv::region::AdminSplitCommand& command,
                                  int persistedPhase, RegionMetadata parent,
                                  RegionMetadata child);
  TxnStatus ApplyConfChange(const stratakv::region::ConfChangeCommand& command);
  bool IsPeerCaughtUp(uint64_t peerId, int maxLag = 100) const;
  int GetPeerLag(uint64_t peerId) const;
  bool MaybePromoteLearner(uint64_t peerId, int maxLag = 100);
  void SetPeerMatchIndexForTest(uint64_t peerId, int matchIndex);
  // Local split phase for operator progress reporting.
  int SplitPhase() const { return m_splitPhase.load(std::memory_order_acquire); }

  void SetSplitCompletedCallback(
      std::function<void(const RegionMetadata& shrunken, const RegionMetadata& child)> callback) {
    m_splitCompletedCallback = std::move(callback);
  }
  void SetRemovedCallback(
      std::function<void(int regionId, uint64_t peerId, uint64_t revision)> callback) {
    m_removedCallback = std::move(callback);
  }
  RegionPeerState LifecycleState() const { return m_lifecycle.load(std::memory_order_acquire); }
  void MarkServing();
  bool TryAcquireRequest();
  void ReleaseRequest();
  bool BeginRetire();
  bool WaitForDrain(std::chrono::steady_clock::time_point deadline,
                    uint64_t* remainingRequests = nullptr);
  void MarkStopped();
  uint64_t InFlightRequests() const {
    return m_inFlightRequests.load(std::memory_order_acquire);
  }
  Raft* RaftNode() const { return m_raftNode.get(); }
  // Test seams: apply one Op through the state-machine path, and reach the
  // local KV engine to inspect post-split data.
  bool ApplyOpForTest(const Op& op) {
    ApplyMsg message;
    message.CommandValid = true;
    message.CommandIndex = m_testApplyIndex_.fetch_add(1) + 1;
    message.Command = op.asString();
    return GetCommandFromRaft(message);
  }
  IKVEngine* EngineForTest() { return m_kvEngine.get(); }

  uint64_t ApproximateBytes() const { return m_approximateBytes.load(std::memory_order_relaxed); }
  void SetApproximateBytes(uint64_t bytes) { m_approximateBytes.store(bytes, std::memory_order_relaxed); }

  uint64_t RequestCount() const { return m_totalRequests.load(std::memory_order_relaxed); }
  void SetRequestCount(uint64_t count) { m_totalRequests.store(count, std::memory_order_relaxed); }

  uint64_t SplitCandidateGeneration() const {
    return m_splitCandidateGeneration.load(std::memory_order_relaxed);
  }
  void SetSplitCandidateGeneration(uint64_t gen) {
    m_splitCandidateGeneration.store(gen, std::memory_order_relaxed);
  }

  void SetSplitCandidateResolver(std::function<std::string(uint64_t)> resolver) {
    m_splitCandidateResolver = std::move(resolver);
  }
  std::string ResolveSplitCandidate(uint64_t generation) const;

  NodeTxnScheduler* NodeSchedulerForTest() const {
    const auto scheduler = m_nodeTxnScheduler.lock();
    return scheduler.get();
  }

  int TxnRegionId() const override { return m_regionId; }
  bool IsTxnLeader() override;
  PreparedMvccWrite PrepareTxn(const TxnCommand& command) override;
  PreparedMvccBatch PrepareTxnBatch(const TxnCommand& command) override;
  bool ProposeTxn(const Op& op, int* raftIndex) override;
  std::vector<std::pair<std::string, MvccLock>> ExpiredLocks(uint64_t currentPhysicalMs) override;

  void DprintfKVDB();

  bool ExecuteAppendOpOnKVDB(Op op);

  void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);

  bool ExecutePutOpOnKVDB(Op op);

  // Serves one point read through the Raft-backed KV state machine; the RPC
  // shape keeps server failover invisible to the caller.
  void Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply);

  // Consumes one committed entry delivered by the local Raft applier. This is
  // the state-machine apply path, unrelated to the point-read Get above.
  bool GetCommandFromRaft(ApplyMsg message);

  bool LinearizableReadBarrier(std::chrono::steady_clock::time_point deadline,
                               int* confirmedTerm = nullptr);
  void AdvanceStateMachineApplied(int raftIndex);
  void MarkStateMachineUnhealthy();
  bool OwnsKey(const std::string& key) const;

  bool ifRequestDuplicate(std::string ClientId, int RequestId);

  // clerk 使用RPC远程调用
  void PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply);

  ////一直等待raft传来的applyCh
  void ReadRaftApplyCommandLoop();

  void ReadSnapShotToInstall(std::string snapshot);

  bool SendMessageToWaitChan(const Op &op, const std::string& reqKey);

  WaitApplyQueue AcquireWaitApplyQueue(const std::string& reqKey);

  void ReleaseWaitApplyQueue(const std::string& reqKey, const WaitApplyQueue& queue);

  // Periodically compact applied Raft logs. Soft GC stays behind every
  // replicated peer; hard count/size limits may require snapshot catch-up.
  void RaftLogGcLoop();

  // Install a snapshot delivered through the Region apply queue.
  void GetSnapShotFromRaft(ApplyMsg message);

  std::string MakeSnapShot();
  void WriteRaftStatusLoop();

 public:  // for rpc
  void PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                 ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done);

  void Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
           ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done);

  void List(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::ListArgs *request,
            ::raftKVRpcProctoc::ListReply *response, ::google::protobuf::Closure *done);

  void TxnGet(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnGetArgs *request,
              ::raftKVRpcProctoc::TxnGetReply *response, ::google::protobuf::Closure *done);

  void TxnScan(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnScanArgs *request,
               ::raftKVRpcProctoc::TxnScanReply *response, ::google::protobuf::Closure *done);

  void TxnPrewrite(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnPrewriteArgs *request,
                   ::raftKVRpcProctoc::TxnPrewriteReply *response, ::google::protobuf::Closure *done);
  void TxnBatchPrewrite(google::protobuf::RpcController*, const ::raftKVRpcProctoc::TxnBatchPrewriteArgs*,
                        ::raftKVRpcProctoc::TxnBatchPrewriteReply*, google::protobuf::Closure*);

  void TxnCommit(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnCommitArgs *request,
                 ::raftKVRpcProctoc::TxnCommitReply *response, ::google::protobuf::Closure *done);
  void TxnBatchCommit(google::protobuf::RpcController*, const ::raftKVRpcProctoc::TxnBatchCommitArgs*,
                      ::raftKVRpcProctoc::TxnBatchCommitReply*, google::protobuf::Closure*);

  void TxnRollback(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnRollbackArgs *request,
                   ::raftKVRpcProctoc::TxnRollbackReply *response, ::google::protobuf::Closure *done);
  void TxnBatchRollback(google::protobuf::RpcController*, const ::raftKVRpcProctoc::TxnBatchRollbackArgs*,
                        ::raftKVRpcProctoc::TxnBatchRollbackReply*, google::protobuf::Closure*);

  void TxnGetLock(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnGetLockArgs *request,
                  ::raftKVRpcProctoc::TxnGetLockReply *response, ::google::protobuf::Closure *done);

  void TxnAcquirePessimisticLock(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnAcquirePessimisticLockArgs *request,
                                 ::raftKVRpcProctoc::TxnAcquirePessimisticLockReply *response, ::google::protobuf::Closure *done);

  void TxnCheckStatus(google::protobuf::RpcController *controller,
                      const ::raftKVRpcProctoc::TxnCheckStatusArgs *request,
                      ::raftKVRpcProctoc::TxnCheckStatusReply *response,
                      ::google::protobuf::Closure *done);

  void TxnResolveLock(google::protobuf::RpcController *controller,
                      const ::raftKVRpcProctoc::TxnResolveLockArgs *request,
                      ::raftKVRpcProctoc::TxnResolveLockReply *response,
                      ::google::protobuf::Closure *done);

  void TxnFindCommitTs(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnFindCommitTsArgs *request,
                       ::raftKVRpcProctoc::TxnFindCommitTsReply *response, ::google::protobuf::Closure *done);

  void TxnExpiredLocks(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnExpiredLocksArgs *request,
                       ::raftKVRpcProctoc::TxnExpiredLocksReply *response, ::google::protobuf::Closure *done);

  void TxnGarbageCollect(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnGarbageCollectArgs *request,
                         ::raftKVRpcProctoc::TxnGarbageCollectReply *response, ::google::protobuf::Closure *done);

  // Proposes an AdminSplit entry into this peer's Raft log. Leader-only: a
  // follower answers ErrWrongLeader and the caller rotates peers.
  void ProposeAdminSplit(google::protobuf::RpcController *controller,
                         const ::raftKVRpcProctoc::ProposeAdminSplitArgs *request,
                         ::raftKVRpcProctoc::ProposeAdminSplitReply *response,
                         ::google::protobuf::Closure *done);

  void ProposeConfChange(google::protobuf::RpcController *controller,
                         const ::raftKVRpcProctoc::ProposeConfChangeArgs *request,
                         ::raftKVRpcProctoc::ProposeConfChangeReply *response,
                         ::google::protobuf::Closure *done);

  void TxnMaxObservedTs(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnMaxObservedTsArgs *request,
                        ::raftKVRpcProctoc::TxnMaxObservedTsReply *response, ::google::protobuf::Closure *done);

  /////////////////serialiazation start ///////////////////////////////
  // notice ： func serialize
 private:
  friend class boost::serialization::access;

  // When the class Archive corresponds to an output archive, the
  // & operator is defined similar to <<.  Likewise, when the class Archive
  // is a type of input archive the & operator is defined similar to >>.
  template <class Archive>
  void serialize(Archive &ar, const unsigned int version)  //这里面写需要序列话和反序列化的字段
  {
    ar &m_serializedKVData;

    // ar & m_kvDB;
    ar &m_lastRequestId;
  }

  struct SnapshotPayload {
    std::string kvData;
    std::unordered_map<std::string, int> lastRequestId;

    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
      ar &kvData;
      ar &lastRequestId;
    }
  };

  std::string encodeSnapshotData(std::string kvData,
                                 std::unordered_map<std::string, int> lastRequestId) {
    SnapshotPayload payload{std::move(kvData), std::move(lastRequestId)};
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << payload;
    return "STRATAKV_REGION_SNAPSHOT_V2\n" + ss.str();
  }

  void parseFromString(const std::string &str) {
    static constexpr char kSnapshotV2Prefix[] = "STRATAKV_REGION_SNAPSHOT_V2\n";
    if (str.rfind(kSnapshotV2Prefix, 0) == 0) {
      SnapshotPayload payload;
      std::stringstream ss(str.substr(sizeof(kSnapshotV2Prefix) - 1));
      boost::archive::text_iarchive ia(ss);
      ia >> payload;
      m_serializedKVData = std::move(payload.kvData);
      m_lastRequestId = std::move(payload.lastRequestId);
    } else {
      // Read snapshots created before the V2 envelope was introduced.
      std::stringstream ss(str);
      boost::archive::text_iarchive ia(ss);
      ia >> *this;
    }
    if (!m_mvccStorage->RestoreSnapshot(m_serializedKVData)) {
      throw std::runtime_error("failed to restore MVCC state from Raft snapshot");
    }
    m_serializedKVData.clear();
  }

  /////////////////serialiazation end ///////////////////////////////
};

#endif  // STRATAKV_RAFT_REGION_PEER_H
