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
#include <unordered_map>
#include <utility>
#include "kv_engine.h"
#include "kv_engine_factory.h"
#include "kv_server_rpc.pb.h"
#include "raft.h"
#include "mvcc_storage.h"
#include "txn_scheduler.h"

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

  RegionPeer(int physicalNodeId, int regionId, int localPeerId,
             RaftLogGcConfig raftLogGcConfig,
             std::string regionStartKey, std::string regionEndKey,
             std::vector<std::pair<std::string, short>> peerAddresses,
             const std::shared_ptr<NodeTxnScheduler>& nodeTxnScheduler);

  // Start Region-local Raft and apply workers after the NodeServer listener is
  // accepting RPCs. The call is non-blocking after initialization completes.
  void Start();
  int RegionId() const { return m_regionId; }
  Raft* RaftNode() const { return m_raftNode.get(); }
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

  void Get(const raftKVRpcProctoc::GetArgs *args,
           raftKVRpcProctoc::GetReply
               *reply);  //将 GetArgs 改为rpc调用的，因为是远程客户端，即服务器宕机对客户端来说是无感的
  /**
   * 從raft節點中獲取消息  （不要誤以爲是執行【GET】命令）
   * @param message
   */
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

  std::string getSnapshotData() {
    m_serializedKVData = m_kvEngine->Dump();
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << *this;
    m_serializedKVData.clear();
    return ss.str();
  }

  void parseFromString(const std::string &str) {
    std::stringstream ss(str);
    boost::archive::text_iarchive ia(ss);
    ia >> *this;
    if (!m_mvccStorage->RestoreSnapshot(m_serializedKVData)) {
      throw std::runtime_error("failed to restore MVCC state from Raft snapshot");
    }
    m_serializedKVData.clear();
  }

  /////////////////serialiazation end ///////////////////////////////
};

#endif  // STRATAKV_RAFT_REGION_PEER_H
