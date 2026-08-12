//
// Created by swx on 23-6-1.
//

#ifndef STRATAKV_RAFT_REGION_PEER_H
#define STRATAKV_RAFT_REGION_PEER_H

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <atomic>
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
class RegionPeer {
 private:
  std::mutex m_mtx;
  int m_me;
  int m_physicalNodeId = -1;
  int m_regionId = -1;
  std::shared_ptr<Raft> m_raftNode;
  std::shared_ptr<Persister> m_persister;
  std::shared_ptr<LockQueue<ApplyMsg> > applyChan;  // kvServer和raft节点的通信管道
  std::vector<std::pair<std::string, short>> m_peerAddresses;
  int m_maxRaftState;                               // snapshot if log grows this big

  // Your definitions here.
  std::string m_serializedKVData;  // todo ： 序列化后的kv数据，理论上可以不用，但是目前没有找到特别好的替代方法
  std::unique_ptr<IKVEngine> m_kvEngine;
  std::shared_ptr<MvccStorage> m_mvccStorage;
  std::unique_ptr<TxnScheduler> m_txnScheduler;

  using WaitApplyQueue = std::shared_ptr<LockQueue<Op>>;
  std::unordered_map<std::string, WaitApplyQueue> waitApplyCh;
  // reqKey -> chan  waitApplyCh是一个map，键是std::string，值 is Op类型的管道

  std::unordered_map<std::string, int> m_lastRequestId;  // clientid -> requestID  //一个kV服务器可能连接多个client

  std::atomic<uint64_t> m_prewritePrecheckConflicts{0};
  std::atomic<uint64_t> m_prewriteApplyConflicts{0};
  std::atomic<uint64_t> m_prewriteRaftProposals{0};
  std::atomic<uint64_t> m_txnRaftApplies{0};

  // last SnapShot point , raftIndex
  int m_lastSnapShotRaftLogIndex;

 public:
  RegionPeer() = delete;

  RegionPeer(int physicalNodeId, int regionId, int localPeerId, int maxraftstate,
             std::vector<std::pair<std::string, short>> peerAddresses);

  // Start Region-local Raft and apply workers after the NodeServer listener is
  // accepting RPCs. The call is non-blocking after initialization completes.
  void Start();
  int RegionId() const { return m_regionId; }
  Raft* RaftNode() const { return m_raftNode.get(); }

  void DprintfKVDB();

  void ExecuteAppendOpOnKVDB(Op op);

  void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);

  void ExecutePutOpOnKVDB(Op op);

  void Get(const raftKVRpcProctoc::GetArgs *args,
           raftKVRpcProctoc::GetReply
               *reply);  //将 GetArgs 改为rpc调用的，因为是远程客户端，即服务器宕机对客户端来说是无感的
  /**
   * 從raft節點中獲取消息  （不要誤以爲是執行【GET】命令）
   * @param message
   */
  void GetCommandFromRaft(ApplyMsg message);

  bool ifRequestDuplicate(std::string ClientId, int RequestId);

  // clerk 使用RPC远程调用
  void PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply);

  ////一直等待raft传来的applyCh
  void ReadRaftApplyCommandLoop();

  void ReadSnapShotToInstall(std::string snapshot);

  bool SendMessageToWaitChan(const Op &op, const std::string& reqKey);

  WaitApplyQueue AcquireWaitApplyQueue(const std::string& reqKey);

  void ReleaseWaitApplyQueue(const std::string& reqKey, const WaitApplyQueue& queue);

  // 检查是否需要制作快照，需要的话就向raft之下制作快照
  void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

  // Handler the SnapShot from kv.rf.applyCh
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

  void TxnCommit(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnCommitArgs *request,
                 ::raftKVRpcProctoc::TxnCommitReply *response, ::google::protobuf::Closure *done);

  void TxnRollback(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnRollbackArgs *request,
                   ::raftKVRpcProctoc::TxnRollbackReply *response, ::google::protobuf::Closure *done);

  void TxnGetLock(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnGetLockArgs *request,
                  ::raftKVRpcProctoc::TxnGetLockReply *response, ::google::protobuf::Closure *done);

  void TxnAcquirePessimisticLock(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnAcquirePessimisticLockArgs *request,
                                 ::raftKVRpcProctoc::TxnAcquirePessimisticLockReply *response, ::google::protobuf::Closure *done);

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
