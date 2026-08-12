#include "region_peer.h"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <stdexcept>

namespace {

std::string EscapeJson(const std::string& value) {
  std::string escaped;
  escaped.reserve(value.size());
  for (const unsigned char ch : value) {
    switch (ch) {
      case '"': escaped += "\\\""; break;
      case '\\': escaped += "\\\\"; break;
      case '\b': escaped += "\\b"; break;
      case '\f': escaped += "\\f"; break;
      case '\n': escaped += "\\n"; break;
      case '\r': escaped += "\\r"; break;
      case '\t': escaped += "\\t"; break;
      default:
        if (ch < 0x20) {
          char encoded[7];
          std::snprintf(encoded, sizeof(encoded), "\\u%04x", ch);
          escaped += encoded;
        } else {
          escaped += static_cast<char>(ch);
        }
    }
  }
  return escaped;
}

}  // namespace

void RegionPeer::DprintfKVDB() {
  if (!Debug) {
    return;
  }
  std::lock_guard<std::mutex> lg(m_mtx);
  m_kvEngine->DebugPrint();
}

void RegionPeer::ExecuteAppendOpOnKVDB(Op op) {
  // if op.IfDuplicate {   //get请求是可重复执行的，因此可以不用判复
  //	return
  // }
  {
    std::lock_guard<std::mutex> lg(m_mtx);
    m_kvEngine->Append(op.Key, op.Value);

    m_lastRequestId[op.ClientId] = op.RequestId;
  }

  //    DPrintf("[KVServerExeAPPEND-----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId,
  //    op.Key, op.Value)
  DprintfKVDB();
}

void RegionPeer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist) {
  {
    std::lock_guard<std::mutex> lg(m_mtx);
    *value = "";
    *exist = false;
    if (m_kvEngine->Get(op.Key, value)) {
      *exist = true;
    }
    m_lastRequestId[op.ClientId] = op.RequestId;
  }

  if (*exist) {
    //                DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, value :%v", op.ClientId,
    //                op.RequestId, op.Key, value)
  } else {
    //        DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, But No KEY!!!!", op.ClientId,
    //        op.RequestId, op.Key)
  }
  DprintfKVDB();
}

void RegionPeer::ExecutePutOpOnKVDB(Op op) {
  {
    std::lock_guard<std::mutex> lg(m_mtx);
    m_kvEngine->Put(op.Key, op.Value);
    m_lastRequestId[op.ClientId] = op.RequestId;
  }

  //    DPrintf("[KVServerExePUT----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId,
  //    op.Key, op.Value)
  DprintfKVDB();
}

// 处理来自clerk的Get RPC
void RegionPeer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply) {
  Op op;
  op.Operation = "Get";
  op.Key = args->key();
  op.Value = "";
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();

  // Register the waiter before publishing the proposal. On a fast local
  // cluster the entry can otherwise be applied between Start() and queue
  // creation, permanently losing the completion notification.
  std::string reqKey = op.ClientId + "_" + std::to_string(op.RequestId);
  auto chForRaftIndex = AcquireWaitApplyQueue(reqKey);
  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &_,
                    &isLeader);  // raftIndex：raft预计的logIndex
                                 // ，虽然是预计，但是正确情况下是准确的，op的具体内容对raft来说 是隔离的

  if (!isLeader) {
    ReleaseWaitApplyQueue(reqKey, chForRaftIndex);
    reply->set_err(ErrWrongLeader);
    return;
  }

  // timeout
  Op raftCommitOp;

  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    //        DPrintf("[GET TIMEOUT!!!]From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId,
    //        args.RequestId, kv.me, op.Key, raftIndex)
    // todo 2023年06月01日
    int _ = -1;
    bool isLeader = false;
    m_raftNode->GetState(&_, &isLeader);

    if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader) {
      //如果超时，代表raft集群不保证已经commitIndex该日志，但是如果是已经提交过的get请求，是可以再执行的。
      // 不会违反线性一致性
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist) {
        reply->set_err(OK);
        reply->set_value(value);
      } else {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    } else {
      reply->set_err(ErrWrongLeader);  //返回这个，其实就是让clerk换一个节点重试
    }
  } else {
    // raft已经提交了该command（op），可以正式开始执行了
    //         DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId
    //         %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key,
    //         op.Value)
    // todo 这里还要再次检验的原因：感觉不用检验，因为leader只要正确的提交了，那么这些肯定是符合的
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist) {
        reply->set_err(OK);
        reply->set_value(value);
      } else {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    } else {
      reply->set_err(ErrWrongLeader);
      //            DPrintf("[GET ] 不满足：raftCommitOp.ClientId{%v} == op.ClientId{%v} && raftCommitOp.RequestId{%v}
      //            == op.RequestId{%v}", raftCommitOp.ClientId, op.ClientId, raftCommitOp.RequestId, op.RequestId)
    }
  }
  ReleaseWaitApplyQueue(reqKey, chForRaftIndex);
}

void RegionPeer::GetCommandFromRaft(ApplyMsg message) {
  Op op;
  op.parseFromString(message.Command);
  std::string reqKey = op.ClientId + "_" + std::to_string(op.RequestId);

  DPrintf(
      "[RegionPeer::GetCommandFromRaft-kvserver{%d}] , Got Command --> ReqKey:{%s} , ClientId {%s}, RequestId {%d}, "
      "Opreation {%s}, Key :{%s}, Value :{%s}",
      m_me, reqKey.c_str(), op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(),
      op.Value.c_str());
  if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
    return;
  }

  // State Machine (KVServer solute the duplicate problem)
  // duplicate command will not be exed
  if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
    // execute command
    if (op.Operation == "Put") {
      ExecutePutOpOnKVDB(op);
    } else if (op.Operation == "Append") {
      ExecuteAppendOpOnKVDB(op);
    } else if (op.Operation == "TxnGet") {
      // A transaction read uses the Raft log as a linearizability barrier.  Keep
      // the snapshot timestamp inside the replicated command and return the
      // state-machine result through the apply channel; reading again in the
      // RPC handler can otherwise observe a different local state.
      TxnOpPayload payload;
      if (!payload.parseFromString(op.Value)) {
        op.Status = std::to_string(static_cast<int>(TxnStatus::StorageError));
        op.Value.clear();
      } else {
        std::string value;
        const TxnStatus status = m_mvccStorage->Get(op.Key, payload.startTs, &value);
        op.Status = std::to_string(static_cast<int>(status));
        op.Value = std::move(value);
      }
    } else if (op.Operation.rfind("TxnPrepared", 0) == 0) {
      PreparedMvccWrite prepared;
      TxnStatus status = TxnStatus::StorageError;
      if (prepared.Parse(op.Value)) {
        status = m_mvccStorage->ApplyPrepared(op.Key, prepared, message.CommandIndex);
      }
      op.Status = std::to_string(static_cast<int>(status));
      m_txnRaftApplies.fetch_add(1, std::memory_order_relaxed);
      if (op.Operation == "TxnPreparedPrewrite" &&
          (status == TxnStatus::LockConflict || status == TxnStatus::WriteConflict)) {
        m_prewriteApplyConflicts.fetch_add(1, std::memory_order_relaxed);
      }
      {
        std::lock_guard<std::mutex> lg(m_mtx);
        m_lastRequestId[op.ClientId] = op.RequestId;
      }
    } else if (op.Operation == "TxnPrewrite") {
      TxnOpPayload payload;
      payload.parseFromString(op.Value);
      TxnStatus status;
      if (payload.isDelete) {
        status = m_mvccStorage->PrewriteDelete(op.Key, payload.primaryKey, payload.startTs, payload.ttlMs);
      } else {
        status = m_mvccStorage->Prewrite(op.Key, payload.value, payload.primaryKey, payload.startTs, payload.ttlMs);
      }
      op.Status = std::to_string(static_cast<int>(status));
      m_txnRaftApplies.fetch_add(1, std::memory_order_relaxed);
      if (status == TxnStatus::LockConflict || status == TxnStatus::WriteConflict) {
        m_prewriteApplyConflicts.fetch_add(1, std::memory_order_relaxed);
      }
      {
        std::lock_guard<std::mutex> lg(m_mtx);
        m_lastRequestId[op.ClientId] = op.RequestId;
      }
    } else if (op.Operation == "TxnCommit") {
      TxnOpPayload payload;
      payload.parseFromString(op.Value);
      TxnStatus status = m_mvccStorage->Commit(op.Key, payload.startTs, payload.commitTs);
      op.Status = std::to_string(static_cast<int>(status));
      {
        std::lock_guard<std::mutex> lg(m_mtx);
        m_lastRequestId[op.ClientId] = op.RequestId;
      }
    } else if (op.Operation == "TxnRollback") {
      TxnOpPayload payload;
      payload.parseFromString(op.Value);
      TxnStatus status = m_mvccStorage->Rollback(op.Key, payload.startTs);
      op.Status = std::to_string(static_cast<int>(status));
      {
        std::lock_guard<std::mutex> lg(m_mtx);
        m_lastRequestId[op.ClientId] = op.RequestId;
      }
    } else if (op.Operation == "TxnAcquirePessimisticLock") {
      TxnOpPayload payload;
      payload.parseFromString(op.Value);
      TxnStatus status = m_mvccStorage->AcquirePessimisticLock(op.Key, payload.primaryKey, payload.startTs, payload.ttlMs);
      op.Status = std::to_string(static_cast<int>(status));
      {
        std::lock_guard<std::mutex> lg(m_mtx);
        m_lastRequestId[op.ClientId] = op.RequestId;
      }
    } else if (op.Operation == "TxnGarbageCollect") {
      TxnOpPayload payload;
      payload.parseFromString(op.Value);
      size_t count = m_mvccStorage->GarbageCollect(payload.startTs);
      op.Status = std::to_string(count);
      {
        std::lock_guard<std::mutex> lg(m_mtx);
        m_lastRequestId[op.ClientId] = op.RequestId;
      }
    }
  } else {
    if (op.Operation.rfind("TxnPrepared", 0) == 0) {
      op.Status = std::to_string(static_cast<int>(TxnStatus::Ok));
    } else if (op.Operation == "TxnPrewrite" || op.Operation == "TxnAcquirePessimisticLock") {
      TxnOpPayload payload;
      payload.parseFromString(op.Value);
      auto lock = m_mvccStorage->GetLock(op.Key);
      if (lock.has_value() && lock->startTs == payload.startTs) {
        op.Status = std::to_string(static_cast<int>(TxnStatus::Ok));
      } else {
        op.Status = std::to_string(static_cast<int>(TxnStatus::WriteConflict));
      }
    } else if (op.Operation == "TxnCommit") {
      TxnOpPayload payload;
      payload.parseFromString(op.Value);
      TxnStatus status = m_mvccStorage->Commit(op.Key, payload.startTs, payload.commitTs);
      op.Status = std::to_string(static_cast<int>(status));
    } else if (op.Operation == "TxnRollback") {
      TxnOpPayload payload;
      payload.parseFromString(op.Value);
      TxnStatus status = m_mvccStorage->Rollback(op.Key, payload.startTs);
      op.Status = std::to_string(static_cast<int>(status));
    } else if (op.Operation == "TxnGarbageCollect") {
      TxnOpPayload payload;
      payload.parseFromString(op.Value);
      size_t count = m_mvccStorage->GarbageCollect(payload.startTs);
      op.Status = std::to_string(count);
    }
  }
  //到这里kvDB已经制作了快照
  if (m_maxRaftState != -1) {
    IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    //如果raft的log太大（大于指定的比例）就把制作快照
  }

  // Send message to the chan of op.ClientId
  SendMessageToWaitChan(op, reqKey);
}

bool RegionPeer::ifRequestDuplicate(std::string ClientId, int RequestId) {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_lastRequestId.find(ClientId) == m_lastRequestId.end()) {
    return false;
    // todo :不存在这个client就创建
  }
  return RequestId <= m_lastRequestId[ClientId];
}

// get和put//append執行的具體細節是不一樣的
// PutAppend在收到raft消息之後執行，具體函數裏面只判斷冪等性（是否重複）
// get函數收到raft消息之後在，因爲get無論是否重複都可以再執行
void RegionPeer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply) {
  Op op;
  op.Operation = args->op();
  op.Key = args->key();
  op.Value = args->value();
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();
  std::string reqKey = op.ClientId + "_" + std::to_string(op.RequestId);
  auto chForRaftIndex = AcquireWaitApplyQueue(reqKey);
  int raftIndex = -1;
  int _ = -1;
  bool isleader = false;

  m_raftNode->Start(op, &raftIndex, &_, &isleader);

  if (!isleader) {
    ReleaseWaitApplyQueue(reqKey, chForRaftIndex);
    DPrintf(
        "[func -RegionPeer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , but "
        "not leader",
      m_me, args->clientid().c_str(), args->requestid(), m_me, op.Key.c_str(), raftIndex);

    reply->set_err(ErrWrongLeader);
    return;
  }
  DPrintf(
      "[func -RegionPeer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is "
      "leader ",
      m_me, args->clientid().c_str(), args->requestid(), m_me, op.Key.c_str(), raftIndex);
  // timeout
  Op raftCommitOp;

  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    DPrintf(
        "[func -RegionPeer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
      "ClientId %s, RequestId %d, Opreation %s Key :%s, Value :%s",
      m_me, m_me, raftIndex, op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(),
      op.Value.c_str());

    if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
      reply->set_err(OK);  // 超时了,但因为是重复的请求，返回ok，实际上就算没有超时，在真正执行的时候也要判断是否重复
    } else {
      reply->set_err(ErrWrongLeader);  ///这里返回这个的目的让clerk重新尝试
    }
  } else {
    DPrintf(
        "[func -RegionPeer::PutAppend -kvserver{%d}]WaitChanGetRaftApplyMessage<--Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %d, Opreation %s, Key :%s, Value :%s",
      m_me, m_me, raftIndex, op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(),
      op.Value.c_str());
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
      //可能发生leader的变更导致日志被覆盖，因此必须检查
      reply->set_err(OK);
    } else {
      reply->set_err(ErrWrongLeader);
    }
  }

  ReleaseWaitApplyQueue(reqKey, chForRaftIndex);
}

void RegionPeer::ReadRaftApplyCommandLoop() {
  while (true) {
    //如果只操作applyChan不用拿锁，因为applyChan自己带锁
    auto message = applyChan->Pop();  //阻塞弹出
    DPrintf(
        "---------------tmp-------------[func-RegionPeer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息",
        m_me);
    // listen to every command applied by its raft ,delivery to relative RPC Handler

    if (message.CommandValid) {
      GetCommandFromRaft(message);
    }
    if (message.SnapshotValid) {
      GetSnapShotFromRaft(message);
    }
  }
}

// raft会与persist层交互，kvserver层也会，因为kvserver层开始的时候需要恢复kvdb的状态
//  关于快照raft层与persist的交互：保存kvserver传来的snapshot；生成leaderInstallSnapshot RPC的时候也需要读取snapshot；
//  因此snapshot的具体格式是由kvserver层来定的，raft只负责传递这个东西
//  snapShot里面包含kvserver需要维护的persist_lastRequestId 以及kvDB真正保存的数据persist_kvdb
void RegionPeer::ReadSnapShotToInstall(std::string snapshot) {
  if (snapshot.empty()) {
    // bootstrap without any state?
    return;
  }
  parseFromString(snapshot);

  //    r := bytes.NewBuffer(snapshot)
  //    d := labgob.NewDecoder(r)
  //
  //    var persist_kvdb map[string]string  //理应快照
  //    var persist_lastRequestId map[int64]int //快照这个为了维护线性一致性
  //
  //    if d.Decode(&persist_kvdb) != nil || d.Decode(&persist_lastRequestId) != nil {
  //                DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!",kv.me)
  //        } else {
  //        kv.kvDB = persist_kvdb
  //        kv.lastRequestId = persist_lastRequestId
  //    }
}

bool RegionPeer::SendMessageToWaitChan(const Op &op, const std::string& reqKey) {
  WaitApplyQueue queue;
  {
    std::lock_guard<std::mutex> lg(m_mtx);
    const auto it = waitApplyCh.find(reqKey);
    if (it == waitApplyCh.end()) {
      return false;
    }
    queue = it->second;
  }
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> ReqKey:{%s} , ClientId {%s}, RequestId "
      "{%d}, Opreation {%s}, Key :{%s}, Value :{%s}",
      m_me, reqKey.c_str(), op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(), op.Value.c_str());

  queue->Push(op);
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> ReqKey:{%s} , ClientId {%s}, RequestId "
      "{%d}, Opreation {%s}, Key :{%s}, Value :{%s}",
      m_me, reqKey.c_str(), op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(), op.Value.c_str());
  return true;
}

RegionPeer::WaitApplyQueue RegionPeer::AcquireWaitApplyQueue(const std::string& reqKey) {
  std::lock_guard<std::mutex> lock(m_mtx);
  auto& queue = waitApplyCh[reqKey];
  if (!queue) {
    queue = std::make_shared<LockQueue<Op>>();
  }
  return queue;
}

void RegionPeer::ReleaseWaitApplyQueue(const std::string& reqKey, const WaitApplyQueue& queue) {
  std::lock_guard<std::mutex> lock(m_mtx);
  const auto it = waitApplyCh.find(reqKey);
  // A timed-out request can be retried with the same request ID. Do not erase
  // a newer retry's queue while releasing the older handler's shared owner.
  if (it != waitApplyCh.end() && it->second == queue) {
    waitApplyCh.erase(it);
  }
}

void RegionPeer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
  if (m_maxRaftState == -1) {
    return;
  }

  const double snapshotThreshold = m_maxRaftState * proportion / 10.0;
  if (m_raftNode->GetRaftStateSize() > snapshotThreshold) {
    // Send SnapShot Command
    auto snapshot = MakeSnapShot();
    m_raftNode->Snapshot(raftIndex, snapshot);
  }
}

void RegionPeer::GetSnapShotFromRaft(ApplyMsg message) {
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)) {
    ReadSnapShotToInstall(message.Snapshot);
    m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
  }
}

std::string RegionPeer::MakeSnapShot() {
  std::lock_guard<std::mutex> lg(m_mtx);
  std::string snapshotData = getSnapshotData();
  return snapshotData;
}

void RegionPeer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                         ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) {
  RegionPeer::PutAppend(request, response);
  done->Run();
}

void RegionPeer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
                   ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) {
  RegionPeer::Get(request, response);
  done->Run();
}

void RegionPeer::List(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::ListArgs *request,
                    ::raftKVRpcProctoc::ListReply *response, ::google::protobuf::Closure *done) {
  int term = -1;
  bool isLeader = false;
  m_raftNode->GetState(&term, &isLeader);
  if (!isLeader && !request->allowfollowerread()) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }

  constexpr size_t kDefaultListLimit = 100;
  constexpr size_t kMaxListLimit = 1000;
  const size_t requestedLimit = request->limit() == 0 ? kDefaultListLimit : request->limit();
  const size_t limit = std::min(requestedLimit, kMaxListLimit);
  std::vector<std::pair<std::string, std::string>> items;
  {
    std::lock_guard<std::mutex> lock(m_mtx);
    items = m_kvEngine->ScanPrefix(request->prefix());
  }

  for (size_t index = 0; index < items.size() && index < limit; ++index) {
    auto* entry = response->add_entries();
    entry->set_key(items[index].first);
    entry->set_value(items[index].second);
  }
  response->set_err(OK);
  done->Run();
}

RegionPeer::RegionPeer(int physicalNodeId, int regionId, int localPeerId, int maxraftstate,
                       std::vector<std::pair<std::string, short>> peerAddresses)
    : m_me(localPeerId),
      m_physicalNodeId(physicalNodeId),
      m_regionId(regionId),
      m_maxRaftState(maxraftstate),
      m_peerAddresses(std::move(peerAddresses)) {
  if (localPeerId < 0 || localPeerId >= static_cast<int>(m_peerAddresses.size())) {
    throw std::invalid_argument("local peer index is outside the Region peer list");
  }

  const std::string identity = "region" + std::to_string(regionId) + "_node" + std::to_string(physicalNodeId) +
                               "_peer" + std::to_string(localPeerId);
  m_persister = std::make_shared<Persister>(identity);

  const std::string dbPath = "run_data/rocksdb_" + identity;
  m_kvEngine = KVEngineFactory::Create(dbPath);
  std::shared_ptr<IKVEngine> engineShared(m_kvEngine.get(), [](IKVEngine*) {});
  m_mvccStorage = std::make_shared<MvccStorage>(engineShared);
  m_txnScheduler = std::make_unique<TxnScheduler>();
  applyChan = std::make_shared<LockQueue<ApplyMsg>>();
  m_raftNode = std::make_shared<Raft>();
  m_lastSnapShotRaftLogIndex = 0;
}

void RegionPeer::Start() {
  std::vector<std::shared_ptr<RaftRpcUtil>> servers;
  servers.reserve(m_peerAddresses.size());
  for (size_t peerIndex = 0; peerIndex < m_peerAddresses.size(); ++peerIndex) {
    if (static_cast<int>(peerIndex) == m_me) {
      servers.push_back(nullptr);
      continue;
    }
    servers.push_back(std::make_shared<RaftRpcUtil>(m_peerAddresses[peerIndex].first,
                                                   m_peerAddresses[peerIndex].second, m_regionId));
  }
  m_raftNode->init(std::move(servers), m_me, m_persister, applyChan);

  const auto snapshot = m_persister->ReadSnapshot();
  if (!snapshot.empty()) {
    ReadSnapShotToInstall(snapshot);
  }
  std::thread statusWriter(&RegionPeer::WriteRaftStatusLoop, this);
  statusWriter.detach();
  std::thread applyThread(&RegionPeer::ReadRaftApplyCommandLoop, this);
  applyThread.detach();
}

void RegionPeer::WriteRaftStatusLoop() {
  while (true) {
    const Raft::NodeStatus status = m_raftNode->GetStatus();
    const MvccStats mvccStats = m_mvccStorage->Stats();
    const TxnScheduler::Stats schedulerStats = m_txnScheduler->GetStats();
    const std::string suffix = m_regionId < 0 ? std::to_string(m_me)
                                                : "region_" + std::to_string(m_regionId) + "_node_" +
                                                      std::to_string(m_physicalNodeId) + "_peer_" +
                                                      std::to_string(m_me);
    std::ofstream output("run_data/raft_status_" + suffix + ".json", std::ios::trunc);
    output << "{\"nodeId\":" << (m_regionId < 0 ? m_me : m_physicalNodeId) << ",\"regionId\":" << m_regionId
           << ",\"term\":" << status.term << ",\"isLeader\":"
           << (status.isLeader ? "true" : "false") << ",\"commitIndex\":" << status.commitIndex
           << ",\"lastApplied\":" << status.lastApplied << ",\"lastLogIndex\":" << status.lastLogIndex
           << ",\"mvccLockCount\":" << mvccStats.lockCount << ",\"mvccWriteCount\":" << mvccStats.writeCount
           << ",\"mvccDataVersionCount\":" << mvccStats.dataVersionCount
           << ",\"mvccWriteBatchCount\":" << mvccStats.writeBatchCount
           << ",\"mvccAppliedRaftIndex\":" << mvccStats.appliedRaftIndex
           << ",\"prewritePrecheckConflicts\":" << m_prewritePrecheckConflicts.load(std::memory_order_relaxed)
           << ",\"prewriteApplyConflicts\":" << m_prewriteApplyConflicts.load(std::memory_order_relaxed)
           << ",\"prewriteRaftProposals\":" << m_prewriteRaftProposals.load(std::memory_order_relaxed)
           << ",\"txnRaftApplies\":" << m_txnRaftApplies.load(std::memory_order_relaxed)
           << ",\"latchAcquisitions\":" << schedulerStats.acquisitions
           << ",\"latchWaits\":" << schedulerStats.waits
           << ",\"latchWaitMicros\":" << schedulerStats.waitMicros
           << ",\"latchCurrentWaiters\":" << schedulerStats.currentWaiters
           << ",\"latchMaxWaiters\":" << schedulerStats.maxWaiters
           << "}";
    output.flush();

    const auto logEntries = m_raftNode->GetLogEntries(100);
    std::ofstream raftLogOutput("run_data/raft_log_" + suffix + ".json", std::ios::trunc);
    raftLogOutput << "{\"nodeId\":" << (m_regionId < 0 ? m_me : m_physicalNodeId) << ",\"regionId\":" << m_regionId
                  << ",\"term\":" << status.term << ",\"commitIndex\":" << status.commitIndex
                  << ",\"lastApplied\":" << status.lastApplied << ",\"entries\":[";
    for (size_t index = 0; index < logEntries.size(); ++index) {
      const auto& entry = logEntries[index];
      Op op;
      const bool parsed = op.parseFromString(entry.command());
      if (index != 0) {
        raftLogOutput << ',';
      }
      raftLogOutput << "{\"index\":" << entry.logindex() << ",\"term\":" << entry.logterm()
                    << ",\"committed\":" << (entry.logindex() <= status.commitIndex ? "true" : "false")
                    << ",\"parsed\":" << (parsed ? "true" : "false");
      if (parsed) {
        raftLogOutput << ",\"operation\":\"" << EscapeJson(op.Operation) << "\",\"key\":\""
                      << EscapeJson(op.Key) << "\",\"value\":\"" << EscapeJson(op.Value)
                      << "\",\"clientId\":\"" << EscapeJson(op.ClientId) << "\",\"requestId\":"
                      << op.RequestId;
      }
      raftLogOutput << '}';
    }
    raftLogOutput << "]}";
    raftLogOutput.flush();
    sleep(1);
  }
}

void RegionPeer::TxnGet(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnGetArgs *request,
                      ::raftKVRpcProctoc::TxnGetReply *response, ::google::protobuf::Closure *done) {
  Op op;
  op.Operation = "TxnGet";
  op.Key = request->key();
  op.ClientId = request->clientid();
  op.RequestId = request->requestid();
  TxnOpPayload payload;
  payload.startTs = request->readts();
  op.Value = payload.asString();

  std::string reqKey = op.ClientId + "_" + std::to_string(op.RequestId);
  auto chForRaftIndex = AcquireWaitApplyQueue(reqKey);
  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &_, &isLeader);

  if (!isLeader) {
    ReleaseWaitApplyQueue(reqKey, chForRaftIndex);
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }

  Op raftCommitOp;
  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    response->set_err(ErrWrongLeader);
  } else {
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
      response->set_err(raftCommitOp.Status);
      response->set_value(raftCommitOp.Value);
    } else {
      response->set_err(ErrWrongLeader);
    }
  }
  ReleaseWaitApplyQueue(reqKey, chForRaftIndex);
  done->Run();
}

void RegionPeer::TxnPrewrite(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnPrewriteArgs *request,
                           ::raftKVRpcProctoc::TxnPrewriteReply *response, ::google::protobuf::Closure *done) {
  int leaderTerm = -1;
  bool leaderNow = false;
  m_raftNode->GetState(&leaderTerm, &leaderNow);
  if (!leaderNow) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  auto schedulerGuard = m_txnScheduler->Acquire(request->key());
  // Leadership may have changed while this RPC waited for a hot-key latch.
  m_raftNode->GetState(&leaderTerm, &leaderNow);
  if (!leaderNow) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  const TxnStatus precheck = m_mvccStorage->PrecheckPrewrite(request->key(), request->startts());
  if (precheck != TxnStatus::Ok) {
    m_prewritePrecheckConflicts.fetch_add(1, std::memory_order_relaxed);
    response->set_err(std::to_string(static_cast<int>(precheck)));
    done->Run();
    return;
  }

  Op op;
  op.Operation = "TxnPreparedPrewrite";
  op.Key = request->key();
  op.ClientId = request->clientid();
  op.RequestId = request->requestid();

  const PreparedMvccWrite prepared = m_mvccStorage->PreparePrewrite(
      request->key(), request->value(), request->primarykey(), request->startts(), request->ttlms(),
      request->isdelete());
  if (prepared.status != TxnStatus::Ok) {
    m_prewritePrecheckConflicts.fetch_add(1, std::memory_order_relaxed);
    response->set_err(std::to_string(static_cast<int>(prepared.status)));
    done->Run();
    return;
  }
  if (!prepared.HasChanges()) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
    done->Run();
    return;
  }
  op.Value = prepared.Serialize();

  std::string reqKey = op.ClientId + "_" + std::to_string(op.RequestId);
  auto ch = AcquireWaitApplyQueue(reqKey);
  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;
  m_prewriteRaftProposals.fetch_add(1, std::memory_order_relaxed);
  m_raftNode->Start(op, &raftIndex, &_, &isLeader);

  if (!isLeader) {
    ReleaseWaitApplyQueue(reqKey, ch);
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }

  Op committedOp;
  if (!ch->timeOutPop(CONSENSUS_TIMEOUT, &committedOp)) {
    if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
      response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
    } else {
      response->set_err(ErrWrongLeader);
    }
  } else {
    if (committedOp.ClientId == op.ClientId && committedOp.RequestId == op.RequestId) {
      response->set_err(committedOp.Status);
    } else {
      response->set_err(ErrWrongLeader);
    }
  }

  ReleaseWaitApplyQueue(reqKey, ch);
  done->Run();
}

void RegionPeer::TxnCommit(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnCommitArgs *request,
                         ::raftKVRpcProctoc::TxnCommitReply *response, ::google::protobuf::Closure *done) {
  auto schedulerGuard = m_txnScheduler->Acquire(request->key());
  int leaderTerm = -1;
  bool leaderNow = false;
  m_raftNode->GetState(&leaderTerm, &leaderNow);
  if (!leaderNow) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  const PreparedMvccWrite prepared =
      m_mvccStorage->PrepareCommit(request->key(), request->startts(), request->committs());
  if (prepared.status != TxnStatus::Ok || !prepared.HasChanges()) {
    response->set_err(std::to_string(static_cast<int>(prepared.status)));
    done->Run();
    return;
  }
  Op op;
  op.Operation = "TxnPreparedCommit";
  op.Key = request->key();
  op.ClientId = request->clientid();
  op.RequestId = request->requestid();

  op.Value = prepared.Serialize();

  std::string reqKey = op.ClientId + "_" + std::to_string(op.RequestId);
  auto ch = AcquireWaitApplyQueue(reqKey);
  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &_, &isLeader);

  if (!isLeader) {
    ReleaseWaitApplyQueue(reqKey, ch);
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }

  Op committedOp;
  if (!ch->timeOutPop(CONSENSUS_TIMEOUT, &committedOp)) {
    response->set_err(ErrWrongLeader);
  } else {
    if (committedOp.ClientId == op.ClientId && committedOp.RequestId == op.RequestId) {
      response->set_err(committedOp.Status);
    } else {
      response->set_err(ErrWrongLeader);
    }
  }

  ReleaseWaitApplyQueue(reqKey, ch);
  done->Run();
}

void RegionPeer::TxnRollback(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnRollbackArgs *request,
                           ::raftKVRpcProctoc::TxnRollbackReply *response, ::google::protobuf::Closure *done) {
  auto schedulerGuard = m_txnScheduler->Acquire(request->key());
  int leaderTerm = -1;
  bool leaderNow = false;
  m_raftNode->GetState(&leaderTerm, &leaderNow);
  if (!leaderNow) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  const PreparedMvccWrite prepared = m_mvccStorage->PrepareRollback(request->key(), request->startts());
  if (prepared.status != TxnStatus::Ok || !prepared.HasChanges()) {
    response->set_err(std::to_string(static_cast<int>(prepared.status)));
    done->Run();
    return;
  }
  Op op;
  op.Operation = "TxnPreparedRollback";
  op.Key = request->key();
  op.ClientId = request->clientid();
  op.RequestId = request->requestid();

  op.Value = prepared.Serialize();

  std::string reqKey = op.ClientId + "_" + std::to_string(op.RequestId);
  auto ch = AcquireWaitApplyQueue(reqKey);
  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &_, &isLeader);

  if (!isLeader) {
    ReleaseWaitApplyQueue(reqKey, ch);
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }

  Op committedOp;
  if (!ch->timeOutPop(CONSENSUS_TIMEOUT, &committedOp)) {
    response->set_err(ErrWrongLeader);
  } else {
    if (committedOp.ClientId == op.ClientId && committedOp.RequestId == op.RequestId) {
      response->set_err(committedOp.Status);
    } else {
      response->set_err(ErrWrongLeader);
    }
  }

  ReleaseWaitApplyQueue(reqKey, ch);
  done->Run();
}

void RegionPeer::TxnGetLock(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnGetLockArgs *request,
                          ::raftKVRpcProctoc::TxnGetLockReply *response, ::google::protobuf::Closure *done) {
  int term = -1;
  bool isLeader = false;
  m_raftNode->GetState(&term, &isLeader);
  if (!isLeader) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  auto lockOpt = m_mvccStorage->GetLock(request->key());
  if (lockOpt) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
    response->set_haslock(true);
    response->set_primarykey(lockOpt->primaryKey);
    response->set_value(lockOpt->value);
    response->set_startts(lockOpt->startTs);
    response->set_ttlms(lockOpt->ttlMs);
    response->set_createtimems(lockOpt->createTimeMs);
    response->set_isdelete(lockOpt->isDelete);
    response->set_ispessimistic(lockOpt->isPessimistic);
  } else {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::NotFound)));
    response->set_haslock(false);
  }
  done->Run();
}

void RegionPeer::TxnAcquirePessimisticLock(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnAcquirePessimisticLockArgs *request,
                                         ::raftKVRpcProctoc::TxnAcquirePessimisticLockReply *response, ::google::protobuf::Closure *done) {
  auto schedulerGuard = m_txnScheduler->Acquire(request->key());
  int leaderTerm = -1;
  bool leaderNow = false;
  m_raftNode->GetState(&leaderTerm, &leaderNow);
  if (!leaderNow) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  const PreparedMvccWrite prepared = m_mvccStorage->PreparePessimisticLock(
      request->key(), request->primarykey(), request->startts(), request->ttlms());
  if (prepared.status != TxnStatus::Ok || !prepared.HasChanges()) {
    response->set_err(std::to_string(static_cast<int>(prepared.status)));
    done->Run();
    return;
  }
  Op op;
  op.Operation = "TxnPreparedPessimisticLock";
  op.Key = request->key();
  op.ClientId = request->clientid();
  op.RequestId = request->requestid();

  op.Value = prepared.Serialize();

  std::string reqKey = op.ClientId + "_" + std::to_string(op.RequestId);
  auto ch = AcquireWaitApplyQueue(reqKey);
  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &_, &isLeader);

  if (!isLeader) {
    ReleaseWaitApplyQueue(reqKey, ch);
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }

  Op committedOp;
  if (!ch->timeOutPop(CONSENSUS_TIMEOUT, &committedOp)) {
    if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
      response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
    } else {
      response->set_err(ErrWrongLeader);
    }
  } else {
    if (committedOp.ClientId == op.ClientId && committedOp.RequestId == op.RequestId) {
      response->set_err(committedOp.Status);
    } else {
      response->set_err(ErrWrongLeader);
    }
  }

  ReleaseWaitApplyQueue(reqKey, ch);
  done->Run();
}

void RegionPeer::TxnFindCommitTs(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnFindCommitTsArgs *request,
                               ::raftKVRpcProctoc::TxnFindCommitTsReply *response, ::google::protobuf::Closure *done) {
  int term = -1;
  bool isLeader = false;
  m_raftNode->GetState(&term, &isLeader);
  if (!isLeader) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  auto commitTsOpt = m_mvccStorage->FindCommitTs(request->key(), request->startts());
  if (commitTsOpt) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
    response->set_found(true);
    response->set_committs(*commitTsOpt);
  } else {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::NotFound)));
    response->set_found(false);
  }
  done->Run();
}

void RegionPeer::TxnExpiredLocks(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnExpiredLocksArgs *request,
                               ::raftKVRpcProctoc::TxnExpiredLocksReply *response, ::google::protobuf::Closure *done) {
  int term = -1;
  bool isLeader = false;
  m_raftNode->GetState(&term, &isLeader);
  if (!isLeader) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  auto expired = m_mvccStorage->ExpiredLocks(request->nowms());
  response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
  for (const auto& pair : expired) {
    response->add_keys(pair.first);
    auto* lockReply = response->add_locks();
    lockReply->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
    lockReply->set_haslock(true);
    lockReply->set_primarykey(pair.second.primaryKey);
    lockReply->set_value(pair.second.value);
    lockReply->set_startts(pair.second.startTs);
    lockReply->set_ttlms(pair.second.ttlMs);
    lockReply->set_createtimems(pair.second.createTimeMs);
    lockReply->set_isdelete(pair.second.isDelete);
    lockReply->set_ispessimistic(pair.second.isPessimistic);
  }
  done->Run();
}

void RegionPeer::TxnGarbageCollect(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnGarbageCollectArgs *request,
                                 ::raftKVRpcProctoc::TxnGarbageCollectReply *response, ::google::protobuf::Closure *done) {
  auto schedulerGuard = m_txnScheduler->AcquireRegionExclusive();
  Op op;
  op.Operation = "TxnGarbageCollect";
  op.ClientId = request->clientid();
  op.RequestId = request->requestid();

  TxnOpPayload payload;
  payload.startTs = request->safepointts();

  op.Value = payload.asString();

  std::string reqKey = op.ClientId + "_" + std::to_string(op.RequestId);
  auto ch = AcquireWaitApplyQueue(reqKey);
  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &_, &isLeader);

  if (!isLeader) {
    ReleaseWaitApplyQueue(reqKey, ch);
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }

  Op committedOp;
  if (!ch->timeOutPop(CONSENSUS_TIMEOUT, &committedOp)) {
    response->set_err(ErrWrongLeader);
  } else {
    if (committedOp.ClientId == op.ClientId && committedOp.RequestId == op.RequestId) {
      response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
      response->set_removedcount(std::stoull(committedOp.Status));
    } else {
      response->set_err(ErrWrongLeader);
    }
  }

  ReleaseWaitApplyQueue(reqKey, ch);
  done->Run();
}

void RegionPeer::TxnMaxObservedTs(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnMaxObservedTsArgs *request,
                                ::raftKVRpcProctoc::TxnMaxObservedTsReply *response, ::google::protobuf::Closure *done) {
  int term = -1;
  bool isLeader = false;
  m_raftNode->GetState(&term, &isLeader);
  if (!isLeader) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  uint64_t maxTs = m_mvccStorage->MaxObservedTs();
  response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
  response->set_maxts(maxTs);
  done->Run();
}
