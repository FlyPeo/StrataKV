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

bool IsNodeScheduledTxn(const std::string& operation) {
  return operation.rfind("TxnPrepared", 0) == 0 || operation == "TxnGarbageCollect";
}

std::chrono::steady_clock::time_point RequestDeadline(uint64_t remainingBudgetMs) {
  const uint64_t budget = remainingBudgetMs == 0
                              ? static_cast<uint64_t>(CONSENSUS_TIMEOUT)
                              : std::min<uint64_t>(remainingBudgetMs, CONSENSUS_TIMEOUT);
  return std::chrono::steady_clock::now() + std::chrono::milliseconds(budget);
}

raftKVRpcProctoc::TxnRecordStateProto ToProtoTxnState(TxnRecordState state) {
  switch (state) {
    case TxnRecordState::Locked: return raftKVRpcProctoc::TXN_RECORD_LOCKED;
    case TxnRecordState::Committed: return raftKVRpcProctoc::TXN_RECORD_COMMITTED;
    case TxnRecordState::RolledBack: return raftKVRpcProctoc::TXN_RECORD_ROLLED_BACK;
    case TxnRecordState::NotFound: return raftKVRpcProctoc::TXN_RECORD_NOT_FOUND;
  }
  return raftKVRpcProctoc::TXN_RECORD_NOT_FOUND;
}

TxnRecordState FromProtoTxnState(raftKVRpcProctoc::TxnRecordStateProto state) {
  switch (state) {
    case raftKVRpcProctoc::TXN_RECORD_LOCKED: return TxnRecordState::Locked;
    case raftKVRpcProctoc::TXN_RECORD_COMMITTED: return TxnRecordState::Committed;
    case raftKVRpcProctoc::TXN_RECORD_ROLLED_BACK: return TxnRecordState::RolledBack;
    case raftKVRpcProctoc::TXN_RECORD_NOT_FOUND: return TxnRecordState::NotFound;
  }
  return TxnRecordState::NotFound;
}

void FillLockReply(const MvccLock& lock, raftKVRpcProctoc::TxnGetLockReply* response) {
  response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
  response->set_haslock(true);
  response->set_primarykey(lock.primaryKey);
  response->set_value(lock.value);
  response->set_startts(lock.startTs);
  response->set_ttlms(lock.ttlMs);
  response->set_createtimems(lock.createTimeMs);
  response->set_isdelete(lock.isDelete);
  response->set_ispessimistic(lock.isPessimistic);
  response->set_forupdatets(lock.forUpdateTs);
  response->set_expireatphysicalms(lock.expireAtPhysicalMs);
  response->set_legacyexpiry(lock.legacyExpiry);
  response->set_islockonly(lock.isLockOnly);
}

}  // namespace

void RegionPeer::DprintfKVDB() {
  if (!Debug) {
    return;
  }
  std::lock_guard<std::mutex> lg(m_mtx);
  m_kvEngine->DebugPrint();
}

bool RegionPeer::ExecuteAppendOpOnKVDB(Op op) {
  // if op.IfDuplicate {   //get请求是可重复执行的，因此可以不用判复
  //	return
  // }
  {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (!m_kvEngine->Append(op.Key, op.Value)) return false;
    m_lastRequestId[op.ClientId] = op.RequestId;
  }

  //    DPrintf("[KVServerExeAPPEND-----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId,
  //    op.Key, op.Value)
  DprintfKVDB();
  return true;
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

bool RegionPeer::ExecutePutOpOnKVDB(Op op) {
  {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (!m_kvEngine->Put(op.Key, op.Value)) return false;
    m_lastRequestId[op.ClientId] = op.RequestId;
  }

  //    DPrintf("[KVServerExePUT----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId,
  //    op.Key, op.Value)
  DprintfKVDB();
  return true;
}

// 处理来自clerk的Get RPC
void RegionPeer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply) {
  if (!OwnsKey(args->key())) {
    reply->set_err(ErrWrongLeader);
    return;
  }
  int term = -1;
  if (!LinearizableReadBarrier(RequestDeadline(0), &term)) {
    reply->set_err(ErrWrongLeader);
    return;
  }

  std::string value;
  bool exists = false;
  {
    std::lock_guard<std::mutex> lock(m_mtx);
    exists = m_kvEngine->Get(args->key(), &value);
  }
  if (!m_raftNode->IsLeaderInTerm(term)) {
    reply->set_err(ErrWrongLeader);
    return;
  }
  reply->set_err(exists ? OK : ErrNoKey);
  reply->set_value(exists ? value : "");
}

bool RegionPeer::GetCommandFromRaft(ApplyMsg message) {
  Op op;
  if (!op.parseFromString(message.Command)) return false;
  std::string reqKey = op.ClientId + "_" + std::to_string(op.RequestId);

  DPrintf(
      "[RegionPeer::GetCommandFromRaft-kvserver{%d}] , Got Command --> ReqKey:{%s} , ClientId {%s}, RequestId {%d}, "
      "Opreation {%s}, Key :{%s}, Value :{%s}",
      m_me, reqKey.c_str(), op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(),
      op.Value.c_str());
  if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
    return true;
  }

  bool applicationSucceeded = true;
  // State Machine (KVServer solute the duplicate problem)
  // duplicate command will not be exed
  if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
    // execute command
    if (op.Operation == "Put") {
      applicationSucceeded = ExecutePutOpOnKVDB(op);
    } else if (op.Operation == "Append") {
      applicationSucceeded = ExecuteAppendOpOnKVDB(op);
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
    } else if (op.Operation.rfind("TxnPreparedBatch", 0) == 0) {
      PreparedMvccBatch prepared;
      TxnStatus status = TxnStatus::StorageError;
      if (prepared.Parse(op.Value)) {
        status = m_mvccStorage->ApplyPreparedBatch(prepared, message.CommandIndex);
      }
      op.Status = std::to_string(static_cast<int>(status));
      applicationSucceeded = status == TxnStatus::Ok;
      m_txnRaftApplies.fetch_add(1, std::memory_order_relaxed);
      if (applicationSucceeded) {
        std::lock_guard<std::mutex> lock(m_mtx);
        m_lastRequestId[op.ClientId] = op.RequestId;
      }
    } else if (op.Operation.rfind("TxnPrepared", 0) == 0) {
      PreparedMvccWrite prepared;
      TxnStatus status = TxnStatus::StorageError;
      if (prepared.Parse(op.Value)) {
        status = m_mvccStorage->ApplyPrepared(op.Key, prepared, message.CommandIndex);
      }
      applicationSucceeded = status == TxnStatus::Ok;
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
      TxnStatus status = TxnStatus::StorageError;
      if (!payload.parseFromString(op.Value)) {
        applicationSucceeded = false;
      } else if (payload.isDelete) {
        status = m_mvccStorage->PrewriteDelete(op.Key, payload.primaryKey, payload.startTs, payload.ttlMs);
      } else {
        status = m_mvccStorage->Prewrite(op.Key, payload.value, payload.primaryKey, payload.startTs, payload.ttlMs);
      }
      applicationSucceeded = status != TxnStatus::StorageError;
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
      const TxnStatus status = payload.parseFromString(op.Value)
                                   ? m_mvccStorage->Commit(op.Key, payload.startTs, payload.commitTs)
                                   : TxnStatus::StorageError;
      applicationSucceeded = status != TxnStatus::StorageError;
      op.Status = std::to_string(static_cast<int>(status));
      {
        std::lock_guard<std::mutex> lg(m_mtx);
        m_lastRequestId[op.ClientId] = op.RequestId;
      }
    } else if (op.Operation == "TxnRollback") {
      TxnOpPayload payload;
      const TxnStatus status = payload.parseFromString(op.Value)
                                   ? m_mvccStorage->Rollback(op.Key, payload.startTs)
                                   : TxnStatus::StorageError;
      applicationSucceeded = status != TxnStatus::StorageError;
      op.Status = std::to_string(static_cast<int>(status));
      {
        std::lock_guard<std::mutex> lg(m_mtx);
        m_lastRequestId[op.ClientId] = op.RequestId;
      }
    } else if (op.Operation == "TxnAcquirePessimisticLock") {
      TxnOpPayload payload;
      const TxnStatus status = payload.parseFromString(op.Value)
                                   ? m_mvccStorage->AcquirePessimisticLock(
                                         op.Key, payload.primaryKey, payload.startTs, payload.ttlMs)
                                   : TxnStatus::StorageError;
      applicationSucceeded = status != TxnStatus::StorageError;
      op.Status = std::to_string(static_cast<int>(status));
      {
        std::lock_guard<std::mutex> lg(m_mtx);
        m_lastRequestId[op.ClientId] = op.RequestId;
      }
    } else if (op.Operation == "TxnGarbageCollect") {
      TxnOpPayload payload;
      if (!payload.parseFromString(op.Value)) applicationSucceeded = false;
      const size_t count = applicationSucceeded ? m_mvccStorage->GarbageCollect(payload.startTs) : 0;
      op.Status = std::to_string(count);
      {
        std::lock_guard<std::mutex> lg(m_mtx);
        m_lastRequestId[op.ClientId] = op.RequestId;
      }
    } else if (op.Operation != "Get" && op.Operation != "RaftNoop") {
      applicationSucceeded = false;
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
  if (!applicationSucceeded) return false;
  //到这里kvDB已经制作了快照
  if (m_maxRaftState != -1) {
    IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    //如果raft的log太大（大于指定的比例）就把制作快照
  }

  // Transaction writes use the node-level pending-task table. Raw KV and the
  // current TxnGet read barrier keep the legacy per-Region wait channel.
  if (IsNodeScheduledTxn(op.Operation)) {
    const auto scheduler = m_nodeTxnScheduler.lock();
    if (scheduler) scheduler->OnApplied(m_regionId, op, message.CommandIndex);
  } else {
    SendMessageToWaitChan(op, reqKey);
  }
  return true;
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
  if (!OwnsKey(args->key())) {
    reply->set_err(ErrWrongLeader);
    return;
  }
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
      if (!GetCommandFromRaft(message)) {
        MarkStateMachineUnhealthy();
        return;
      }
      AdvanceStateMachineApplied(message.CommandIndex);
    }
    if (message.ProposalRejected) {
      Op rejected;
      if (rejected.parseFromString(message.Command) && IsNodeScheduledTxn(rejected.Operation)) {
        const auto scheduler = m_nodeTxnScheduler.lock();
        if (scheduler) scheduler->OnProposalRejected(m_regionId, rejected);
      }
    }
    if (message.SnapshotValid) {
      GetSnapShotFromRaft(message);
    }
  }
}

bool RegionPeer::LinearizableReadBarrier(std::chrono::steady_clock::time_point deadline,
                                         int* confirmedTerm) {
  const Raft::ReadIndexResult read = m_raftNode->ReadIndex(deadline);
  if (!read.ok()) return false;

  std::unique_lock<std::mutex> lock(m_applyProgressMutex);
  while (m_stateMachineHealthy && m_stateMachineAppliedIndex < read.readIndex) {
    const auto wakeAt = std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds(10));
    m_applyProgressCv.wait_until(lock, wakeAt);
    if (std::chrono::steady_clock::now() >= deadline) return false;
    lock.unlock();
    const bool sameLeader = m_raftNode->IsLeaderInTerm(read.term);
    lock.lock();
    if (!sameLeader) return false;
  }
  if (!m_stateMachineHealthy || m_stateMachineAppliedIndex < read.readIndex) return false;
  lock.unlock();
  if (!m_raftNode->IsLeaderInTerm(read.term)) return false;
  if (confirmedTerm) *confirmedTerm = read.term;
  return true;
}

void RegionPeer::AdvanceStateMachineApplied(int raftIndex) {
  {
    std::lock_guard<std::mutex> lock(m_applyProgressMutex);
    m_stateMachineAppliedIndex = std::max(m_stateMachineAppliedIndex, raftIndex);
  }
  m_applyProgressCv.notify_all();
}

void RegionPeer::MarkStateMachineUnhealthy() {
  {
    std::lock_guard<std::mutex> lock(m_applyProgressMutex);
    m_stateMachineHealthy = false;
  }
  m_applyProgressCv.notify_all();
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
  try {
    std::lock_guard<std::mutex> lg(m_mtx);
    // Raft has already accepted, persisted and advanced to an InstallSnapshot
    // before publishing this ApplyMsg. Calling CondInstallSnapshot again used
    // to reject the same index and silently skip the RocksDB restore.
    ReadSnapShotToInstall(message.Snapshot);
    m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
  } catch (const std::exception& error) {
    std::cerr << "failed to install Region " << m_regionId << " snapshot: " << error.what() << std::endl;
    MarkStateMachineUnhealthy();
    return;
  }
  AdvanceStateMachineApplied(message.SnapshotIndex);
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
  if (!request->allowfollowerread() && !LinearizableReadBarrier(RequestDeadline(0), &term)) {
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
  if (!request->allowfollowerread() && !m_raftNode->IsLeaderInTerm(term)) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
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
                       std::string regionStartKey, std::string regionEndKey,
                       std::vector<std::pair<std::string, short>> peerAddresses,
                       const std::shared_ptr<NodeTxnScheduler>& nodeTxnScheduler)
    : m_me(localPeerId),
      m_physicalNodeId(physicalNodeId),
      m_regionId(regionId),
      m_regionStartKey(std::move(regionStartKey)),
      m_regionEndKey(std::move(regionEndKey)),
      m_maxRaftState(maxraftstate),
      m_peerAddresses(std::move(peerAddresses)),
      m_nodeTxnScheduler(nodeTxnScheduler) {
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
  applyChan = std::make_shared<LockQueue<ApplyMsg>>();
  m_raftNode = std::make_shared<Raft>();
  m_lastSnapShotRaftLogIndex = 0;
}

bool RegionPeer::OwnsKey(const std::string& key) const {
  return key >= m_regionStartKey && (m_regionEndKey.empty() || key < m_regionEndKey);
}

bool RegionPeer::IsTxnLeader() {
  int term = -1;
  bool isLeader = false;
  m_raftNode->GetState(&term, &isLeader);
  return isLeader;
}

PreparedMvccWrite RegionPeer::PrepareTxn(const TxnCommand& command) {
  switch (command.type) {
    case TxnCommandType::Prewrite:
      if (command.isLockOnly) {
        return m_mvccStorage->PreparePrewriteLock(command.key, command.primaryKey,
                                                  command.startTs, command.ttlMs,
                                                  command.forUpdateTs);
      }
      return m_mvccStorage->PreparePrewrite(command.key, command.value, command.primaryKey,
                                            command.startTs, command.ttlMs, command.isDelete,
                                            command.forUpdateTs);
    case TxnCommandType::Commit:
      return m_mvccStorage->PrepareCommit(command.key, command.startTs, command.commitTs);
    case TxnCommandType::Rollback:
      return m_mvccStorage->PrepareRollback(command.key, command.startTs);
    case TxnCommandType::BatchPrewrite:
    case TxnCommandType::BatchCommit:
    case TxnCommandType::BatchRollback:
      return {};
    case TxnCommandType::PessimisticLock:
      return m_mvccStorage->PreparePessimisticLock(command.key, command.primaryKey,
                                                   command.startTs, command.ttlMs,
                                                   command.forUpdateTs,
                                                   command.expireAtPhysicalMs);
    case TxnCommandType::CheckTxnStatus:
      return m_mvccStorage->PrepareCheckTxnStatus(command.key, command.startTs,
                                                  command.currentPhysicalMs,
                                                  command.rollbackIfExpired);
    case TxnCommandType::ResolveLock:
      return m_mvccStorage->PrepareResolveLock(command.key, command.startTs,
                                               command.resolutionState,
                                               command.commitTs);
    case TxnCommandType::GarbageCollect:
      return {};
  }
  return {};
}

PreparedMvccBatch RegionPeer::PrepareTxnBatch(const TxnCommand& command) {
  switch (command.type) {
    case TxnCommandType::BatchPrewrite:
      return m_mvccStorage->PrepareBatchPrewrite(command.mutations, command.primaryKey,
                                                 command.startTs, command.ttlMs,
                                                 command.forUpdateTs);
    case TxnCommandType::BatchCommit:
      return m_mvccStorage->PrepareBatchCommit(command.keys, command.startTs, command.commitTs);
    case TxnCommandType::BatchRollback:
      return m_mvccStorage->PrepareBatchRollback(command.keys, command.startTs);
    default:
      return {};
  }
}

bool RegionPeer::ProposeTxn(const Op& op, int* raftIndex) {
  int term = 0;
  bool isLeader = false;
  m_raftNode->Start(op, raftIndex, &term, &isLeader);
  return isLeader;
}

std::vector<std::pair<std::string, MvccLock>> RegionPeer::ExpiredLocks(uint64_t currentPhysicalMs) {
  if (!IsTxnLeader()) return {};
  return m_mvccStorage->ExpiredLocks(currentPhysicalMs);
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
    const auto status = m_raftNode->GetStatus();
    m_lastSnapShotRaftLogIndex = status.lastApplied;
    AdvanceStateMachineApplied(status.lastApplied);
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
    int stateMachineAppliedIndex = 0;
    bool stateMachineHealthy = false;
    {
      std::lock_guard<std::mutex> lock(m_applyProgressMutex);
      stateMachineAppliedIndex = m_stateMachineAppliedIndex;
      stateMachineHealthy = m_stateMachineHealthy;
    }
    const auto nodeScheduler = m_nodeTxnScheduler.lock();
    const NodeTxnScheduler::Stats schedulerStats =
        nodeScheduler ? nodeScheduler->GetStats() : NodeTxnScheduler::Stats{};
    const std::string suffix = m_regionId < 0 ? std::to_string(m_me)
                                                : "region_" + std::to_string(m_regionId) + "_node_" +
                                                      std::to_string(m_physicalNodeId) + "_peer_" +
                                                      std::to_string(m_me);
    std::ofstream output("run_data/raft_status_" + suffix + ".json", std::ios::trunc);
    output << "{\"nodeId\":" << (m_regionId < 0 ? m_me : m_physicalNodeId) << ",\"regionId\":" << m_regionId
           << ",\"term\":" << status.term << ",\"isLeader\":"
           << (status.isLeader ? "true" : "false") << ",\"commitIndex\":" << status.commitIndex
           << ",\"lastApplied\":" << status.lastApplied << ",\"lastLogIndex\":" << status.lastLogIndex
           << ",\"stateMachineAppliedIndex\":" << stateMachineAppliedIndex
           << ",\"stateMachineHealthy\":" << (stateMachineHealthy ? "true" : "false")
           << ",\"readIndexRequests\":" << status.readIndexRequests
           << ",\"readIndexRounds\":" << status.readIndexRounds
           << ",\"readIndexCompleted\":" << status.readIndexCompleted
           << ",\"appendEntriesSent\":" << status.appendEntriesSent
           << ",\"raftPersistCount\":" << status.persistCount
           << ",\"mvccLockCount\":" << mvccStats.lockCount << ",\"mvccWriteCount\":" << mvccStats.writeCount
           << ",\"mvccDataVersionCount\":" << mvccStats.dataVersionCount
           << ",\"mvccWriteBatchCount\":" << mvccStats.writeBatchCount
           << ",\"mvccAppliedRaftIndex\":" << mvccStats.appliedRaftIndex
           << ",\"prewriteApplyConflicts\":" << m_prewriteApplyConflicts.load(std::memory_order_relaxed)
           << ",\"txnRaftApplies\":" << m_txnRaftApplies.load(std::memory_order_relaxed)
           << ",\"nodeTxnPrepareConflicts\":" << schedulerStats.prepareConflicts
           << ",\"nodeTxnRaftProposals\":" << schedulerStats.raftProposals
           << ",\"nodeTxnApplies\":" << schedulerStats.applied
           << ",\"latchAcquisitions\":" << schedulerStats.latches.acquisitions
           << ",\"latchWaits\":" << schedulerStats.latches.waits
           << ",\"latchWaitMicros\":" << schedulerStats.latches.waitMicros
           << ",\"latchCurrentWaiters\":" << schedulerStats.latches.currentWaiters
           << ",\"latchMaxWaiters\":" << schedulerStats.latches.maxWaiters
           << ",\"nodeTxnPendingTasks\":" << schedulerStats.pendingTasks
           << ",\"nodeTxnWorkerThreads\":" << schedulerStats.workerThreads
           << ",\"nodeTxnQueuedWorkers\":" << schedulerStats.queuedWorkers
           << ",\"nodeTxnActiveWorkers\":" << schedulerStats.activeWorkers
           << ",\"nodeTxnResponseTimeouts\":" << schedulerStats.responseTimeouts
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
  if (!OwnsKey(request->key())) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  int term = -1;
  if (!LinearizableReadBarrier(RequestDeadline(0), &term)) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }

  std::string value;
  const TxnStatus status = m_mvccStorage->Get(request->key(), request->readts(), &value);
  if (!m_raftNode->IsLeaderInTerm(term)) {
    response->set_err(ErrWrongLeader);
  } else {
    response->set_err(std::to_string(static_cast<int>(status)));
    response->set_value(value);
  }
  done->Run();
}

void RegionPeer::TxnPrewrite(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnPrewriteArgs *request,
                           ::raftKVRpcProctoc::TxnPrewriteReply *response, ::google::protobuf::Closure *done) {
  if (!OwnsKey(request->key())) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  const auto scheduler = m_nodeTxnScheduler.lock();
  if (!scheduler) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  if (request->maxforupdatets() != 0 &&
      request->protocolversion() < kPessimisticTxnProtocolVersion) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  TxnCommand command;
  command.type = TxnCommandType::Prewrite;
  command.regionId = m_regionId;
  command.key = request->key();
  command.keys = {command.key};
  command.value = request->value();
  command.primaryKey = request->primarykey();
  command.startTs = request->startts();
  command.ttlMs = request->ttlms();
  command.forUpdateTs = request->maxforupdatets();
  command.isDelete = request->isdelete();
  command.isLockOnly = request->islockonly();
  command.clientId = request->clientid();
  command.requestId = request->requestid();
  command.deadline = RequestDeadline(request->remainingbudgetms());
  scheduler->Schedule(std::move(command), [response, done](const TxnScheduleResult& result) {
    response->set_err(result.status);
    done->Run();
  });
}

void RegionPeer::TxnBatchPrewrite(google::protobuf::RpcController*,
                                  const ::raftKVRpcProctoc::TxnBatchPrewriteArgs* request,
                                  ::raftKVRpcProctoc::TxnBatchPrewriteReply* response,
                                  ::google::protobuf::Closure* done) {
  const auto scheduler = m_nodeTxnScheduler.lock();
  if (!scheduler || request->protocolversion() < kBatchTxnProtocolVersion ||
      request->mutations_size() <= 0 || request->mutations_size() > 10000) {
    response->set_err(scheduler ? std::to_string(static_cast<int>(TxnStatus::StorageError))
                                : ErrWrongLeader);
    done->Run();
    return;
  }
  TxnCommand command;
  command.type = TxnCommandType::BatchPrewrite;
  command.regionId = m_regionId;
  command.primaryKey = request->primarykey();
  command.startTs = request->startts();
  command.ttlMs = request->ttlms();
  command.forUpdateTs = request->maxforupdatets();
  command.clientId = request->clientid();
  command.requestId = request->requestid();
  command.deadline = RequestDeadline(request->remainingbudgetms());
  command.mutations.reserve(static_cast<size_t>(request->mutations_size()));
  for (const auto& mutation : request->mutations()) {
    if (!OwnsKey(mutation.key())) {
      response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
      done->Run();
      return;
    }
    command.mutations.push_back(
        {mutation.key(), mutation.value(), mutation.isdelete(), mutation.islockonly()});
  }
  std::sort(command.mutations.begin(), command.mutations.end(),
            [](const MvccMutation& lhs, const MvccMutation& rhs) { return lhs.key < rhs.key; });
  for (size_t index = 0; index < command.mutations.size(); ++index) {
    if (command.mutations[index].key.empty() ||
        (index != 0 && command.mutations[index - 1].key == command.mutations[index].key)) {
      response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
      done->Run();
      return;
    }
    command.keys.push_back(command.mutations[index].key);
  }
  scheduler->Schedule(std::move(command), [response, done](const TxnScheduleResult& result) {
    response->set_err(result.status);
    done->Run();
  });
}

void RegionPeer::TxnCommit(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnCommitArgs *request,
                         ::raftKVRpcProctoc::TxnCommitReply *response, ::google::protobuf::Closure *done) {
  if (!OwnsKey(request->key())) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  const auto scheduler = m_nodeTxnScheduler.lock();
  if (!scheduler) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  TxnCommand command;
  command.type = TxnCommandType::Commit;
  command.regionId = m_regionId;
  command.key = request->key();
  command.keys = {command.key};
  command.startTs = request->startts();
  command.commitTs = request->committs();
  command.clientId = request->clientid();
  command.requestId = request->requestid();
  command.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(CONSENSUS_TIMEOUT);
  scheduler->Schedule(std::move(command), [response, done](const TxnScheduleResult& result) {
    response->set_err(result.status);
    done->Run();
  });
}

void RegionPeer::TxnBatchCommit(google::protobuf::RpcController*,
                                const ::raftKVRpcProctoc::TxnBatchCommitArgs* request,
                                ::raftKVRpcProctoc::TxnBatchCommitReply* response,
                                ::google::protobuf::Closure* done) {
  const auto scheduler = m_nodeTxnScheduler.lock();
  if (!scheduler || request->protocolversion() < kBatchTxnProtocolVersion ||
      request->keys_size() <= 0 || request->keys_size() > 10000) {
    response->set_err(scheduler ? std::to_string(static_cast<int>(TxnStatus::StorageError))
                                : ErrWrongLeader);
    done->Run();
    return;
  }
  TxnCommand command;
  command.type = TxnCommandType::BatchCommit;
  command.regionId = m_regionId;
  command.keys.assign(request->keys().begin(), request->keys().end());
  if (std::any_of(command.keys.begin(), command.keys.end(),
                  [this](const std::string& key) { return key.empty() || !OwnsKey(key); })) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  std::sort(command.keys.begin(), command.keys.end());
  command.keys.erase(std::unique(command.keys.begin(), command.keys.end()), command.keys.end());
  command.key = command.keys.front();
  command.startTs = request->startts();
  command.commitTs = request->committs();
  command.clientId = request->clientid();
  command.requestId = request->requestid();
  command.deadline = RequestDeadline(request->remainingbudgetms());
  scheduler->Schedule(std::move(command), [response, done](const TxnScheduleResult& result) {
    response->set_err(result.status);
    done->Run();
  });
}

void RegionPeer::TxnRollback(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnRollbackArgs *request,
                           ::raftKVRpcProctoc::TxnRollbackReply *response, ::google::protobuf::Closure *done) {
  if (!OwnsKey(request->key())) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  const auto scheduler = m_nodeTxnScheduler.lock();
  if (!scheduler) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  TxnCommand command;
  command.type = TxnCommandType::Rollback;
  command.regionId = m_regionId;
  command.key = request->key();
  command.keys = {command.key};
  command.startTs = request->startts();
  command.clientId = request->clientid();
  command.requestId = request->requestid();
  command.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(CONSENSUS_TIMEOUT);
  scheduler->Schedule(std::move(command), [response, done](const TxnScheduleResult& result) {
    response->set_err(result.status);
    done->Run();
  });
}

void RegionPeer::TxnBatchRollback(google::protobuf::RpcController*,
                                  const ::raftKVRpcProctoc::TxnBatchRollbackArgs* request,
                                  ::raftKVRpcProctoc::TxnBatchRollbackReply* response,
                                  ::google::protobuf::Closure* done) {
  const auto scheduler = m_nodeTxnScheduler.lock();
  if (!scheduler || request->protocolversion() < kBatchTxnProtocolVersion ||
      request->keys_size() <= 0 || request->keys_size() > 10000) {
    response->set_err(scheduler ? std::to_string(static_cast<int>(TxnStatus::StorageError))
                                : ErrWrongLeader);
    done->Run();
    return;
  }
  TxnCommand command;
  command.type = TxnCommandType::BatchRollback;
  command.regionId = m_regionId;
  command.keys.assign(request->keys().begin(), request->keys().end());
  if (std::any_of(command.keys.begin(), command.keys.end(),
                  [this](const std::string& key) { return key.empty() || !OwnsKey(key); })) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  std::sort(command.keys.begin(), command.keys.end());
  command.keys.erase(std::unique(command.keys.begin(), command.keys.end()), command.keys.end());
  command.key = command.keys.front();
  command.startTs = request->startts();
  command.clientId = request->clientid();
  command.requestId = request->requestid();
  command.deadline = RequestDeadline(request->remainingbudgetms());
  scheduler->Schedule(std::move(command), [response, done](const TxnScheduleResult& result) {
    response->set_err(result.status);
    done->Run();
  });
}

void RegionPeer::TxnGetLock(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnGetLockArgs *request,
                          ::raftKVRpcProctoc::TxnGetLockReply *response, ::google::protobuf::Closure *done) {
  if (!OwnsKey(request->key())) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  int term = -1;
  if (!LinearizableReadBarrier(RequestDeadline(0), &term)) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  auto lockOpt = m_mvccStorage->GetLock(request->key());
  if (!m_raftNode->IsLeaderInTerm(term)) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  if (lockOpt) {
    FillLockReply(*lockOpt, response);
  } else {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::NotFound)));
    response->set_haslock(false);
  }
  done->Run();
}

void RegionPeer::TxnAcquirePessimisticLock(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnAcquirePessimisticLockArgs *request,
                                         ::raftKVRpcProctoc::TxnAcquirePessimisticLockReply *response, ::google::protobuf::Closure *done) {
  if (!OwnsKey(request->key())) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  const auto scheduler = m_nodeTxnScheduler.lock();
  if (!scheduler) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  if (request->protocolversion() < kPessimisticTxnProtocolVersion) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  TxnCommand command;
  command.type = TxnCommandType::PessimisticLock;
  command.regionId = m_regionId;
  command.key = request->key();
  command.keys = {command.key};
  command.primaryKey = request->primarykey();
  command.startTs = request->startts();
  command.ttlMs = request->ttlms();
  command.forUpdateTs = request->forupdatets();
  command.expireAtPhysicalMs = request->expireatphysicalms();
  command.clientId = request->clientid();
  command.requestId = request->requestid();
  command.deadline = RequestDeadline(request->remainingbudgetms());
  scheduler->Schedule(std::move(command), [response, done](const TxnScheduleResult& result) {
    response->set_err(result.status);
    response->set_found(result.readStatus == TxnStatus::Ok);
    response->set_value(result.value);
    response->set_valuecommitts(result.valueCommitTs);
    response->set_applied(result.applied);
    done->Run();
  });
}

void RegionPeer::TxnCheckStatus(google::protobuf::RpcController*,
                                const ::raftKVRpcProctoc::TxnCheckStatusArgs* request,
                                ::raftKVRpcProctoc::TxnCheckStatusReply* response,
                                ::google::protobuf::Closure* done) {
  if (!OwnsKey(request->primarykey())) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  const auto scheduler = m_nodeTxnScheduler.lock();
  if (!scheduler || request->protocolversion() < kPessimisticTxnProtocolVersion) {
    response->set_err(scheduler ? std::to_string(static_cast<int>(TxnStatus::StorageError))
                                : ErrWrongLeader);
    done->Run();
    return;
  }
  TxnCommand command;
  command.type = TxnCommandType::CheckTxnStatus;
  command.regionId = m_regionId;
  command.key = request->primarykey();
  command.keys = {command.key};
  command.startTs = request->startts();
  command.currentPhysicalMs = request->currentphysicalms();
  command.rollbackIfExpired = request->rollbackifexpired();
  command.clientId = request->clientid();
  command.requestId = request->requestid();
  command.deadline = RequestDeadline(request->remainingbudgetms());
  scheduler->Schedule(std::move(command), [response, done](const TxnScheduleResult& result) {
    response->set_err(result.status);
    response->set_state(ToProtoTxnState(result.txnRecordStatus.state));
    response->set_committs(result.txnRecordStatus.commitTs);
    if (result.txnRecordStatus.lock.has_value()) {
      FillLockReply(*result.txnRecordStatus.lock, response->mutable_lock());
    }
    response->set_applied(result.applied);
    done->Run();
  });
}

void RegionPeer::TxnResolveLock(google::protobuf::RpcController*,
                                const ::raftKVRpcProctoc::TxnResolveLockArgs* request,
                                ::raftKVRpcProctoc::TxnResolveLockReply* response,
                                ::google::protobuf::Closure* done) {
  if (!OwnsKey(request->key())) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  const auto scheduler = m_nodeTxnScheduler.lock();
  if (!scheduler || request->protocolversion() < kPessimisticTxnProtocolVersion) {
    response->set_err(scheduler ? std::to_string(static_cast<int>(TxnStatus::StorageError))
                                : ErrWrongLeader);
    done->Run();
    return;
  }
  TxnCommand command;
  command.type = TxnCommandType::ResolveLock;
  command.regionId = m_regionId;
  command.key = request->key();
  command.keys = {command.key};
  command.startTs = request->startts();
  command.resolutionState = FromProtoTxnState(request->decision());
  command.commitTs = request->committs();
  command.clientId = request->clientid();
  command.requestId = request->requestid();
  command.deadline = RequestDeadline(request->remainingbudgetms());
  scheduler->Schedule(std::move(command), [response, done](const TxnScheduleResult& result) {
    response->set_err(result.status);
    response->set_applied(result.applied);
    done->Run();
  });
}

void RegionPeer::TxnFindCommitTs(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnFindCommitTsArgs *request,
                               ::raftKVRpcProctoc::TxnFindCommitTsReply *response, ::google::protobuf::Closure *done) {
  if (!OwnsKey(request->key())) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  int term = -1;
  if (!LinearizableReadBarrier(RequestDeadline(0), &term)) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  auto commitTsOpt = m_mvccStorage->FindCommitTs(request->key(), request->startts());
  if (!m_raftNode->IsLeaderInTerm(term)) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
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
  if (!LinearizableReadBarrier(RequestDeadline(0), &term)) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  auto expired = m_mvccStorage->ExpiredLocks(request->nowms());
  if (!m_raftNode->IsLeaderInTerm(term)) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
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
    lockReply->set_forupdatets(pair.second.forUpdateTs);
    lockReply->set_expireatphysicalms(pair.second.expireAtPhysicalMs);
    lockReply->set_legacyexpiry(pair.second.legacyExpiry);
  }
  done->Run();
}

void RegionPeer::TxnGarbageCollect(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnGarbageCollectArgs *request,
                                 ::raftKVRpcProctoc::TxnGarbageCollectReply *response, ::google::protobuf::Closure *done) {
  const auto scheduler = m_nodeTxnScheduler.lock();
  if (!scheduler) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  TxnCommand command;
  command.type = TxnCommandType::GarbageCollect;
  command.latchMode = TxnLatchMode::RegionExclusive;
  command.regionId = m_regionId;
  command.safePointTs = request->safepointts();
  command.clientId = request->clientid();
  command.requestId = request->requestid();
  command.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(CONSENSUS_TIMEOUT);
  scheduler->Schedule(std::move(command), [response, done](const TxnScheduleResult& result) {
    response->set_err(result.status);
    if (result.status == std::to_string(static_cast<int>(TxnStatus::Ok))) {
      response->set_removedcount(result.removedCount);
    }
    done->Run();
  });
}

void RegionPeer::TxnMaxObservedTs(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnMaxObservedTsArgs *request,
                                ::raftKVRpcProctoc::TxnMaxObservedTsReply *response, ::google::protobuf::Closure *done) {
  int term = -1;
  if (!LinearizableReadBarrier(RequestDeadline(0), &term)) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  uint64_t maxTs = m_mvccStorage->MaxObservedTs();
  if (!m_raftNode->IsLeaderInTerm(term)) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
  response->set_maxts(maxTs);
  done->Run();
}
