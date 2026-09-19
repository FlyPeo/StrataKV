#include "region_peer.h"
#include "region_request_validator.h"

#include <filesystem>
#include <fstream>
#include <sstream>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <stdexcept>

namespace {

template <typename Reply>
void PopulateKeyNotInRegion(const RegionPeer* peer, Reply* response) {
  response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
  RegionValidationResult validation{stratakv::region::REGION_ERROR_KEY_NOT_IN_REGION,
                                   "key not in region"};
  const RegionMetadata descriptor = peer->Descriptor();
  PopulateRegionError(validation, &descriptor, response->mutable_header());
}

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

void DeleteKeysInRange(IKVEngine* engine, const std::string& rangeBegin,
                       const std::string& rangeEnd) {
  if (engine == nullptr) return;

  // 1. Delete MVCC prefixes: data/, lock/, write/, meta/revision/
  static const std::vector<std::pair<std::string, std::string>> prefixes = {
      {"data/", "data0"},
      {"lock/", "lock0"},
      {"write/", "write0"},
      {"meta/revision/", "meta/revision0"},
  };
  for (const auto& [prefix, prefixEnd] : prefixes) {
    const std::string start = rangeBegin.empty() ? prefix : prefix + rangeBegin;
    const std::string end = rangeEnd.empty() ? prefixEnd : prefix + rangeEnd;
    engine->DeleteRange(start, end);
  }

  // 2. Delete raw user keys (for non-txn operations) that fall in [rangeBegin, rangeEnd)
  // and do not belong to internal system prefixes.
  auto isSystemKey = [](const std::string& key) {
    return key.rfind("data/", 0) == 0 || key.rfind("lock/", 0) == 0 ||
           key.rfind("write/", 0) == 0 || key.rfind("meta/", 0) == 0;
  };
  std::vector<std::string> rawKeysToDelete;
  for (const auto& item : engine->ScanPrefix("")) {
    if (isSystemKey(item.first)) continue;
    if (item.first >= rangeBegin && (rangeEnd.empty() || item.first < rangeEnd)) {
      rawKeysToDelete.push_back(item.first);
    }
  }
  for (const auto& k : rawKeysToDelete) {
    engine->Delete(k);
  }
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
  {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (!m_kvEngine->Append(op.Key, op.Value)) return false;
    m_lastRequestId[op.ClientId] = op.RequestId;
  }

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

  DprintfKVDB();
}

bool RegionPeer::ExecutePutOpOnKVDB(Op op) {
  {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (!m_kvEngine->Put(op.Key, op.Value)) return false;
    m_lastRequestId[op.ClientId] = op.RequestId;
  }

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
    } else if (op.Operation == "AdminSplit") {
      std::cerr << "{\"trace\":\"apply_admin_split\",\"region\":" << m_regionId
                << ",\"index\":" << message.CommandIndex << "}" << std::endl;
      stratakv::region::AdminSplitCommand split;
      const TxnStatus splitStatus = split.ParseFromString(op.Value)
                                        ? ApplyAdminSplit(split)
                                        : TxnStatus::StorageError;
      op.Status = std::to_string(static_cast<int>(splitStatus));
      m_lastRequestId[op.ClientId] = op.RequestId;
    } else if (op.Operation == "ConfChange") {
      stratakv::region::ConfChangeCommand confChange;
      const TxnStatus confStatus = confChange.ParseFromString(op.Value)
                                       ? ApplyConfChange(confChange)
                                       : TxnStatus::StorageError;
      op.Status = std::to_string(static_cast<int>(confStatus));
      m_lastRequestId[op.ClientId] = op.RequestId;
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
  while (!m_stopRequested.load(std::memory_order_acquire)) {
    //如果只操作applyChan不用拿锁，因为applyChan自己带锁
    auto message = applyChan->Pop();  //阻塞弹出
    if (m_stopRequested.load(std::memory_order_acquire)) break;
    DPrintf(
        "---------------tmp-------------[func-RegionPeer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息",
        m_me);
    // listen to every command applied by its raft ,delivery to relative RPC Handler

    if (message.CommandValid) {
      std::lock_guard<std::mutex> executionLock(m_stateMachineExecutionMutex);
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
      std::lock_guard<std::mutex> executionLock(m_stateMachineExecutionMutex);
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

void RegionPeer::RaftLogGcLoop() {
  bool firstRun = true;
  while (!m_stopRequested.load(std::memory_order_acquire)) {
    auto delay = m_raftLogGcConfig.tickInterval;
    if (firstRun) {
      // Nine Region peers otherwise begin full-state serialization together.
      // Spread their first pass across one tick to avoid periodic CPU and disk
      // bursts; later passes naturally retain the offset.
      const int slot = std::max(0, m_physicalNodeId) * 3 + (m_regionId % 100);
      delay += m_raftLogGcConfig.tickInterval * slot / 9;
      firstRun = false;
    }
    {
      std::unique_lock<std::mutex> stopLock(m_stopMutex);
      if (m_stopCondition.wait_for(stopLock, delay, [this] {
            return m_stopRequested.load(std::memory_order_acquire);
          })) {
        break;
      }
    }

    RaftLogGcDecision decision;
    std::unique_ptr<IKVSnapshot> kvSnapshot;
    std::unordered_map<std::string, int> lastRequestId;
    int appliedIndex = 0;
    bool healthy = false;
    {
      // Capture the RocksDB sequence and request de-duplication map at one
      // applied-index boundary. RocksDB keeps that view stable while the slow
      // scan and serialization continue without blocking state-machine apply.
      std::lock_guard<std::mutex> executionLock(m_stateMachineExecutionMutex);
      {
        std::lock_guard<std::mutex> progressLock(m_applyProgressMutex);
        appliedIndex = m_stateMachineAppliedIndex;
        healthy = m_stateMachineHealthy;
      }
      if (!healthy) continue;

      decision = m_raftNode->EvaluateLogGc(appliedIndex, m_raftLogGcConfig);
      if (!decision.ShouldGc()) continue;

      kvSnapshot = m_kvEngine->CaptureSnapshot();
      {
        std::lock_guard<std::mutex> stateLock(m_mtx);
        lastRequestId = m_lastRequestId;
      }
    }

    const auto gcStartedAt = std::chrono::steady_clock::now();
    std::string snapshot = encodeSnapshotData(kvSnapshot->Serialize(), std::move(lastRequestId));
    if (!m_raftNode->Snapshot(decision.compactIndex, std::move(snapshot))) continue;
    const uint64_t gcDurationMicros = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - gcStartedAt)
            .count());

    {
      // Apply reads this boundary under the execution mutex. An incoming
      // snapshot may have advanced it while the local GC was serializing.
      std::lock_guard<std::mutex> executionLock(m_stateMachineExecutionMutex);
      m_lastSnapShotRaftLogIndex = std::max(m_lastSnapShotRaftLogIndex, decision.compactIndex);
    }
    m_raftLogGcRuns.fetch_add(1, std::memory_order_relaxed);
    m_raftLogGcReclaimedEntries.fetch_add(decision.reclaimableCount,
                                          std::memory_order_relaxed);
    m_raftLogGcLastDurationMicros.store(gcDurationMicros, std::memory_order_relaxed);
    m_raftLogGcTotalDurationMicros.fetch_add(gcDurationMicros, std::memory_order_relaxed);
    uint64_t previousMax = m_raftLogGcMaxDurationMicros.load(std::memory_order_relaxed);
    while (previousMax < gcDurationMicros &&
           !m_raftLogGcMaxDurationMicros.compare_exchange_weak(
               previousMax, gcDurationMicros, std::memory_order_relaxed)) {
    }
    if (decision.Forced()) {
      m_raftLogGcForcedRuns.fetch_add(1, std::memory_order_relaxed);
    } else {
      m_raftLogGcSoftRuns.fetch_add(1, std::memory_order_relaxed);
    }
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
  std::unique_ptr<IKVSnapshot> kvSnapshot;
  std::unordered_map<std::string, int> lastRequestId;
  {
    std::lock_guard<std::mutex> executionLock(m_stateMachineExecutionMutex);
    kvSnapshot = m_kvEngine->CaptureSnapshot();
    std::lock_guard<std::mutex> stateLock(m_mtx);
    lastRequestId = m_lastRequestId;
  }
  return encodeSnapshotData(kvSnapshot->Serialize(), std::move(lastRequestId));
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

RegionPeer::RegionPeer(int physicalNodeId, RegionMetadata descriptor, int localPeerId,
                       RaftLogGcConfig raftLogGcConfig,
                       const std::shared_ptr<NodeTxnScheduler>& nodeTxnScheduler)
    : m_me(localPeerId),
      m_physicalNodeId(physicalNodeId),
      m_regionId(descriptor.regionId),
      m_regionStartKey(descriptor.startKey),
      m_regionEndKey(descriptor.endKey),
      m_descriptor(std::move(descriptor)),
      m_raftLogGcConfig(std::move(raftLogGcConfig)),
      m_nodeTxnScheduler(nodeTxnScheduler) {
  m_peerAddresses.reserve(m_descriptor.peers.size());
  for (const auto& peer : m_descriptor.peers) {
    m_peerAddresses.emplace_back(peer.host, peer.port);
  }
  if (localPeerId < 0 || localPeerId >= static_cast<int>(m_peerAddresses.size())) {
    throw std::invalid_argument("local peer index is outside the Region peer list");
  }

  const std::string identity = "region" + std::to_string(m_regionId) + "_node" + std::to_string(physicalNodeId) +
                               "_peer" + std::to_string(localPeerId);
  m_persister = std::make_shared<Persister>(identity);

  m_dbPath = "run_data/rocksdb_" + identity;
  m_splitStatePath = "splitstate_" + identity + ".json";
  m_kvEngine = KVEngineFactory::Create(m_dbPath);
  std::shared_ptr<IKVEngine> engineShared(m_kvEngine.get(), [](IKVEngine*) {});
  m_mvccStorage = std::make_shared<MvccStorage>(engineShared);
  applyChan = std::make_shared<LockQueue<ApplyMsg>>();
  m_raftNode = std::make_shared<Raft>();
  m_lastSnapShotRaftLogIndex = 0;
}

RegionPeer::~RegionPeer() { Stop(); }

bool RegionPeer::TryAcquireRequest() {
  if (LifecycleState() != RegionPeerState::Serving) return false;
  m_inFlightRequests.fetch_add(1, std::memory_order_acq_rel);
  m_totalRequests.fetch_add(1, std::memory_order_relaxed);
  if (LifecycleState() == RegionPeerState::Serving) return true;
  ReleaseRequest();
  return false;
}

void RegionPeer::MarkServing() {
  RegionPeerState expected = RegionPeerState::Initializing;
  if (!m_lifecycle.compare_exchange_strong(expected, RegionPeerState::Serving,
                                           std::memory_order_acq_rel)) {
    throw std::logic_error("Region peer can only serve after initialization");
  }
}

void RegionPeer::ReleaseRequest() {
  const uint64_t previous = m_inFlightRequests.fetch_sub(1, std::memory_order_acq_rel);
  if (previous == 0) std::abort();
  if (previous == 1) m_lifecycleCondition.notify_all();
}

bool RegionPeer::BeginRetire() {
  RegionPeerState expected = RegionPeerState::Serving;
  if (m_lifecycle.compare_exchange_strong(expected, RegionPeerState::Retiring,
                                          std::memory_order_acq_rel)) {
    return true;
  }
  return expected == RegionPeerState::Retiring || expected == RegionPeerState::Stopped;
}

bool RegionPeer::WaitForDrain(std::chrono::steady_clock::time_point deadline,
                              uint64_t* remainingRequests) {
  std::unique_lock<std::mutex> lock(m_lifecycleMutex);
  const bool drained = m_lifecycleCondition.wait_until(lock, deadline, [this] {
    return m_inFlightRequests.load(std::memory_order_acquire) == 0;
  });
  if (remainingRequests) {
    *remainingRequests = m_inFlightRequests.load(std::memory_order_acquire);
  }
  return drained;
}

void RegionPeer::MarkStopped() {
  if (LifecycleState() == RegionPeerState::Stopped) return;
  if (LifecycleState() != RegionPeerState::Retiring || InFlightRequests() != 0) {
    throw std::logic_error("Region peer cannot stop before retirement drains");
  }
  m_lifecycle.store(RegionPeerState::Stopped, std::memory_order_release);
}

namespace {

std::string HexEncode(const std::string& bytes) {
  static const char* digits = "0123456789abcdef";
  std::string out;
  out.reserve(bytes.size() * 2);
  for (unsigned char byte : bytes) {
    out.push_back(digits[byte >> 4]);
    out.push_back(digits[byte & 0x0F]);
  }
  return out;
}

std::string HexDecode(const std::string& hex) {
  auto value = [](char c) -> int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
  };
  if (hex.size() % 2 != 0) return {};
  std::string out;
  out.reserve(hex.size() / 2);
  for (size_t i = 0; i < hex.size(); i += 2) {
    const int high = value(hex[i]);
    const int low = value(hex[i + 1]);
    if (high < 0 || low < 0) return {};
    out.push_back(static_cast<char>((high << 4) | low));
  }
  return out;
}

constexpr int kSplitPhaseCheckpointing = 1;
constexpr int kSplitPhaseChildReady = 2;
constexpr int kSplitPhaseParentShrunk = 3;
constexpr int kSplitPhaseComplete = 4;

}  // namespace

RegionMetadata RegionPeer::Descriptor() const {
  std::shared_lock<std::shared_mutex> lock(m_descriptorMutex);
  return m_descriptor;
}

RegionPeerLocation RegionPeer::LocalPeerDescriptor() const {
  std::shared_lock<std::shared_mutex> lock(m_descriptorMutex);
  return m_descriptor.peers.at(static_cast<size_t>(m_me));
}

void RegionPeer::UpdateDescriptorShrunk(const RegionMetadata& shrunken) {
  std::unique_lock<std::shared_mutex> lock(m_descriptorMutex);
  if (shrunken.regionId != m_descriptor.regionId ||
      shrunken.epoch.version <= m_descriptor.epoch.version) {
    throw std::logic_error("Region descriptor shrink must advance the epoch");
  }
  m_descriptor = shrunken;
  m_regionEndKey = shrunken.endKey;
  if (m_raftNode) {
    m_raftNode->SetEpoch(shrunken.epoch);
  }
}

void RegionPeer::WriteSplitStateLocked(int phase, uint64_t generation,
                                       const stratakv::region::AdminSplitCommand& command) const {
  std::string blob;
  command.SerializeToString(&blob);
  std::ostringstream out;
  out << "{\"phase\":" << phase << ",\"generation\":" << generation
      << ",\"split_hex\":\"" << HexEncode(blob) << "\"}";
  const std::string temporary = m_splitStatePath + ".tmp";
  {
    std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
    output.write(out.str().data(), static_cast<std::streamsize>(out.str().size()));
    output.flush();
    if (!output.good()) throw std::runtime_error("unable to persist split state");
  }
  std::filesystem::rename(temporary, m_splitStatePath);
}

// Background materialization: everything heavy (checkpoint clone, child
// directory rename/cleanup, out-of-range parent deletion) runs off the apply
// thread so Raft heartbeats/replication are never stalled by split I/O. The
// descriptor shrink happens synchronously at the apply position; the deferred
// steps only move data that is already unreachable.
void RegionPeer::MaterializeSplitChildAsync(
    const stratakv::region::AdminSplitCommand& command, int persistedPhase,
    RegionMetadata parent, RegionMetadata child) {
  std::lock_guard<std::mutex> splitLock(m_splitMutex);
  std::cerr << "{\"trace\":\"materialize_begin\",\"region\":" << m_regionId
            << ",\"persisted\":" << persistedPhase << "}" << std::endl;
  auto stepTrace = [&](const char* step) {
    std::cerr << "{\"trace\":\"mat_step\",\"region\":" << m_regionId
              << ",\"step\":\"" << step << "\"}" << std::endl;
  };
  const uint64_t generation =
      persistedPhase > 0 ? static_cast<uint64_t>(persistedPhase) : 1;
  const std::string childBase = "run_data/rocksdb_region" +
      std::to_string(command.child().regionid()) + "_node" +
      std::to_string(m_physicalNodeId) + "_peer" + std::to_string(m_me);
  const std::string pendingDir = childBase + "-gen" + std::to_string(generation) + ".pending";
  const std::string finalDir = childBase;

  try {
    if (persistedPhase < kSplitPhaseChildReady) {
      std::error_code fsError;
      stepTrace("remove_pending");
      std::filesystem::remove_all(pendingDir, fsError);
      m_splitPhase.store(kSplitPhaseCheckpointing, std::memory_order_release);
      stepTrace("create_checkpoint");
      if (!m_kvEngine->CreateCheckpoint(pendingDir)) {
        std::cerr << "{\"level\":\"error\",\"component\":\"region_peer\","
                     "\"event\":\"split_checkpoint_failed\",\"region\":" << m_regionId
                  << ",\"dir\":\"" << pendingDir << "\"}" << std::endl;
        m_splitPhase.store(0, std::memory_order_release);
        return;
      }
      WriteSplitStateLocked(kSplitPhaseChildReady, generation, command);
    }
    m_splitPhase.store(kSplitPhaseChildReady, std::memory_order_release);

    if (persistedPhase < kSplitPhaseParentShrunk) {
      std::error_code renameError;
      if (!std::filesystem::exists(finalDir)) {
        stepTrace("rename");
        std::filesystem::rename(pendingDir, finalDir, renameError);
        if (renameError) {
          std::cerr << "{\"level\":\"error\",\"component\":\"region_peer\","
                       "\"event\":\"split_rename_failed\",\"region\":" << m_regionId
                    << ",\"ec\":" << renameError.value() << "}" << std::endl;
          return;
        }
      }
      // The clone carries the whole parent keyspace; drop the left half from
      // the child while nothing has opened it yet.
      {
        stepTrace("open_child");
        auto childEngine = KVEngineFactory::Create(finalDir);
        stepTrace("child_cleanup");
        DeleteKeysInRange(childEngine.get(), parent.startKey, command.splitkey());
        childEngine->Delete("meta/mvcc_applied_raft_index");
      }
      WriteSplitStateLocked(kSplitPhaseParentShrunk, generation, command);
    }
    m_splitPhase.store(kSplitPhaseParentShrunk, std::memory_order_release);

    if (persistedPhase < kSplitPhaseComplete) {
      stepTrace("parent_cleanup");
      DeleteKeysInRange(m_kvEngine.get(), command.splitkey(), parent.endKey);
      m_mvccStorage->DropKeysInRange(command.splitkey(), parent.endKey);
      WriteSplitStateLocked(kSplitPhaseComplete, generation, command);
    }
    m_splitPhase.store(kSplitPhaseComplete, std::memory_order_release);
  } catch (const std::exception& error) {
    std::cerr << "{\"level\":\"error\",\"component\":\"region_peer\","
                 "\"event\":\"split_materialize_error\",\"region\":" << m_regionId
              << ",\"what\":\"" << error.what() << "\"}" << std::endl;
    m_splitPhase.store(0, std::memory_order_release);
    return;
  }

  if (m_splitCompletedCallback) {
    RegionMetadata shrunken = Descriptor();
    RegionMetadata materializedChild = child;
    materializedChild.metadataRevision = shrunken.metadataRevision;
    m_splitCompletedCallback(shrunken, materializedChild);
  }
}

TxnStatus RegionPeer::ApplyAdminSplit(const stratakv::region::AdminSplitCommand& command) {
  // Idempotency and validation run against the current descriptor. A previous
  // complete split of the same child is an acknowledged no-op; an already
  // shrunken range (epoch advanced past the command's parent) resumes the
  // deferred materialization.
  int persistedPhase = 0;
  {
    std::ifstream input(m_splitStatePath, std::ios::binary);
    if (input.good()) {
      std::string line((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
      const auto phaseAt = line.find("\"phase\":");
      if (phaseAt != std::string::npos) {
        persistedPhase = std::atoi(line.c_str() + phaseAt + 8);
      }
    }
  }

  RegionMetadata parent;
  parent.regionId = static_cast<int>(command.parent().regionid());
  parent.startKey = command.parent().startkey();
  parent.endKey = command.parent().endkey();
  parent.epoch.version = command.parent().epoch().version();
  parent.epoch.confVersion = command.parent().epoch().confversion();

  RegionMetadata child;
  child.regionId = static_cast<int>(command.child().regionid());
  child.startKey = command.child().startkey();
  child.endKey = command.child().endkey();
  child.epoch.version = command.child().epoch().version();
  child.epoch.confVersion = command.child().epoch().confversion();
  child.metadataRevision = command.child().metadatarevision();
  child.leaderPeerId = command.child().leaderpeerid();
  for (const auto& peer : command.child().peers()) {
    RegionPeerLocation location;
    location.nodeId = peer.storeid() > 0 ? static_cast<int>(peer.storeid()) - 1 : -1;
    location.host = peer.host();
    location.port = static_cast<short>(peer.port());
    location.storeId = peer.storeid();
    location.peerId = peer.peerid();
    child.peers.push_back(location);
  }

  bool alreadyShrunkNow = false;
  {
    std::shared_lock<std::shared_mutex> descriptorLock(m_descriptorMutex);
    const bool exactMatch = m_descriptor.regionId == parent.regionId &&
                            m_descriptor.epoch == parent.epoch &&
                            m_descriptor.startKey == parent.startKey &&
                            m_descriptor.endKey == parent.endKey;
    alreadyShrunkNow = m_descriptor.regionId == parent.regionId &&
                       m_descriptor.epoch.version == parent.epoch.version + 1 &&
                       m_descriptor.endKey == command.splitkey();
    if (!exactMatch && !alreadyShrunkNow) {
      std::cerr << "{\"trace\":\"split_reject\"}" << std::endl;
      return TxnStatus::StorageError;
    }
    if (alreadyShrunkNow && persistedPhase >= kSplitPhaseComplete) {
      return TxnStatus::Ok;
    }
  }

  // Shrink the descriptor synchronously at the apply position: from this
  // instant right-half keys are rejected by range validation, which is the
  // single-owner invariant. Everything else is deferred.
  if (!alreadyShrunkNow) {
    RegionMetadata shrunken;
    shrunken.regionId = parent.regionId;
    shrunken.startKey = parent.startKey;
    shrunken.endKey = command.splitkey();
    shrunken.epoch.version = parent.epoch.version + 1;
    shrunken.epoch.confVersion = parent.epoch.confVersion;
    shrunken.metadataRevision = command.metadatarevision();
    shrunken.leaderPeerId = parent.leaderPeerId;
    {
      std::shared_lock<std::shared_mutex> lock(m_descriptorMutex);
      shrunken.peers = m_descriptor.peers;
    }
    UpdateDescriptorShrunk(shrunken);
  }

  WriteSplitStateLocked(kSplitPhaseCheckpointing, 1, command);

  // The child descriptor is stale by one revision (it was stamped at prepare);
  // re-stamp with the revision this apply publishes.
  RegionMetadata stampedChild = child;
  stampedChild.metadataRevision = command.metadatarevision();

  bool alreadyRunning = m_splitMaterializing.exchange(true, std::memory_order_acq_rel);
  if (!alreadyRunning) {
    std::thread([this, command, persistedPhase, parent, stampedChild]() {
      MaterializeSplitChildAsync(command, persistedPhase, parent, stampedChild);
      m_splitMaterializing.store(false, std::memory_order_release);
    }).detach();
  }
  return TxnStatus::Ok;
}

bool RegionPeer::OwnsKey(const std::string& key) const {
  std::shared_lock<std::shared_mutex> lock(m_descriptorMutex);
  return key >= m_regionStartKey && (m_regionEndKey.empty() || key < m_regionEndKey);
}

bool RegionPeer::IsTxnLeader() {
  int term = -1;
  bool isLeader = false;
  m_raftNode->GetState(&term, &isLeader);
  if (!isLeader) return false;
  if (m_txnReadyTerm.load(std::memory_order_acquire) == term) return true;

  int confirmedTerm = -1;
  if (!LinearizableReadBarrier(RequestDeadline(0), &confirmedTerm) ||
      confirmedTerm != term) {
    return false;
  }
  m_txnReadyTerm.store(term, std::memory_order_release);
  return true;
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
  auto locks = m_mvccStorage->ExpiredLocks(currentPhysicalMs);
  std::vector<std::pair<std::string, MvccLock>> owned;
  owned.reserve(locks.size());
  for (auto& item : locks) {
    if (OwnsKey(item.first)) {
      owned.push_back(std::move(item));
    }
  }
  return owned;
}

void RegionPeer::Start() {
  if (LifecycleState() != RegionPeerState::Initializing) {
    throw std::logic_error("Region peer can only start from Initializing state");
  }
  if (m_started.exchange(true, std::memory_order_acq_rel)) {
    throw std::logic_error("Region peer is already started");
  }
  std::vector<std::shared_ptr<RaftRpcUtil>> servers;
  servers.reserve(m_peerAddresses.size());
  for (size_t peerIndex = 0; peerIndex < m_peerAddresses.size(); ++peerIndex) {
    if (static_cast<int>(peerIndex) == m_me) {
      servers.push_back(nullptr);
      continue;
    }
    servers.push_back(std::make_shared<RaftRpcUtil>(
        m_peerAddresses[peerIndex].first, m_peerAddresses[peerIndex].second, m_regionId,
        LocalPeerDescriptor().peerId, m_descriptor.peers[peerIndex].peerId,
        m_descriptor.epoch));
  }
  std::vector<uint64_t> peerIds;
  std::vector<bool> learners;
  peerIds.reserve(m_descriptor.peers.size());
  learners.reserve(m_descriptor.peers.size());
  for (const auto& peer : m_descriptor.peers) {
    peerIds.push_back(peer.peerId);
    learners.push_back(peer.isLearner);
  }
  // Keep inbound Raft RPCs fenced until local snapshot recovery and the
  // Region apply loop are ready. Otherwise InstallSnapshot can race this
  // recovery and a newer snapshot can be overwritten by stale local data.
  m_raftNode->init(std::move(servers), m_me, m_persister, applyChan, true,
                   m_regionId, LocalPeerDescriptor().peerId, std::move(peerIds), std::move(learners));

  const auto snapshot = m_persister->ReadSnapshot();
  if (!snapshot.empty()) {
    ReadSnapShotToInstall(snapshot);
    const auto status = m_raftNode->GetStatus();
    m_lastSnapShotRaftLogIndex = status.lastApplied;
    AdvanceStateMachineApplied(status.lastApplied);
  }
  m_statusThread = std::thread(&RegionPeer::WriteRaftStatusLoop, this);
  m_applyThread = std::thread(&RegionPeer::ReadRaftApplyCommandLoop, this);
  m_raftLogGcThread = std::thread(&RegionPeer::RaftLogGcLoop, this);
  m_raftNode->Activate();
  MarkServing();
}

void RegionPeer::Campaign() {
  if (m_raftNode) m_raftNode->doElection();
}

bool RegionPeer::IsRaftLeader() const {
  if (!m_raftNode) return false;
  int term = -1;
  bool isLeader = false;
  m_raftNode->GetState(&term, &isLeader);
  return isLeader;
}

void RegionPeer::Stop() {
  if (m_stopRequested.exchange(true, std::memory_order_acq_rel)) return;
  m_stopCondition.notify_all();
  if (m_raftNode) m_raftNode->Stop();
  if (applyChan) applyChan->Push(ApplyMsg{});

  const auto self = std::this_thread::get_id();
  for (std::thread* worker : {&m_statusThread, &m_applyThread, &m_raftLogGcThread}) {
    if (worker->joinable() && worker->get_id() != self) worker->join();
  }

  if (LifecycleState() == RegionPeerState::Serving) BeginRetire();
  if (LifecycleState() == RegionPeerState::Retiring && InFlightRequests() == 0) {
    MarkStopped();
  }
  m_mvccStorage.reset();
  m_kvEngine.reset();
}

bool RegionPeer::DeleteStorageData() {
  Stop();
  std::error_code error;
  const bool removed = std::filesystem::remove_all(m_dbPath, error) > 0;
  return !error && (removed || !std::filesystem::exists(m_dbPath));
}

void RegionPeer::WriteRaftStatusLoop() {
  while (!m_stopRequested.load(std::memory_order_acquire)) {
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
           << ",\"lastSnapshotIndex\":" << status.lastSnapshotIndex
           << ",\"raftLogEntryCount\":" << status.logEntryCount
           << ",\"raftStateBytes\":" << status.raftStateBytes
           << ",\"raftLogGcRuns\":" << m_raftLogGcRuns.load(std::memory_order_relaxed)
           << ",\"raftLogGcSoftRuns\":" << m_raftLogGcSoftRuns.load(std::memory_order_relaxed)
           << ",\"raftLogGcForcedRuns\":" << m_raftLogGcForcedRuns.load(std::memory_order_relaxed)
           << ",\"raftLogGcReclaimedEntries\":"
           << m_raftLogGcReclaimedEntries.load(std::memory_order_relaxed)
           << ",\"raftLogGcLastDurationMicros\":"
           << m_raftLogGcLastDurationMicros.load(std::memory_order_relaxed)
           << ",\"raftLogGcTotalDurationMicros\":"
           << m_raftLogGcTotalDurationMicros.load(std::memory_order_relaxed)
           << ",\"raftLogGcMaxDurationMicros\":"
           << m_raftLogGcMaxDurationMicros.load(std::memory_order_relaxed)
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
    std::unique_lock<std::mutex> stopLock(m_stopMutex);
    m_stopCondition.wait_for(stopLock, std::chrono::seconds(1), [this] {
      return m_stopRequested.load(std::memory_order_acquire);
    });
  }
}

void RegionPeer::TxnGet(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnGetArgs *request,
                      ::raftKVRpcProctoc::TxnGetReply *response, ::google::protobuf::Closure *done) {
  if (!OwnsKey(request->key())) {
    PopulateKeyNotInRegion(this, response);
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

void RegionPeer::TxnScan(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnScanArgs *request,
                         ::raftKVRpcProctoc::TxnScanReply *response, ::google::protobuf::Closure *done) {
  // 调用方(SDK)保证起点落在本 Region;空起点表示从本 Region 下界开始,
  // 仅当 Region 下界本身无界("")时合法;终点在 Region 上界内裁剪。
  bool startInside = false;
  {
    std::shared_lock<std::shared_mutex> lock(m_descriptorMutex);
    startInside = request->startkey().empty()
                      ? m_regionStartKey.empty()
                      : OwnsKey(request->startkey());
  }
  if (!startInside) {
    PopulateKeyNotInRegion(this, response);
    done->Run();
    return;
  }
  std::string endKey = request->endkey();
  {
    std::shared_lock<std::shared_mutex> lock(m_descriptorMutex);
    if (!m_regionEndKey.empty() && (endKey.empty() || endKey > m_regionEndKey)) {
      endKey = m_regionEndKey;
    }
  }
  if (!endKey.empty() && endKey <= request->startkey()) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
    done->Run();
    return;
  }

  int term = -1;
  if (!LinearizableReadBarrier(RequestDeadline(0), &term)) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }

  std::vector<std::pair<std::string, std::string>> entries;
  const TxnStatus status = m_mvccStorage->Scan(request->startkey(), endKey, request->readts(),
                                               request->limit(), &entries);
  if (!m_raftNode->IsLeaderInTerm(term)) {
    response->set_err(ErrWrongLeader);
  } else {
    response->set_err(std::to_string(static_cast<int>(status)));
    if (status == TxnStatus::Ok) {
      for (auto& entry : entries) {
        auto* kv = response->add_entries();
        kv->set_key(std::move(entry.first));
        kv->set_value(std::move(entry.second));
      }
    }
  }
  done->Run();
}

void RegionPeer::TxnPrewrite(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::TxnPrewriteArgs *request,
                           ::raftKVRpcProctoc::TxnPrewriteReply *response, ::google::protobuf::Closure *done) {
  if (!OwnsKey(request->key())) {
    PopulateKeyNotInRegion(this, response);
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
      PopulateKeyNotInRegion(this, response);
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
    PopulateKeyNotInRegion(this, response);
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
    PopulateKeyNotInRegion(this, response);
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
    PopulateKeyNotInRegion(this, response);
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
    std::cerr << "{\"trace\":\"batch_rollback_reject\",\"region\":" << m_regionId
              << ",\"scheduler_alive\":" << (scheduler ? 1 : 0)
              << ",\"keys\":" << request->keys_size() << "}" << std::endl;
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
    PopulateKeyNotInRegion(this, response);
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
    PopulateKeyNotInRegion(this, response);
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
    PopulateKeyNotInRegion(this, response);
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
    PopulateKeyNotInRegion(this, response);
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
    PopulateKeyNotInRegion(this, response);
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
    PopulateKeyNotInRegion(this, response);
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

void RegionPeer::ProposeAdminSplit(google::protobuf::RpcController *controller,
                                   const ::raftKVRpcProctoc::ProposeAdminSplitArgs *request,
                                   ::raftKVRpcProctoc::ProposeAdminSplitReply *response,
                                   ::google::protobuf::Closure *done) {
  (void)controller;
  stratakv::region::AdminSplitCommand split;
  if (!request->split().IsInitialized() || request->split().splitkey().empty()) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  split = request->split();

  Op op;
  op.Operation = "AdminSplit";
  if (!split.SerializeToString(&op.Value)) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    done->Run();
    return;
  }
  op.ClientId = "admin-split";
  op.RequestId = m_adminSplitRequestId.fetch_add(1);

  int raftIndex = -1;
  int term = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &term, &isLeader);
  std::cerr << "{\"trace\":\"propose_admin_split\",\"region\":" << m_regionId
            << ",\":isleader\":" << isLeader << ",\"value_size\":" << op.Value.size()
            << "}" << std::endl;
  if (!isLeader) {
    response->set_err(ErrWrongLeader);
    done->Run();
    return;
  }
  // The entry is queued; materialization progress is reported through the
  // split status/metrics, not this synchronous reply.
  response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
  done->Run();
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

TxnStatus RegionPeer::ApplyConfChange(const stratakv::region::ConfChangeCommand& command) {
  std::unique_lock<std::shared_mutex> lock(m_descriptorMutex);

  auto changeType = command.changetype();
  const auto& targetPeer = command.peer();
  uint64_t targetPeerId = targetPeer.peerid();

  if (command.has_expectedepoch()) {
    if (m_descriptor.epoch.confVersion != command.expectedepoch().confversion() ||
        m_descriptor.epoch.version != command.expectedepoch().version()) {
      return TxnStatus::StorageError;
    }
  }

  bool isTargetSelf = false;
  if (m_me >= 0 && m_me < static_cast<int>(m_descriptor.peers.size())) {
    isTargetSelf = (targetPeerId == m_descriptor.peers[m_me].peerId);
  }

  if (changeType == stratakv::region::CONF_CHANGE_ADD_LEARNER) {
    bool found = false;
    for (auto& p : m_descriptor.peers) {
      if (p.peerId == targetPeerId) {
        p.isLearner = true;
        found = true;
        break;
      }
    }
    if (!found) {
      RegionPeerLocation loc;
      loc.nodeId = targetPeer.storeid() > 0 ? static_cast<int>(targetPeer.storeid()) - 1 : -1;
      loc.host = targetPeer.host();
      loc.port = static_cast<short>(targetPeer.port());
      loc.storeId = targetPeer.storeid();
      loc.peerId = targetPeer.peerid();
      loc.isLearner = true;
      m_descriptor.peers.push_back(loc);
    }
  } else if (changeType == stratakv::region::CONF_CHANGE_PROMOTE_LEARNER) {
    for (auto& p : m_descriptor.peers) {
      if (p.peerId == targetPeerId) {
        p.isLearner = false;
        break;
      }
    }
  } else if (changeType == stratakv::region::CONF_CHANGE_REMOVE_PEER) {
    auto it = std::remove_if(m_descriptor.peers.begin(), m_descriptor.peers.end(),
                             [targetPeerId](const RegionPeerLocation& p) {
                               return p.peerId == targetPeerId;
                             });
    m_descriptor.peers.erase(it, m_descriptor.peers.end());
  }

  m_descriptor.epoch.confVersion++;
  if (command.metadatarevision() > 0) {
    m_descriptor.metadataRevision = command.metadatarevision();
  }

  if (m_raftNode) {
    m_raftNode->ApplyConfChange(command);
    m_raftNode->SetEpoch(m_descriptor.epoch);
  }

  const uint64_t appliedRevision = m_descriptor.metadataRevision;
  lock.unlock();
  if (changeType == stratakv::region::CONF_CHANGE_REMOVE_PEER && isTargetSelf) {
    BeginRetire();
    if (InFlightRequests() == 0) {
      MarkStopped();
    }
    if (m_removedCallback) {
      m_removedCallback(m_regionId, targetPeerId, appliedRevision);
    }
  }

  return TxnStatus::Ok;
}

void RegionPeer::ProposeConfChange(google::protobuf::RpcController *controller,
                                   const ::raftKVRpcProctoc::ProposeConfChangeArgs *request,
                                   ::raftKVRpcProctoc::ProposeConfChangeReply *response,
                                   ::google::protobuf::Closure *done) {
  (void)controller;
  std::lock_guard<std::mutex> lock(m_confChangeMutex);

  if (!request->confchange().IsInitialized()) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    if (done) done->Run();
    return;
  }

  if (m_raftNode->HasConfChangeInFlight()) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    response->mutable_header()->mutable_error()->set_code(stratakv::region::REGION_ERROR_CONF_CHANGE_IN_FLIGHT);
    if (done) done->Run();
    return;
  }

  {
    std::shared_lock<std::shared_mutex> descLock(m_descriptorMutex);
    if (request->confchange().has_expectedepoch()) {
      if (m_descriptor.epoch.confVersion != request->confchange().expectedepoch().confversion() ||
          m_descriptor.epoch.version != request->confchange().expectedepoch().version()) {
        response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
        response->mutable_header()->mutable_error()->set_code(stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH);
        if (done) done->Run();
        return;
      }
    }
  }

  Op op;
  op.Operation = "ConfChange";
  if (!request->confchange().SerializeToString(&op.Value)) {
    response->set_err(std::to_string(static_cast<int>(TxnStatus::StorageError)));
    if (done) done->Run();
    return;
  }
  op.ClientId = "conf-change";
  op.RequestId = m_adminConfChangeRequestId.fetch_add(1);

  int raftIndex = -1;
  int term = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &term, &isLeader);
  if (!isLeader) {
    response->set_err(ErrWrongLeader);
    if (done) done->Run();
    return;
  }

  response->set_err(std::to_string(static_cast<int>(TxnStatus::Ok)));
  if (done) done->Run();
}

bool RegionPeer::IsPeerCaughtUp(uint64_t peerId, int maxLag) const {
  if (!m_raftNode) return false;
  return m_raftNode->IsPeerCaughtUp(peerId, maxLag);
}

int RegionPeer::GetPeerLag(uint64_t peerId) const {
  if (!m_raftNode) return -1;
  return m_raftNode->GetPeerLag(peerId);
}

bool RegionPeer::MaybePromoteLearner(uint64_t peerId, int maxLag) {
  if (!m_raftNode || !m_raftNode->GetStatus().isLeader) {
    return false;
  }
  if (!m_raftNode->IsPeerCaughtUp(peerId, maxLag)) {
    return false;
  }
  if (m_raftNode->HasConfChangeInFlight()) {
    return false;
  }

  stratakv::region::ConfChangeCommand cmd;
  cmd.set_changetype(stratakv::region::CONF_CHANGE_PROMOTE_LEARNER);
  auto* p = cmd.mutable_peer();
  p->set_peerid(peerId);
  {
    std::shared_lock<std::shared_mutex> lock(m_descriptorMutex);
    bool isLearner = false;
    for (const auto& peer : m_descriptor.peers) {
      if (peer.peerId == peerId) {
        isLearner = peer.isLearner;
        p->set_storeid(peer.storeId);
        p->set_host(peer.host);
        p->set_port(peer.port);
        break;
      }
    }
    if (!isLearner) {
      return false; // already voter or not found
    }
    auto* expEpoch = cmd.mutable_expectedepoch();
    expEpoch->set_version(m_descriptor.epoch.version);
    expEpoch->set_confversion(m_descriptor.epoch.confVersion);
  }

  raftKVRpcProctoc::ProposeConfChangeArgs args;
  args.set_regionid(m_regionId);
  *args.mutable_confchange() = cmd;
  raftKVRpcProctoc::ProposeConfChangeReply reply;
  ProposeConfChange(nullptr, &args, &reply, nullptr);
  return reply.err() == std::to_string(static_cast<int>(TxnStatus::Ok));
}

void RegionPeer::SetPeerMatchIndexForTest(uint64_t peerId, int matchIndex) {
  if (m_raftNode) m_raftNode->SetPeerMatchIndexForTest(peerId, matchIndex);
}

std::string RegionPeer::ResolveSplitCandidate(uint64_t generation) const {
  if (generation == 0) return "";
  if (m_splitCandidateResolver) {
    return m_splitCandidateResolver(generation);
  }
  std::shared_lock<std::shared_mutex> lock(m_descriptorMutex);
  if (!m_descriptor.startKey.empty() && !m_descriptor.endKey.empty()) {
    return m_descriptor.startKey + "_split_" + std::to_string(generation);
  }
  if (!m_descriptor.endKey.empty()) {
    return m_descriptor.endKey.substr(0, 1);
  }
  return "m";
}
