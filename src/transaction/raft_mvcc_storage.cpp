// Transaction subsystem: MVCC operations routed through Raft.
#include "raft_mvcc_storage.h"

#include <iostream>
#include <thread>
#include "util.h"
#include <random>
#include <algorithm>

namespace {
  constexpr int kMaxRpcAttempts = 12;
  std::mutex g_rpcDiagnosticMutex;

  using RpcDeadline = std::chrono::steady_clock::time_point;

  RpcDeadline MakeRpcDeadline(uint64_t budgetMs) {
    if (budgetMs == 0) return RpcDeadline::max();
    return std::chrono::steady_clock::now() + std::chrono::milliseconds(budgetMs);
  }

  bool RpcBudgetExpired(const RpcDeadline& deadline) {
    return deadline != RpcDeadline::max() && std::chrono::steady_clock::now() >= deadline;
  }

  uint64_t RemainingRpcBudgetMs(const RpcDeadline& deadline) {
    if (deadline == RpcDeadline::max()) return 0;
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) return 1;
    return std::max<uint64_t>(
        1, static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                     deadline - now)
                                     .count()));
  }

  void ExponentialBackoff(int& attempt) {
    const int base_ms = 10;
    const int max_ms = 500;
    int delay = base_ms * (1 << std::min(attempt, 6));
    delay = std::min(delay, max_ms);
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<int> distribution(0, delay);
    std::this_thread::sleep_for(std::chrono::milliseconds(distribution(generator)));
    attempt++;
  }

  void LogRpcRetry(int shardId, const char* operation, int server, int attempt,
                   const MprpcController& controller, const std::string& replyError) {
    if (attempt != 1 && attempt != 6 && attempt != kMaxRpcAttempts) {
      return;
    }
    std::lock_guard<std::mutex> lock(g_rpcDiagnosticMutex);
    std::cerr << "[raft-mvcc retry] shard=" << shardId
              << " operation=" << operation
              << " server=" << server
              << " attempt=" << attempt
              << " transport=" << (controller.Failed() ? controller.ErrorText() : "ok")
              << " reply=" << replyError << std::endl;
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

  raftKVRpcProctoc::TxnRecordStateProto ToProtoTxnState(TxnRecordState state) {
    switch (state) {
      case TxnRecordState::Locked: return raftKVRpcProctoc::TXN_RECORD_LOCKED;
      case TxnRecordState::Committed: return raftKVRpcProctoc::TXN_RECORD_COMMITTED;
      case TxnRecordState::RolledBack: return raftKVRpcProctoc::TXN_RECORD_ROLLED_BACK;
      case TxnRecordState::NotFound: return raftKVRpcProctoc::TXN_RECORD_NOT_FOUND;
    }
    return raftKVRpcProctoc::TXN_RECORD_NOT_FOUND;
  }

  MvccLock DecodeLockReply(const raftKVRpcProctoc::TxnGetLockReply& reply) {
    MvccLock lock;
    lock.primaryKey = reply.primarykey();
    lock.value = reply.value();
    lock.startTs = reply.startts();
    lock.ttlMs = reply.ttlms();
    lock.createTimeMs = reply.createtimems();
    lock.isDelete = reply.isdelete();
    lock.isPessimistic = reply.ispessimistic();
    lock.forUpdateTs = reply.forupdatets() == 0 ? lock.startTs : reply.forupdatets();
    lock.expireAtPhysicalMs = reply.expireatphysicalms();
    lock.legacyExpiry = reply.legacyexpiry() || lock.expireAtPhysicalMs == 0;
    lock.isLockOnly = reply.islockonly();
    return lock;
  }
}



RaftMvccStorage::RaftMvccStorage(int shardId, const std::vector<std::pair<std::string, short>>& addresses)
    : MvccStorage(nullptr),
      shardId_(shardId),
      addresses_(addresses),
      recentLeaderId_(0),
      clientId_(Uuid()),
      requestId_(0),
      maintenanceClientId_(Uuid()),
      maintenanceRequestId_(0) {
  constexpr size_t kMutationLaneCount = 16;
  mutationLanes_.reserve(kMutationLaneCount);
  for (size_t index = 0; index < kMutationLaneCount; ++index) {
    auto lane = std::make_unique<MutationLane>();
    lane->clientId = clientId_ + "-mutation-" + std::to_string(index);
    mutationLanes_.push_back(std::move(lane));
  }
  for (const auto& addr : addresses_) {
    auto channel = std::make_unique<MprpcChannel>(addr.first, addr.second, false);
    auto stub = std::make_unique<raftKVRpcProctoc::kvServerRpc_Stub>(channel.get());
    channels_.push_back(std::move(channel));
    stubs_.push_back(std::move(stub));
  }
}

RaftMvccStorage::~RaftMvccStorage() {}

RaftMvccStorage::MutationLane& RaftMvccStorage::PickMutationLane() {
  const size_t index = nextMutationLane_.fetch_add(1, std::memory_order_relaxed) % mutationLanes_.size();
  return *mutationLanes_[index];
}

TxnStatus RaftMvccStorage::Get(const std::string& key, uint64_t readTs, std::string* value) {
  const int reqId = requestId_.fetch_add(1, std::memory_order_relaxed) + 1;
  int server = recentLeaderId_.load(std::memory_order_relaxed);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnGetArgs args;
    args.set_regionid(shardId_);
    args.set_key(key);
    args.set_readts(readTs);
    args.set_clientid(clientId_);
    args.set_requestid(reqId);

    raftKVRpcProctoc::TxnGetReply reply;
    MprpcController controller;
    stubs_[server]->TxnGet(&controller, &args, &reply, nullptr);

    if (controller.Failed() || reply.err() == "ErrWrongLeader") {
      const int failedServer = server;
      server = (server + 1) % stubs_.size();
      ExponentialBackoff(attempt);
      LogRpcRetry(shardId_, "Get", failedServer, attempt, controller, reply.err());
      if (attempt >= kMaxRpcAttempts) {
        return TxnStatus::StorageError;
      }
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    TxnStatus status = static_cast<TxnStatus>(std::stoi(reply.err()));
    if (status == TxnStatus::Ok) {
      *value = reply.value();
    }
    return status;
  }
}

TxnStatus RaftMvccStorage::Prewrite(const std::string& key, const std::string& value, const std::string& primaryKey,
                                   uint64_t startTs, uint64_t ttlMs,
                                   uint64_t forUpdateTs, uint64_t remainingBudgetMs) {
  MutationLane& lane = PickMutationLane();
  std::lock_guard<std::mutex> mutationLock(lane.mutex);
  const int reqId = ++lane.requestId;
  int server = recentLeaderId_.load(std::memory_order_relaxed);
  const RpcDeadline deadline = MakeRpcDeadline(remainingBudgetMs);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnPrewriteArgs args;
    args.set_regionid(shardId_);
    args.set_key(key);
    args.set_value(value);
    args.set_primarykey(primaryKey);
    args.set_startts(startTs);
    args.set_ttlms(ttlMs);
    args.set_isdelete(false);
    args.set_clientid(lane.clientId);
    args.set_requestid(reqId);
    args.set_maxforupdatets(forUpdateTs);
    args.set_protocolversion(forUpdateTs == 0 ? 0 : kTxnProtocolVersion);
    args.set_remainingbudgetms(RemainingRpcBudgetMs(deadline));

    raftKVRpcProctoc::TxnPrewriteReply reply;
    MprpcController controller;
    stubs_[server]->TxnPrewrite(&controller, &args, &reply, nullptr);

    if (controller.Failed() || reply.err() == "ErrWrongLeader") {
      const int failedServer = server;
      server = (server + 1) % stubs_.size();
      if (RpcBudgetExpired(deadline)) return TxnStatus::ResultUnknown;
      ExponentialBackoff(attempt);
      LogRpcRetry(shardId_, "Prewrite", failedServer, attempt, controller, reply.err());
      if (attempt >= kMaxRpcAttempts) {
        return TxnStatus::ResultUnknown;
      }
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    return static_cast<TxnStatus>(std::stoi(reply.err()));
  }
}

TxnStatus RaftMvccStorage::PrewriteDelete(const std::string& key, const std::string& primaryKey, uint64_t startTs,
                                         uint64_t ttlMs, uint64_t forUpdateTs,
                                         uint64_t remainingBudgetMs) {
  MutationLane& lane = PickMutationLane();
  std::lock_guard<std::mutex> mutationLock(lane.mutex);
  const int reqId = ++lane.requestId;
  int server = recentLeaderId_.load(std::memory_order_relaxed);
  const RpcDeadline deadline = MakeRpcDeadline(remainingBudgetMs);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnPrewriteArgs args;
    args.set_regionid(shardId_);
    args.set_key(key);
    args.set_value("");
    args.set_primarykey(primaryKey);
    args.set_startts(startTs);
    args.set_ttlms(ttlMs);
    args.set_isdelete(true);
    args.set_clientid(lane.clientId);
    args.set_requestid(reqId);
    args.set_maxforupdatets(forUpdateTs);
    args.set_protocolversion(forUpdateTs == 0 ? 0 : kTxnProtocolVersion);
    args.set_remainingbudgetms(RemainingRpcBudgetMs(deadline));

    raftKVRpcProctoc::TxnPrewriteReply reply;
    MprpcController controller;
    stubs_[server]->TxnPrewrite(&controller, &args, &reply, nullptr);

    if (controller.Failed() || reply.err() == "ErrWrongLeader") {
      server = (server + 1) % stubs_.size();
      if (RpcBudgetExpired(deadline)) return TxnStatus::ResultUnknown;
      ExponentialBackoff(attempt);
      if (attempt >= kMaxRpcAttempts) {
        return TxnStatus::ResultUnknown;
      }
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    return static_cast<TxnStatus>(std::stoi(reply.err()));
  }
}

TxnStatus RaftMvccStorage::PrewriteLock(const std::string& key, const std::string& primaryKey,
                                        uint64_t startTs, uint64_t ttlMs,
                                        uint64_t forUpdateTs, uint64_t remainingBudgetMs) {
  MutationLane& lane = PickMutationLane();
  std::lock_guard<std::mutex> mutationLock(lane.mutex);
  const int reqId = ++lane.requestId;
  int server = recentLeaderId_.load(std::memory_order_relaxed);
  const RpcDeadline deadline = MakeRpcDeadline(remainingBudgetMs);
  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnPrewriteArgs args;
    args.set_regionid(shardId_);
    args.set_key(key);
    args.set_primarykey(primaryKey);
    args.set_startts(startTs);
    args.set_ttlms(ttlMs);
    args.set_islockonly(true);
    args.set_clientid(lane.clientId);
    args.set_requestid(reqId);
    args.set_maxforupdatets(forUpdateTs);
    args.set_protocolversion(kTxnProtocolVersion);
    args.set_remainingbudgetms(RemainingRpcBudgetMs(deadline));

    raftKVRpcProctoc::TxnPrewriteReply reply;
    MprpcController controller;
    stubs_[server]->TxnPrewrite(&controller, &args, &reply, nullptr);
    if (controller.Failed() || reply.err() == "ErrWrongLeader") {
      server = (server + 1) % stubs_.size();
      if (RpcBudgetExpired(deadline)) return TxnStatus::ResultUnknown;
      ExponentialBackoff(attempt);
      if (attempt >= kMaxRpcAttempts) return TxnStatus::ResultUnknown;
      continue;
    }
    recentLeaderId_.store(server, std::memory_order_relaxed);
    return static_cast<TxnStatus>(std::stoi(reply.err()));
  }
}

TxnStatus RaftMvccStorage::AcquirePessimisticLock(const std::string& key, const std::string& primaryKey,
                                                 uint64_t startTs, uint64_t ttlMs,
                                                 uint64_t forUpdateTs,
                                                 uint64_t expireAtPhysicalMs) {
  return AcquirePessimisticLockForUpdate(key, primaryKey, startTs, ttlMs, forUpdateTs,
                                         expireAtPhysicalMs, 0)
      .status;
}

PessimisticLockResult RaftMvccStorage::AcquirePessimisticLockForUpdate(
    const std::string& key, const std::string& primaryKey, uint64_t startTs,
    uint64_t ttlMs, uint64_t forUpdateTs, uint64_t expireAtPhysicalMs,
    uint64_t remainingBudgetMs) {
  MutationLane& lane = PickMutationLane();
  std::lock_guard<std::mutex> mutationLock(lane.mutex);
  const int reqId = ++lane.requestId;
  int server = recentLeaderId_.load(std::memory_order_relaxed);
  const RpcDeadline deadline = MakeRpcDeadline(remainingBudgetMs);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnAcquirePessimisticLockArgs args;
    args.set_regionid(shardId_);
    args.set_key(key);
    args.set_primarykey(primaryKey);
    args.set_startts(startTs);
    args.set_ttlms(ttlMs);
    args.set_forupdatets(forUpdateTs);
    args.set_expireatphysicalms(expireAtPhysicalMs);
    args.set_remainingbudgetms(RemainingRpcBudgetMs(deadline));
    args.set_protocolversion(kTxnProtocolVersion);
    args.set_returnvalue(true);
    args.set_clientid(lane.clientId);
    args.set_requestid(reqId);

    raftKVRpcProctoc::TxnAcquirePessimisticLockReply reply;
    MprpcController controller;
    stubs_[server]->TxnAcquirePessimisticLock(&controller, &args, &reply, nullptr);

    if (controller.Failed() || reply.err() == "ErrWrongLeader") {
      server = (server + 1) % stubs_.size();
      if (RpcBudgetExpired(deadline)) {
        PessimisticLockResult unknown;
        unknown.status = TxnStatus::ResultUnknown;
        return unknown;
      }
      ExponentialBackoff(attempt);
      if (attempt >= kMaxRpcAttempts) {
        PessimisticLockResult unknown;
        unknown.status = TxnStatus::ResultUnknown;
        return unknown;
      }
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    PessimisticLockResult result;
    result.status = static_cast<TxnStatus>(std::stoi(reply.err()));
    result.found = reply.found();
    result.value = reply.value();
    result.valueCommitTs = reply.valuecommitts();
    result.applied = reply.applied();
    return result;
  }
}

TxnStatus RaftMvccStorage::Commit(const std::string& key, uint64_t startTs, uint64_t commitTs) {
  MutationLane& lane = PickMutationLane();
  std::lock_guard<std::mutex> mutationLock(lane.mutex);
  const int reqId = ++lane.requestId;
  int server = recentLeaderId_.load(std::memory_order_relaxed);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnCommitArgs args;
    args.set_regionid(shardId_);
    args.set_key(key);
    args.set_startts(startTs);
    args.set_committs(commitTs);
    args.set_clientid(lane.clientId);
    args.set_requestid(reqId);

    raftKVRpcProctoc::TxnCommitReply reply;
    MprpcController controller;
    stubs_[server]->TxnCommit(&controller, &args, &reply, nullptr);

    if (controller.Failed() || reply.err() == "ErrWrongLeader") {
      const int failedServer = server;
      server = (server + 1) % stubs_.size();
      ExponentialBackoff(attempt);
      LogRpcRetry(shardId_, "Commit", failedServer, attempt, controller, reply.err());
      if (attempt >= kMaxRpcAttempts) {
        return TxnStatus::StorageError;
      }
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    return static_cast<TxnStatus>(std::stoi(reply.err()));
  }
}

TxnStatus RaftMvccStorage::Rollback(const std::string& key, uint64_t startTs) {
  MutationLane& lane = PickMutationLane();
  std::lock_guard<std::mutex> mutationLock(lane.mutex);
  const int reqId = ++lane.requestId;
  int server = recentLeaderId_.load(std::memory_order_relaxed);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnRollbackArgs args;
    args.set_regionid(shardId_);
    args.set_key(key);
    args.set_startts(startTs);
    args.set_clientid(lane.clientId);
    args.set_requestid(reqId);

    raftKVRpcProctoc::TxnRollbackReply reply;
    MprpcController controller;
    stubs_[server]->TxnRollback(&controller, &args, &reply, nullptr);

    if (controller.Failed() || reply.err() == "ErrWrongLeader") {
      server = (server + 1) % stubs_.size();
      ExponentialBackoff(attempt);
      if (attempt >= kMaxRpcAttempts) {
        return TxnStatus::StorageError;
      }
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    return static_cast<TxnStatus>(std::stoi(reply.err()));
  }
}

std::optional<MvccLock> RaftMvccStorage::GetLock(const std::string& key) {
  int server = recentLeaderId_.load(std::memory_order_relaxed);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnGetLockArgs args;
    args.set_regionid(shardId_);
    args.set_key(key);

    raftKVRpcProctoc::TxnGetLockReply reply;
    MprpcController controller;
    stubs_[server]->TxnGetLock(&controller, &args, &reply, nullptr);

    if (controller.Failed() || reply.err() == "ErrWrongLeader") {
      server = (server + 1) % stubs_.size();
      ExponentialBackoff(attempt);
      if (attempt >= kMaxRpcAttempts) {
        return std::nullopt;
      }
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    TxnStatus status = static_cast<TxnStatus>(std::stoi(reply.err()));
    if (status == TxnStatus::Ok && reply.haslock()) {
      return DecodeLockReply(reply);
    }
    return std::nullopt;
  }
}

std::optional<uint64_t> RaftMvccStorage::FindCommitTs(const std::string& key, uint64_t startTs) {
  int server = recentLeaderId_.load(std::memory_order_relaxed);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnFindCommitTsArgs args;
    args.set_regionid(shardId_);
    args.set_key(key);
    args.set_startts(startTs);

    raftKVRpcProctoc::TxnFindCommitTsReply reply;
    MprpcController controller;
    stubs_[server]->TxnFindCommitTs(&controller, &args, &reply, nullptr);

    if (controller.Failed() || reply.err() == "ErrWrongLeader") {
      server = (server + 1) % stubs_.size();
      ExponentialBackoff(attempt);
      if (attempt >= kMaxRpcAttempts) {
        return std::nullopt;
      }
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    TxnStatus status = static_cast<TxnStatus>(std::stoi(reply.err()));
    if (status == TxnStatus::Ok && reply.found()) {
      return reply.committs();
    }
    return std::nullopt;
  }
}

TxnStatus RaftMvccStorage::CheckTxnStatus(const std::string& primaryKey, uint64_t startTs,
                                          uint64_t currentPhysicalMs,
                                          bool rollbackIfExpired,
                                          uint64_t remainingBudgetMs,
                                          TxnRecordStatus* status) {
  if (status == nullptr) return TxnStatus::StorageError;
  MutationLane& lane = PickMutationLane();
  std::lock_guard<std::mutex> mutationLock(lane.mutex);
  const int reqId = ++lane.requestId;
  int server = recentLeaderId_.load(std::memory_order_relaxed);
  const RpcDeadline deadline = MakeRpcDeadline(remainingBudgetMs);
  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnCheckStatusArgs args;
    args.set_regionid(shardId_);
    args.set_primarykey(primaryKey);
    args.set_startts(startTs);
    args.set_currentphysicalms(currentPhysicalMs);
    args.set_rollbackifexpired(rollbackIfExpired);
    args.set_clientid(lane.clientId);
    args.set_requestid(reqId);
    args.set_remainingbudgetms(RemainingRpcBudgetMs(deadline));
    args.set_protocolversion(kTxnProtocolVersion);

    raftKVRpcProctoc::TxnCheckStatusReply reply;
    MprpcController controller;
    stubs_[server]->TxnCheckStatus(&controller, &args, &reply, nullptr);
    if (controller.Failed() || reply.err() == ErrWrongLeader) {
      server = (server + 1) % stubs_.size();
      if (RpcBudgetExpired(deadline)) return TxnStatus::Timeout;
      ExponentialBackoff(attempt);
      if (attempt >= kMaxRpcAttempts) return TxnStatus::StorageError;
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    const TxnStatus rpcStatus = static_cast<TxnStatus>(std::stoi(reply.err()));
    if (rpcStatus != TxnStatus::Ok) return rpcStatus;
    status->state = FromProtoTxnState(reply.state());
    status->commitTs = reply.committs();
    status->lock.reset();
    if (status->state == TxnRecordState::Locked && reply.has_lock() && reply.lock().haslock()) {
      status->lock = DecodeLockReply(reply.lock());
    }
    return TxnStatus::Ok;
  }
}

TxnStatus RaftMvccStorage::ResolveLock(const std::string& key, uint64_t startTs,
                                       TxnRecordState decision, uint64_t commitTs) {
  MutationLane& lane = PickMutationLane();
  std::lock_guard<std::mutex> mutationLock(lane.mutex);
  const int reqId = ++lane.requestId;
  int server = recentLeaderId_.load(std::memory_order_relaxed);
  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnResolveLockArgs args;
    args.set_regionid(shardId_);
    args.set_key(key);
    args.set_startts(startTs);
    args.set_decision(ToProtoTxnState(decision));
    args.set_committs(commitTs);
    args.set_clientid(lane.clientId);
    args.set_requestid(reqId);
    args.set_protocolversion(kTxnProtocolVersion);

    raftKVRpcProctoc::TxnResolveLockReply reply;
    MprpcController controller;
    stubs_[server]->TxnResolveLock(&controller, &args, &reply, nullptr);
    if (controller.Failed() || reply.err() == ErrWrongLeader) {
      server = (server + 1) % stubs_.size();
      ExponentialBackoff(attempt);
      if (attempt >= kMaxRpcAttempts) return TxnStatus::ResultUnknown;
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    const TxnStatus resolved = static_cast<TxnStatus>(std::stoi(reply.err()));
    return resolved == TxnStatus::AlreadyCommitted ? TxnStatus::Ok : resolved;
  }
}

std::vector<std::pair<std::string, MvccLock>> RaftMvccStorage::ExpiredLocks(uint64_t nowMs) {
  int server = recentLeaderId_.load(std::memory_order_relaxed);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnExpiredLocksArgs args;
    args.set_regionid(shardId_);
    args.set_nowms(nowMs);

    raftKVRpcProctoc::TxnExpiredLocksReply reply;
    MprpcController controller;
    stubs_[server]->TxnExpiredLocks(&controller, &args, &reply, nullptr);

    if (controller.Failed() || reply.err() == "ErrWrongLeader") {
      server = (server + 1) % stubs_.size();
      ExponentialBackoff(attempt);
      if (attempt >= kMaxRpcAttempts) {
        return {};
      }
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    std::vector<std::pair<std::string, MvccLock>> expired;
    for (int i = 0; i < reply.keys_size(); ++i) {
      const auto& lockReply = reply.locks(i);
      expired.emplace_back(reply.keys(i), DecodeLockReply(lockReply));
    }
    return expired;
  }
}

size_t RaftMvccStorage::GarbageCollect(uint64_t safePointTs) {
  const int reqId = maintenanceRequestId_.fetch_add(1, std::memory_order_relaxed) + 1;
  int server = recentLeaderId_.load(std::memory_order_relaxed);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnGarbageCollectArgs args;
    args.set_regionid(shardId_);
    args.set_safepointts(safePointTs);
    args.set_clientid(maintenanceClientId_);
    args.set_requestid(reqId);

    raftKVRpcProctoc::TxnGarbageCollectReply reply;
    MprpcController controller;
    stubs_[server]->TxnGarbageCollect(&controller, &args, &reply, nullptr);

    if (controller.Failed() || reply.err() == "ErrWrongLeader") {
      server = (server + 1) % stubs_.size();
      ExponentialBackoff(attempt);
      if (attempt >= kMaxRpcAttempts) {
        return 0;
      }
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    return reply.removedcount();
  }
}

ProtocolCapabilities RaftMvccStorage::Capabilities() {
  int server = recentLeaderId_.load(std::memory_order_relaxed);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnProtocolCapabilitiesArgs args;
    raftKVRpcProctoc::TxnProtocolCapabilitiesReply reply;
    MprpcController controller;
    stubs_[server]->TxnProtocolCapabilities(&controller, &args, &reply, nullptr);

    if (controller.Failed() || !reply.err().empty()) {
      server = (server + 1) % stubs_.size();
      ExponentialBackoff(attempt);
      if (attempt >= kMaxRpcAttempts) {
        return ProtocolCapabilities{};
      }
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    ProtocolCapabilities caps;
    caps.protocolVersion = reply.protocolversion();
    caps.preparedCommandVersion = reply.preparedcommandversion();
    caps.lockFormatVersion = reply.lockformatversion();
    caps.hlcExpiry = reply.hlcexpiry();
    return caps;
  }
}

MvccStats RaftMvccStorage::Stats() { return MvccStats(); }

uint64_t RaftMvccStorage::MaxObservedTs() {
  int server = recentLeaderId_.load(std::memory_order_relaxed);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnMaxObservedTsArgs args;
    args.set_regionid(shardId_);

    raftKVRpcProctoc::TxnMaxObservedTsReply reply;
    MprpcController controller;
    stubs_[server]->TxnMaxObservedTs(&controller, &args, &reply, nullptr);

    if (controller.Failed() || reply.err() == "ErrWrongLeader") {
      server = (server + 1) % stubs_.size();
      ExponentialBackoff(attempt);
      if (attempt >= kMaxRpcAttempts) {
        return 0;
      }
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    return reply.maxts();
  }
}
