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
  for (const auto& addr : addresses_) {
    auto channel = std::make_unique<MprpcChannel>(addr.first, addr.second, false);
    auto stub = std::make_unique<raftKVRpcProctoc::kvServerRpc_Stub>(channel.get());
    channels_.push_back(std::move(channel));
    stubs_.push_back(std::move(stub));
  }
}

RaftMvccStorage::~RaftMvccStorage() {}

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
                                   uint64_t startTs, uint64_t ttlMs) {
  std::lock_guard<std::mutex> mutationLock(mutationMutex_);
  const int reqId = requestId_.fetch_add(1, std::memory_order_relaxed) + 1;
  int server = recentLeaderId_.load(std::memory_order_relaxed);

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
    args.set_clientid(clientId_);
    args.set_requestid(reqId);

    raftKVRpcProctoc::TxnPrewriteReply reply;
    MprpcController controller;
    stubs_[server]->TxnPrewrite(&controller, &args, &reply, nullptr);

    if (controller.Failed() || reply.err() == "ErrWrongLeader") {
      const int failedServer = server;
      server = (server + 1) % stubs_.size();
      ExponentialBackoff(attempt);
      LogRpcRetry(shardId_, "Prewrite", failedServer, attempt, controller, reply.err());
      if (attempt >= kMaxRpcAttempts) {
        return TxnStatus::StorageError;
      }
      continue;
    }

    recentLeaderId_.store(server, std::memory_order_relaxed);
    return static_cast<TxnStatus>(std::stoi(reply.err()));
  }
}

TxnStatus RaftMvccStorage::PrewriteDelete(const std::string& key, const std::string& primaryKey, uint64_t startTs,
                                         uint64_t ttlMs) {
  std::lock_guard<std::mutex> mutationLock(mutationMutex_);
  const int reqId = requestId_.fetch_add(1, std::memory_order_relaxed) + 1;
  int server = recentLeaderId_.load(std::memory_order_relaxed);

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
    args.set_clientid(clientId_);
    args.set_requestid(reqId);

    raftKVRpcProctoc::TxnPrewriteReply reply;
    MprpcController controller;
    stubs_[server]->TxnPrewrite(&controller, &args, &reply, nullptr);

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

TxnStatus RaftMvccStorage::AcquirePessimisticLock(const std::string& key, const std::string& primaryKey,
                                                 uint64_t startTs, uint64_t ttlMs) {
  std::lock_guard<std::mutex> mutationLock(mutationMutex_);
  const int reqId = requestId_.fetch_add(1, std::memory_order_relaxed) + 1;
  int server = recentLeaderId_.load(std::memory_order_relaxed);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnAcquirePessimisticLockArgs args;
    args.set_regionid(shardId_);
    args.set_key(key);
    args.set_primarykey(primaryKey);
    args.set_startts(startTs);
    args.set_ttlms(ttlMs);
    args.set_clientid(clientId_);
    args.set_requestid(reqId);

    raftKVRpcProctoc::TxnAcquirePessimisticLockReply reply;
    MprpcController controller;
    stubs_[server]->TxnAcquirePessimisticLock(&controller, &args, &reply, nullptr);

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

TxnStatus RaftMvccStorage::Commit(const std::string& key, uint64_t startTs, uint64_t commitTs) {
  std::lock_guard<std::mutex> mutationLock(mutationMutex_);
  const int reqId = requestId_.fetch_add(1, std::memory_order_relaxed) + 1;
  int server = recentLeaderId_.load(std::memory_order_relaxed);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnCommitArgs args;
    args.set_regionid(shardId_);
    args.set_key(key);
    args.set_startts(startTs);
    args.set_committs(commitTs);
    args.set_clientid(clientId_);
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
  std::lock_guard<std::mutex> mutationLock(mutationMutex_);
  const int reqId = requestId_.fetch_add(1, std::memory_order_relaxed) + 1;
  int server = recentLeaderId_.load(std::memory_order_relaxed);

  int attempt = 0;
  while (true) {
    raftKVRpcProctoc::TxnRollbackArgs args;
    args.set_regionid(shardId_);
    args.set_key(key);
    args.set_startts(startTs);
    args.set_clientid(clientId_);
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
      MvccLock mvccLock;
      mvccLock.primaryKey = reply.primarykey();
      mvccLock.value = reply.value();
      mvccLock.startTs = reply.startts();
      mvccLock.ttlMs = reply.ttlms();
      mvccLock.createTimeMs = reply.createtimems();
      mvccLock.isDelete = reply.isdelete();
      mvccLock.isPessimistic = reply.ispessimistic();
      return mvccLock;
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
      MvccLock mvccLock;
      const auto& lockReply = reply.locks(i);
      mvccLock.primaryKey = lockReply.primarykey();
      mvccLock.value = lockReply.value();
      mvccLock.startTs = lockReply.startts();
      mvccLock.ttlMs = lockReply.ttlms();
      mvccLock.createTimeMs = lockReply.createtimems();
      mvccLock.isDelete = lockReply.isdelete();
      mvccLock.isPessimistic = lockReply.ispessimistic();
      expired.emplace_back(reply.keys(i), mvccLock);
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
