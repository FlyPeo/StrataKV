#ifndef STRATAKV_TRANSACTION_RAFT_MVCC_STORAGE_H
#define STRATAKV_TRANSACTION_RAFT_MVCC_STORAGE_H

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <random>
#include <chrono>

#include "mvcc_storage.h"
#include "kv_server_rpc.pb.h"
#include "mprpc_channel.h"
#include "mprpc_controller.h"

class RaftMvccStorage : public MvccStorage {
 public:
  RaftMvccStorage(int shardId, const std::vector<std::pair<std::string, short>>& addresses);
  ~RaftMvccStorage() override;

  TxnStatus Get(const std::string& key, uint64_t readTs, std::string* value) override;
  TxnStatus Prewrite(const std::string& key, const std::string& value, const std::string& primaryKey, uint64_t startTs,
                     uint64_t ttlMs, uint64_t forUpdateTs = 0,
                     uint64_t remainingBudgetMs = 0) override;
  TxnStatus PrewriteDelete(const std::string& key, const std::string& primaryKey, uint64_t startTs,
                           uint64_t ttlMs, uint64_t forUpdateTs = 0,
                           uint64_t remainingBudgetMs = 0) override;
  TxnStatus PrewriteLock(const std::string& key, const std::string& primaryKey,
                         uint64_t startTs, uint64_t ttlMs, uint64_t forUpdateTs = 0,
                         uint64_t remainingBudgetMs = 0) override;
  TxnStatus AcquirePessimisticLock(const std::string& key, const std::string& primaryKey, uint64_t startTs,
                                   uint64_t ttlMs, uint64_t forUpdateTs = 0,
                                   uint64_t expireAtPhysicalMs = 0) override;
  PessimisticLockResult AcquirePessimisticLockForUpdate(
      const std::string& key, const std::string& primaryKey, uint64_t startTs,
      uint64_t ttlMs, uint64_t forUpdateTs, uint64_t expireAtPhysicalMs,
      uint64_t remainingBudgetMs = 0) override;
  TxnStatus Commit(const std::string& key, uint64_t startTs, uint64_t commitTs) override;
  TxnStatus Rollback(const std::string& key, uint64_t startTs) override;
  std::optional<MvccLock> GetLock(const std::string& key) override;
  std::optional<uint64_t> FindCommitTs(const std::string& key, uint64_t startTs) override;
  TxnStatus CheckTxnStatus(const std::string& primaryKey, uint64_t startTs,
                           uint64_t currentPhysicalMs, bool rollbackIfExpired,
                           uint64_t remainingBudgetMs, TxnRecordStatus* status) override;
  TxnStatus ResolveLock(const std::string& key, uint64_t startTs,
                        TxnRecordState decision, uint64_t commitTs = 0) override;
  std::vector<std::pair<std::string, MvccLock>> ExpiredLocks(uint64_t nowMs) override;
  size_t GarbageCollect(uint64_t safePointTs) override;
  ProtocolCapabilities Capabilities() override;
  MvccStats Stats() override;
  uint64_t MaxObservedTs() override;

 private:
  struct MutationLane {
    std::mutex mutex;
    std::string clientId;
    int requestId = 0;
  };

  std::string Uuid() {
    static std::mt19937_64 rng([] {
      std::random_device rd;
      std::seed_seq seed{rd(), rd(), rd(), rd(), static_cast<unsigned>(time(nullptr))};
      return std::mt19937_64(seed);
    }());
    std::uniform_int_distribution<std::uint64_t> dist;
    return std::to_string(dist(rng)) + std::to_string(dist(rng));
  }

  int shardId_;
  std::vector<std::pair<std::string, short>> addresses_;
  std::vector<std::unique_ptr<MprpcChannel>> channels_;
  std::vector<std::unique_ptr<raftKVRpcProctoc::kvServerRpc_Stub>> stubs_;
  // Leader is only a routing hint. Concurrent requests may update it
  // independently; stale values merely cause one ErrWrongLeader retry.
  std::atomic<int> recentLeaderId_;
  std::string clientId_;
  std::atomic<int> requestId_;
  // Each lane is an ordered idempotency stream, while different lanes can use
  // the channel pool and RegionScheduler concurrently. This removes the former
  // whole-Region client mutex without allowing request IDs from one stream to
  // apply out of order.
  std::vector<std::unique_ptr<MutationLane>> mutationLanes_;
  std::atomic<size_t> nextMutationLane_{0};
  MutationLane& PickMutationLane();
  std::string maintenanceClientId_;
  std::atomic<int> maintenanceRequestId_;
};

#endif  // STRATAKV_TRANSACTION_RAFT_MVCC_STORAGE_H
