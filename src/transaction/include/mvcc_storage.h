#ifndef STRATAKV_TRANSACTION_MVCC_STORAGE_H
#define STRATAKV_TRANSACTION_MVCC_STORAGE_H

#include <cstdint>
#include <atomic>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <vector>

#include "kv_engine.h"

enum class TxnStatus {
  Ok,
  NotFound,
  LockConflict,
  WriteConflict,
  AlreadyCommitted,
  StorageError,
};

struct MvccLock {
  std::string primaryKey;
  std::string value;
  uint64_t startTs = 0;
  uint64_t ttlMs = 0;
  uint64_t createTimeMs = 0;
  bool isDelete = false;
  bool isPessimistic = false;
};

enum class MvccWriteType {
  Put,
  Delete,
  Rollback,
};

struct MvccWrite {
  uint64_t startTs = 0;
  uint64_t commitTs = 0;
  MvccWriteType type = MvccWriteType::Put;
};

struct MvccStats {
  size_t lockCount = 0;
  size_t writeCount = 0;
  size_t dataVersionCount = 0;
  uint64_t writeBatchCount = 0;
  uint64_t appliedRaftIndex = 0;
};

// Versioned, storage-engine-neutral mutations carried by a Raft entry.  The
// payload contains strings and Put/Delete operations only; it never contains a
// RocksDB object, pointer, or process-local handle.
struct PreparedMvccWrite {
  static constexpr uint32_t kCommandVersion = 1;

  uint32_t commandVersion = kCommandVersion;
  TxnStatus status = TxnStatus::StorageError;
  uint64_t expectedRevision = 0;
  uint64_t newRevision = 0;
  std::vector<KVBatchOp> modifications;

  bool HasChanges() const { return status == TxnStatus::Ok && !modifications.empty(); }
  std::string Serialize() const;
  bool Parse(const std::string& encoded);
};

class MvccStorage {
 public:
  explicit MvccStorage(std::shared_ptr<IKVEngine> engine);
  virtual ~MvccStorage() = default;

  virtual TxnStatus Get(const std::string& key, uint64_t readTs, std::string* value);
  virtual TxnStatus Prewrite(const std::string& key, const std::string& value, const std::string& primaryKey, uint64_t startTs,
                     uint64_t ttlMs);
  virtual TxnStatus PrewriteDelete(const std::string& key, const std::string& primaryKey, uint64_t startTs, uint64_t ttlMs);
  // Read-only fast rejection used by a Region leader before proposing a
  // semantic Prewrite command.  Callers must still keep the authoritative
  // Apply-time check unless a scheduler latch protects the entire lifecycle.
  virtual TxnStatus PrecheckPrewrite(const std::string& key, uint64_t startTs);
  PreparedMvccWrite PreparePrewrite(const std::string& key, const std::string& value,
                                    const std::string& primaryKey, uint64_t startTs, uint64_t ttlMs,
                                    bool isDelete);
  PreparedMvccWrite PreparePessimisticLock(const std::string& key, const std::string& primaryKey,
                                           uint64_t startTs, uint64_t ttlMs);
  PreparedMvccWrite PrepareCommit(const std::string& key, uint64_t startTs, uint64_t commitTs);
  PreparedMvccWrite PrepareRollback(const std::string& key, uint64_t startTs);
  // When appliedRaftIndex is non-zero it is persisted in the same WriteBatch
  // as the MVCC mutations, providing an atomic business-data/apply-progress
  // checkpoint. Unit tests and non-Raft callers may leave it at zero.
  TxnStatus ApplyPrepared(const std::string& key, const PreparedMvccWrite& prepared,
                          uint64_t appliedRaftIndex = 0);
  virtual TxnStatus AcquirePessimisticLock(const std::string& key, const std::string& primaryKey, uint64_t startTs,
                                   uint64_t ttlMs);
  virtual TxnStatus Commit(const std::string& key, uint64_t startTs, uint64_t commitTs);
  virtual TxnStatus Rollback(const std::string& key, uint64_t startTs);
  virtual std::optional<MvccLock> GetLock(const std::string& key);
  virtual std::optional<uint64_t> FindCommitTs(const std::string& key, uint64_t startTs);
  virtual std::vector<std::pair<std::string, MvccLock>> ExpiredLocks(uint64_t nowMs);
  virtual size_t GarbageCollect(uint64_t safePointTs);
  virtual MvccStats Stats();
  virtual uint64_t MaxObservedTs();
  // Replace the engine contents with a Raft snapshot and rebuild all in-memory
  // MVCC indexes while holding the same lock used by foreground operations.
  bool RestoreSnapshot(const std::string& snapshot);

 private:
  TxnStatus PrewriteLocked(const std::string& key, const std::string& value, const std::string& primaryKey,
                           uint64_t startTs, uint64_t ttlMs, bool isDelete);
  void RecoverMetadataFromEngine();
  void RecoverMetadataFromEngineLocked();

  static std::string DataKey(const std::string& key, uint64_t startTs);
  static std::string LockKey(const std::string& key);
  static std::string WriteKey(const std::string& key, uint64_t commitTs);
  static std::string RevisionKey(const std::string& key);

  uint64_t CurrentRevisionLocked(const std::string& key) const;
  void AppendRevisionMutationLocked(const std::string& key, std::vector<KVBatchOp>* ops,
                                    uint64_t* newRevision);
  void ApplyMetadataMutationLocked(const KVBatchOp& op);

  std::shared_ptr<IKVEngine> engine_;
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, MvccLock> locks_;
  std::unordered_map<std::string, std::map<uint64_t, MvccWrite>> writes_;
  std::unordered_map<std::string, uint64_t> revisions_;
  std::atomic<uint64_t> writeBatchCount_{0};
  uint64_t appliedRaftIndex_ = 0;
};

#endif  // STRATAKV_TRANSACTION_MVCC_STORAGE_H
