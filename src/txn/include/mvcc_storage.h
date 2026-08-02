#ifndef STRATAKV_MVCC_STORAGE_H
#define STRATAKV_MVCC_STORAGE_H

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <vector>

#include "kvEngine.h"

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
};

class MvccStorage {
 public:
  explicit MvccStorage(std::shared_ptr<IKVEngine> engine);
  virtual ~MvccStorage() = default;

  virtual TxnStatus Get(const std::string& key, uint64_t readTs, std::string* value);
  virtual TxnStatus Prewrite(const std::string& key, const std::string& value, const std::string& primaryKey, uint64_t startTs,
                     uint64_t ttlMs);
  virtual TxnStatus PrewriteDelete(const std::string& key, const std::string& primaryKey, uint64_t startTs, uint64_t ttlMs);
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

  std::shared_ptr<IKVEngine> engine_;
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, MvccLock> locks_;
  std::unordered_map<std::string, std::map<uint64_t, MvccWrite>> writes_;
};

#endif  // STRATAKV_MVCC_STORAGE_H
