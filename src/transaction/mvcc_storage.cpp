// Transaction subsystem: MVCC state over the local storage engine.
#include "mvcc_storage.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <vector>

namespace {
uint64_t NowMs() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

std::string EncodeTs(uint64_t ts) {
  std::ostringstream os;
  os.width(20);
  os.fill('0');
  os << ts;
  return os.str();
}

uint64_t DecodeTs(const std::string& encoded) { return std::strtoull(encoded.c_str(), nullptr, 10); }

void AppendField(std::string* output, const std::string& value) {
  *output += std::to_string(value.size());
  *output += ":";
  *output += value;
}

std::string EncodeFields(const std::vector<std::string>& fields) {
  std::string output;
  for (const auto& field : fields) {
    AppendField(&output, field);
  }
  return output;
}

bool DecodeFields(const std::string& input, std::vector<std::string>* fields) {
  fields->clear();
  size_t pos = 0;
  while (pos < input.size()) {
    const size_t colon = input.find(':', pos);
    if (colon == std::string::npos) {
      return false;
    }
    const size_t length = static_cast<size_t>(std::strtoull(input.substr(pos, colon - pos).c_str(), nullptr, 10));
    const size_t begin = colon + 1;
    if (begin + length > input.size()) {
      return false;
    }
    fields->push_back(input.substr(begin, length));
    pos = begin + length;
  }
  return true;
}

std::string WriteTypeName(MvccWriteType type) {
  switch (type) {
    case MvccWriteType::Put:
      return "put";
    case MvccWriteType::Delete:
      return "delete";
    case MvccWriteType::Rollback:
      return "rollback";
  }
  return "put";
}

MvccWriteType ParseWriteType(const std::string& type) {
  if (type == "delete") {
    return MvccWriteType::Delete;
  }
  if (type == "rollback") {
    return MvccWriteType::Rollback;
  }
  return MvccWriteType::Put;
}

std::string EncodeLockValue(const MvccLock& lock) {
  return EncodeFields({lock.primaryKey, std::to_string(lock.startTs), std::to_string(lock.ttlMs),
                       std::to_string(lock.createTimeMs), lock.isDelete ? "1" : "0",
                       lock.isPessimistic ? "1" : "0"});
}

bool DecodeLockValue(const std::string& encoded, MvccLock* lock) {
  std::vector<std::string> fields;
  if (DecodeFields(encoded, &fields) && fields.size() >= 5) {
    lock->primaryKey = fields[0];
    lock->startTs = DecodeTs(fields[1]);
    lock->ttlMs = DecodeTs(fields[2]);
    lock->createTimeMs = DecodeTs(fields[3]);
    lock->isDelete = fields[4] == "1";
    lock->isPessimistic = fields.size() >= 6 && fields[5] == "1";
    return true;
  }

  const size_t separator = encoded.rfind('#');
  if (separator == std::string::npos) {
    return false;
  }
  lock->primaryKey = encoded.substr(0, separator);
  lock->startTs = DecodeTs(encoded.substr(separator + 1));
  lock->ttlMs = 0;
  lock->createTimeMs = NowMs();
  lock->isDelete = false;
  lock->isPessimistic = false;
  return true;
}

std::string EncodeWriteValue(const MvccWrite& write) {
  return EncodeFields({std::to_string(write.startTs), WriteTypeName(write.type)});
}

bool DecodeWriteValue(const std::string& encoded, MvccWrite* write) {
  std::vector<std::string> fields;
  if (DecodeFields(encoded, &fields) && fields.size() >= 2) {
    write->startTs = DecodeTs(fields[0]);
    write->type = ParseWriteType(fields[1]);
    return true;
  }

  write->startTs = DecodeTs(encoded);
  write->type = MvccWriteType::Put;
  return write->startTs > 0;
}

std::string StripPrefix(const std::string& value, const std::string& prefix) { return value.substr(prefix.size()); }

bool SplitKeyTs(const std::string& suffix, std::string* key, uint64_t* ts) {
  const size_t separator = suffix.rfind('/');
  if (separator == std::string::npos) {
    return false;
  }
  *key = suffix.substr(0, separator);
  *ts = DecodeTs(suffix.substr(separator + 1));
  return true;
}
}  // namespace

MvccStorage::MvccStorage(std::shared_ptr<IKVEngine> engine) : engine_(std::move(engine)) { RecoverMetadataFromEngine(); }

TxnStatus MvccStorage::Get(const std::string& key, uint64_t readTs, std::string* value) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  const auto lockIt = locks_.find(key);
  if (lockIt != locks_.end() && lockIt->second.startTs <= readTs) {
    const auto writeItForLock = writes_.find(key);
    bool hasFinishedWrite = false;
    if (writeItForLock != writes_.end()) {
      for (const auto& item : writeItForLock->second) {
        if (item.second.startTs == lockIt->second.startTs) {
          hasFinishedWrite = true;
          break;
        }
      }
    }
    if (!hasFinishedWrite) {
      return TxnStatus::LockConflict;
    }
  }

  const auto writeIt = writes_.find(key);
  if (writeIt == writes_.end()) {
    return TxnStatus::NotFound;
  }

  const auto visible = writeIt->second.upper_bound(readTs);
  if (visible == writeIt->second.begin()) {
    return TxnStatus::NotFound;
  }

  auto version = visible;
  do {
    --version;
    const MvccWrite& write = version->second;
    if (write.type == MvccWriteType::Rollback) {
      if (version == writeIt->second.begin()) {
        return TxnStatus::NotFound;
      }
      continue;
    }
    if (write.type == MvccWriteType::Delete) {
      return TxnStatus::NotFound;
    }
    return engine_->Get(DataKey(key, write.startTs), value) ? TxnStatus::Ok : TxnStatus::StorageError;
  } while (version != writeIt->second.begin());

  return TxnStatus::NotFound;
}

TxnStatus MvccStorage::Prewrite(const std::string& key, const std::string& value, const std::string& primaryKey,
                                uint64_t startTs, uint64_t ttlMs) {
  return PrewriteLocked(key, value, primaryKey, startTs, ttlMs, false);
}

TxnStatus MvccStorage::PrewriteDelete(const std::string& key, const std::string& primaryKey, uint64_t startTs,
                                      uint64_t ttlMs) {
  return PrewriteLocked(key, "", primaryKey, startTs, ttlMs, true);
}

TxnStatus MvccStorage::AcquirePessimisticLock(const std::string& key, const std::string& primaryKey, uint64_t startTs,
                                              uint64_t ttlMs) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  const auto lockIt = locks_.find(key);
  if (lockIt != locks_.end()) {
    return lockIt->second.startTs == startTs ? TxnStatus::Ok : TxnStatus::LockConflict;
  }

  const auto writeIt = writes_.find(key);
  if (writeIt != writes_.end()) {
    for (const auto& item : writeIt->second) {
      if (item.second.startTs == startTs && item.second.type == MvccWriteType::Rollback) {
        return TxnStatus::WriteConflict;
      }
    }
    if (!writeIt->second.empty() && writeIt->second.rbegin()->first >= startTs) {
      return TxnStatus::WriteConflict;
    }
  }

  MvccLock mvccLock{primaryKey, "", startTs, ttlMs, NowMs(), false, true};
  locks_[key] = mvccLock;
  if (!engine_->Put(LockKey(key), EncodeLockValue(mvccLock))) {
    locks_.erase(key);
    return TxnStatus::StorageError;
  }
  return TxnStatus::Ok;
}

TxnStatus MvccStorage::PrewriteLocked(const std::string& key, const std::string& value, const std::string& primaryKey,
                                      uint64_t startTs, uint64_t ttlMs, bool isDelete) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  const auto existingLock = locks_.find(key);
  if (existingLock != locks_.end()) {
    if (existingLock->second.startTs != startTs || !existingLock->second.isPessimistic) {
      return TxnStatus::LockConflict;
    }
  }

  const auto writeIt = writes_.find(key);
  if (writeIt != writes_.end()) {
    for (const auto& item : writeIt->second) {
      if (item.second.startTs == startTs && item.second.type == MvccWriteType::Rollback) {
        return TxnStatus::WriteConflict;
      }
    }
    if (!writeIt->second.empty() && writeIt->second.rbegin()->first >= startTs) {
      return TxnStatus::WriteConflict;
    }
  }

  MvccLock mvccLock{primaryKey, value, startTs, ttlMs, NowMs(), isDelete, false};
  std::vector<KVBatchOp> ops;
  if (isDelete) {
    ops.push_back({KVBatchOpType::Delete, DataKey(key, startTs), {}});
  } else {
    ops.push_back({KVBatchOpType::Put, DataKey(key, startTs), value});
  }
  ops.push_back({KVBatchOpType::Put, LockKey(key), EncodeLockValue(mvccLock)});
  if (!engine_->WriteBatch(ops)) {
    return TxnStatus::StorageError;
  }
  locks_[key] = mvccLock;
  return TxnStatus::Ok;
}

TxnStatus MvccStorage::Commit(const std::string& key, uint64_t startTs, uint64_t commitTs) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  const auto lockIt = locks_.find(key);
  if (lockIt == locks_.end()) {
    const auto writeIt = writes_.find(key);
    if (writeIt != writes_.end()) {
      for (const auto& item : writeIt->second) {
        if (item.second.startTs == startTs) {
          return item.second.type == MvccWriteType::Rollback ? TxnStatus::WriteConflict : TxnStatus::AlreadyCommitted;
        }
      }
    }
    return TxnStatus::NotFound;
  }
  if (lockIt->second.startTs != startTs) {
    return TxnStatus::LockConflict;
  }

  const auto writeIt = writes_.find(key);
  if (writeIt != writes_.end()) {
    const auto commitPos = writeIt->second.find(commitTs);
    if (commitPos != writeIt->second.end()) {
      return commitPos->second.startTs == startTs ? TxnStatus::AlreadyCommitted : TxnStatus::WriteConflict;
    }
    const auto lower = writeIt->second.lower_bound(startTs);
    for (auto it = lower; it != writeIt->second.end() && it->first <= commitTs; ++it) {
      if (it->second.startTs != startTs && it->second.type != MvccWriteType::Rollback) {
        return TxnStatus::WriteConflict;
      }
    }
  }

  const MvccWriteType type = lockIt->second.isDelete ? MvccWriteType::Delete : MvccWriteType::Put;
  MvccWrite write{startTs, commitTs, type};
  if (!engine_->WriteBatch({{KVBatchOpType::Put, WriteKey(key, commitTs), EncodeWriteValue(write)},
                            {KVBatchOpType::Delete, LockKey(key), {}}})) {
    return TxnStatus::StorageError;
  }
  writes_[key][commitTs] = write;
  locks_.erase(lockIt);
  return TxnStatus::Ok;
}

TxnStatus MvccStorage::Rollback(const std::string& key, uint64_t startTs) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  const auto writeIt = writes_.find(key);
  if (writeIt != writes_.end()) {
    for (const auto& item : writeIt->second) {
      if (item.second.startTs == startTs && item.second.type != MvccWriteType::Rollback) {
        return TxnStatus::AlreadyCommitted;
      }
    }
  }

  const auto lockIt = locks_.find(key);
  MvccWrite rollback{startTs, startTs, MvccWriteType::Rollback};
  std::vector<KVBatchOp> ops;
  if (lockIt != locks_.end() && lockIt->second.startTs == startTs) {
    ops.push_back({KVBatchOpType::Delete, LockKey(key), {}});
  }
  ops.push_back({KVBatchOpType::Delete, DataKey(key, startTs), {}});
  ops.push_back({KVBatchOpType::Put, WriteKey(key, startTs), EncodeWriteValue(rollback)});
  if (!engine_->WriteBatch(ops)) {
    return TxnStatus::StorageError;
  }
  if (lockIt != locks_.end() && lockIt->second.startTs == startTs) {
    locks_.erase(lockIt);
  }
  writes_[key][startTs] = rollback;
  return TxnStatus::Ok;
}

std::optional<MvccLock> MvccStorage::GetLock(const std::string& key) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  const auto it = locks_.find(key);
  if (it == locks_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<uint64_t> MvccStorage::FindCommitTs(const std::string& key, uint64_t startTs) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  const auto writeIt = writes_.find(key);
  if (writeIt == writes_.end()) {
    return std::nullopt;
  }
  for (const auto& item : writeIt->second) {
    if (item.second.startTs == startTs) {
      return item.first;
    }
  }
  return std::nullopt;
}

std::vector<std::pair<std::string, MvccLock>> MvccStorage::ExpiredLocks(uint64_t nowMs) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  std::vector<std::pair<std::string, MvccLock>> expired;
  for (const auto& item : locks_) {
    if (item.second.createTimeMs + item.second.ttlMs <= nowMs) {
      expired.push_back(item);
    }
  }
  return expired;
}

size_t MvccStorage::GarbageCollect(uint64_t safePointTs) {
  std::vector<std::string> keys_to_delete;
  size_t removed = 0;
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    for (auto& keyWrites : writes_) {
      auto& versions = keyWrites.second;
      if (versions.size() <= 1) {
        continue;
      }

      std::optional<uint64_t> keepCommitTs;
      for (auto it = versions.begin(); it != versions.end() && it->first < safePointTs; ++it) {
        if (it->second.type != MvccWriteType::Rollback) {
          keepCommitTs = it->first;
        }
      }

      for (auto it = versions.begin(); it != versions.end();) {
        if (it->first >= safePointTs || (keepCommitTs.has_value() && it->first == keepCommitTs.value())) {
          ++it;
          continue;
        }
        if (it->second.type == MvccWriteType::Put) {
          keys_to_delete.push_back(DataKey(keyWrites.first, it->second.startTs));
        }
        keys_to_delete.push_back(WriteKey(keyWrites.first, it->first));
        it = versions.erase(it);
        ++removed;
      }
    }
  }

  if (!keys_to_delete.empty()) {
    std::vector<KVBatchOp> ops;
    ops.reserve(keys_to_delete.size());
    for (const auto& key : keys_to_delete) {
      ops.push_back({KVBatchOpType::Delete, key, {}});
    }
    engine_->WriteBatch(ops);
  }
  return removed;
}

MvccStats MvccStorage::Stats() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  MvccStats stats;
  stats.lockCount = locks_.size();
  for (const auto& keyWrites : writes_) {
    stats.writeCount += keyWrites.second.size();
    stats.dataVersionCount += keyWrites.second.size();
  }
  stats.dataVersionCount += locks_.size();
  return stats;
}

uint64_t MvccStorage::MaxObservedTs() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  uint64_t maxTs = 0;
  for (const auto& item : locks_) {
    maxTs = std::max(maxTs, item.second.startTs);
  }
  for (const auto& keyWrites : writes_) {
    for (const auto& item : keyWrites.second) {
      maxTs = std::max(maxTs, std::max(item.first, item.second.startTs));
    }
  }
  return maxTs;
}

void MvccStorage::RecoverMetadataFromEngine() {
  if (!engine_) {
    return;
  }
  std::unique_lock<std::shared_mutex> lock(mutex_);
  RecoverMetadataFromEngineLocked();
}

bool MvccStorage::RestoreSnapshot(const std::string& snapshot) {
  if (!engine_) {
    return false;
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);
  if (!engine_->Load(snapshot)) {
    return false;
  }
  RecoverMetadataFromEngineLocked();
  return true;
}

void MvccStorage::RecoverMetadataFromEngineLocked() {
  locks_.clear();
  writes_.clear();

  for (const auto& item : engine_->ScanPrefix("write/")) {
    std::string key;
    uint64_t commitTs = 0;
    if (!SplitKeyTs(StripPrefix(item.first, "write/"), &key, &commitTs)) {
      continue;
    }
    MvccWrite write;
    if (!DecodeWriteValue(item.second, &write)) {
      continue;
    }
    write.commitTs = commitTs;
    writes_[key][commitTs] = write;
  }

  for (const auto& item : engine_->ScanPrefix("lock/")) {
    MvccLock lock;
    if (!DecodeLockValue(item.second, &lock)) {
      continue;
    }
    const std::string key = StripPrefix(item.first, "lock/");

    bool hasFinishedWrite = false;
    const auto writeIt = writes_.find(key);
    if (writeIt != writes_.end()) {
      for (const auto& version : writeIt->second) {
        if (version.second.startTs == lock.startTs) {
          hasFinishedWrite = true;
          break;
        }
      }
    }
    if (hasFinishedWrite) {
      engine_->Delete(LockKey(key));
      continue;
    }

    std::string value;
    if (!lock.isDelete && engine_->Get(DataKey(key, lock.startTs), &value)) {
      lock.value = value;
    }
    locks_[key] = lock;
  }
}

std::string MvccStorage::DataKey(const std::string& key, uint64_t startTs) { return "data/" + key + "/" + EncodeTs(startTs); }

std::string MvccStorage::LockKey(const std::string& key) { return "lock/" + key; }

std::string MvccStorage::WriteKey(const std::string& key, uint64_t commitTs) {
  return "write/" + key + "/" + EncodeTs(commitTs);
}
