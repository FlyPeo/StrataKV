// Transaction subsystem: MVCC state over the local storage engine.
#include "mvcc_storage.h"
#include "timestamp_oracle.h"

#include "kv_server_rpc.pb.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <unordered_set>
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
    case MvccWriteType::Lock:
      return "lock";
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
  if (type == "lock") {
    return MvccWriteType::Lock;
  }
  return MvccWriteType::Put;
}

std::string EncodeLockValue(const MvccLock& lock) {
  return EncodeFields({lock.primaryKey, std::to_string(lock.startTs), std::to_string(lock.ttlMs),
                       std::to_string(lock.createTimeMs), lock.isDelete ? "1" : "0",
                       lock.isPessimistic ? "1" : "0",
                       std::to_string(lock.forUpdateTs == 0 ? lock.startTs : lock.forUpdateTs),
                       std::to_string(lock.expireAtPhysicalMs), lock.isLockOnly ? "1" : "0"});
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
    lock->forUpdateTs = fields.size() >= 7 ? DecodeTs(fields[6]) : lock->startTs;
    if (lock->forUpdateTs == 0) lock->forUpdateTs = lock->startTs;
    lock->expireAtPhysicalMs = fields.size() >= 8 ? DecodeTs(fields[7]) : 0;
    lock->legacyExpiry = fields.size() < 8 || lock->expireAtPhysicalMs == 0;
    lock->isLockOnly = fields.size() >= 9 && fields[8] == "1";
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
  lock->forUpdateTs = lock->startTs;
  lock->expireAtPhysicalMs = 0;
  lock->legacyExpiry = true;
  lock->isLockOnly = false;
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

std::string PreparedMvccWrite::Serialize() const {
  raftKVRpcProctoc::PreparedMvccWriteCommand command;
  command.set_version(commandVersion);
  command.set_preparedstatus(static_cast<int32_t>(status));
  command.set_expectedrevision(expectedRevision);
  command.set_newrevision(newRevision);
  for (const auto& modification : modifications) {
    auto* encoded = command.add_modifications();
    encoded->set_type(modification.type == KVBatchOpType::Delete
                          ? raftKVRpcProctoc::MVCC_MODIFICATION_DELETE
                          : raftKVRpcProctoc::MVCC_MODIFICATION_PUT);
    encoded->set_key(modification.key);
    encoded->set_value(modification.value);
  }
  std::string encoded;
  if (!command.SerializeToString(&encoded)) return {};
  return encoded;
}

bool PreparedMvccWrite::Parse(const std::string& encoded) {
  raftKVRpcProctoc::PreparedMvccWriteCommand command;
  if (!command.ParseFromString(encoded) ||
      (command.version() != kLegacyCommandVersion && command.version() != kCommandVersion) ||
      command.modifications_size() > 1000000) return false;
  commandVersion = command.version();
  const int32_t rawStatus = command.preparedstatus();
  if (rawStatus < static_cast<int32_t>(TxnStatus::Ok) ||
      rawStatus > static_cast<int32_t>(TxnStatus::StorageError)) {
    return false;
  }
  status = static_cast<TxnStatus>(rawStatus);
  expectedRevision = command.expectedrevision();
  newRevision = command.newrevision();
  modifications.clear();
  modifications.reserve(static_cast<size_t>(command.modifications_size()));
  for (const auto& encodedModification : command.modifications()) {
    KVBatchOp modification;
    if (encodedModification.type() != raftKVRpcProctoc::MVCC_MODIFICATION_PUT &&
        encodedModification.type() != raftKVRpcProctoc::MVCC_MODIFICATION_DELETE) {
      modifications.clear();
      return false;
    }
    modification.type = encodedModification.type() == raftKVRpcProctoc::MVCC_MODIFICATION_DELETE
                            ? KVBatchOpType::Delete
                            : KVBatchOpType::Put;
    modification.key = encodedModification.key();
    modification.value = encodedModification.value();
    modifications.push_back(std::move(modification));
  }
  return true;
}

bool PreparedMvccBatch::HasChanges() const {
  if (status != TxnStatus::Ok) return false;
  return std::any_of(items.begin(), items.end(), [](const PreparedMvccBatchItem& item) {
    return item.prepared.HasChanges();
  });
}

std::string PreparedMvccBatch::Serialize() const {
  raftKVRpcProctoc::PreparedMvccBatchCommand command;
  command.set_version(commandVersion);
  for (const auto& item : items) {
    auto* encodedItem = command.add_items();
    encodedItem->set_key(item.key);
    const std::string encodedPrepared = item.prepared.Serialize();
    if (encodedPrepared.empty() ||
        !encodedItem->mutable_prepared()->ParseFromString(encodedPrepared)) {
      return {};
    }
  }
  std::string encoded;
  return command.SerializeToString(&encoded) ? encoded : std::string{};
}

bool PreparedMvccBatch::Parse(const std::string& encoded) {
  raftKVRpcProctoc::PreparedMvccBatchCommand command;
  if (!command.ParseFromString(encoded) || command.version() != kCommandVersion ||
      command.items_size() <= 0 || command.items_size() > 100000) {
    return false;
  }
  commandVersion = command.version();
  status = TxnStatus::Ok;
  items.clear();
  items.reserve(static_cast<size_t>(command.items_size()));
  std::string previousKey;
  for (const auto& encodedItem : command.items()) {
    if (encodedItem.key().empty() || (!previousKey.empty() && encodedItem.key() <= previousKey)) {
      items.clear();
      return false;
    }
    PreparedMvccBatchItem item;
    item.key = encodedItem.key();
    if (!item.prepared.Parse(encodedItem.prepared().SerializeAsString()) ||
        item.prepared.status != TxnStatus::Ok) {
      items.clear();
      return false;
    }
    previousKey = item.key;
    items.push_back(std::move(item));
  }
  return true;
}

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
    if (write.type == MvccWriteType::Rollback || write.type == MvccWriteType::Lock) {
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
                                uint64_t startTs, uint64_t ttlMs, uint64_t forUpdateTs,
                                uint64_t remainingBudgetMs) {
  (void)remainingBudgetMs;
  return PrewriteLocked(key, value, primaryKey, startTs, ttlMs, false, forUpdateTs);
}

TxnStatus MvccStorage::PrewriteDelete(const std::string& key, const std::string& primaryKey, uint64_t startTs,
                                      uint64_t ttlMs, uint64_t forUpdateTs,
                                      uint64_t remainingBudgetMs) {
  (void)remainingBudgetMs;
  return PrewriteLocked(key, "", primaryKey, startTs, ttlMs, true, forUpdateTs);
}

TxnStatus MvccStorage::PrewriteLock(const std::string& key, const std::string& primaryKey,
                                    uint64_t startTs, uint64_t ttlMs, uint64_t forUpdateTs,
                                    uint64_t remainingBudgetMs) {
  (void)remainingBudgetMs;
  PreparedMvccWrite prepared =
      PreparePrewriteLock(key, primaryKey, startTs, ttlMs, forUpdateTs);
  if (prepared.status != TxnStatus::Ok || !prepared.HasChanges()) return prepared.status;
  return ApplyPrepared(key, prepared);
}

TxnStatus MvccStorage::PrecheckPrewrite(const std::string& key, uint64_t startTs) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  const auto existingLock = locks_.find(key);
  if (existingLock != locks_.end() && existingLock->second.startTs != startTs) {
    return TxnStatus::LockConflict;
  }

  const auto writeIt = writes_.find(key);
  if (writeIt == writes_.end()) {
    return TxnStatus::Ok;
  }
  for (const auto& item : writeIt->second) {
    if (item.second.startTs == startTs && item.second.type == MvccWriteType::Rollback) {
      return TxnStatus::WriteConflict;
    }
  }
  if (!writeIt->second.empty() && writeIt->second.rbegin()->first >= startTs) {
    return TxnStatus::WriteConflict;
  }
  return TxnStatus::Ok;
}

uint64_t MvccStorage::CurrentRevisionLocked(const std::string& key) const {
  const auto revision = revisions_.find(key);
  return revision == revisions_.end() ? 0 : revision->second;
}

TxnStatus MvccStorage::ReadCommittedLocked(const std::string& key, uint64_t readTs,
                                           std::string* value, uint64_t* commitTs) const {
  const auto writeIt = writes_.find(key);
  if (writeIt == writes_.end()) return TxnStatus::NotFound;

  auto version = writeIt->second.upper_bound(readTs);
  while (version != writeIt->second.begin()) {
    --version;
    const MvccWrite& write = version->second;
    if (write.type == MvccWriteType::Rollback || write.type == MvccWriteType::Lock) continue;
    if (commitTs != nullptr) *commitTs = version->first;
    if (write.type == MvccWriteType::Delete) return TxnStatus::NotFound;
    return engine_->Get(DataKey(key, write.startTs), value) ? TxnStatus::Ok
                                                            : TxnStatus::StorageError;
  }
  return TxnStatus::NotFound;
}

void MvccStorage::AppendRevisionMutationLocked(const std::string& key, std::vector<KVBatchOp>* ops,
                                               uint64_t* newRevision) {
  *newRevision = CurrentRevisionLocked(key) + 1;
  ops->push_back({KVBatchOpType::Put, RevisionKey(key), std::to_string(*newRevision)});
}

PreparedMvccWrite MvccStorage::PreparePrewrite(const std::string& key, const std::string& value,
                                               const std::string& primaryKey, uint64_t startTs,
                                               uint64_t ttlMs, bool isDelete,
                                               uint64_t forUpdateTs) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  PreparedMvccWrite prepared;
  prepared.expectedRevision = CurrentRevisionLocked(key);

  const auto existingLock = locks_.find(key);
  if (existingLock != locks_.end()) {
    if (existingLock->second.startTs != startTs) {
      prepared.status = TxnStatus::LockConflict;
      return prepared;
    }
    if (existingLock->second.primaryKey != primaryKey) {
      prepared.status = TxnStatus::LockConflict;
      return prepared;
    }
    if (!existingLock->second.isPessimistic) {
      // A retry of an already applied Prewrite is successful without another
      // Raft entry. The persisted lock is the idempotency evidence.
      prepared.status = TxnStatus::Ok;
      return prepared;
    }
    if (forUpdateTs != 0 && forUpdateTs < existingLock->second.forUpdateTs) {
      prepared.status = TxnStatus::WriteConflict;
      return prepared;
    }
    if (forUpdateTs == 0 && !existingLock->second.legacyExpiry && existingLock->second.isPessimistic) {
      prepared.status = TxnStatus::WriteConflict;
      return prepared;
    }
  }

  const auto writeIt = writes_.find(key);
  if (writeIt != writes_.end()) {
    for (const auto& item : writeIt->second) {
      if (item.second.startTs == startTs && item.second.type == MvccWriteType::Rollback) {
        prepared.status = TxnStatus::WriteConflict;
        return prepared;
      }
    }
    if (!writeIt->second.empty() && writeIt->second.rbegin()->first >= startTs) {
      prepared.status = TxnStatus::WriteConflict;
      return prepared;
    }
  }

  MvccLock mvccLock{primaryKey, value, startTs, ttlMs, NowMs(), isDelete, false};
  if (existingLock != locks_.end() && existingLock->second.isPessimistic) {
    mvccLock.forUpdateTs = existingLock->second.forUpdateTs;
    mvccLock.expireAtPhysicalMs = existingLock->second.expireAtPhysicalMs;
    mvccLock.legacyExpiry = existingLock->second.legacyExpiry;
  } else {
    mvccLock.forUpdateTs = forUpdateTs == 0 ? startTs : forUpdateTs;
    const uint64_t baseMs = HlcTimestamp::PhysicalMs(mvccLock.forUpdateTs);
    mvccLock.expireAtPhysicalMs = (baseMs > 0 ? baseMs : NowMs()) + ttlMs;
    mvccLock.legacyExpiry = false;
  }
  if (isDelete) {
    prepared.modifications.push_back({KVBatchOpType::Delete, DataKey(key, startTs), {}});
  } else {
    prepared.modifications.push_back({KVBatchOpType::Put, DataKey(key, startTs), value});
  }
  prepared.modifications.push_back({KVBatchOpType::Put, LockKey(key), EncodeLockValue(mvccLock)});
  AppendRevisionMutationLocked(key, &prepared.modifications, &prepared.newRevision);
  prepared.status = TxnStatus::Ok;
  return prepared;
}

PreparedMvccWrite MvccStorage::PreparePessimisticLock(const std::string& key, const std::string& primaryKey,
                                                      uint64_t startTs, uint64_t ttlMs,
                                                      uint64_t forUpdateTs,
                                                      uint64_t expireAtPhysicalMs) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  PreparedMvccWrite prepared;
  prepared.commandVersion = PreparedMvccWrite::kCommandVersion;
  prepared.expectedRevision = CurrentRevisionLocked(key);
  const uint64_t requestedForUpdateTs = forUpdateTs == 0 ? startTs : forUpdateTs;
  const auto existingLock = locks_.find(key);
  if (existingLock != locks_.end()) {
    if (existingLock->second.startTs != startTs) {
      prepared.status = TxnStatus::LockConflict;
      return prepared;
    }
    if (!existingLock->second.isPessimistic || existingLock->second.primaryKey != primaryKey) {
      prepared.status = TxnStatus::LockConflict;
      return prepared;
    }

    const uint64_t effectiveForUpdateTs =
        std::max(existingLock->second.forUpdateTs, requestedForUpdateTs);
    prepared.readStatus =
        ReadCommittedLocked(key, effectiveForUpdateTs, &prepared.readValue, &prepared.readCommitTs);
    if (prepared.readStatus == TxnStatus::StorageError) {
      prepared.status = TxnStatus::StorageError;
      return prepared;
    }
    const uint64_t effectiveExpiry =
        std::max(existingLock->second.expireAtPhysicalMs, expireAtPhysicalMs);
    if (effectiveForUpdateTs == existingLock->second.forUpdateTs &&
        effectiveExpiry == existingLock->second.expireAtPhysicalMs) {
      prepared.status = TxnStatus::Ok;
      return prepared;
    }

    MvccLock advanced = existingLock->second;
    advanced.forUpdateTs = effectiveForUpdateTs;
    advanced.expireAtPhysicalMs = effectiveExpiry;
    advanced.legacyExpiry = effectiveExpiry == 0;
    prepared.modifications.push_back({KVBatchOpType::Put, LockKey(key), EncodeLockValue(advanced)});
    AppendRevisionMutationLocked(key, &prepared.modifications, &prepared.newRevision);
    prepared.status = TxnStatus::Ok;
    return prepared;
  }
  const auto writeIt = writes_.find(key);
  if (writeIt != writes_.end()) {
    for (const auto& item : writeIt->second) {
      if (item.second.startTs == startTs && item.second.type == MvccWriteType::Rollback) {
        prepared.status = TxnStatus::WriteConflict;
        return prepared;
      }
    }
    for (auto latest = writeIt->second.rbegin(); latest != writeIt->second.rend(); ++latest) {
      if (latest->second.type == MvccWriteType::Rollback) continue;
      if (latest->first > requestedForUpdateTs) prepared.status = TxnStatus::WriteConflict;
      if (prepared.status == TxnStatus::WriteConflict) return prepared;
      break;
    }
  }
  prepared.readStatus =
      ReadCommittedLocked(key, requestedForUpdateTs, &prepared.readValue, &prepared.readCommitTs);
  if (prepared.readStatus == TxnStatus::StorageError) {
    prepared.status = TxnStatus::StorageError;
    return prepared;
  }
  MvccLock mvccLock{primaryKey, "", startTs, ttlMs, NowMs(), false, true};
  mvccLock.forUpdateTs = requestedForUpdateTs;
  mvccLock.expireAtPhysicalMs = expireAtPhysicalMs;
  mvccLock.legacyExpiry = expireAtPhysicalMs == 0;
  prepared.modifications.push_back({KVBatchOpType::Put, LockKey(key), EncodeLockValue(mvccLock)});
  AppendRevisionMutationLocked(key, &prepared.modifications, &prepared.newRevision);
  prepared.commandVersion = PreparedMvccWrite::kCommandVersion;
  prepared.status = TxnStatus::Ok;
  return prepared;
}

PreparedMvccWrite MvccStorage::PreparePrewriteLock(const std::string& key,
                                                   const std::string& primaryKey,
                                                   uint64_t startTs, uint64_t ttlMs,
                                                   uint64_t forUpdateTs) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  PreparedMvccWrite prepared;
  prepared.commandVersion = PreparedMvccWrite::kCommandVersion;
  prepared.expectedRevision = CurrentRevisionLocked(key);
  const auto existing = locks_.find(key);
  if (existing == locks_.end() || existing->second.startTs != startTs ||
      existing->second.primaryKey != primaryKey) {
    prepared.status = existing == locks_.end() ? TxnStatus::NotFound : TxnStatus::LockConflict;
    return prepared;
  }
  if (!existing->second.isPessimistic) {
    prepared.status = existing->second.isLockOnly ? TxnStatus::Ok : TxnStatus::LockConflict;
    return prepared;
  }
  if (forUpdateTs != 0 && forUpdateTs < existing->second.forUpdateTs) {
    prepared.status = TxnStatus::WriteConflict;
    return prepared;
  }

  MvccLock upgraded = existing->second;
  upgraded.isPessimistic = false;
  upgraded.isLockOnly = true;
  upgraded.ttlMs = ttlMs;
  prepared.modifications.push_back({KVBatchOpType::Put, LockKey(key), EncodeLockValue(upgraded)});
  AppendRevisionMutationLocked(key, &prepared.modifications, &prepared.newRevision);
  prepared.status = TxnStatus::Ok;
  return prepared;
}

PreparedMvccWrite MvccStorage::PrepareCommit(const std::string& key, uint64_t startTs, uint64_t commitTs) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  PreparedMvccWrite prepared;
  prepared.expectedRevision = CurrentRevisionLocked(key);
  const auto lockIt = locks_.find(key);
  if (lockIt == locks_.end()) {
    const auto writeIt = writes_.find(key);
    if (writeIt != writes_.end()) {
      for (const auto& item : writeIt->second) {
        if (item.second.startTs == startTs) {
          prepared.status = item.second.type == MvccWriteType::Rollback ? TxnStatus::WriteConflict
                                                                        : TxnStatus::AlreadyCommitted;
          return prepared;
        }
      }
    }
    prepared.status = TxnStatus::NotFound;
    return prepared;
  }
  if (lockIt->second.startTs != startTs) {
    prepared.status = TxnStatus::LockConflict;
    return prepared;
  }
  if (commitTs <= std::max(startTs, lockIt->second.forUpdateTs)) {
    prepared.status = TxnStatus::WriteConflict;
    return prepared;
  }

  const auto writeIt = writes_.find(key);
  if (writeIt != writes_.end()) {
    const auto commitPos = writeIt->second.find(commitTs);
    if (commitPos != writeIt->second.end()) {
      prepared.status = commitPos->second.startTs == startTs ? TxnStatus::AlreadyCommitted
                                                              : TxnStatus::WriteConflict;
      return prepared;
    }
    const auto lower = writeIt->second.lower_bound(startTs);
    for (auto it = lower; it != writeIt->second.end() && it->first <= commitTs; ++it) {
      if (it->second.startTs != startTs && it->second.type != MvccWriteType::Rollback) {
        prepared.status = TxnStatus::WriteConflict;
        return prepared;
      }
    }
  }

  const MvccWrite write{
      startTs, commitTs,
      lockIt->second.isLockOnly
          ? MvccWriteType::Lock
          : (lockIt->second.isDelete ? MvccWriteType::Delete : MvccWriteType::Put)};
  prepared.modifications.push_back({KVBatchOpType::Put, WriteKey(key, commitTs), EncodeWriteValue(write)});
  prepared.modifications.push_back({KVBatchOpType::Delete, LockKey(key), {}});
  AppendRevisionMutationLocked(key, &prepared.modifications, &prepared.newRevision);
  prepared.status = TxnStatus::Ok;
  return prepared;
}

PreparedMvccWrite MvccStorage::PrepareRollback(const std::string& key, uint64_t startTs) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return PrepareRollbackLocked(key, startTs);
}

PreparedMvccWrite MvccStorage::PrepareRollbackLocked(const std::string& key,
                                                      uint64_t startTs) {
  PreparedMvccWrite prepared;
  prepared.expectedRevision = CurrentRevisionLocked(key);
  const auto writeIt = writes_.find(key);
  if (writeIt != writes_.end()) {
    for (const auto& item : writeIt->second) {
      if (item.second.startTs != startTs) continue;
      if (item.second.type == MvccWriteType::Rollback) {
        prepared.status = TxnStatus::Ok;
      } else {
        prepared.status = TxnStatus::AlreadyCommitted;
      }
      return prepared;
    }
  }

  const auto lockIt = locks_.find(key);
  if (lockIt != locks_.end() && lockIt->second.startTs == startTs) {
    prepared.modifications.push_back({KVBatchOpType::Delete, LockKey(key), {}});
  }
  const MvccWrite rollback{startTs, startTs, MvccWriteType::Rollback};
  prepared.modifications.push_back({KVBatchOpType::Delete, DataKey(key, startTs), {}});
  prepared.modifications.push_back({KVBatchOpType::Put, WriteKey(key, startTs), EncodeWriteValue(rollback)});
  AppendRevisionMutationLocked(key, &prepared.modifications, &prepared.newRevision);
  prepared.status = TxnStatus::Ok;
  return prepared;
}

TxnRecordStatus MvccStorage::InspectTxnStatusLocked(const std::string& key,
                                                    uint64_t startTs) const {
  const auto writeIt = writes_.find(key);
  if (writeIt != writes_.end()) {
    for (const auto& item : writeIt->second) {
      if (item.second.startTs != startTs) continue;
      if (item.second.type == MvccWriteType::Rollback) {
        return TxnRecordStatus{TxnRecordState::RolledBack, 0, std::nullopt};
      }
      return TxnRecordStatus{TxnRecordState::Committed, item.first, std::nullopt};
    }
  }

  const auto lockIt = locks_.find(key);
  if (lockIt != locks_.end() && lockIt->second.startTs == startTs) {
    return TxnRecordStatus{TxnRecordState::Locked, 0, lockIt->second};
  }
  return TxnRecordStatus{};
}

PreparedMvccWrite MvccStorage::PrepareCheckTxnStatus(const std::string& primaryKey,
                                                     uint64_t startTs,
                                                     uint64_t currentPhysicalMs,
                                                     bool rollbackIfExpired) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  PreparedMvccWrite prepared;
  prepared.expectedRevision = CurrentRevisionLocked(primaryKey);
  prepared.status = TxnStatus::Ok;
  prepared.txnRecordStatus = InspectTxnStatusLocked(primaryKey, startTs);
  if (!rollbackIfExpired || prepared.txnRecordStatus.state != TxnRecordState::Locked ||
      !prepared.txnRecordStatus.lock.has_value()) {
    return prepared;
  }

  const MvccLock& txnLock = *prepared.txnRecordStatus.lock;
  // Legacy createTimeMs came from another process's steady_clock epoch and is
  // not comparable to HLC physical time. It must never be auto-expired here.
  const bool expired = !txnLock.legacyExpiry && txnLock.expireAtPhysicalMs != 0 &&
                       txnLock.expireAtPhysicalMs <= currentPhysicalMs;
  if (!expired) return prepared;

  prepared = PrepareRollbackLocked(primaryKey, startTs);
  if (prepared.status == TxnStatus::Ok) {
    prepared.commandVersion = PreparedMvccWrite::kCommandVersion;
    prepared.txnRecordStatus =
        TxnRecordStatus{TxnRecordState::RolledBack, 0, std::nullopt};
  }
  return prepared;
}

PreparedMvccWrite MvccStorage::PrepareResolveLock(const std::string& key, uint64_t startTs,
                                                  TxnRecordState decision,
                                                  uint64_t commitTs) {
  if (decision == TxnRecordState::Committed && commitTs != 0) {
    PreparedMvccWrite prepared = PrepareCommit(key, startTs, commitTs);
    prepared.commandVersion = PreparedMvccWrite::kCommandVersion;
    return prepared;
  }
  if (decision == TxnRecordState::RolledBack) {
    PreparedMvccWrite prepared = PrepareRollback(key, startTs);
    prepared.commandVersion = PreparedMvccWrite::kCommandVersion;
    return prepared;
  }
  PreparedMvccWrite prepared;
  prepared.status = TxnStatus::StorageError;
  return prepared;
}

void MvccStorage::ApplyMetadataMutationLocked(const KVBatchOp& op) {
  if (op.key.rfind("lock/", 0) == 0) {
    const std::string key = StripPrefix(op.key, "lock/");
    if (op.type == KVBatchOpType::Delete) {
      locks_.erase(key);
    } else {
      MvccLock lock;
      if (DecodeLockValue(op.value, &lock)) locks_[key] = std::move(lock);
    }
    return;
  }
  if (op.key.rfind("write/", 0) == 0) {
    std::string key;
    uint64_t commitTs = 0;
    if (!SplitKeyTs(StripPrefix(op.key, "write/"), &key, &commitTs)) return;
    if (op.type == KVBatchOpType::Delete) {
      auto writes = writes_.find(key);
      if (writes != writes_.end()) writes->second.erase(commitTs);
    } else {
      MvccWrite write;
      if (DecodeWriteValue(op.value, &write)) {
        write.commitTs = commitTs;
        writes_[key][commitTs] = write;
      }
    }
    return;
  }
  if (op.key.rfind("meta/revision/", 0) == 0 && op.type == KVBatchOpType::Put) {
    revisions_[StripPrefix(op.key, "meta/revision/")] = DecodeTs(op.value);
  }
}

PreparedMvccBatch MvccStorage::PrepareBatchPrewrite(
    const std::vector<MvccMutation>& mutations, const std::string& primaryKey,
    uint64_t startTs, uint64_t ttlMs, uint64_t forUpdateTs) {
  PreparedMvccBatch batch;
  std::vector<MvccMutation> ordered = mutations;
  std::sort(ordered.begin(), ordered.end(), [](const MvccMutation& lhs, const MvccMutation& rhs) {
    return lhs.key < rhs.key;
  });
  if (ordered.empty()) return batch;
  for (size_t index = 0; index < ordered.size(); ++index) {
    if (ordered[index].key.empty() || (index != 0 && ordered[index - 1].key == ordered[index].key)) {
      return batch;
    }
    PreparedMvccWrite prepared = ordered[index].isLockOnly
                                     ? PreparePrewriteLock(ordered[index].key, primaryKey, startTs,
                                                           ttlMs, forUpdateTs)
                                     : PreparePrewrite(ordered[index].key, ordered[index].value,
                                                       primaryKey, startTs, ttlMs,
                                                       ordered[index].isDelete, forUpdateTs);
    if (prepared.status != TxnStatus::Ok) {
      batch.status = prepared.status;
      batch.items.clear();
      return batch;
    }
    batch.items.push_back({ordered[index].key, std::move(prepared)});
  }
  batch.status = TxnStatus::Ok;
  return batch;
}

PreparedMvccBatch MvccStorage::PrepareBatchCommit(const std::vector<std::string>& keys,
                                                   uint64_t startTs, uint64_t commitTs) {
  PreparedMvccBatch batch;
  std::vector<std::string> ordered = keys;
  std::sort(ordered.begin(), ordered.end());
  ordered.erase(std::unique(ordered.begin(), ordered.end()), ordered.end());
  if (ordered.empty() || ordered.front().empty()) return batch;
  for (const auto& key : ordered) {
    PreparedMvccWrite prepared = PrepareCommit(key, startTs, commitTs);
    if (prepared.status == TxnStatus::AlreadyCommitted) prepared.status = TxnStatus::Ok;
    if (prepared.status != TxnStatus::Ok) {
      batch.status = prepared.status;
      batch.items.clear();
      return batch;
    }
    batch.items.push_back({key, std::move(prepared)});
  }
  batch.status = TxnStatus::Ok;
  return batch;
}

PreparedMvccBatch MvccStorage::PrepareBatchRollback(const std::vector<std::string>& keys,
                                                     uint64_t startTs) {
  PreparedMvccBatch batch;
  std::vector<std::string> ordered = keys;
  std::sort(ordered.begin(), ordered.end());
  ordered.erase(std::unique(ordered.begin(), ordered.end()), ordered.end());
  if (ordered.empty() || ordered.front().empty()) return batch;
  for (const auto& key : ordered) {
    PreparedMvccWrite prepared = PrepareRollback(key, startTs);
    if (prepared.status != TxnStatus::Ok) {
      batch.status = prepared.status;
      batch.items.clear();
      return batch;
    }
    batch.items.push_back({key, std::move(prepared)});
  }
  batch.status = TxnStatus::Ok;
  return batch;
}

TxnStatus MvccStorage::ApplyPrepared(const std::string& key, const PreparedMvccWrite& prepared,
                                     uint64_t appliedRaftIndex) {
  if ((prepared.commandVersion != PreparedMvccWrite::kLegacyCommandVersion &&
       prepared.commandVersion != PreparedMvccWrite::kCommandVersion) ||
      prepared.status != TxnStatus::Ok ||
      prepared.modifications.empty() || prepared.newRevision != prepared.expectedRevision + 1) {
    return TxnStatus::StorageError;
  }
  std::unique_lock<std::shared_mutex> lock(mutex_);
  // Replaying an entry already included in the atomic data/progress batch is
  // idempotent. A revision mismatch for a newer entry is not: advancing only
  // the progress marker would permanently skip a committed state transition.
  if (appliedRaftIndex != 0 && appliedRaftIndex <= appliedRaftIndex_) {
    return TxnStatus::Ok;
  }
  if (CurrentRevisionLocked(key) != prepared.expectedRevision) {
    return TxnStatus::StorageError;
  }
  std::vector<KVBatchOp> batch = prepared.modifications;
  if (appliedRaftIndex != 0) {
    batch.push_back({KVBatchOpType::Put, "meta/mvcc_applied_raft_index", std::to_string(appliedRaftIndex)});
  }
  if (!engine_->WriteBatch(batch)) {
    return TxnStatus::StorageError;
  }
  writeBatchCount_.fetch_add(1, std::memory_order_relaxed);
  for (const auto& modification : prepared.modifications) {
    ApplyMetadataMutationLocked(modification);
  }
  if (CurrentRevisionLocked(key) != prepared.newRevision) {
    return TxnStatus::StorageError;
  }
  if (appliedRaftIndex != 0) appliedRaftIndex_ = appliedRaftIndex;
  return TxnStatus::Ok;
}

TxnStatus MvccStorage::ApplyPreparedBatch(const PreparedMvccBatch& prepared,
                                          uint64_t appliedRaftIndex) {
  if (prepared.commandVersion != PreparedMvccBatch::kCommandVersion ||
      prepared.status != TxnStatus::Ok || prepared.items.empty()) {
    return TxnStatus::StorageError;
  }
  std::unique_lock<std::shared_mutex> lock(mutex_);
  if (appliedRaftIndex != 0 && appliedRaftIndex <= appliedRaftIndex_) return TxnStatus::Ok;

  std::vector<KVBatchOp> modifications;
  std::string previousKey;
  for (const auto& item : prepared.items) {
    if (item.key.empty() || (!previousKey.empty() && item.key <= previousKey) ||
        item.prepared.status != TxnStatus::Ok) {
      return TxnStatus::StorageError;
    }
    previousKey = item.key;
    if (item.prepared.modifications.empty()) continue;
    if (item.prepared.newRevision != item.prepared.expectedRevision + 1 ||
        CurrentRevisionLocked(item.key) != item.prepared.expectedRevision) {
      return TxnStatus::StorageError;
    }
    modifications.insert(modifications.end(), item.prepared.modifications.begin(),
                         item.prepared.modifications.end());
  }
  if (appliedRaftIndex != 0) {
    modifications.push_back({KVBatchOpType::Put, "meta/mvcc_applied_raft_index",
                             std::to_string(appliedRaftIndex)});
  }
  if (modifications.empty() || !engine_->WriteBatch(modifications)) return TxnStatus::StorageError;
  writeBatchCount_.fetch_add(1, std::memory_order_relaxed);
  for (const auto& item : prepared.items) {
    for (const auto& modification : item.prepared.modifications) {
      ApplyMetadataMutationLocked(modification);
    }
    if (!item.prepared.modifications.empty() &&
        CurrentRevisionLocked(item.key) != item.prepared.newRevision) {
      return TxnStatus::StorageError;
    }
  }
  if (appliedRaftIndex != 0) appliedRaftIndex_ = appliedRaftIndex;
  return TxnStatus::Ok;
}

TxnStatus MvccStorage::BatchPrewrite(const std::vector<MvccMutation>& mutations,
                                     const std::string& primaryKey, uint64_t startTs,
                                     uint64_t ttlMs, uint64_t forUpdateTs,
                                     uint64_t) {
  PreparedMvccBatch prepared =
      PrepareBatchPrewrite(mutations, primaryKey, startTs, ttlMs, forUpdateTs);
  if (prepared.status != TxnStatus::Ok || !prepared.HasChanges()) return prepared.status;
  return ApplyPreparedBatch(prepared);
}

TxnStatus MvccStorage::BatchCommit(const std::vector<std::string>& keys, uint64_t startTs,
                                   uint64_t commitTs, uint64_t) {
  PreparedMvccBatch prepared = PrepareBatchCommit(keys, startTs, commitTs);
  if (prepared.status != TxnStatus::Ok || !prepared.HasChanges()) return prepared.status;
  return ApplyPreparedBatch(prepared);
}

TxnStatus MvccStorage::BatchRollback(const std::vector<std::string>& keys, uint64_t startTs,
                                     uint64_t) {
  PreparedMvccBatch prepared = PrepareBatchRollback(keys, startTs);
  if (prepared.status != TxnStatus::Ok || !prepared.HasChanges()) return prepared.status;
  return ApplyPreparedBatch(prepared);
}

TxnStatus MvccStorage::AcquirePessimisticLock(const std::string& key, const std::string& primaryKey, uint64_t startTs,
                                              uint64_t ttlMs, uint64_t forUpdateTs,
                                              uint64_t expireAtPhysicalMs) {
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
  mvccLock.forUpdateTs = forUpdateTs == 0 ? startTs : forUpdateTs;
  mvccLock.expireAtPhysicalMs = expireAtPhysicalMs;
  mvccLock.legacyExpiry = expireAtPhysicalMs == 0;
  std::vector<KVBatchOp> ops{{KVBatchOpType::Put, LockKey(key), EncodeLockValue(mvccLock)}};
  uint64_t newRevision = 0;
  AppendRevisionMutationLocked(key, &ops, &newRevision);
  if (!engine_->WriteBatch(ops)) {
    return TxnStatus::StorageError;
  }
  writeBatchCount_.fetch_add(1, std::memory_order_relaxed);
  locks_[key] = mvccLock;
  revisions_[key] = newRevision;
  return TxnStatus::Ok;
}

PessimisticLockResult MvccStorage::AcquirePessimisticLockForUpdate(
    const std::string& key, const std::string& primaryKey, uint64_t startTs,
    uint64_t ttlMs, uint64_t forUpdateTs, uint64_t expireAtPhysicalMs,
    uint64_t remainingBudgetMs) {
  (void)remainingBudgetMs;
  PreparedMvccWrite prepared = PreparePessimisticLock(
      key, primaryKey, startTs, ttlMs, forUpdateTs, expireAtPhysicalMs);
  PessimisticLockResult result;
  result.status = prepared.status;
  result.found = prepared.readStatus == TxnStatus::Ok;
  result.value = prepared.readValue;
  result.valueCommitTs = prepared.readCommitTs;
  if (prepared.status != TxnStatus::Ok) return result;
  if (prepared.HasChanges()) {
    result.status = ApplyPrepared(key, prepared);
    result.applied = result.status == TxnStatus::Ok;
  } else {
    result.applied = true;
  }
  return result;
}

TxnStatus MvccStorage::PrewriteLocked(const std::string& key, const std::string& value, const std::string& primaryKey,
                                      uint64_t startTs, uint64_t ttlMs, bool isDelete,
                                      uint64_t forUpdateTs) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  const auto existingLock = locks_.find(key);
  if (existingLock != locks_.end()) {
    if (existingLock->second.startTs != startTs || !existingLock->second.isPessimistic ||
        existingLock->second.primaryKey != primaryKey ||
        (forUpdateTs != 0 && forUpdateTs < existingLock->second.forUpdateTs)) {
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
  if (existingLock != locks_.end()) {
    mvccLock.forUpdateTs = existingLock->second.forUpdateTs;
    mvccLock.expireAtPhysicalMs = existingLock->second.expireAtPhysicalMs;
    mvccLock.legacyExpiry = existingLock->second.legacyExpiry;
  } else {
    mvccLock.forUpdateTs = forUpdateTs == 0 ? startTs : forUpdateTs;
    const uint64_t baseMs = HlcTimestamp::PhysicalMs(mvccLock.forUpdateTs);
    mvccLock.expireAtPhysicalMs = (baseMs > 0 ? baseMs : NowMs()) + ttlMs;
    mvccLock.legacyExpiry = false;
  }
  std::vector<KVBatchOp> ops;
  if (isDelete) {
    ops.push_back({KVBatchOpType::Delete, DataKey(key, startTs), {}});
  } else {
    ops.push_back({KVBatchOpType::Put, DataKey(key, startTs), value});
  }
  ops.push_back({KVBatchOpType::Put, LockKey(key), EncodeLockValue(mvccLock)});
  uint64_t newRevision = 0;
  AppendRevisionMutationLocked(key, &ops, &newRevision);
  if (!engine_->WriteBatch(ops)) {
    return TxnStatus::StorageError;
  }
  writeBatchCount_.fetch_add(1, std::memory_order_relaxed);
  locks_[key] = mvccLock;
  revisions_[key] = newRevision;
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
  if (commitTs <= std::max(startTs, lockIt->second.forUpdateTs)) {
    return TxnStatus::WriteConflict;
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

  const MvccWriteType type =
      lockIt->second.isLockOnly
          ? MvccWriteType::Lock
          : (lockIt->second.isDelete ? MvccWriteType::Delete : MvccWriteType::Put);
  MvccWrite write{startTs, commitTs, type};
  std::vector<KVBatchOp> ops{{KVBatchOpType::Put, WriteKey(key, commitTs), EncodeWriteValue(write)},
                             {KVBatchOpType::Delete, LockKey(key), {}}};
  uint64_t newRevision = 0;
  AppendRevisionMutationLocked(key, &ops, &newRevision);
  if (!engine_->WriteBatch(ops)) {
    return TxnStatus::StorageError;
  }
  writeBatchCount_.fetch_add(1, std::memory_order_relaxed);
  writes_[key][commitTs] = write;
  locks_.erase(lockIt);
  revisions_[key] = newRevision;
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
  uint64_t newRevision = 0;
  AppendRevisionMutationLocked(key, &ops, &newRevision);
  if (!engine_->WriteBatch(ops)) {
    return TxnStatus::StorageError;
  }
  writeBatchCount_.fetch_add(1, std::memory_order_relaxed);
  if (lockIt != locks_.end() && lockIt->second.startTs == startTs) {
    locks_.erase(lockIt);
  }
  writes_[key][startTs] = rollback;
  revisions_[key] = newRevision;
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
    if (item.second.startTs == startTs && item.second.type != MvccWriteType::Rollback) {
      return item.first;
    }
  }
  return std::nullopt;
}

TxnStatus MvccStorage::CheckTxnStatus(const std::string& primaryKey, uint64_t startTs,
                                      TxnRecordStatus* status) {
  return CheckTxnStatus(primaryKey, startTs, 0, false, 0, status);
}

TxnStatus MvccStorage::CheckTxnStatus(const std::string& primaryKey, uint64_t startTs,
                                      uint64_t currentPhysicalMs, bool rollbackIfExpired,
                                      uint64_t remainingBudgetMs, TxnRecordStatus* status) {
  (void)remainingBudgetMs;
  if (status == nullptr) return TxnStatus::StorageError;
  PreparedMvccWrite prepared = PrepareCheckTxnStatus(primaryKey, startTs, currentPhysicalMs,
                                                      rollbackIfExpired);
  if (prepared.status != TxnStatus::Ok) return prepared.status;
  if (prepared.HasChanges()) {
    const TxnStatus applied = ApplyPrepared(primaryKey, prepared);
    if (applied != TxnStatus::Ok) return applied;
  }
  *status = prepared.txnRecordStatus;
  return TxnStatus::Ok;
}

TxnStatus MvccStorage::ResolveLock(const std::string& key, uint64_t startTs,
                                   TxnRecordState decision, uint64_t commitTs) {
  PreparedMvccWrite prepared = PrepareResolveLock(key, startTs, decision, commitTs);
  if (prepared.status == TxnStatus::AlreadyCommitted) return TxnStatus::Ok;
  if (prepared.status != TxnStatus::Ok) return prepared.status;
  if (!prepared.HasChanges()) return TxnStatus::Ok;
  return ApplyPrepared(key, prepared);
}

std::vector<std::pair<std::string, MvccLock>> MvccStorage::ExpiredLocks(uint64_t nowMs) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  std::vector<std::pair<std::string, MvccLock>> expired;
  for (const auto& item : locks_) {
    const bool isExpired = (!item.second.legacyExpiry && item.second.expireAtPhysicalMs != 0)
                               ? (item.second.expireAtPhysicalMs <= nowMs)
                               : (item.second.createTimeMs != 0 && item.second.ttlMs != 0 &&
                                  item.second.createTimeMs + item.second.ttlMs <= nowMs);
    if (isExpired) {
      expired.push_back(item);
    }
  }
  return expired;
}

size_t MvccStorage::GarbageCollect(uint64_t safePointTs) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  std::vector<std::pair<std::string, uint64_t>> versionsToRemove;
  std::unordered_set<std::string> touchedKeys;
  std::vector<KVBatchOp> ops;

  for (const auto& keyWrites : writes_) {
    const auto& versions = keyWrites.second;
    if (versions.size() <= 1) continue;

    std::optional<uint64_t> keepCommitTs;
    for (auto it = versions.begin(); it != versions.end() && it->first < safePointTs; ++it) {
      if (it->second.type == MvccWriteType::Put || it->second.type == MvccWriteType::Delete) {
        keepCommitTs = it->first;
      }
    }

    for (const auto& version : versions) {
      if (version.first >= safePointTs ||
          (keepCommitTs.has_value() && version.first == keepCommitTs.value())) {
        continue;
      }
      if (version.second.type == MvccWriteType::Put) {
        ops.push_back({KVBatchOpType::Delete, DataKey(keyWrites.first, version.second.startTs), {}});
      }
      ops.push_back({KVBatchOpType::Delete, WriteKey(keyWrites.first, version.first), {}});
      versionsToRemove.emplace_back(keyWrites.first, version.first);
      touchedKeys.insert(keyWrites.first);
    }
  }

  if (versionsToRemove.empty()) return 0;

  std::unordered_map<std::string, uint64_t> newRevisions;
  for (const auto& key : touchedKeys) {
    uint64_t newRevision = 0;
    AppendRevisionMutationLocked(key, &ops, &newRevision);
    newRevisions.emplace(key, newRevision);
  }
  if (!engine_->WriteBatch(ops)) return 0;

  writeBatchCount_.fetch_add(1, std::memory_order_relaxed);
  for (const auto& version : versionsToRemove) {
    auto writes = writes_.find(version.first);
    if (writes != writes_.end()) writes->second.erase(version.second);
  }
  for (const auto& revision : newRevisions) revisions_[revision.first] = revision.second;
  return versionsToRemove.size();
}

MvccStats MvccStorage::Stats() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  MvccStats stats;
  stats.lockCount = locks_.size();
  for (const auto& lock : locks_) {
    if (lock.second.isPessimistic) {
      stats.pessimisticLockCount++;
    }
  }
  for (const auto& keyWrites : writes_) {
    stats.writeCount += keyWrites.second.size();
    stats.dataVersionCount += keyWrites.second.size();
  }
  stats.dataVersionCount += locks_.size();
  stats.writeBatchCount = writeBatchCount_.load(std::memory_order_relaxed);
  stats.appliedRaftIndex = appliedRaftIndex_;
  return stats;
}

ProtocolCapabilities MvccStorage::Capabilities() {
  ProtocolCapabilities caps;
  caps.protocolVersion = kTxnProtocolVersion;
  caps.preparedCommandVersion = 2;
  caps.lockFormatVersion = 1;
  caps.hlcExpiry = true;
  return caps;
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
  revisions_.clear();
  appliedRaftIndex_ = 0;

  std::string appliedIndex;
  if (engine_->Get("meta/mvcc_applied_raft_index", &appliedIndex)) {
    appliedRaftIndex_ = DecodeTs(appliedIndex);
  }

  for (const auto& item : engine_->ScanPrefix("meta/revision/")) {
    revisions_[StripPrefix(item.first, "meta/revision/")] = DecodeTs(item.second);
  }

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

std::string MvccStorage::RevisionKey(const std::string& key) { return "meta/revision/" + key; }
