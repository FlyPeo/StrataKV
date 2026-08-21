#include "stratakv/client.h"

#include <stdexcept>
#include <utility>
#include <vector>

#include "distributed_transaction_coordinator.h"
#include "raft_mvcc_storage.h"
#include "remote_timestamp_oracle.h"
#include "region_metadata.h"
#include "shard_router.h"

namespace stratakv {
namespace {

Status ToPublicStatus(TxnStatus status) {
  switch (status) {
    case TxnStatus::Ok: return Status::kOk;
    case TxnStatus::NotFound: return Status::kNotFound;
    case TxnStatus::LockConflict: return Status::kLockConflict;
    case TxnStatus::WriteConflict: return Status::kWriteConflict;
    case TxnStatus::AlreadyCommitted: return Status::kAlreadyCommitted;
    case TxnStatus::Timeout: return Status::kTimeout;
    case TxnStatus::AbortOnly: return Status::kAbortOnly;
    case TxnStatus::CleanupPending: return Status::kCleanupPending;
    case TxnStatus::ResultUnknown: return Status::kResultUnknown;
    case TxnStatus::StorageError: return Status::kUnavailable;
  }
  return Status::kUnavailable;
}

Result FromTxnStatus(TxnStatus status, std::string value = {}) {
  Result result{ToPublicStatus(status), std::move(value), StatusName(ToPublicStatus(status))};
  result.retryable = status == TxnStatus::LockConflict || status == TxnStatus::WriteConflict ||
                     status == TxnStatus::Timeout;
  return result;
}

TransactionRecordState ToPublicRecordState(TxnRecordState state) {
  switch (state) {
    case TxnRecordState::Locked: return TransactionRecordState::kLocked;
    case TxnRecordState::Committed: return TransactionRecordState::kCommitted;
    case TxnRecordState::RolledBack: return TransactionRecordState::kRolledBack;
    case TxnRecordState::NotFound: return TransactionRecordState::kNotFound;
  }
  return TransactionRecordState::kNotFound;
}

}  // namespace

struct Client::Impl {
  std::shared_ptr<DistributedTransactionCoordinator> coordinator;
};

struct Transaction::Impl {
  Impl(::Transaction transaction, TxnOptions options) : transaction(std::move(transaction)), options(options) {}

  ::Transaction transaction;
  TxnOptions options;
  bool finished = false;
};

const char* StatusName(Status status) {
  switch (status) {
    case Status::kOk: return "OK";
    case Status::kNotFound: return "NOT_FOUND";
    case Status::kLockConflict: return "LOCK_CONFLICT";
    case Status::kWriteConflict: return "WRITE_CONFLICT";
    case Status::kAlreadyCommitted: return "ALREADY_COMMITTED";
    case Status::kTimeout: return "TIMEOUT";
    case Status::kAbortOnly: return "ABORT_ONLY";
    case Status::kCleanupPending: return "CLEANUP_PENDING";
    case Status::kResultUnknown: return "RESULT_UNKNOWN";
    case Status::kUnavailable: return "UNAVAILABLE";
    case Status::kInvalidTransaction: return "INVALID_TRANSACTION";
  }
  return "UNAVAILABLE";
}

Transaction::Transaction(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}

uint64_t Transaction::StartTimestamp() const { return impl_ == nullptr ? 0 : impl_->transaction.StartTs(); }

bool Transaction::Finished() const { return impl_ == nullptr || impl_->finished; }

Client::Client(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}

std::shared_ptr<Client> Client::Connect(const std::string& regionConfigPath) {
  return Connect(regionConfigPath,
                 std::string("127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302"));
}

std::shared_ptr<Client> Client::Connect(const std::string& regionConfigPath,
                                        const std::string& tsoHost, uint16_t tsoPort) {
  return Client::Connect(regionConfigPath, std::string(tsoHost + ":" + std::to_string(tsoPort)));
}

std::shared_ptr<Client> Client::Connect(const std::string& regionConfigPath,
                                        const std::string& tsoEndpoints) {
  const RegionCatalog catalog = RegionCatalog::LoadFromConfig(regionConfigPath);
  std::vector<ShardRouter::RegionRoute> routes;
  routes.reserve(catalog.Regions().size());
  for (const auto& region : catalog.Regions()) {
    std::vector<std::pair<std::string, short>> endpoints;
    endpoints.reserve(region.peers.size());
    for (const auto& peer : region.peers) endpoints.emplace_back(peer.host, peer.port);
    routes.push_back({region, std::make_shared<RaftMvccStorage>(region.regionId, endpoints)});
  }

  auto impl = std::make_shared<Impl>();
  impl->coordinator = std::make_shared<DistributedTransactionCoordinator>(
      std::make_shared<ShardRouter>(std::move(routes)),
      std::make_shared<RemoteTimestampOracle>(tsoEndpoints));
  return std::shared_ptr<Client>(new Client(std::move(impl)));
}

std::shared_ptr<Transaction> Client::Begin(uint64_t lockTtlMs) {
  TxnOptions options;
  options.lockTtlMs = lockTtlMs;
  return std::shared_ptr<Transaction>(new Transaction(std::make_shared<Transaction::Impl>(impl_->coordinator->Begin(), options)));
}

Result Client::Get(const std::shared_ptr<Transaction>& transaction, const std::string& key) {
  if (transaction == nullptr || transaction->impl_ == nullptr || transaction->impl_->finished) {
    return {Status::kInvalidTransaction, {}, "transaction is not active"};
  }
  // Read-your-writes is part of the public transaction contract. The remote
  // MVCC snapshot only knows committed data, so consult local mutations first.
  const auto pending = transaction->impl_->transaction.Mutations().find(key);
  if (pending != transaction->impl_->transaction.Mutations().end()) {
    if (pending->second.isDelete) return {Status::kNotFound, {}, "NOT_FOUND"};
    Result result{Status::kOk, pending->second.value, "OK"};
    result.found = true;
    result.startTimestamp = transaction->impl_->transaction.StartTs();
    result.primaryKey = transaction->impl_->transaction.PrimaryKey();
    return result;
  }
  std::string value;
  const TxnStatus status = impl_->coordinator->Get(&transaction->impl_->transaction, key, &value, transaction->impl_->options);
  Result result = FromTxnStatus(status);
  result.value = std::move(value);
  result.found = status == TxnStatus::Ok;
  result.startTimestamp = transaction->impl_->transaction.StartTs();
  result.primaryKey = transaction->impl_->transaction.PrimaryKey();
  return result;
}

Result Client::GetForUpdate(const std::shared_ptr<Transaction>& transaction,
                            const std::string& key) {
  if (transaction == nullptr || transaction->impl_ == nullptr || transaction->impl_->finished ||
      key.empty()) {
    return {Status::kInvalidTransaction, {}, "transaction is not active or key is empty"};
  }
  const PessimisticLockResult locked =
      impl_->coordinator->GetForUpdate(&transaction->impl_->transaction, key, transaction->impl_->options);
  Result result = FromTxnStatus(locked.status, locked.value);
  result.found = locked.found;
  result.startTimestamp = transaction->impl_->transaction.StartTs();
  result.primaryKey = transaction->impl_->transaction.PrimaryKey();
  return result;
}

BatchResult Client::BatchGetForUpdate(const std::shared_ptr<Transaction>& transaction,
                                      const std::vector<std::string>& keys) {
  BatchResult result;
  if (transaction == nullptr || transaction->impl_ == nullptr || transaction->impl_->finished) {
    result.status = Status::kInvalidTransaction;
    result.message = "transaction is not active";
    return result;
  }
  BatchLockingReadResult locked =
      impl_->coordinator->BatchGetForUpdate(&transaction->impl_->transaction, keys, transaction->impl_->options);
  result.status = ToPublicStatus(locked.status);
  result.message = StatusName(result.status);
  result.retryable = locked.status == TxnStatus::LockConflict ||
                     locked.status == TxnStatus::WriteConflict || locked.status == TxnStatus::Timeout;
  if (locked.status != TxnStatus::Ok) return result;
  result.values.reserve(locked.values.size());
  for (auto& item : locked.values) {
    Result value = FromTxnStatus(item.second.status, std::move(item.second.value));
    value.found = item.second.found;
    value.startTimestamp = transaction->impl_->transaction.StartTs();
    value.primaryKey = transaction->impl_->transaction.PrimaryKey();
    result.values.emplace_back(std::move(item.first), std::move(value));
  }
  return result;
}

Result Client::LockKeys(const std::shared_ptr<Transaction>& transaction,
                        const std::vector<std::string>& keys) {
  if (transaction == nullptr || transaction->impl_ == nullptr || transaction->impl_->finished) {
    return {Status::kInvalidTransaction, {}, "transaction is not active"};
  }
  Result result =
      FromTxnStatus(impl_->coordinator->LockKeys(&transaction->impl_->transaction, keys, transaction->impl_->options));
  result.startTimestamp = transaction->impl_->transaction.StartTs();
  result.primaryKey = transaction->impl_->transaction.PrimaryKey();
  return result;
}

Result Client::Put(const std::shared_ptr<Transaction>& transaction, const std::string& key, const std::string& value) {
  if (transaction == nullptr || transaction->impl_ == nullptr || transaction->impl_->finished || key.empty()) {
    return {Status::kInvalidTransaction, {}, "transaction is not active or key is empty"};
  }
  const TxnStatus active = impl_->coordinator->Validate(&transaction->impl_->transaction, transaction->impl_->options);
  if (active != TxnStatus::Ok) return FromTxnStatus(active);
  transaction->impl_->transaction.Put(key, value);
  return {Status::kOk, {}, "OK"};
}

Result Client::Delete(const std::shared_ptr<Transaction>& transaction, const std::string& key) {
  if (transaction == nullptr || transaction->impl_ == nullptr || transaction->impl_->finished || key.empty()) {
    return {Status::kInvalidTransaction, {}, "transaction is not active or key is empty"};
  }
  const TxnStatus active = impl_->coordinator->Validate(&transaction->impl_->transaction, transaction->impl_->options);
  if (active != TxnStatus::Ok) return FromTxnStatus(active);
  transaction->impl_->transaction.Delete(key);
  return {Status::kOk, {}, "OK"};
}

Result Client::Commit(const std::shared_ptr<Transaction>& transaction) {
  if (transaction == nullptr || transaction->impl_ == nullptr || transaction->impl_->finished) {
    return {Status::kInvalidTransaction, {}, "transaction is not active"};
  }
  const TxnStatus status = impl_->coordinator->Commit(&transaction->impl_->transaction, transaction->impl_->options);
  transaction->impl_->finished =
      transaction->impl_->transaction.State() == TransactionState::Finished;
  Result result = FromTxnStatus(status);
  result.startTimestamp = transaction->impl_->transaction.StartTs();
  result.primaryKey = transaction->impl_->transaction.PrimaryKey();
  return result;
}

Result Client::Rollback(const std::shared_ptr<Transaction>& transaction) {
  if (transaction == nullptr || transaction->impl_ == nullptr || transaction->impl_->finished) {
    return {Status::kInvalidTransaction, {}, "transaction is not active"};
  }
  const TxnStatus status = impl_->coordinator->Rollback(&transaction->impl_->transaction, transaction->impl_->options);
  transaction->impl_->finished =
      transaction->impl_->transaction.State() == TransactionState::Finished;
  Result result = FromTxnStatus(status);
  result.startTimestamp = transaction->impl_->transaction.StartTs();
  result.primaryKey = transaction->impl_->transaction.PrimaryKey();
  return result;
}

TransactionStatusResult Client::QueryTransactionStatus(
    const std::shared_ptr<Transaction>& transaction) {
  TransactionStatusResult result;
  if (transaction == nullptr || transaction->impl_ == nullptr) {
    result.status = Status::kInvalidTransaction;
    result.message = "transaction is invalid";
    return result;
  }
  TxnRecordStatus record;
  const TxnStatus status = impl_->coordinator->QueryStatus(transaction->impl_->transaction, &record, transaction->impl_->options);
  result.status = ToPublicStatus(status);
  result.message = StatusName(result.status);
  if (status == TxnStatus::Ok) {
    result.state = ToPublicRecordState(record.state);
    result.commitTimestamp = record.commitTs;
  }
  return result;
}

ClientMetrics Client::Metrics() const {
  const DistributedTxnMetrics metrics = impl_->coordinator->Metrics();
  return {metrics.rollbackRegionCount};
}

}  // namespace stratakv
