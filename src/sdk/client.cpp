#include "stratakv/client.h"

#include <stdexcept>
#include <utility>
#include <vector>

#include "distributed_transaction_coordinator.h"
#include "raft_mvcc_storage.h"
#include "region_metadata.h"
#include "shard_router.h"
#include "timestamp_oracle.h"

namespace stratakv {
namespace {

Status ToPublicStatus(TxnStatus status) {
  switch (status) {
    case TxnStatus::Ok: return Status::kOk;
    case TxnStatus::NotFound: return Status::kNotFound;
    case TxnStatus::LockConflict: return Status::kLockConflict;
    case TxnStatus::WriteConflict: return Status::kWriteConflict;
    case TxnStatus::AlreadyCommitted: return Status::kAlreadyCommitted;
    case TxnStatus::StorageError: return Status::kUnavailable;
  }
  return Status::kUnavailable;
}

Result FromTxnStatus(TxnStatus status, std::string value = {}) {
  return {ToPublicStatus(status), std::move(value), StatusName(ToPublicStatus(status))};
}

}  // namespace

struct Client::Impl {
  std::shared_ptr<DistributedTransactionCoordinator> coordinator;
};

struct Transaction::Impl {
  explicit Impl(::Transaction transaction) : transaction(std::move(transaction)) {}

  ::Transaction transaction;
  bool finished = false;
};

const char* StatusName(Status status) {
  switch (status) {
    case Status::kOk: return "OK";
    case Status::kNotFound: return "NOT_FOUND";
    case Status::kLockConflict: return "LOCK_CONFLICT";
    case Status::kWriteConflict: return "WRITE_CONFLICT";
    case Status::kAlreadyCommitted: return "ALREADY_COMMITTED";
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
      std::make_shared<ShardRouter>(std::move(routes)), std::make_shared<TimestampOracle>());
  return std::shared_ptr<Client>(new Client(std::move(impl)));
}

std::shared_ptr<Transaction> Client::Begin() {
  return std::shared_ptr<Transaction>(new Transaction(std::make_shared<Transaction::Impl>(impl_->coordinator->Begin())));
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
    return {Status::kOk, pending->second.value, "OK"};
  }
  std::string value;
  const TxnStatus status = impl_->coordinator->Get(transaction->impl_->transaction, key, &value);
  Result result = FromTxnStatus(status);
  result.value = std::move(value);
  return result;
}

Result Client::Put(const std::shared_ptr<Transaction>& transaction, const std::string& key, const std::string& value) {
  if (transaction == nullptr || transaction->impl_ == nullptr || transaction->impl_->finished || key.empty()) {
    return {Status::kInvalidTransaction, {}, "transaction is not active or key is empty"};
  }
  transaction->impl_->transaction.Put(key, value);
  return {Status::kOk, {}, "OK"};
}

Result Client::Delete(const std::shared_ptr<Transaction>& transaction, const std::string& key) {
  if (transaction == nullptr || transaction->impl_ == nullptr || transaction->impl_->finished || key.empty()) {
    return {Status::kInvalidTransaction, {}, "transaction is not active or key is empty"};
  }
  transaction->impl_->transaction.Delete(key);
  return {Status::kOk, {}, "OK"};
}

Result Client::Commit(const std::shared_ptr<Transaction>& transaction) {
  if (transaction == nullptr || transaction->impl_ == nullptr || transaction->impl_->finished) {
    return {Status::kInvalidTransaction, {}, "transaction is not active"};
  }
  const TxnStatus status = impl_->coordinator->Commit(transaction->impl_->transaction);
  transaction->impl_->finished = true;
  return FromTxnStatus(status);
}

Result Client::Rollback(const std::shared_ptr<Transaction>& transaction) {
  if (transaction == nullptr || transaction->impl_ == nullptr || transaction->impl_->finished) {
    return {Status::kInvalidTransaction, {}, "transaction is not active"};
  }
  impl_->coordinator->Rollback(transaction->impl_->transaction);
  transaction->impl_->finished = true;
  return {Status::kOk, {}, "OK"};
}

ClientMetrics Client::Metrics() const {
  const DistributedTxnMetrics metrics = impl_->coordinator->Metrics();
  return {metrics.rollbackRegionCount};
}

}  // namespace stratakv
