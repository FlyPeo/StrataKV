#ifndef STRATAKV_SDK_CLIENT_H
#define STRATAKV_SDK_CLIENT_H

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace stratakv {

// Stable SDK status values.  They intentionally hide internal RPC error text.
enum class Status {
  kOk,
  kNotFound,
  kLockConflict,
  kWriteConflict,
  kAlreadyCommitted,
  kTimeout,
  kAbortOnly,
  kCleanupPending,
  kResultUnknown,
  kUnavailable,
  kInvalidTransaction,
};

struct Result {
  Status status = Status::kOk;
  std::string value;
  std::string message;
  bool found = false;
  bool retryable = false;
  uint64_t startTimestamp = 0;
  std::string primaryKey;

  bool ok() const { return status == Status::kOk || status == Status::kAlreadyCommitted; }
};

struct BatchResult {
  Status status = Status::kOk;
  std::vector<std::pair<std::string, Result>> values;
  std::string message;
  bool retryable = false;
};

enum class TransactionRecordState {
  kLocked,
  kCommitted,
  kRolledBack,
  kNotFound,
};

struct TransactionStatusResult {
  Status status = Status::kUnavailable;
  TransactionRecordState state = TransactionRecordState::kNotFound;
  uint64_t commitTimestamp = 0;
  std::string message;
};

struct ClientMetrics {
  uint64_t rollbackRegionCount = 0;
};

const char* StatusName(Status status);

class Transaction {
 public:
  uint64_t StartTimestamp() const;
  bool Finished() const;

 private:
  struct Impl;
  explicit Transaction(std::shared_ptr<Impl> impl);

  std::shared_ptr<Impl> impl_;
  friend class Client;
};

// Public entrypoint for the StrataKV distributed transaction API.
// Region configuration is the same static catalog used by StrataKV nodes.
class Client {
 public:
  static std::shared_ptr<Client> Connect(const std::string& regionConfigPath);
  static std::shared_ptr<Client> Connect(const std::string& regionConfigPath,
                                         const std::string& tsoHost, uint16_t tsoPort);
  // Comma-separated control-plane members, for example
  // "127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302".
  static std::shared_ptr<Client> Connect(const std::string& regionConfigPath,
                                         const std::string& tsoEndpoints);

  std::shared_ptr<Transaction> Begin(uint64_t lockTtlMs);
  Result Get(const std::shared_ptr<Transaction>& transaction, const std::string& key);
  Result GetForUpdate(const std::shared_ptr<Transaction>& transaction, const std::string& key);
  BatchResult BatchGetForUpdate(const std::shared_ptr<Transaction>& transaction,
                                const std::vector<std::string>& keys);
  Result LockKeys(const std::shared_ptr<Transaction>& transaction,
                  const std::vector<std::string>& keys);
  Result Put(const std::shared_ptr<Transaction>& transaction, const std::string& key, const std::string& value);
  Result Delete(const std::shared_ptr<Transaction>& transaction, const std::string& key);
  Result Commit(const std::shared_ptr<Transaction>& transaction);
  Result Rollback(const std::shared_ptr<Transaction>& transaction);
  TransactionStatusResult QueryTransactionStatus(
      const std::shared_ptr<Transaction>& transaction);
  ClientMetrics Metrics() const;

 private:
  struct Impl;
  explicit Client(std::shared_ptr<Impl> impl);

  std::shared_ptr<Impl> impl_;
};

}  // namespace stratakv

#endif  // STRATAKV_SDK_CLIENT_H
