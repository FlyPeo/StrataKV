#ifndef STRATAKV_SDK_CLIENT_H
#define STRATAKV_SDK_CLIENT_H

#include <cstdint>
#include <memory>
#include <string>

namespace stratakv {

// Stable SDK status values.  They intentionally hide internal RPC error text.
enum class Status {
  kOk,
  kNotFound,
  kLockConflict,
  kWriteConflict,
  kAlreadyCommitted,
  kUnavailable,
  kInvalidTransaction,
};

struct Result {
  Status status = Status::kOk;
  std::string value;
  std::string message;

  bool ok() const { return status == Status::kOk || status == Status::kAlreadyCommitted; }
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

  std::shared_ptr<Transaction> Begin();
  Result Get(const std::shared_ptr<Transaction>& transaction, const std::string& key);
  Result Put(const std::shared_ptr<Transaction>& transaction, const std::string& key, const std::string& value);
  Result Delete(const std::shared_ptr<Transaction>& transaction, const std::string& key);
  Result Commit(const std::shared_ptr<Transaction>& transaction);
  Result Rollback(const std::shared_ptr<Transaction>& transaction);

 private:
  struct Impl;
  explicit Client(std::shared_ptr<Impl> impl);

  std::shared_ptr<Impl> impl_;
};

}  // namespace stratakv

#endif  // STRATAKV_SDK_CLIENT_H
