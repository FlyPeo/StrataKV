#include <iostream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "stratakv/client.h"

namespace {
void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}
}  // namespace

int main() {
  try {
    using stratakv::Client;
    using stratakv::Status;
    static_assert(std::is_same_v<decltype(&Client::GetForUpdate),
                                 stratakv::Result (Client::*)(
                                     const std::shared_ptr<stratakv::Transaction>&,
                                     const std::string&)>);
    static_assert(std::is_same_v<decltype(&Client::BatchGetForUpdate),
                                 stratakv::BatchResult (Client::*)(
                                     const std::shared_ptr<stratakv::Transaction>&,
                                     const std::vector<std::string>&)>);
    static_assert(std::is_same_v<decltype(&Client::LockKeys),
                                 stratakv::Result (Client::*)(
                                     const std::shared_ptr<stratakv::Transaction>&,
                                     const std::vector<std::string>&)>);
    static_assert(std::is_same_v<decltype(&Client::QueryTransactionStatus),
                                 stratakv::TransactionStatusResult (Client::*)(
                                     const std::shared_ptr<stratakv::Transaction>&)>);

    Require(std::string(stratakv::StatusName(Status::kAbortOnly)) == "ABORT_ONLY",
            "abort-only status must remain public");
    Require(std::string(stratakv::StatusName(Status::kCleanupPending)) == "CLEANUP_PENDING",
            "cleanup-pending status must remain public");
    Require(std::string(stratakv::StatusName(Status::kResultUnknown)) == "RESULT_UNKNOWN",
            "result-unknown status must remain public");
    stratakv::Result absent;
    absent.status = Status::kOk;
    absent.found = false;
    Require(absent.ok() && !absent.found, "successful absent locking read must be representable");
    std::cout << "SDK contract checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "SDK contract checks failed: " << error.what() << std::endl;
    return 1;
  }
}
