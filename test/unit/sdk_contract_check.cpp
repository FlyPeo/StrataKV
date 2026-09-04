/*
 * 测试目标：锁定 StrataKV 对外 C++ SDK 的关键类型签名和状态表达，防止无意破坏兼容性。
 * 测试策略：用 static_assert 检查悲观事务相关成员函数签名，再构造公开 Result/Status
 *           值执行轻量运行时断言，不连接网络或启动服务。
 * 测试规模：固定检查 4 个公开成员函数签名、3 个状态字符串和 1 个“成功但未找到”
 *           Result 语义，共 8 个契约点。
 * 验证内容：确认 GetForUpdate、BatchGetForUpdate、LockKeys、QueryTransactionStatus 签名
 *           不变，关键状态名称稳定，并能表达“调用成功但键不存在”。
 */
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
