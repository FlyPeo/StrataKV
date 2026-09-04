/*
 * 测试目标：声明 Direct SDK 与 Gateway 共用的事务 adapter 和 workload runner 边界。
 * 测试策略：每个 pthread worker 独占 adapter，公共 runner 以原子序号分配确定性 operation。
 * 测试规模：支持 smoke 1/8 workers、10,000 operations，也支持 full 的 32 workers。
 * 验证内容：由 workload_runner_check.cpp 验证事务调用顺序、重试计时、错误分类和线程无关序列。
 */
#ifndef STRATAKV_TEST_SUPPORT_WORKLOAD_RUNNER_H
#define STRATAKV_TEST_SUPPORT_WORKLOAD_RUNNER_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "support/performance/performance_support.h"

namespace stratakv::test::performance {

enum class AdapterStatus {
  kOk,
  kNotFound,
  kConflict,
  kTimeout,
  kUnavailable,
  kCleanupPending,
  kResultUnknown,
  kInvalid,
};

struct AdapterResult {
  AdapterStatus status = AdapterStatus::kOk;
  std::string value;
  bool found = false;
  bool retryable = false;
  std::string message;

  bool ok() const { return status == AdapterStatus::kOk; }
};

class TransactionHandle {
 public:
  virtual ~TransactionHandle() = default;
};

class ClientAdapter {
 public:
  virtual ~ClientAdapter() = default;
  virtual std::unique_ptr<TransactionHandle> Begin(uint64_t lockTtlMs) = 0;
  virtual AdapterResult Get(TransactionHandle* transaction, const std::string& key) = 0;
  virtual AdapterResult Put(TransactionHandle* transaction, const std::string& key,
                            const std::string& value) = 0;
  virtual AdapterResult Commit(TransactionHandle* transaction) = 0;
  virtual AdapterResult Rollback(TransactionHandle* transaction) = 0;
};

using AdapterFactory = std::function<std::unique_ptr<ClientAdapter>()>;

AdapterFactory DirectAdapterFactory(const std::string& regionsConfig,
                                    const std::string& tsoEndpoints);
AdapterFactory GatewayAdapterFactory(const std::string& gateway, int timeoutMs);
std::vector<RegionRange> LoadRegionRanges(const std::string& regionsConfig);

RunSummary LoadRecords(const WorkloadSpec& spec, const RegionKeyCodec& keys,
                       const AdapterFactory& factory, const std::string& caseId);
RunSummary RunRecords(const WorkloadSpec& spec, const RegionKeyCodec& keys,
                      const AdapterFactory& factory, const std::string& caseId);
A2Summary RunA2(const A2TransactionGenerator& generator, const AdapterFactory& factory,
                const std::string& caseId, uint64_t transactionCount, int workers = 8,
                int maxAttempts = 1, int retryDelayMs = 20, int timeoutMs = 180000);
A3Summary RunA3(const A3TransactionGenerator& generator, const AdapterFactory& factory,
                const std::string& caseId, uint64_t transactionCount, int workers = 8,
                int maxAttempts = 1, int retryDelayMs = 20, int timeoutMs = 180000);

}  // namespace stratakv::test::performance

#endif
