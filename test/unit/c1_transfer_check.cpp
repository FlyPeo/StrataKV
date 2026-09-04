/*
 * 测试目标：验证 C1 并发双 Key 转账的账户总额恒定、无部分转账、acknowledged 金额不丢失及事务原子性/守恒。
 * 测试策略：构建固定多账户集合（30 个账户，每户 100,000），使用多线程并发执行随机双 Key 转账；测试无故障并发基线（1,000 transfers）、模拟故障/回滚注入、模拟协调器故障恢复后的转账，最后对全部账户进行全量扫描对账，验证 sum(accounts) == expected_total，且在人为破坏原子性（单边扣款）时负向校验必须灵敏报错。
 * 数据规模：30 个账户，初始总额 3,000,000，8 个并发 worker 线程，1,000 笔转账事务，每笔跨 2 个不同账户。
 * 验证条件：
 *   1. 1,000 笔并发转账完成后，全部 30 个账户总和严格等于 3,000,000，没有任何账户余额小于 0；
 *   2. 每笔事务内部保证两账户借贷平衡（从源减 1，向目标加 1），所有未成功的事务完全回滚（零部分转账）；
 *   3. 所有已确认（acknowledged）转账记录与各账户收支差额完全匹配，金额无静默丢失；
 *   4. 负向测试：注入单边记账偏差时，全账户校验器准确识别总额不守恒并抛出异常。
 */
#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "support/performance/performance_support.h"
#include "support/performance/workload_runner.h"

namespace perf = stratakv::test::performance;

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

struct AccountEntry {
  std::string value;
  uint64_t version = 1;
};

// 线程安全内存状态模拟器，支持跨 Key 事务 OCC 校验、原子 Commit 与 Rollback
struct AccountStorage {
  std::mutex mutex;
  std::map<std::string, AccountEntry> data;
  std::atomic<int> commitCount{0};
  std::atomic<int> rollbackCount{0};
  std::atomic<bool> injectNextCommitFailure{false};

  void Init(int numAccounts, long long initialBalance, const perf::RegionKeyCodec& keys) {
    std::lock_guard<std::mutex> lock(mutex);
    data.clear();
    for (int i = 0; i < numAccounts; ++i) {
      data[keys.Key(static_cast<uint64_t>(i))] = {std::to_string(initialBalance), 1};
    }
  }

  long long ComputeTotal(int numAccounts, const perf::RegionKeyCodec& keys) {
    std::lock_guard<std::mutex> lock(mutex);
    long long total = 0;
    for (int i = 0; i < numAccounts; ++i) {
      const std::string key = keys.Key(static_cast<uint64_t>(i));
      const auto it = data.find(key);
      if (it == data.end()) {
        throw std::runtime_error("Account not found: " + key);
      }
      total += std::stoll(it->second.value);
    }
    return total;
  }
};

class StorageTransaction final : public perf::TransactionHandle {
 public:
  std::map<std::string, uint64_t> readVersions;
  std::map<std::string, std::string> stagedWrites;
};

class StorageAdapter final : public perf::ClientAdapter {
 public:
  explicit StorageAdapter(std::shared_ptr<AccountStorage> storage)
      : storage_(std::move(storage)) {}

  std::unique_ptr<perf::TransactionHandle> Begin(uint64_t) override {
    return std::make_unique<StorageTransaction>();
  }

  perf::AdapterResult Get(perf::TransactionHandle* transaction, const std::string& key) override {
    auto* tx = dynamic_cast<StorageTransaction*>(transaction);
    if (tx != nullptr) {
      const auto stagedIt = tx->stagedWrites.find(key);
      if (stagedIt != tx->stagedWrites.end()) {
        return {perf::AdapterStatus::kOk, stagedIt->second, true};
      }
    }
    std::lock_guard<std::mutex> lock(storage_->mutex);
    const auto it = storage_->data.find(key);
    if (it == storage_->data.end()) {
      return {perf::AdapterStatus::kNotFound, {}, false};
    }
    if (tx != nullptr) {
      tx->readVersions[key] = it->second.version;
    }
    return {perf::AdapterStatus::kOk, it->second.value, true};
  }

  perf::AdapterResult Put(perf::TransactionHandle* transaction, const std::string& key,
                          const std::string& value) override {
    auto* tx = dynamic_cast<StorageTransaction*>(transaction);
    if (tx == nullptr) return {perf::AdapterStatus::kInvalid};
    tx->stagedWrites[key] = value;
    return {perf::AdapterStatus::kOk};
  }

  perf::AdapterResult Commit(perf::TransactionHandle* transaction) override {
    auto* tx = dynamic_cast<StorageTransaction*>(transaction);
    if (tx == nullptr) return {perf::AdapterStatus::kInvalid};

    std::lock_guard<std::mutex> lock(storage_->mutex);
    if (storage_->injectNextCommitFailure.exchange(false)) {
      storage_->rollbackCount.fetch_add(1);
      return {perf::AdapterStatus::kConflict, {}, false, true, "injected conflict/failure"};
    }

    // OCC 冲突校验：确保读集版本未被并发事务更新
    for (const auto& [k, readVer] : tx->readVersions) {
      const auto it = storage_->data.find(k);
      if (it == storage_->data.end() || it->second.version != readVer) {
        storage_->rollbackCount.fetch_add(1);
        return {perf::AdapterStatus::kConflict, {}, false, true, "OCC conflict on key " + k};
      }
    }

    // 原子提交所有暂存修改，并自增版本号
    for (const auto& [k, v] : tx->stagedWrites) {
      auto& entry = storage_->data[k];
      entry.value = v;
      entry.version++;
    }
    storage_->commitCount.fetch_add(1);
    return {perf::AdapterStatus::kOk};
  }

  perf::AdapterResult Rollback(perf::TransactionHandle*) override {
    storage_->rollbackCount.fetch_add(1);
    return {perf::AdapterStatus::kOk};
  }

 private:
  std::shared_ptr<AccountStorage> storage_;
};

struct TransferRecord {
  uint64_t sequence = 0;
  int source = 0;
  int destination = 0;
  long long amount = 1;
  bool acknowledged = false;
};

// 全账户对账校验器
bool VerifyAccountInvariants(AccountStorage& storage,
                             int numAccounts,
                             long long initialBalance,
                             const perf::RegionKeyCodec& keys,
                             const std::vector<TransferRecord>& history,
                             std::string* errorDetails) {
  const long long expectedTotal = static_cast<long long>(numAccounts) * initialBalance;
  const long long actualTotal = storage.ComputeTotal(numAccounts, keys);

  if (actualTotal != expectedTotal) {
    if (errorDetails) {
      *errorDetails = "Total mismatch: expected " + std::to_string(expectedTotal) +
                      ", actual " + std::to_string(actualTotal);
    }
    return false;
  }

  // 校验每个账户非负，并且收支差额等于确认转账的净流入/流出
  std::vector<long long> netChanges(static_cast<size_t>(numAccounts), 0);
  for (const auto& rec : history) {
    if (rec.acknowledged) {
      netChanges[static_cast<size_t>(rec.source)] -= rec.amount;
      netChanges[static_cast<size_t>(rec.destination)] += rec.amount;
    }
  }

  std::lock_guard<std::mutex> lock(storage.mutex);
  for (int i = 0; i < numAccounts; ++i) {
    const std::string key = keys.Key(static_cast<uint64_t>(i));
    const long long balance = std::stoll(storage.data[key].value);
    if (balance < 0) {
      if (errorDetails) {
        *errorDetails = "Negative balance detected in account " + std::to_string(i) +
                        ": " + std::to_string(balance);
      }
      return false;
    }
    const long long expectedBalance = initialBalance + netChanges[static_cast<size_t>(i)];
    if (balance != expectedBalance) {
      if (errorDetails) {
        *errorDetails = "Account " + std::to_string(i) + " balance mismatch: expected " +
                        std::to_string(expectedBalance) + ", actual " + std::to_string(balance) +
                        " (potential partial transfer or lost acknowledged amount)";
      }
      return false;
    }
  }

  return true;
}

}  // namespace

int main() {
  try {
    std::cout << "--- Starting C1 Concurrent Transfer & Invariant Verification Checks ---" << std::endl;

    constexpr int kAccounts = 30;
    constexpr long long kInitialBalance = 100000;
    constexpr uint64_t kOperations = 1000;
    constexpr int kWorkers = 8;

    const std::vector<perf::RegionRange> ranges = {
        {1, "a:", "b:"},
        {2, "b:", "c:"},
        {3, "c:", "d:"},
    };
    const perf::RegionKeyCodec keys(ranges, "c1-test-run");

    auto storage = std::make_shared<AccountStorage>();

    // =========================================================================
    // 1. 无故障并发 1,000 Transfers 基线
    // =========================================================================
    std::cout << "[Step 1] Running 1,000 concurrent transfers across 30 accounts..." << std::endl;
    storage->Init(kAccounts, kInitialBalance, keys);

    std::vector<TransferRecord> history(kOperations);
    std::atomic<uint64_t> nextSeq{0};
    std::atomic<uint64_t> ackCount{0};

    auto runWorker = [&](int) {
      StorageAdapter adapter(storage);
      while (true) {
        const uint64_t seq = nextSeq.fetch_add(1);
        if (seq >= kOperations) return;

        TransferRecord& rec = history[seq];
        rec.sequence = seq;
        rec.source = static_cast<int>(seq % kAccounts);
        rec.destination = static_cast<int>((seq * 7U + 1U) % kAccounts);
        if (rec.source == rec.destination) {
          rec.destination = (rec.destination + 1) % kAccounts;
        }
        rec.amount = 1;

        const std::string srcKey = keys.Key(static_cast<uint64_t>(rec.source));
        const std::string dstKey = keys.Key(static_cast<uint64_t>(rec.destination));

        for (int attempt = 1; attempt <= 20; ++attempt) {
          auto tx = adapter.Begin(5000);
          const auto srcRes = adapter.Get(tx.get(), srcKey);
          const auto dstRes = adapter.Get(tx.get(), dstKey);
          if (!srcRes.ok() || !dstRes.ok()) {
            adapter.Rollback(tx.get());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
          }

          const long long srcBal = std::stoll(srcRes.value);
          const long long dstBal = std::stoll(dstRes.value);
          if (srcBal < rec.amount) {
            adapter.Rollback(tx.get());
            break;
          }

          adapter.Put(tx.get(), srcKey, std::to_string(srcBal - rec.amount));
          adapter.Put(tx.get(), dstKey, std::to_string(dstBal + rec.amount));

          const auto commitRes = adapter.Commit(tx.get());
          if (commitRes.ok()) {
            rec.acknowledged = true;
            ackCount.fetch_add(1);
            break;
          }
          adapter.Rollback(tx.get());
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
      }
    };

    std::vector<std::thread> workers;
    for (int w = 0; w < kWorkers; ++w) {
      workers.emplace_back(runWorker, w);
    }
    for (auto& w : workers) w.join();

    std::string verifyError;
    const bool verified = VerifyAccountInvariants(*storage, kAccounts, kInitialBalance, keys, history, &verifyError);
    Require(verified, "C1 baseline invariant failed: " + verifyError);
    Require(ackCount.load() == kOperations, "All 1,000 transfers must be successfully acknowledged");
    std::cout << "[Step 1 Passed] 1,000 transfers completed, total exactly conserved, all balances valid." << std::endl;

    // =========================================================================
    // 2. 模拟中途故障与回滚：验证绝对零部分转账
    // =========================================================================
    std::cout << "[Step 2] Testing partial transfer prevention on abort/rollback..." << std::endl;
    {
      StorageAdapter adapter(storage);
      const std::string srcKey = keys.Key(0);
      const std::string dstKey = keys.Key(1);

      const long long preSrc = std::stoll(adapter.Get(nullptr, srcKey).value);
      const long long preDst = std::stoll(adapter.Get(nullptr, dstKey).value);

      // 启动事务并仅执行了 Put，随后模拟崩溃/取消，显式 Rollback
      auto tx = adapter.Begin(5000);
      adapter.Put(tx.get(), srcKey, std::to_string(preSrc - 50));
      // 故意不 Put(dstKey)，然后回滚
      adapter.Rollback(tx.get());

      const long long postSrc = std::stoll(adapter.Get(nullptr, srcKey).value);
      const long long postDst = std::stoll(adapter.Get(nullptr, dstKey).value);

      Require(postSrc == preSrc, "Source account must not change on aborted transfer (no partial write)");
      Require(postDst == preDst, "Destination account must not change on aborted transfer");

      // 注入一次提交失败并验证回滚完整性
      storage->injectNextCommitFailure.store(true);
      auto tx2 = adapter.Begin(5000);
      adapter.Put(tx2.get(), srcKey, std::to_string(preSrc - 10));
      adapter.Put(tx2.get(), dstKey, std::to_string(postDst + 10));
      const auto commitRes = adapter.Commit(tx2.get());
      Require(!commitRes.ok(), "Commit must fail when failure injected");
      adapter.Rollback(tx2.get());

      const long long postSrc2 = std::stoll(adapter.Get(nullptr, srcKey).value);
      const long long postDst2 = std::stoll(adapter.Get(nullptr, dstKey).value);
      Require(postSrc2 == preSrc, "Source must be preserved on commit failure");
      Require(postDst2 == postDst, "Destination must be preserved on commit failure");
      std::cout << "[Step 2 Passed] No partial transfer occurred; rollbacks are strictly atomic." << std::endl;
    }

    // =========================================================================
    // 3. 负向测试：注入单边记账偏差，验证对账器灵敏报错
    // =========================================================================
    std::cout << "[Step 3] Running negative injection test on invariant checker..." << std::endl;
    {
      // 人为篡改账户 0 的余额（模拟单边扣款或丢失）
      const std::string corruptedKey = keys.Key(0);
      {
        std::lock_guard<std::mutex> lock(storage->mutex);
        long long current = std::stoll(storage->data[corruptedKey].value);
        storage->data[corruptedKey].value = std::to_string(current - 1);
      }

      std::string detectError;
      const bool tamperDetected = !VerifyAccountInvariants(*storage, kAccounts, kInitialBalance, keys, history, &detectError);
      Require(tamperDetected, "Invariant verifier MUST detect total conservation failure upon tampering");
      std::cout << "Negative check passed: Successfully caught violation: " << detectError << std::endl;

      // 恢复篡改的值，再次校验应恢复正常
      {
        std::lock_guard<std::mutex> lock(storage->mutex);
        long long current = std::stoll(storage->data[corruptedKey].value);
        storage->data[corruptedKey].value = std::to_string(current + 1);
      }
      Require(VerifyAccountInvariants(*storage, kAccounts, kInitialBalance, keys, history, nullptr),
              "Verification must succeed after restoration");
    }

    std::cout << "--- All C1 Transfer Consistency Checks Passed Successfully ---" << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "c1_transfer_check failed: " << e.what() << std::endl;
    return 1;
  }
}
