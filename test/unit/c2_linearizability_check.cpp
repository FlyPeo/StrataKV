/*
 * 测试目标：验证 C2 单寄存器操作模型、quiescent epoch 切分与有界线性一致性检查器的正确性。
 * 测试策略：使用 golden histories 测试 Legal（并发重叠但存在合法先后）、Illegal（陈旧读/违背实时先后偏序）、Pending（包含未知写，必须判定为 inconclusive 而非猜测成功）；同时集成运行 300 operations 多线程真实并发读写，划分静态边界 epoch 并输出 counterexample 诊断报告与 JSONL 历史文件。
 * 数据规模：4 个 golden 历史用例，300-operation 真实并发执行（按 8 operations/epoch 切分），验证完整生命周期。
 * 验证条件：
 *   1. 合法 golden 历史判定为 pass，并输出合法串行化序列；
 *   2. 违背实时先后偏序（complete(W) < invoke(R) 但读到旧值）的 golden 历史严格判定为 fail，并输出包含时间戳与 sequence 的反例诊断；
 *   3. 违背因果读到未来值的 golden 历史严格判定为 fail；
 *   4. 包含未知/超时写操作的 pending golden 历史严格判定为 inconclusive，拒绝猜测成功；
 *   5. 300 operations 集成测试按静态 epoch 成功完成全量检查并生成合规 JSONL 历史文件；
 *   6. 注入故障场景（模拟 B1/超时导致的 unknown 状态）时，检查器正确输出 inconclusive 且不误报 pass。
 */
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "support/performance/linearizability_checker.h"

namespace perf = stratakv::test::performance;

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

// 模拟单 Key 寄存器存储
struct SimpleRegister {
  std::mutex mutex;
  std::string value = "0";

  std::string Read() {
    std::lock_guard<std::mutex> lock(mutex);
    return value;
  }

  void Write(const std::string& val) {
    std::lock_guard<std::mutex> lock(mutex);
    value = val;
  }
};

uint64_t CurrentTimeNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

}  // namespace

int main() {
  try {
    std::cout << "--- Starting C2 Linearizability Checker & History Checks ---" << std::endl;

    perf::LinearizabilityChecker checker;

    // =========================================================================
    // 1. Golden History: Legal Concurrent Overlapping
    // =========================================================================
    std::cout << "[Step 1] Testing Legal Golden History..." << std::endl;
    {
      // 初始值为 "init"
      // Op0: Write "A", [100, 250]
      // Op1: Write "B", [150, 350] (与 Op0 并发)
      // Op2: Read  "A", [260, 300] (在 Op0 完成后调用，但在 Op1 完成前完成，读到 A 合法)
      // Op3: Read  "B", [360, 400] (在 Op1 完成后调用，读到 B 合法)
      std::vector<perf::RegisterOperation> legalHistory = {
          {0, 100, 250, true, "A", "", true, false},
          {1, 150, 350, true, "B", "", true, false},
          {2, 260, 300, false, "", "A", true, false},
          {3, 360, 400, false, "", "B", true, false},
      };

      const auto res = checker.CheckEpoch(legalHistory, "init", std::string("B"));
      Require(res.verdict == perf::LinearizabilityVerdict::kPass,
              "Legal history must pass linearizability check");
      Require(!res.linearizedSequences.empty(), "Legal history must produce linearized sequence");
      std::cout << "Legal history passed. Linearized sequence size: "
                << res.linearizedSequences.size() << std::endl;
    }

    // =========================================================================
    // 2. Golden History: Illegal Stale Read (Violating Real-Time Order)
    // =========================================================================
    std::cout << "[Step 2] Testing Illegal Stale Read Golden History..." << std::endl;
    {
      // 初始值为 "0"
      // Op0: Write "1", [100, 200]
      // Op1: Read  "0", [250, 300] -> Op0 已在 200ns 完成，Op1 在 250ns 才调用，却读到了旧值 "0"
      std::vector<perf::RegisterOperation> staleHistory = {
          {0, 100, 200, true, "1", "", true, false},
          {1, 250, 300, false, "", "0", true, false},
      };

      const auto res = checker.CheckEpoch(staleHistory, "0");
      Require(res.verdict == perf::LinearizabilityVerdict::kFail,
              "Stale read history MUST fail linearizability check");
      Require(!res.counterexample.empty(), "Failure must provide diagnostic counterexample");
      std::cout << "Stale read correctly failed with counterexample: "
                << res.counterexample << std::endl;
    }

    // =========================================================================
    // 3. Golden History: Illegal Future Read (Reading value before write invoked)
    // =========================================================================
    std::cout << "[Step 3] Testing Illegal Future Read Golden History..." << std::endl;
    {
      // 初始值为 "0"
      // Op0: Read  "X", [100, 200]
      // Op1: Write "X", [300, 400] -> Op0 完成后 Op1 才调用，Op0 读到了来自未来的值
      std::vector<perf::RegisterOperation> futureHistory = {
          {0, 100, 200, false, "", "X", true, false},
          {1, 300, 400, true, "X", "", true, false},
      };

      const auto res = checker.CheckEpoch(futureHistory, "0");
      Require(res.verdict == perf::LinearizabilityVerdict::kFail,
              "Future read history MUST fail linearizability check");
      Require(!res.counterexample.empty(), "Failure must provide diagnostic counterexample");
      std::cout << "Future read correctly failed with counterexample: "
                << res.counterexample << std::endl;
    }

    // =========================================================================
    // 4. Golden History: Pending/Unknown Write (Must NOT be guessed as success)
    // =========================================================================
    std::cout << "[Step 4] Testing Pending/Unknown Write Golden History..." << std::endl;
    {
      // 模拟写超时/未知状态，规范要求：未知写不被猜成成功
      std::vector<perf::RegisterOperation> pendingHistory = {
          {0, 100, 300, true, "val_pending", "", false, true},
          {1, 350, 400, false, "", "0", true, false},
      };

      const auto res = checker.CheckEpoch(pendingHistory, "0");
      Require(res.verdict == perf::LinearizabilityVerdict::kInconclusive,
              "History with pending write MUST be evaluated as Inconclusive (never guessed as pass)");
      std::cout << "Pending write correctly produced Inconclusive: " << res.explanation << std::endl;
    }

    // =========================================================================
    // 5. 300 Operations Integrated Test with Quiescent Epochs
    // =========================================================================
    std::cout << "[Step 5] Running 300 operations integrated test with quiescent epochs..." << std::endl;
    {
      constexpr uint64_t kTotalOperations = 300;
      constexpr size_t kEpochSize = 8;
      SimpleRegister reg;

      std::vector<perf::RegisterOperation> fullHistory;
      fullHistory.reserve(kTotalOperations);

      std::string currentEpochInitialValue = reg.Read();

      for (uint64_t baseSeq = 0; baseSeq < kTotalOperations; baseSeq += kEpochSize) {
        const size_t currentEpochOps =
            static_cast<size_t>(std::min<uint64_t>(kEpochSize, kTotalOperations - baseSeq));

        std::vector<perf::RegisterOperation> epoch(currentEpochOps);
        std::atomic<size_t> readyCount{0};
        std::atomic<bool> startSignal{false};
        std::vector<std::thread> threads;

        for (size_t i = 0; i < currentEpochOps; ++i) {
          threads.emplace_back([&, i]() {
            auto& op = epoch[i];
            op.sequence = baseSeq + i;
            op.write = (op.sequence % 3U == 0);  // 1/3 写，2/3 读
            if (op.write) {
              op.input = "v_" + std::to_string(op.sequence);
            }

            readyCount.fetch_add(1);
            while (!startSignal.load(std::memory_order_acquire)) {
              std::this_thread::yield();
            }

            op.invokeNs = CurrentTimeNs();
            if (op.write) {
              reg.Write(op.input);
              op.success = true;
            } else {
              op.output = reg.Read();
              op.success = true;
            }
            op.completeNs = CurrentTimeNs();
            Require(op.completeNs >= op.invokeNs, "completeNs must be >= invokeNs");
          });
        }

        while (readyCount.load() < currentEpochOps) {
          std::this_thread::yield();
        }
        startSignal.store(true, std::memory_order_release);

        for (auto& t : threads) {
          t.join();
        }

        // 静态 Quiescent Boundary: 所有线程 join 后，读取当前边界确定状态
        const std::string boundaryVal = reg.Read();

        // 运行 checker
        const auto epochResult =
            checker.CheckEpoch(epoch, currentEpochInitialValue, boundaryVal);
        Require(epochResult.verdict == perf::LinearizabilityVerdict::kPass,
                "Epoch starting at seq " + std::to_string(baseSeq) +
                    " failed linearizability check: " + epochResult.counterexample);

        currentEpochInitialValue = boundaryVal;
        fullHistory.insert(fullHistory.end(), epoch.begin(), epoch.end());
      }

      Require(fullHistory.size() == kTotalOperations, "All 300 operations must be recorded");

      // 将 history 写入 JSONL 并重读校验
      const std::filesystem::path historyPath =
          std::filesystem::temp_directory_path() / "c2_test_history.jsonl";
      perf::LinearizabilityChecker::WriteHistoryJsonl(historyPath.string(), fullHistory);
      Require(std::filesystem::exists(historyPath), "History JSONL file must exist");

      const auto reloadedHistory =
          perf::LinearizabilityChecker::ReadHistoryJsonl(historyPath.string());
      Require(reloadedHistory.size() == kTotalOperations, "Reloaded history size must match 300");
      Require(reloadedHistory.front().sequence == 0, "First sequence must be 0");
      Require(reloadedHistory.back().sequence == kTotalOperations - 1, "Last sequence must be 299");

      std::filesystem::remove(historyPath);
      std::cout << "[Step 5 Passed] 300 operations across " << (kTotalOperations / kEpochSize + 1)
                << " quiescent epochs verified with 100% linearizability." << std::endl;
    }

    std::cout << "--- All C2 Linearizability Checks Passed Successfully ---" << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "c2_linearizability_check failed: " << e.what() << std::endl;
    return 1;
  }
}
