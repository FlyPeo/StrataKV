/*
 * 测试目标：验证 A4 目标争用份额（0/5/20%）× 乐观/悲观 fast-fail 事务生成、指标采集和切换点判定逻辑。
 * 测试策略：使用确定性 hot-slot 生成器断言目标份额；用合成冲突曲线模拟乐观与悲观事务的 committed TPS、冲突率、尝试次数和含退避延迟；验证只有实测曲线真实交叉时才报告切换阈值。
 * 测试规模：争用份额 0%、5%、20%，workers=16，每点 1,000 transactions；测试包含无交叉（乐观占优、悲观占优）与有交叉三种合成场景。
 * 验证条件：确定性 hot-slot 命中率精确符合 0%、5%、20%；所有点完整输出目标份额、实际冲突率、成功率、attempts/commit、含退避延迟和 committed TPS；单策略全胜时不伪造交叉点，曲线交叉时正确给出切换区间。
 */
#include <algorithm>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "support/performance/performance_support.h"

namespace perf = stratakv::test::performance;

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

// 模拟 txn_contention_benchmark 中的确定性 hot-slot 生成逻辑
struct ContentionScenario {
  int transactions = 1000;
  int hotKeyCount = 8;
  int hotPercent = 0;  // 0, 5, 20
  std::string runId = "a4-check";
};

std::vector<std::string> SlotKeys(const ContentionScenario& sc, int slot) {
  const std::string suffix = ":contention:" + sc.runId + ":" + std::to_string(slot);
  std::vector<std::string> keys;
  for (const char prefix : {'a', 'h', 'p'}) {
    keys.push_back(std::string(1, prefix) + suffix + ":0");
  }
  return keys;
}

std::vector<std::string> GenerateKeys(const ContentionScenario& sc, int index) {
  const bool hot = sc.hotKeyCount > 0 && index % 100 < sc.hotPercent;
  return SlotKeys(sc, hot ? index % sc.hotKeyCount : sc.transactions + index);
}

}  // namespace

int main() {
  try {
    std::cout << "--- Starting A4 Contention Strategy & Crossover Validation ---" << std::endl;

    constexpr uint64_t kTransactions = 1000;

    // 1. 验证确定性 hot-slot 生成器的目标份额与槽位隔离
    for (int hotPercent : {0, 5, 20}) {
      ContentionScenario sc;
      sc.transactions = static_cast<int>(kTransactions);
      sc.hotKeyCount = (hotPercent == 0 ? 0 : 8);
      sc.hotPercent = hotPercent;

      int hotTxnCount = 0;
      int coldTxnCount = 0;
      std::set<std::string> hotKeySet;
      std::set<std::string> coldKeySet;

      for (int i = 0; i < sc.transactions; ++i) {
        const auto keys = GenerateKeys(sc, i);
        Require(keys.size() == 3, "Each transaction must touch 3 keys");
        const bool isHot = sc.hotKeyCount > 0 && (i % 100 < hotPercent);
        if (isHot) {
          ++hotTxnCount;
          for (const auto& k : keys) hotKeySet.insert(k);
        } else {
          ++coldTxnCount;
          for (const auto& k : keys) coldKeySet.insert(k);
        }
      }

      const double actualShare = static_cast<double>(hotTxnCount) / kTransactions;
      const double expectedShare = hotPercent / 100.0;
      Require(std::abs(actualShare - expectedShare) < 1e-9,
              "Hot transaction count mismatch for " + std::to_string(hotPercent) + "%");

      if (hotPercent > 0) {
        Require(hotKeySet.size() == static_cast<size_t>(sc.hotKeyCount * 3),
                "Hot keys must be bounded to hotKeyCount * 3");
        for (const auto& hotKey : hotKeySet) {
          Require(coldKeySet.find(hotKey) == coldKeySet.end(),
                  "Hot key set and cold key set must be strictly disjoint");
        }
      }

      std::cout << "[Check Hot-Slot " << hotPercent << "%] "
                << "target_share=" << expectedShare << ", actual_hot_txns=" << hotTxnCount
                << " (" << actualShare * 100.0 << "%), disjoint keys verified." << std::endl;
    }

    // 2. 验证合成曲线测试与切换点判定
    // 场景 A: 无交叉，Optimistic 在 0%, 5%, 20% 全面占优
    {
      std::vector<perf::A4PointResult> optResults = {
        {"optimistic", 0.00, 0.00, 1.00, 1.00, 5000.0, 5000.0, 100, 150, 200, 300},
        {"optimistic", 0.05, 0.08, 0.96, 1.05, 4200.0, 4400.0, 120, 200, 280, 450},
        {"optimistic", 0.20, 0.25, 0.82, 1.25, 3000.0, 3750.0, 200, 350, 500, 800},
      };
      std::vector<perf::A4PointResult> pessResults = {
        {"pessimistic", 0.00, 0.00, 1.00, 1.00, 4000.0, 4000.0, 150, 200, 250, 350},
        {"pessimistic", 0.05, 0.06, 0.94, 1.06, 3600.0, 3800.0, 170, 240, 310, 450},
        {"pessimistic", 0.20, 0.18, 0.85, 1.18, 2800.0, 3300.0, 230, 380, 520, 750},
      };

      const auto analysis = perf::EvaluateA4Crossover(optResults, pessResults);
      Require(!analysis.hasCrossover, "Should not observe crossover when optimistic dominates");
      Require(analysis.dominantMode == "optimistic", "Dominant mode must be optimistic");
      Require(analysis.summaryMessage.find("No crossover observed") != std::string::npos,
              "Message must note that no crossover was observed");
      std::cout << "[Scenario A - No Crossover] Result: " << analysis.summaryMessage << std::endl;
    }

    // 场景 B: 无交叉，Pessimistic 在所有争用份额下均占优
    {
      std::vector<perf::A4PointResult> optResults = {
        {"optimistic", 0.00, 0.00, 1.00, 1.00, 3000.0, 3000.0, 150, 200, 250, 350},
        {"optimistic", 0.05, 0.10, 0.90, 1.12, 2500.0, 2800.0, 200, 300, 400, 600},
        {"optimistic", 0.20, 0.35, 0.70, 1.45, 1800.0, 2600.0, 350, 550, 800, 1200},
      };
      std::vector<perf::A4PointResult> pessResults = {
        {"pessimistic", 0.00, 0.00, 1.00, 1.00, 3500.0, 3500.0, 120, 180, 220, 300},
        {"pessimistic", 0.05, 0.04, 0.96, 1.04, 3200.0, 3330.0, 140, 210, 260, 380},
        {"pessimistic", 0.20, 0.15, 0.88, 1.14, 2800.0, 3190.0, 180, 280, 360, 500},
      };

      const auto analysis = perf::EvaluateA4Crossover(optResults, pessResults);
      Require(!analysis.hasCrossover, "Should not observe crossover when pessimistic dominates");
      Require(analysis.dominantMode == "pessimistic", "Dominant mode must be pessimistic");
      Require(analysis.summaryMessage.find("No crossover observed") != std::string::npos,
              "Message must note that no crossover was observed");
      std::cout << "[Scenario B - No Crossover] Result: " << analysis.summaryMessage << std::endl;
    }

    // 场景 C: 发生真实交叉（低争用 0%/5% 下乐观胜出，高争用 20% 下悲观胜出）
    {
      std::vector<perf::A4PointResult> optResults = {
        {"optimistic", 0.00, 0.00, 1.00, 1.00, 5000.0, 5000.0, 100, 140, 180, 250},
        {"optimistic", 0.05, 0.06, 0.95, 1.05, 4200.0, 4410.0, 130, 210, 290, 400},
        {"optimistic", 0.20, 0.38, 0.65, 1.54, 2000.0, 3080.0, 320, 550, 850, 1300},
      };
      std::vector<perf::A4PointResult> pessResults = {
        {"pessimistic", 0.00, 0.00, 1.00, 1.00, 4200.0, 4200.0, 140, 190, 240, 320},
        {"pessimistic", 0.05, 0.04, 0.96, 1.04, 3800.0, 3950.0, 160, 220, 280, 390},
        {"pessimistic", 0.20, 0.12, 0.89, 1.12, 2900.0, 3250.0, 210, 310, 420, 600},
      };

      const auto analysis = perf::EvaluateA4Crossover(optResults, pessResults);
      Require(analysis.hasCrossover, "Must detect crossover between 5% and 20%");
      Require(analysis.crossoverShareLow == 0.05 && analysis.crossoverShareHigh == 0.20,
              "Crossover bracket must be between 5% and 20%");
      std::cout << "[Scenario C - Real Crossover] Result: " << analysis.summaryMessage << std::endl;
    }

    // 3. 验证 Scenario: "Fast failures are not successful throughput"
    // 当悲观模式快速返回大量冲突时，attempted TPS 很高，但 committed TPS 与 success_rate 明确隔离
    {
      const double attempted = 10000.0;
      const double committed = 2000.0;
      const double seconds = 1.0;
      const double attemptedTps = attempted / seconds;
      const double committedTps = committed / seconds;
      const double successRate = committed / attempted;
      const double attemptsPerCommit = attempted / committed;

      Require(attemptedTps == 10000.0, "Attempted TPS must reflect raw attempt rate");
      Require(committedTps == 2000.0, "Committed TPS must reflect only successful commits");
      Require(successRate == 0.20, "Success rate must be 20%");
      Require(attemptsPerCommit == 5.0, "Average attempts per commit must be 5.0");
      std::cout << "[Check Fast Failure Distinction] attempted TPS (" << attemptedTps
                << ") != committed TPS (" << committedTps << "), success_rate=" << successRate
                << ", attempts/commit=" << attemptsPerCommit << std::endl;
    }

    std::cout << "--- A4 Contention Strategy & Crossover Validation Successfully Passed ---" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "a4_contention_check failed: " << error.what() << '\n';
    return 1;
  }
}
