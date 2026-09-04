/*
 * 测试目标：声明 C2 单寄存器操作模型、quiescent epoch 分段切分、有界 DFS 线性一致性检查器及诊断反例输出。
 * 测试策略：对并发读写操作按调用和完成时间戳施加实时先后偏序约束（complete(A) < invoke(B) => A 在 B 前生效），有界回溯搜索合法串行执行路径，对未知写判定 inconclusive 并输出反例。
 * 数据规模：支持 smoke 300 operations、full 1,000 operations，单个 epoch 上限通常为 8~16 operations。
 * 验证内容：由 c2_linearizability_check.cpp 验证 legal、illegal、pending golden 历史、实时偏序、反例诊断及未知状态不猜为成功。
 */
#ifndef STRATAKV_TEST_SUPPORT_PERFORMANCE_LINEARIZABILITY_CHECKER_H
#define STRATAKV_TEST_SUPPORT_PERFORMANCE_LINEARIZABILITY_CHECKER_H

#include <cstdint>
#include <string>
#include <vector>
#include <optional>

namespace stratakv::test::performance {

enum class OpStatus {
  kOk,
  kFail,
  kPendingUnknown,
};

struct RegisterOperation {
  uint64_t sequence = 0;
  uint64_t invokeNs = 0;
  uint64_t completeNs = 0;
  bool write = false;
  std::string input;
  std::string output;
  bool success = false;
  bool unknown = false;

  OpStatus Status() const {
    if (unknown) return OpStatus::kPendingUnknown;
    if (success) return OpStatus::kOk;
    return OpStatus::kFail;
  }

  std::string StatusString() const {
    if (unknown) return "pending";
    if (success) return "ok";
    return "fail";
  }

  std::string ToJson() const;
};

enum class LinearizabilityVerdict {
  kPass,
  kFail,
  kInconclusive,
};

const char* VerdictToString(LinearizabilityVerdict verdict);
LinearizabilityVerdict ParseVerdict(const std::string& str);

struct LinearizabilityResult {
  LinearizabilityVerdict verdict = LinearizabilityVerdict::kFail;
  std::string explanation;
  std::string counterexample;
  std::vector<uint64_t> linearizedSequences;

  std::string ToJson() const;
};

class LinearizabilityChecker {
 public:
  LinearizabilityChecker() = default;

  // 检查单个 epoch 的线性一致性
  // initialValue: epoch 开始前已知确定的寄存器状态
  // requiredFinalValue: epoch 结束后读取到的确定寄存器状态（可选）
  LinearizabilityResult CheckEpoch(const std::vector<RegisterOperation>& operations,
                                   const std::string& initialValue,
                                   const std::optional<std::string>& requiredFinalValue = std::nullopt) const;

  // 将 history 输出为 jsonl
  static void WriteHistoryJsonl(const std::string& filePath,
                                const std::vector<RegisterOperation>& history);

  // 从 jsonl 解析 history
  static std::vector<RegisterOperation> ReadHistoryJsonl(const std::string& filePath);
};

}  // namespace stratakv::test::performance

#endif  // STRATAKV_TEST_SUPPORT_PERFORMANCE_LINEARIZABILITY_CHECKER_H
