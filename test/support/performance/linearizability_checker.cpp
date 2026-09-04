/*
 * 测试目标：实现 C2 单寄存器操作模型、quiescent epoch 分段切分、有界 DFS 线性一致性检查器及诊断反例输出。
 * 测试策略：对并发读写操作按调用和完成时间戳施加实时先后偏序约束（complete(A) < invoke(B) => A 在 B 前生效），有界回溯搜索合法串行执行路径，对未知写判定 inconclusive 并输出反例。
 * 数据规模：支持 smoke 300 operations、full 1,000 operations，单个 epoch 上限通常为 8~16 operations。
 * 验证内容：由 c2_linearizability_check.cpp 验证 legal、illegal、pending golden 历史、实时偏序、反例诊断及未知状态不猜为成功。
 */
#include "support/performance/linearizability_checker.h"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

#include "support/performance/performance_support.h"

namespace stratakv::test::performance {

const char* VerdictToString(LinearizabilityVerdict verdict) {
  switch (verdict) {
    case LinearizabilityVerdict::kPass:
      return "pass";
    case LinearizabilityVerdict::kFail:
      return "fail";
    case LinearizabilityVerdict::kInconclusive:
      return "inconclusive";
  }
  return "unknown";
}

LinearizabilityVerdict ParseVerdict(const std::string& str) {
  if (str == "pass") return LinearizabilityVerdict::kPass;
  if (str == "fail") return LinearizabilityVerdict::kFail;
  return LinearizabilityVerdict::kInconclusive;
}

std::string RegisterOperation::ToJson() const {
  std::ostringstream ss;
  ss << "{\"type\":\"operation\",\"sequence\":" << sequence
     << ",\"invoke_ns\":" << invokeNs
     << ",\"complete_ns\":" << completeNs
     << ",\"operation\":\"" << (write ? "write" : "read") << "\""
     << ",\"input\":\"" << JsonEscape(input) << "\""
     << ",\"output\":\"" << JsonEscape(output) << "\""
     << ",\"status\":\"" << StatusString() << "\"}";
  return ss.str();
}

std::string LinearizabilityResult::ToJson() const {
  std::ostringstream ss;
  ss << "{\"result\":\"" << VerdictToString(verdict) << "\""
     << ",\"explanation\":\"" << JsonEscape(explanation) << "\"";
  if (!counterexample.empty()) {
    ss << ",\"counterexample\":\"" << JsonEscape(counterexample) << "\"";
  }
  ss << "}";
  return ss.str();
}

namespace {

// DFS 搜索合法线性化
bool DfsLinearize(const std::vector<RegisterOperation>& ops,
                  uint64_t remainingMask,
                  const std::string& currentValue,
                  const std::optional<std::string>& requiredFinalValue,
                  std::vector<uint64_t>* currentPath,
                  std::vector<uint64_t>* solutionPath,
                  std::unordered_set<std::string>* memo) {
  if (remainingMask == 0) {
    if (!requiredFinalValue.has_value() || currentValue == *requiredFinalValue) {
      if (solutionPath != nullptr && currentPath != nullptr) {
        *solutionPath = *currentPath;
      }
      return true;
    }
    return false;
  }

  const std::string stateKey = std::to_string(remainingMask) + ":" + currentValue;
  if (memo->count(stateKey) != 0) {
    return false;
  }

  for (size_t i = 0; i < ops.size(); ++i) {
    const uint64_t bit = uint64_t{1} << i;
    if ((remainingMask & bit) == 0) continue;

    const auto& candidate = ops[i];

    // 实时先后顺序检查：若存在另一个尚未生效的操作 other，且 other 在 candidate 调用前已完成
    // 即 other.completeNs < candidate.invokeNs，则 candidate 不能先于 other 生效
    bool predecessorRemains = false;
    for (size_t j = 0; j < ops.size(); ++j) {
      if (i == j) continue;
      const uint64_t otherBit = uint64_t{1} << j;
      if ((remainingMask & otherBit) != 0 && ops[j].completeNs < candidate.invokeNs) {
        predecessorRemains = true;
        break;
      }
    }
    if (predecessorRemains) continue;

    // 单寄存器顺序语义检查
    if (!candidate.write) {
      // 读操作输出必须与当前寄存器值相等
      if (candidate.output != currentValue) continue;
      if (currentPath != nullptr) currentPath->push_back(candidate.sequence);
      if (DfsLinearize(ops, remainingMask & ~bit, currentValue, requiredFinalValue,
                       currentPath, solutionPath, memo)) {
        return true;
      }
      if (currentPath != nullptr) currentPath->pop_back();
    } else {
      // 写操作将寄存器状态变为 input
      const std::string nextValue = candidate.input;
      if (currentPath != nullptr) currentPath->push_back(candidate.sequence);
      if (DfsLinearize(ops, remainingMask & ~bit, nextValue, requiredFinalValue,
                       currentPath, solutionPath, memo)) {
        return true;
      }
      if (currentPath != nullptr) currentPath->pop_back();
    }
  }

  memo->insert(stateKey);
  return false;
}

std::string FindCounterexample(const std::vector<RegisterOperation>& ops,
                               const std::string& initialValue) {
  // 检查是否有读到未来值的反例 (invoke < write.invoke)
  for (const auto& op : ops) {
    if (!op.write && op.success) {
      bool foundMatchingWrite = false;
      if (op.output == initialValue) {
        foundMatchingWrite = true;
      }
      for (const auto& other : ops) {
        if (other.write && other.input == op.output) {
          if (other.invokeNs > op.completeNs) {
            return "Future read violation: Read(seq=" + std::to_string(op.sequence) +
                   ", val=\"" + op.output + "\") completed at " + std::to_string(op.completeNs) +
                   "ns, but matching Write(seq=" + std::to_string(other.sequence) +
                   ") was invoked later at " + std::to_string(other.invokeNs) + "ns.";
          }
          foundMatchingWrite = true;
        }
      }
      if (!foundMatchingWrite) {
        return "Unknown value read: Read(seq=" + std::to_string(op.sequence) +
               ", val=\"" + op.output + "\") read a value not produced by initial state or any write.";
      }
    }
  }

  // 检查是否有陈旧读反例 (stale read: write completed before read invoked, but read got older value)
  for (const auto& w : ops) {
    if (w.write && w.success) {
      for (const auto& r : ops) {
        if (!r.write && r.success && w.completeNs < r.invokeNs) {
          // 在 w 完成之后调用的 r，如果读到了在 w 之前被覆盖的值
          if (r.output != w.input) {
            // 检查在这之间是否有另一个写 w2 也完成了且覆盖了 w
            bool overwritten = false;
            for (const auto& w2 : ops) {
              if (w2.write && w2.sequence != w.sequence && w2.success) {
                if (w.completeNs <= w2.invokeNs && w2.completeNs < r.invokeNs && r.output == w2.input) {
                  overwritten = true;
                  break;
                }
              }
            }
            if (!overwritten) {
              return "Stale read violation: Write(seq=" + std::to_string(w.sequence) +
                     ", val=\"" + w.input + "\") completed at " + std::to_string(w.completeNs) +
                     "ns, but later Read(seq=" + std::to_string(r.sequence) +
                     ", invoke=" + std::to_string(r.invokeNs) + "ns) returned stale value \"" +
                     r.output + "\".";
            }
          }
        }
      }
    }
  }

  return "Real-time precedence violation: No valid linearization exists that satisfies complete(A) < invoke(B).";
}

}  // namespace

LinearizabilityResult LinearizabilityChecker::CheckEpoch(
    const std::vector<RegisterOperation>& operations,
    const std::string& initialValue,
    const std::optional<std::string>& requiredFinalValue) const {
  LinearizabilityResult result;

  if (operations.empty()) {
    if (!requiredFinalValue.has_value() || initialValue == *requiredFinalValue) {
      result.verdict = LinearizabilityVerdict::kPass;
      result.explanation = "Empty epoch matches initial/final value.";
    } else {
      result.verdict = LinearizabilityVerdict::kFail;
      result.explanation = "Empty epoch final value mismatch.";
      result.counterexample = "Initial value (" + initialValue + ") != final value (" + *requiredFinalValue + ")";
    }
    return result;
  }

  // 检查是否存在 pending/unknown 的操作
  bool hasPending = false;
  std::vector<RegisterOperation> effectiveOps;
  for (const auto& op : operations) {
    if (op.unknown || op.Status() == OpStatus::kPendingUnknown) {
      hasPending = true;
    } else if (op.success) {
      effectiveOps.push_back(op);
    }
    // op.success == false && !op.unknown 说明明确失败，不纳入寄存器实际生效操作
  }

  if (hasPending) {
    // 存在未知状态的操作。按契约：未知写不被猜成成功。
    // 首先校验：即使尝试所有可能（包含或不包含 pending 写），是否存在任何合法的串行化？
    // 若即使假定生效或不生效都无法线性化，则判定为 Fail 并给出反例。
    // 否则由于写结果未决，判定为 Inconclusive，绝不猜测为 Pass。
    bool anyLegal = false;
    {
      std::vector<RegisterOperation> candidateOps = effectiveOps;
      std::unordered_set<std::string> memo;
      std::vector<uint64_t> path;
      std::vector<uint64_t> sol;
      const uint64_t mask = (candidateOps.empty() ? 0 : (uint64_t{1} << candidateOps.size()) - 1U);
      if (DfsLinearize(candidateOps, mask, initialValue, requiredFinalValue, &path, &sol, &memo)) {
        anyLegal = true;
      }
    }
    if (!anyLegal) {
      // 尝试把 pending 的写当成成功
      std::vector<RegisterOperation> withPending = effectiveOps;
      for (const auto& op : operations) {
        if ((op.unknown || op.Status() == OpStatus::kPendingUnknown) && op.write) {
          RegisterOperation assumed = op;
          assumed.success = true;
          withPending.push_back(assumed);
        }
      }
      std::unordered_set<std::string> memo;
      std::vector<uint64_t> path;
      std::vector<uint64_t> sol;
      const uint64_t mask = (withPending.empty() ? 0 : (uint64_t{1} << withPending.size()) - 1U);
      if (DfsLinearize(withPending, mask, initialValue, requiredFinalValue, &path, &sol, &memo)) {
        anyLegal = true;
      }
    }

    if (!anyLegal) {
      result.verdict = LinearizabilityVerdict::kFail;
      result.explanation = "Epoch violated register linearizability even with pending writes considered.";
      result.counterexample = FindCounterexample(operations, initialValue);
    } else {
      result.verdict = LinearizabilityVerdict::kInconclusive;
      result.explanation = "Epoch contains pending/unknown operations; outcome cannot be authoritatively resolved without guessing success.";
    }
    return result;
  }

  if (effectiveOps.size() > 64) {
    result.verdict = LinearizabilityVerdict::kInconclusive;
    result.explanation = "Epoch size " + std::to_string(effectiveOps.size()) + " exceeds bounded DFS limit (64).";
    return result;
  }

  std::unordered_set<std::string> memo;
  std::vector<uint64_t> currentPath;
  std::vector<uint64_t> solutionPath;
  const uint64_t initialMask = (effectiveOps.empty() ? 0 : (uint64_t{1} << effectiveOps.size()) - 1U);

  if (DfsLinearize(effectiveOps, initialMask, initialValue, requiredFinalValue,
                   &currentPath, &solutionPath, &memo)) {
    result.verdict = LinearizabilityVerdict::kPass;
    result.explanation = "Valid linear order found for all operations respecting real-time precedence.";
    result.linearizedSequences = solutionPath;
  } else {
    result.verdict = LinearizabilityVerdict::kFail;
    result.explanation = "No valid linearization exists respecting real-time order and register semantics.";
    result.counterexample = FindCounterexample(effectiveOps, initialValue);
  }

  return result;
}

void LinearizabilityChecker::WriteHistoryJsonl(const std::string& filePath,
                                              const std::vector<RegisterOperation>& history) {
  std::ostringstream ss;
  for (const auto& op : history) {
    ss << op.ToJson() << "\n";
  }
  AtomicWrite(filePath, ss.str());
}

std::vector<RegisterOperation> LinearizabilityChecker::ReadHistoryJsonl(const std::string& filePath) {
  std::vector<RegisterOperation> ops;
  std::ifstream file(filePath);
  if (!file.is_open()) return ops;

  std::string line;
  while (std::getline(file, line)) {
    if (line.empty()) continue;
    // Simple json parser for RegisterOperation
    RegisterOperation op;
    auto findKey = [&](const std::string& key) -> std::string {
      const std::string needle = "\"" + key + "\":";
      auto pos = line.find(needle);
      if (pos == std::string::npos) return "";
      pos += needle.size();
      while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\"')) ++pos;
      size_t end = pos;
      while (end < line.size() && line[end] != '\"' && line[end] != ',' && line[end] != '}') ++end;
      return line.substr(pos, end - pos);
    };

    const std::string seqStr = findKey("sequence");
    const std::string invStr = findKey("invoke_ns");
    const std::string compStr = findKey("complete_ns");
    const std::string opStr = findKey("operation");
    const std::string inStr = findKey("input");
    const std::string outStr = findKey("output");
    const std::string statusStr = findKey("status");

    if (!seqStr.empty()) op.sequence = std::stoull(seqStr);
    if (!invStr.empty()) op.invokeNs = std::stoull(invStr);
    if (!compStr.empty()) op.completeNs = std::stoull(compStr);
    op.write = (opStr == "write");
    op.input = inStr;
    op.output = outStr;
    if (statusStr == "ok") {
      op.success = true;
      op.unknown = false;
    } else if (statusStr == "pending") {
      op.success = false;
      op.unknown = true;
    } else {
      op.success = false;
      op.unknown = false;
    }
    ops.push_back(op);
  }
  return ops;
}

}  // namespace stratakv::test::performance
