#ifndef STRATAKV_METADATA_AUTO_BALANCER_PLANNER_H
#define STRATAKV_METADATA_AUTO_BALANCER_PLANNER_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "metadata_state_machine.h"

enum class SchedulingPlanReason { SafetyRepair = 0, AutomaticSplit = 1, Balance = 2 };

struct SchedulingPlan {
  metadataRpcProtocol::SchedulingOperator operation;
  SchedulingPlanReason reason = SchedulingPlanReason::Balance;
  double scoreBenefit = 0.0;
  std::string explanation;
};

struct SchedulingRejection {
  uint64_t regionId = 0;
  std::string reason;
};

struct ClusterSchedulingSnapshot {
  std::shared_ptr<const MetadataView> metadata;
  uint64_t nowMs = 0;
  std::unordered_map<uint64_t, uint32_t> splitEvidenceWindows;
  std::unordered_map<uint64_t, uint64_t> cooldownUntilMs;
};

struct AutoBalancerPlanResult {
  std::vector<SchedulingPlan> plans;
  std::vector<SchedulingRejection> rejections;
  std::unordered_map<uint64_t, uint32_t> splitEvidenceWindows;
};

class AutoBalancerPlanner {
 public:
  AutoBalancerPlanResult Plan(const ClusterSchedulingSnapshot& snapshot) const;
};

#endif  // STRATAKV_METADATA_AUTO_BALANCER_PLANNER_H
