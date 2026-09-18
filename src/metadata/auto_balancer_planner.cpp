#include "auto_balancer_planner.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <set>
#include <tuple>
#include <unordered_set>

namespace {

struct StoreScore {
  uint64_t storeId = 0;
  bool fresh = false;
  bool eligible = false;
  double score = 0.0;
  uint32_t regionCount = 0;
  uint64_t requestCount = 0;
};

bool IsTerminal(metadataRpcProtocol::SchedulingOperatorPhase phase) {
  return phase == metadataRpcProtocol::SCHEDULING_OPERATOR_SUCCEEDED ||
         phase == metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED ||
         phase == metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED;
}

std::string OperatorId(uint64_t revision, uint64_t regionId, const char* kind,
                       uint64_t source, uint64_t target) {
  return "auto:" + std::to_string(revision) + ":" + std::to_string(regionId) + ":" + kind +
         ":" + std::to_string(source) + ":" + std::to_string(target);
}

metadataRpcProtocol::SchedulingOperator BaseOperator(const MetadataView& view,
                                                     const RegionMetadata& region,
                                                     uint64_t nowMs) {
  metadataRpcProtocol::SchedulingOperator operation;
  operation.set_regionid(static_cast<uint64_t>(region.regionId));
  operation.set_sourcerevision(view.revision);
  operation.mutable_regionepoch()->set_version(region.epoch.version);
  operation.mutable_regionepoch()->set_confversion(region.epoch.confVersion);
  operation.set_phase(metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING);
  operation.set_createdatms(nowMs);
  operation.set_updatedatms(nowMs);
  const uint64_t retryWindow = std::max<uint64_t>(30000,
      view.balancerConfig.heartbeattimeoutms() * 4);
  operation.set_deadlinems(nowMs + retryWindow);
  return operation;
}

bool HostsRegion(const RegionMetadata& region, uint64_t storeId) {
  return std::any_of(region.peers.begin(), region.peers.end(),
                     [storeId](const RegionPeerLocation& peer) {
                       return peer.storeId == storeId;
                     });
}

const metadataRpcProtocol::RegionSchedulingObservation* FindObservation(
    const MetadataView& view, uint64_t storeId, uint64_t regionId) {
  const auto heartbeat = view.storeHeartbeats.find(storeId);
  if (heartbeat == view.storeHeartbeats.end()) return nullptr;
  for (const auto& observation : heartbeat->second.regions()) {
    if (observation.regionid() == regionId) return &observation;
  }
  return nullptr;
}

}  // namespace

AutoBalancerPlanResult AutoBalancerPlanner::Plan(
    const ClusterSchedulingSnapshot& snapshot) const {
  AutoBalancerPlanResult result;
  result.splitEvidenceWindows = snapshot.splitEvidenceWindows;
  if (!snapshot.metadata || !snapshot.metadata->catalog) return result;
  const MetadataView& view = *snapshot.metadata;
  const auto& config = view.balancerConfig;
  if (!config.enabled() || config.paused()) return result;

  std::unordered_map<uint64_t, StoreScore> scores;
  uint32_t maxRegionCount = 1;
  uint64_t maxRequestCount = 1;
  for (const auto& store : view.stores) scores[store.first].storeId = store.first;
  for (const auto& item : view.storeHeartbeats) {
    auto& score = scores[item.first];
    const auto& heartbeat = item.second;
    score.storeId = item.first;
    score.fresh = heartbeat.expiresatms() > snapshot.nowMs;
    const double usedRatio = heartbeat.capacitybytes() == 0
                                 ? 1.0
                                 : 1.0 - static_cast<double>(heartbeat.availablebytes()) /
                                             static_cast<double>(heartbeat.capacitybytes());
    score.eligible = score.fresh &&
                     heartbeat.availablebytes() >= config.minimumfreebytes() &&
                     usedRatio <= config.maximumdiskusedratio();
    score.regionCount = static_cast<uint32_t>(heartbeat.regions_size());
    score.requestCount = heartbeat.requestcount();
    maxRegionCount = std::max(maxRegionCount, score.regionCount);
    maxRequestCount = std::max(maxRequestCount, score.requestCount);
  }
  for (auto& item : scores) {
    const auto heartbeat = view.storeHeartbeats.find(item.first);
    if (heartbeat == view.storeHeartbeats.end()) continue;
    const auto& report = heartbeat->second;
    const double usedRatio = report.capacitybytes() == 0
                                 ? 1.0
                                 : 1.0 - static_cast<double>(report.availablebytes()) /
                                             static_cast<double>(report.capacitybytes());
    item.second.score = usedRatio * 0.55 +
                        static_cast<double>(item.second.regionCount) / maxRegionCount * 0.25 +
                        static_cast<double>(item.second.requestCount) / maxRequestCount * 0.20;
  }

  size_t activeCount = 0;
  std::unordered_map<uint64_t, uint32_t> activeByStore;
  for (const auto& item : view.schedulingOperators) {
    const auto& operation = item.second;
    if (IsTerminal(operation.phase())) continue;
    ++activeCount;
    if (operation.sourcestoreid()) ++activeByStore[operation.sourcestoreid()];
    if (operation.targetstoreid()) ++activeByStore[operation.targetstoreid()];
  }
  size_t remainingBudget = activeCount >= config.maximumactiveoperators()
                               ? 0
                               : config.maximumactiveoperators() - activeCount;
  std::unordered_set<uint64_t> plannedRegions;

  auto targetFor = [&](const RegionMetadata& region) -> uint64_t {
    uint64_t target = 0;
    double targetScore = std::numeric_limits<double>::infinity();
    for (const auto& item : scores) {
      const auto& store = item.second;
      if (!store.eligible || HostsRegion(region, store.storeId) ||
          activeByStore[store.storeId] >= config.maximumactiveperstore()) {
        continue;
      }
      if (store.score < targetScore ||
          (store.score == targetScore && store.storeId < target)) {
        target = store.storeId;
        targetScore = store.score;
      }
    }
    return target;
  };

  // Safety repair is evaluated first and may bypass cooldown, but never
  // freshness, placement, per-Store budget, or existing topology work.
  for (const auto& region : view.catalog->Regions()) {
    const uint64_t regionId = static_cast<uint64_t>(region.regionId);
    if (remainingBudget == 0) break;
    if (view.activeOperatorByRegion.count(regionId) || view.migrations.count(regionId) ||
        region.splitPending) {
      result.rejections.push_back({regionId, "topology operation already active"});
      continue;
    }
    uint32_t liveVoters = 0;
    uint64_t staleSource = 0;
    for (const auto& peer : region.peers) {
      if (peer.isLearner) continue;
      const auto store = scores.find(peer.storeId);
      if (store != scores.end() && store->second.fresh) {
        ++liveVoters;
      } else if (staleSource == 0 || peer.storeId < staleSource) {
        staleSource = peer.storeId;
      }
    }
    if (liveVoters >= config.replicationfactor() || staleSource == 0) continue;
    const uint64_t target = targetFor(region);
    if (target == 0) {
      result.rejections.push_back({regionId, "no safe destination for replica repair"});
      continue;
    }
    auto operation = BaseOperator(view, region, snapshot.nowMs);
    operation.set_type(metadataRpcProtocol::SCHEDULING_OPERATOR_MOVE_PEER);
    operation.set_sourcestoreid(staleSource);
    operation.set_targetstoreid(target);
    operation.set_operatorid(OperatorId(view.revision, regionId, "repair", staleSource, target));
    result.plans.push_back({operation, SchedulingPlanReason::SafetyRepair, 1.0,
                            "replace stale replica on a fresh eligible Store"});
    plannedRegions.insert(regionId);
    ++activeByStore[staleSource];
    ++activeByStore[target];
    --remainingBudget;
  }

  // Sustained split evidence is updated for every stable Region even when the
  // active-operation budget is currently full, so a later evaluation can act.
  for (const auto& region : view.catalog->Regions()) {
    const uint64_t regionId = static_cast<uint64_t>(region.regionId);
    const metadataRpcProtocol::RegionSchedulingObservation* best = nullptr;
    for (const auto& peer : region.peers) {
      const auto score = scores.find(peer.storeId);
      if (score == scores.end() || !score->second.fresh) continue;
      const auto* observation = FindObservation(view, peer.storeId, regionId);
      if (!observation) continue;
      if (!best || observation->isleader() ||
          observation->approximatebytes() > best->approximatebytes()) {
        best = observation;
        if (observation->isleader()) break;
      }
    }
    const bool above = best && best->splitcandidategeneration() != 0 &&
                       (best->approximatebytes() >= config.splitsizebytes() ||
                        best->requestcount() >= config.splitrequestthreshold());
    uint32_t& evidence = result.splitEvidenceWindows[regionId];
    evidence = above ? evidence + 1 : 0;
    if (!above || evidence < config.splitconsecutivewindows() || remainingBudget == 0 ||
        plannedRegions.count(regionId)) {
      continue;
    }
    if (view.activeOperatorByRegion.count(regionId) || view.migrations.count(regionId) ||
        region.splitPending) {
      result.rejections.push_back({regionId, "automatic split deferred by topology work"});
      continue;
    }
    const auto cooldown = snapshot.cooldownUntilMs.find(regionId);
    if (cooldown != snapshot.cooldownUntilMs.end() && cooldown->second > snapshot.nowMs) {
      result.rejections.push_back({regionId, "Region is inside scheduling cooldown"});
      continue;
    }
    auto operation = BaseOperator(view, region, snapshot.nowMs);
    operation.set_type(metadataRpcProtocol::SCHEDULING_OPERATOR_SPLIT_REGION);
    operation.set_splitcandidategeneration(best->splitcandidategeneration());
    operation.set_operatorid(OperatorId(view.revision, regionId, "split", 0,
                                        best->splitcandidategeneration()));
    result.plans.push_back({operation, SchedulingPlanReason::AutomaticSplit, 1.0,
                            "sustained Region size or request threshold"});
    plannedRegions.insert(regionId);
    --remainingBudget;
  }

  // Healthy balancing chooses one deterministic high-score source and the
  // lowest-score destination for each remaining slot.
  while (remainingBudget > 0) {
    uint64_t source = 0;
    uint64_t target = 0;
    double sourceScore = -1.0;
    double targetScore = std::numeric_limits<double>::infinity();
    for (const auto& item : scores) {
      const auto& store = item.second;
      if (!store.fresh || activeByStore[store.storeId] >= config.maximumactiveperstore()) {
        continue;
      }
      if (store.score > sourceScore ||
          (store.score == sourceScore && store.storeId < source)) {
        source = store.storeId;
        sourceScore = store.score;
      }
      if (store.eligible &&
          (store.score < targetScore ||
           (store.score == targetScore && store.storeId < target))) {
        target = store.storeId;
        targetScore = store.score;
      }
    }
    if (source == 0 || target == 0 || source == target ||
        sourceScore - targetScore <= config.imbalancethreshold()) {
      break;
    }
    const RegionMetadata* chosen = nullptr;
    for (const auto& region : view.catalog->Regions()) {
      const uint64_t regionId = static_cast<uint64_t>(region.regionId);
      const auto cooldown = snapshot.cooldownUntilMs.find(regionId);
      if (plannedRegions.count(regionId) || view.activeOperatorByRegion.count(regionId) ||
          view.migrations.count(regionId) || region.splitPending ||
          !HostsRegion(region, source) || HostsRegion(region, target) ||
          (cooldown != snapshot.cooldownUntilMs.end() && cooldown->second > snapshot.nowMs)) {
        continue;
      }
      chosen = &region;
      break;
    }
    if (!chosen) {
      result.rejections.push_back({0, "no Region can reduce the current Store skew"});
      break;
    }
    auto operation = BaseOperator(view, *chosen, snapshot.nowMs);
    operation.set_type(metadataRpcProtocol::SCHEDULING_OPERATOR_MOVE_PEER);
    operation.set_sourcestoreid(source);
    operation.set_targetstoreid(target);
    operation.set_operatorid(OperatorId(view.revision,
                                        static_cast<uint64_t>(chosen->regionId),
                                        "balance", source, target));
    result.plans.push_back({operation, SchedulingPlanReason::Balance,
                            sourceScore - targetScore,
                            "move a peer from the highest-score Store"});
    plannedRegions.insert(static_cast<uint64_t>(chosen->regionId));
    ++activeByStore[source];
    ++activeByStore[target];
    --remainingBudget;
  }

  std::sort(result.plans.begin(), result.plans.end(), [](const SchedulingPlan& lhs,
                                                         const SchedulingPlan& rhs) {
    return std::tuple<int, double, uint64_t, uint64_t, uint64_t>(
               static_cast<int>(lhs.reason), -lhs.scoreBenefit, lhs.operation.regionid(),
               lhs.operation.sourcestoreid(), lhs.operation.targetstoreid()) <
           std::tuple<int, double, uint64_t, uint64_t, uint64_t>(
               static_cast<int>(rhs.reason), -rhs.scoreBenefit, rhs.operation.regionid(),
               rhs.operation.sourcestoreid(), rhs.operation.targetstoreid());
  });
  return result;
}
