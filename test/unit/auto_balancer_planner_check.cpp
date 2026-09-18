#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "auto_balancer_planner.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

RegionMetadata Region(int id) {
  RegionMetadata region;
  region.regionId = id;
  region.startKey = id == 10 ? "" : "m";
  region.endKey = id == 10 ? "m" : "";
  region.epoch = {1, 1};
  region.metadataRevision = 9;
  region.leaderPeerId = static_cast<uint64_t>(id * 10 + 1);
  for (uint64_t storeId = 1; storeId <= 3; ++storeId) {
    region.peers.push_back({static_cast<int>(storeId - 1), "127.0.0.1",
                            static_cast<short>(30000 + storeId), storeId,
                            static_cast<uint64_t>(id * 10) + storeId, false});
  }
  return region;
}

metadataRpcProtocol::AutoBalancerConfig Config() {
  metadataRpcProtocol::AutoBalancerConfig config;
  config.set_enabled(true);
  config.set_version(2);
  config.set_evaluationintervalms(1000);
  config.set_heartbeattimeoutms(5000);
  config.set_minimumfreebytes(100);
  config.set_maximumdiskusedratio(0.95);
  config.set_replicationfactor(3);
  config.set_imbalancethreshold(0.10);
  config.set_splitsizebytes(1000);
  config.set_splitrequestthreshold(1000);
  config.set_splitconsecutivewindows(2);
  config.set_regioncooldownms(10000);
  config.set_maximumactiveoperators(2);
  config.set_maximumactiveperstore(1);
  return config;
}

metadataRpcProtocol::StoreHeartbeat Heartbeat(uint64_t storeId, uint64_t expires,
                                              uint64_t available, uint64_t requests,
                                              const std::vector<int>& regionIds = {10, 20},
                                              uint64_t bytes = 100,
                                              uint64_t regionRequests = 10) {
  metadataRpcProtocol::StoreHeartbeat heartbeat;
  heartbeat.set_storeid(storeId);
  heartbeat.set_sequence(1);
  heartbeat.set_observedatms(1000);
  heartbeat.set_expiresatms(expires);
  heartbeat.set_capacitybytes(1000);
  heartbeat.set_availablebytes(available);
  heartbeat.set_requestcount(requests);
  for (int regionId : regionIds) {
    auto* observation = heartbeat.add_regions();
    observation->set_regionid(static_cast<uint64_t>(regionId));
    observation->set_peerid(static_cast<uint64_t>(regionId * 10) + storeId);
    observation->mutable_epoch()->set_version(1);
    observation->mutable_epoch()->set_confversion(1);
    observation->set_approximatebytes(bytes);
    observation->set_requestcount(regionRequests);
    observation->set_isleader(storeId == 1);
    observation->set_splitcandidategeneration(static_cast<uint64_t>(regionId + 100));
  }
  return heartbeat;
}

std::shared_ptr<MetadataView> View() {
  auto view = std::make_shared<MetadataView>();
  view->revision = 9;
  view->catalog = std::make_shared<RegionCatalog>(
      std::vector<RegionMetadata>{Region(10), Region(20)});
  view->balancerConfig = Config();
  for (uint64_t storeId = 1; storeId <= 4; ++storeId) {
    stratakv::region::StoreDescriptor store;
    store.set_storeid(storeId);
    store.set_host("127.0.0.1");
    store.set_port(static_cast<uint32_t>(30000 + storeId));
    view->stores.emplace(storeId, std::move(store));
  }
  view->storeHeartbeats[1] = Heartbeat(1, 10000, 100, 1000);
  view->storeHeartbeats[2] = Heartbeat(2, 10000, 700, 100);
  view->storeHeartbeats[3] = Heartbeat(3, 10000, 700, 100);
  view->storeHeartbeats[4] = Heartbeat(4, 10000, 900, 10, {});
  return view;
}

ClusterSchedulingSnapshot Snapshot(std::shared_ptr<const MetadataView> view) {
  ClusterSchedulingSnapshot snapshot;
  snapshot.metadata = std::move(view);
  snapshot.nowMs = 2000;
  return snapshot;
}

void CheckExpansionAndStableOrdering() {
  AutoBalancerPlanner planner;
  const auto view = View();
  const auto first = planner.Plan(Snapshot(view));
  const auto second = planner.Plan(Snapshot(view));
  Require(!first.plans.empty(), "healthy expansion must produce a balancing candidate");
  Require(first.plans.front().reason == SchedulingPlanReason::Balance &&
              first.plans.front().operation.sourcestoreid() == 1 &&
              first.plans.front().operation.targetstoreid() == 4,
          "planner must move from the deterministic hottest Store to the empty Store");
  Require(first.plans.front().operation.SerializeAsString() ==
              second.plans.front().operation.SerializeAsString(),
          "equal snapshots must produce stable plans");
  Require(first.plans.front().operation.sourcerevision() == 9 &&
              !first.plans.front().operation.operatorid().empty(),
          "plan must fence admission by metadata revision and stable identity");
}

void CheckRepairAndNoDestination() {
  AutoBalancerPlanner planner;
  auto view = View();
  view->storeHeartbeats[3].set_expiresatms(1500);
  auto planned = planner.Plan(Snapshot(view));
  Require(!planned.plans.empty() &&
              planned.plans.front().reason == SchedulingPlanReason::SafetyRepair &&
              planned.plans.front().operation.sourcestoreid() == 3 &&
              planned.plans.front().operation.targetstoreid() == 4,
          "stale Store must be repaired before ordinary balancing");

  view->storeHeartbeats[4].set_expiresatms(1500);
  planned = planner.Plan(Snapshot(view));
  Require(planned.plans.empty(), "repair without a fresh destination must not be emitted");
  bool explained = false;
  for (const auto& rejection : planned.rejections) {
    if (rejection.regionId == 10 &&
        rejection.reason.find("no safe destination") != std::string::npos) {
      explained = true;
    }
  }
  Require(explained, "no-destination rejection must be explainable");
}

void CheckBudgetsThresholdAndCooldown() {
  AutoBalancerPlanner planner;
  auto view = View();
  view->balancerConfig.set_maximumactiveoperators(1);
  metadataRpcProtocol::SchedulingOperator active;
  active.set_operatorid("already-active");
  active.set_regionid(99);
  active.set_sourcestoreid(2);
  active.set_targetstoreid(3);
  active.set_phase(metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING);
  view->schedulingOperators[active.operatorid()] = active;
  Require(planner.Plan(Snapshot(view)).plans.empty(),
          "exhausted global budget must suppress new work");

  view = View();
  view->balancerConfig.set_imbalancethreshold(0.95);
  Require(planner.Plan(Snapshot(view)).plans.empty(),
          "skew below hysteresis must not schedule a move");

  view = View();
  auto snapshot = Snapshot(view);
  snapshot.cooldownUntilMs[10] = 9000;
  snapshot.cooldownUntilMs[20] = 9000;
  Require(planner.Plan(snapshot).plans.empty(),
          "healthy Regions inside cooldown must not oscillate");
}

void CheckSustainedSplitEvidence() {
  AutoBalancerPlanner planner;
  auto view = View();
  view->storeHeartbeats[1] = Heartbeat(1, 10000, 700, 100, {10, 20}, 2000, 10);
  view->storeHeartbeats[2] = Heartbeat(2, 10000, 700, 100, {10, 20}, 2000, 10);
  view->storeHeartbeats[3] = Heartbeat(3, 10000, 700, 100, {10, 20}, 2000, 10);
  view->storeHeartbeats[4] = Heartbeat(4, 10000, 700, 100, {});
  auto firstInput = Snapshot(view);
  const auto first = planner.Plan(firstInput);
  bool firstSplit = false;
  for (const auto& plan : first.plans) {
    firstSplit = firstSplit || plan.reason == SchedulingPlanReason::AutomaticSplit;
  }
  Require(!firstSplit && first.splitEvidenceWindows.at(10) == 1,
          "one threshold window must accumulate evidence without splitting");

  auto secondInput = Snapshot(view);
  secondInput.splitEvidenceWindows = first.splitEvidenceWindows;
  const auto second = planner.Plan(secondInput);
  Require(!second.plans.empty() &&
              second.plans.front().reason == SchedulingPlanReason::AutomaticSplit &&
              second.plans.front().operation.splitcandidategeneration() == 110,
          "sustained evidence must create a split using only the opaque generation");
  Require(second.plans.front().operation.DebugString().find("startkey") ==
              std::string::npos,
          "split plan must not expose a raw user key");

  view->storeHeartbeats[1] = Heartbeat(1, 10000, 700, 100, {10, 20}, 100, 10);
  view->storeHeartbeats[2] = Heartbeat(2, 10000, 700, 100, {10, 20}, 100, 10);
  view->storeHeartbeats[3] = Heartbeat(3, 10000, 700, 100, {10, 20}, 100, 10);
  auto resetInput = Snapshot(view);
  resetInput.splitEvidenceWindows = first.splitEvidenceWindows;
  Require(planner.Plan(resetInput).splitEvidenceWindows.at(10) == 0,
          "a transient spike must reset consecutive-window evidence");
}

void CheckTopologyWorkDefersSplit() {
  AutoBalancerPlanner planner;
  auto view = View();
  view->storeHeartbeats[1] = Heartbeat(1, 10000, 700, 100, {10}, 2000, 10);
  view->storeHeartbeats[2] = Heartbeat(2, 10000, 700, 100, {10}, 2000, 10);
  view->storeHeartbeats[3] = Heartbeat(3, 10000, 700, 100, {10}, 2000, 10);
  stratakv::region::ReplicaMigrationStatus migration;
  migration.set_regionid(10);
  view->migrations[10] = migration;
  auto input = Snapshot(view);
  input.splitEvidenceWindows[10] = 1;
  const auto planned = planner.Plan(input);
  for (const auto& plan : planned.plans) {
    Require(plan.operation.regionid() != 10,
            "Region with topology work must not receive a split or move plan");
  }
}

}  // namespace

int main() {
  try {
    CheckExpansionAndStableOrdering();
    CheckRepairAndNoDestination();
    CheckBudgetsThresholdAndCooldown();
    CheckSustainedSplitEvidence();
    CheckTopologyWorkDefersSplit();
    std::cout << "Auto-Balancer planner checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Auto-Balancer planner checks failed: " << error.what() << std::endl;
    return 1;
  }
}
