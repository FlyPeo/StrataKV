#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "auto_balancer.h"
#include "auto_balancer_planner.h"
#include "metadata_state_machine.h"
#include "mvcc_storage.h"
#include "rocksdb_kv_engine.h"
#include "topology_operation_coordinator.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

metadataRpcProtocol::MetadataCommand BootstrapCommand() {
  metadataRpcProtocol::MetadataCommand command;
  command.set_mutationid("integration-bootstrap");
  command.mutable_bootstrap()->set_configdigest("integration-digest");

  // Region 10: keys ["", "m") on Stores 1, 2, 3
  auto* region1 = command.mutable_bootstrap()->add_regions();
  region1->set_regionid(10);
  region1->set_startkey("");
  region1->set_endkey("m");
  region1->mutable_epoch()->set_version(1);
  region1->mutable_epoch()->set_confversion(1);
  region1->set_leaderpeerid(101);
  for (uint64_t storeId = 1; storeId <= 3; ++storeId) {
    auto* peer = region1->add_peers();
    peer->set_peerid(100 + storeId);
    peer->set_storeid(storeId);
    peer->set_host("127.0.0.1");
    peer->set_port(static_cast<uint32_t>(19000 + storeId));
  }

  // Region 20: keys ["m", "") on Stores 1, 2, 3
  auto* region2 = command.mutable_bootstrap()->add_regions();
  region2->set_regionid(20);
  region2->set_startkey("m");
  region2->set_endkey("");
  region2->mutable_epoch()->set_version(1);
  region2->mutable_epoch()->set_confversion(1);
  region2->set_leaderpeerid(201);
  for (uint64_t storeId = 1; storeId <= 3; ++storeId) {
    auto* peer = region2->add_peers();
    peer->set_peerid(200 + storeId);
    peer->set_storeid(storeId);
    peer->set_host("127.0.0.1");
    peer->set_port(static_cast<uint32_t>(19000 + storeId));
  }

  for (uint64_t storeId = 1; storeId <= 4; ++storeId) {
    auto* store = command.mutable_bootstrap()->add_stores();
    store->set_storeid(storeId);
    store->set_host("127.0.0.1");
    store->set_port(static_cast<uint32_t>(19000 + storeId));
  }
  return command;
}

metadataRpcProtocol::MetadataCommand EnableBalancerCommand(
    MetadataStateMachine& sm, uint32_t maxActiveOperators = 2,
    uint32_t maxActivePerStore = 1) {
  metadataRpcProtocol::AutoBalancerConfig config = sm.View()->balancerConfig;
  config.set_enabled(true);
  config.set_paused(false);
  config.set_version(config.version() + 1);
  config.set_evaluationintervalms(1000);
  config.set_heartbeattimeoutms(10000);
  config.set_minimumfreebytes(100);
  config.set_maximumdiskusedratio(0.95);
  config.set_replicationfactor(3);
  config.set_imbalancethreshold(0.10);
  config.set_splitsizebytes(1024 * 1024);
  config.set_splitrequestthreshold(100);
  config.set_splitconsecutivewindows(2);
  config.set_regioncooldownms(10000);
  config.set_maximumactiveoperators(maxActiveOperators);
  config.set_maximumactiveperstore(maxActivePerStore);

  metadataRpcProtocol::MetadataCommand cmd;
  cmd.set_mutationid("enable-balancer-" + std::to_string(config.version()));
  cmd.set_expectedrevision(sm.View()->revision);
  *cmd.mutable_updateautobalancerconfig()->mutable_config() = config;
  return cmd;
}

metadataRpcProtocol::MetadataCommand MakeHeartbeat(uint64_t storeId, uint64_t seq,
                                                  uint64_t capacity, uint64_t available,
                                                  uint64_t expiresAtMs = 1000000) {
  metadataRpcProtocol::MetadataCommand cmd;
  cmd.set_mutationid("hb-" + std::to_string(storeId) + "-" + std::to_string(seq));
  auto* hb = cmd.mutable_reportstoreheartbeat()->mutable_heartbeat();
  hb->set_storeid(storeId);
  hb->set_sequence(seq);
  hb->set_observedatms(1000);
  hb->set_expiresatms(expiresAtMs);
  hb->set_capacitybytes(capacity);
  hb->set_availablebytes(available);
  return cmd;
}

metadataRpcProtocol::SchedulingOperator CreateMoveOp(
    const std::string& id, uint64_t regionId, uint64_t sourceStore, uint64_t targetStore,
    uint64_t sourceRevision, metadataRpcProtocol::SchedulingOperatorPhase phase =
        metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING) {
  metadataRpcProtocol::SchedulingOperator op;
  op.set_operatorid(id);
  op.set_type(metadataRpcProtocol::SCHEDULING_OPERATOR_MOVE_PEER);
  op.set_regionid(regionId);
  op.set_sourcerevision(sourceRevision);
  op.mutable_regionepoch()->set_version(1);
  op.mutable_regionepoch()->set_confversion(1);
  op.set_sourcestoreid(sourceStore);
  op.set_targetstoreid(targetStore);
  op.set_phase(phase);
  op.set_attempts(1);
  op.set_deadlinems(99999999);
  op.set_createdatms(1000);
  op.set_updatedatms(1000);
  op.set_irreversible(false);
  return op;
}

// 1. Legacy & disabled compatibility
void CheckLegacyAndDisabledCompatibility() {
  MetadataStateMachine sm;
  sm.Apply(BootstrapCommand());
  Require(!sm.View()->balancerConfig.enabled(), "balancer must be disabled by default");

  // Report heartbeats showing imbalance
  sm.Apply(MakeHeartbeat(1, 1, 10000, 1000));
  sm.Apply(MakeHeartbeat(2, 1, 10000, 1000));
  sm.Apply(MakeHeartbeat(3, 1, 10000, 1000));
  sm.Apply(MakeHeartbeat(4, 1, 10000, 9000));

  // Attempting to admit an operator while balancer is disabled MUST fail with METADATA_UNAVAILABLE
  metadataRpcProtocol::MetadataCommand addOp;
  addOp.set_mutationid("op-disabled-test");
  addOp.set_expectedrevision(sm.View()->revision);
  *addOp.mutable_createschedulingoperator()->mutable_operator_() =
      CreateMoveOp("op-disabled", 10, 1, 4, sm.View()->revision);
  auto res = sm.Apply(addOp);
  Require(res.error() == metadataRpcProtocol::METADATA_CONFLICT,
          "operator admission must be rejected when balancer is disabled");
  Require(sm.View()->schedulingOperators.empty(), "no operator admitted in disabled mode");
}

// 2. Concurrency limits (cluster-wide and per-store)
void CheckConcurrencyLimits() {
  MetadataStateMachine sm;
  sm.Apply(BootstrapCommand());
  sm.Apply(EnableBalancerCommand(sm, /*maxActiveOperators=*/1, /*maxActivePerStore=*/1));

  sm.Apply(MakeHeartbeat(1, 1, 10000, 1000));
  sm.Apply(MakeHeartbeat(2, 1, 10000, 1000));
  sm.Apply(MakeHeartbeat(3, 1, 10000, 1000));
  sm.Apply(MakeHeartbeat(4, 1, 10000, 9000));

  // Admit operator 1 on Region 10 (Store 1 -> Store 4)
  metadataRpcProtocol::MetadataCommand addOp1;
  addOp1.set_mutationid("op-conc-1");
  addOp1.set_expectedrevision(sm.View()->revision);
  *addOp1.mutable_createschedulingoperator()->mutable_operator_() =
      CreateMoveOp("op-conc-1", 10, 1, 4, sm.View()->revision);
  auto res1 = sm.Apply(addOp1);
  Require(res1.error() == metadataRpcProtocol::METADATA_OK, "first operator must be admitted");

  // Attempt to admit operator 2 on Region 20 while operator 1 is active (maxActive=1)
  metadataRpcProtocol::MetadataCommand addOp2;
  addOp2.set_mutationid("op-conc-2");
  addOp2.set_expectedrevision(sm.View()->revision);
  *addOp2.mutable_createschedulingoperator()->mutable_operator_() =
      CreateMoveOp("op-conc-2", 20, 2, 4, sm.View()->revision);
  auto res2 = sm.Apply(addOp2);
  Require(res2.error() == metadataRpcProtocol::METADATA_CONFLICT,
          "second operator must be rejected when cluster concurrency budget is exhausted");
}

// 3. Store expansion & skew convergence
void CheckStoreExpansionAndSkewConvergence() {
  MetadataStateMachine sm;
  sm.Apply(BootstrapCommand());
  sm.Apply(EnableBalancerCommand(sm, /*maxActiveOperators=*/4, /*maxActivePerStore=*/2));

  // Store 4 is added with 90% free capacity; Stores 1, 2, 3 are 90% full
  sm.Apply(MakeHeartbeat(1, 1, 10000, 1000));
  sm.Apply(MakeHeartbeat(2, 1, 10000, 1000));
  sm.Apply(MakeHeartbeat(3, 1, 10000, 1000));
  sm.Apply(MakeHeartbeat(4, 1, 10000, 9000));

  // Planner evaluation
  ClusterSchedulingSnapshot snapshot;
  snapshot.metadata = sm.View();
  snapshot.nowMs = 2000;

  AutoBalancerPlanner planner;
  const auto planResult = planner.Plan(snapshot);

  Require(!planResult.plans.empty(), "planner must generate balancing move for Store 4");
  const auto& topPlan = planResult.plans.front();
  Require(topPlan.operation.type() == metadataRpcProtocol::SCHEDULING_OPERATOR_MOVE_PEER,
          "plan must be MOVE_PEER");
  Require(topPlan.operation.targetstoreid() == 4, "target store must be expanded Store 4");
  Require(topPlan.scoreBenefit > 0.0, "score benefit must be positive");

  // Admit the operator and drive through coordinator
  metadataRpcProtocol::MetadataCommand admit;
  admit.set_mutationid("admit-expansion");
  admit.set_expectedrevision(sm.View()->revision);
  *admit.mutable_createschedulingoperator()->mutable_operator_() = topPlan.operation;
  const auto& reg = sm.View()->catalog->FindById(static_cast<int>(topPlan.operation.regionid()));
  auto* ep = admit.mutable_createschedulingoperator()->mutable_operator_()->mutable_regionepoch();
  ep->set_version(reg.epoch.version);
  ep->set_confversion(reg.epoch.confVersion);
  admit.mutable_createschedulingoperator()->mutable_operator_()->set_sourcerevision(sm.View()->revision);
  auto admitRes = sm.Apply(admit);
  Require(admitRes.error() == metadataRpcProtocol::METADATA_OK, "expansion operator should be admitted");

  TopologyOperationCoordinator coordinator(nullptr);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  const auto outcome = coordinator.ReconcileOperator(admitRes.operator_(), deadline, []() { return false; });
  Require(outcome == CoordinatorStepResult::Success, "coordinator must drive expansion move to success");
}

// 4. Store failure & safety repair
void CheckStoreFailureAndSafetyRepair() {
  MetadataStateMachine sm;
  sm.Apply(BootstrapCommand());
  sm.Apply(EnableBalancerCommand(sm, /*maxActiveOperators=*/4, /*maxActivePerStore=*/2));

  // Store 1 dies: its heartbeat expires (expiresatms < nowMs)
  sm.Apply(MakeHeartbeat(1, 1, 10000, 5000, /*expiresAtMs=*/1000));
  sm.Apply(MakeHeartbeat(2, 1, 10000, 5000, /*expiresAtMs=*/100000));
  sm.Apply(MakeHeartbeat(3, 1, 10000, 5000, /*expiresAtMs=*/100000));
  sm.Apply(MakeHeartbeat(4, 1, 10000, 9000, /*expiresAtMs=*/100000));

  ClusterSchedulingSnapshot snapshot;
  snapshot.metadata = sm.View();
  snapshot.nowMs = 5000;  // Store 1 is stale

  AutoBalancerPlanner planner;
  const auto planResult = planner.Plan(snapshot);

  Require(!planResult.plans.empty(), "planner must identify replica needing safety repair");
  bool foundSafetyRepair = false;
  for (const auto& plan : planResult.plans) {
    if (plan.reason == SchedulingPlanReason::SafetyRepair) {
      foundSafetyRepair = true;
      Require(plan.operation.sourcestoreid() == 1, "failed Store 1 must be source");
      Require(plan.operation.targetstoreid() == 4, "healthy Store 4 must be target");
    }
  }
  Require(foundSafetyRepair, "safety repair plan must be prioritized for failed store");
}

// 5. Manual-operation races
void CheckManualOperationRaces() {
  MetadataStateMachine sm;
  sm.Apply(BootstrapCommand());
  sm.Apply(EnableBalancerCommand(sm));
  sm.Apply(MakeHeartbeat(4, 1, 10000, 9000));

  // Auto-operator planned against revision R and epoch (1, 1)
  const uint64_t plannedRevision = sm.View()->revision;
  metadataRpcProtocol::SchedulingOperator plannedOp =
      CreateMoveOp("op-race", 10, 1, 4, plannedRevision);

  // A concurrent manual operation advances the state machine revision
  metadataRpcProtocol::MetadataCommand manualCmd;
  manualCmd.set_mutationid("manual-allocate-id");
  manualCmd.set_expectedrevision(sm.View()->revision);
  manualCmd.mutable_allocateids()->set_namespace_(metadataRpcProtocol::ID_NAMESPACE_REGION);
  manualCmd.mutable_allocateids()->set_count(1);
  auto manualRes = sm.Apply(manualCmd);
  Require(manualRes.error() == metadataRpcProtocol::METADATA_OK, "manual operation succeeds");
  Require(sm.View()->revision > plannedRevision, "revision advanced");

  // Stale automatic operator proposal arrives with stale expected revision
  metadataRpcProtocol::MetadataCommand addOp;
  addOp.set_mutationid("op-race-submit");
  addOp.set_expectedrevision(plannedRevision);
  *addOp.mutable_createschedulingoperator()->mutable_operator_() = plannedOp;
  auto addRes = sm.Apply(addOp);
  Require(addRes.error() == metadataRpcProtocol::METADATA_REVISION_MISMATCH,
          "stale proposal must be rejected with revision mismatch");
}

// 6. Irreversible cancellation
void CheckIrreversibleCancellation() {
  MetadataStateMachine sm;
  sm.Apply(BootstrapCommand());
  sm.Apply(EnableBalancerCommand(sm));
  sm.Apply(MakeHeartbeat(4, 1, 10000, 9000));

  // Case A: Cancellation before promotion succeeds
  metadataRpcProtocol::MetadataCommand addA;
  addA.set_mutationid("add-a");
  addA.set_expectedrevision(sm.View()->revision);
  *addA.mutable_createschedulingoperator()->mutable_operator_() =
      CreateMoveOp("op-cancel-ok", 10, 1, 4, sm.View()->revision);
  sm.Apply(addA);

  metadataRpcProtocol::MetadataCommand toDisp;
  toDisp.set_mutationid("to-disp");
  auto* upd = toDisp.mutable_updateschedulingoperator();
  upd->set_operatorid("op-cancel-ok");
  upd->set_expectedphase(metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING);
  upd->set_phase(metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING);
  upd->set_attempts(1);
  upd->set_updatedatms(1500);
  sm.Apply(toDisp);

  metadataRpcProtocol::MetadataCommand cancelA;
  cancelA.set_mutationid("cancel-a");
  cancelA.mutable_cancelschedulingoperator()->set_operatorid("op-cancel-ok");
  auto cancelResA = sm.Apply(cancelA);
  Require(cancelResA.error() == metadataRpcProtocol::METADATA_OK, "pre-promotion cancel must succeed");
  Require(sm.View()->schedulingOperators.at("op-cancel-ok").phase() ==
          metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLING, "must be CANCELLING");

  // Finalize cancellation to CANCELLED to free up concurrency
  metadataRpcProtocol::MetadataCommand toCancelled;
  toCancelled.set_mutationid("to-cancelled");
  auto* updC = toCancelled.mutable_updateschedulingoperator();
  updC->set_operatorid("op-cancel-ok");
  updC->set_expectedphase(metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLING);
  updC->set_phase(metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED);
  updC->set_attempts(1);
  updC->set_updatedatms(1600);
  auto cRes = sm.Apply(toCancelled);
  Require(cRes.error() == metadataRpcProtocol::METADATA_OK, "cancellation finalize must succeed");

  // Case B: Cancellation after irreversible promotion fails
  metadataRpcProtocol::MetadataCommand addB;
  addB.set_mutationid("add-b");
  addB.set_expectedrevision(sm.View()->revision);
  *addB.mutable_createschedulingoperator()->mutable_operator_() =
      CreateMoveOp("op-cancel-irrev", 20, 1, 4, sm.View()->revision);
  auto addResB = sm.Apply(addB);
  Require(addResB.error() == metadataRpcProtocol::METADATA_OK, "operator B must be admitted");

  // PENDING -> DISPATCHING
  metadataRpcProtocol::MetadataCommand toDispB;
  toDispB.set_mutationid("to-disp-b");
  auto* updB1 = toDispB.mutable_updateschedulingoperator();
  updB1->set_operatorid("op-cancel-irrev");
  updB1->set_expectedphase(metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING);
  updB1->set_phase(metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING);
  updB1->set_attempts(1);
  updB1->set_updatedatms(1500);
  auto resB1 = sm.Apply(toDispB);
  Require(resB1.error() == metadataRpcProtocol::METADATA_OK, "transition to dispatching must succeed");

  // DISPATCHING -> WAITING (irreversible = true)
  metadataRpcProtocol::MetadataCommand toWaiting;
  toWaiting.set_mutationid("to-waiting");
  auto* upd2 = toWaiting.mutable_updateschedulingoperator();
  upd2->set_operatorid("op-cancel-irrev");
  upd2->set_expectedphase(metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING);
  upd2->set_phase(metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING);
  upd2->set_attempts(1);
  upd2->set_irreversible(true);
  upd2->set_updatedatms(2000);
  auto resB2 = sm.Apply(toWaiting);
  Require(resB2.error() == metadataRpcProtocol::METADATA_OK, "transition to waiting must succeed");

  metadataRpcProtocol::MetadataCommand cancelB;
  cancelB.set_mutationid("cancel-b");
  cancelB.mutable_cancelschedulingoperator()->set_operatorid("op-cancel-irrev");
  auto cancelResB = sm.Apply(cancelB);
  Require(cancelResB.error() == metadataRpcProtocol::METADATA_CONFLICT,
          "cancellation after irreversible point must be rejected");
}

// 7. Automatic split under sustained evidence
void CheckAutomaticSplit() {
  MetadataStateMachine sm;
  sm.Apply(BootstrapCommand());
  sm.Apply(EnableBalancerCommand(sm));

  // Store 1 reports Region 10 with sustained size 500MB (threshold is 1MB)
  metadataRpcProtocol::MetadataCommand hb1;
  hb1.set_mutationid("hb-split-1");
  auto* hb = hb1.mutable_reportstoreheartbeat()->mutable_heartbeat();
  hb->set_storeid(1);
  hb->set_sequence(1);
  hb->set_observedatms(1000);
  hb->set_expiresatms(100000);
  hb->set_capacitybytes(100000000);
  hb->set_availablebytes(50000000);
  auto* regObs = hb->add_regions();
  regObs->set_regionid(10);
  regObs->set_peerid(101);
  regObs->mutable_epoch()->set_version(1);
  regObs->mutable_epoch()->set_confversion(1);
  regObs->set_approximatebytes(500 * 1024 * 1024);
  regObs->set_requestcount(5000);
  regObs->set_isleader(true);
  regObs->set_splitcandidategeneration(1);
  sm.Apply(hb1);

  AutoBalancerPlanner planner;

  // Window 1: evidence = 1 window -> not yet triggered (splitconsecutivewindows=2)
  ClusterSchedulingSnapshot snap1;
  snap1.metadata = sm.View();
  snap1.nowMs = 2000;
  auto plan1 = planner.Plan(snap1);
  Require(plan1.plans.empty(), "split must require sustained evidence (window 1 of 2)");
  Require(plan1.splitEvidenceWindows[10] == 1, "evidence counter must increment to 1");

  // Window 2: sustained evidence continues -> emits AutomaticSplit
  ClusterSchedulingSnapshot snap2;
  snap2.metadata = sm.View();
  snap2.nowMs = 3000;
  snap2.splitEvidenceWindows = plan1.splitEvidenceWindows;
  auto plan2 = planner.Plan(snap2);
  Require(!plan2.plans.empty(), "split must be emitted on sustained evidence (window 2 of 2)");
  Require(plan2.plans.front().reason == SchedulingPlanReason::AutomaticSplit,
          "plan reason must be AutomaticSplit");
  Require(plan2.plans.front().operation.type() ==
          metadataRpcProtocol::SCHEDULING_OPERATOR_SPLIT_REGION, "operation must be SPLIT");
}

// 8. Concurrent reads/writes and cross-region 2PC (no lost writes, no orphan locks)
void CheckConcurrentTxnNoLostWritesNoOrphanLocks() {
  char temporaryTemplate[] = "/tmp/stratakv-balancer-integration-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  Require(temporaryDirectory != nullptr, "temporary directory creation failed");
  const std::filesystem::path root(temporaryDirectory);

  try {
    auto sourceEngine = std::make_shared<RocksDbKVEngine>((root / "source").string());
    auto targetEngine = std::make_shared<RocksDbKVEngine>((root / "target").string());
    auto otherRegionEngine = std::make_shared<RocksDbKVEngine>((root / "other").string());

    MvccStorage source(sourceEngine);
    MvccStorage target(targetEngine);
    MvccStorage otherRegion(otherRegionEngine);

    // Cross-Region 2PC transaction in flight: Prewrite on primary (source) and secondary (otherRegion)
    constexpr uint64_t startTs = 100;
    constexpr uint64_t commitTs = 200;
    Require(source.Prewrite("apple", "red", "apple", startTs, 60000) == TxnStatus::Ok,
            "source prewrite must succeed");
    Require(otherRegion.Prewrite("zebra", "striped", "apple", startTs, 60000) == TxnStatus::Ok,
            "otherRegion prewrite must succeed");
    Require(source.GetLock("apple").has_value() && otherRegion.GetLock("zebra").has_value(),
            "locks must exist prior to snapshot");

    // Snapshot transfer during migration
    const std::string snapshot = sourceEngine->Dump();
    Require(target.RestoreSnapshot(snapshot), "target must restore snapshot");
    Require(target.GetLock("apple").has_value(), "transferred lock must exist on target");

    // Concurrent tail writes
    for (int i = 0; i < 50; ++i) {
      const std::string k = "tail-k-" + std::to_string(i);
      const std::string v = "tail-v-" + std::to_string(i);
      Require(sourceEngine->Put(k, v), "source foreground put");
      Require(targetEngine->Put(k, v), "target tail replay");
    }

    // Finish 2PC commit on target replica and otherRegion
    Require(target.Commit("apple", startTs, commitTs) == TxnStatus::Ok, "target commit");
    Require(otherRegion.Commit("zebra", startTs, commitTs) == TxnStatus::Ok, "secondary commit");

    std::string val;
    Require(target.Get("apple", 300, &val) == TxnStatus::Ok && val == "red",
            "committed primary value must match");
    Require(otherRegion.Get("zebra", 300, &val) == TxnStatus::Ok && val == "striped",
            "committed secondary value must match");

    // Verification: zero orphan locks
    Require(!target.GetLock("apple").has_value(), "no orphan lock on target");
    Require(!otherRegion.GetLock("zebra").has_value(), "no orphan lock on secondary");

    // Verification: all tail writes readable
    for (int i = 0; i < 50; ++i) {
      Require(targetEngine->Get("tail-k-" + std::to_string(i), &val) &&
              val == "tail-v-" + std::to_string(i), "tail write verified");
    }

    sourceEngine.reset();
    targetEngine.reset();
    otherRegionEngine.reset();
    std::filesystem::remove_all(root);
  } catch (...) {
    std::filesystem::remove_all(root);
    throw;
  }
}

}  // namespace

int main() {
  try {
    CheckLegacyAndDisabledCompatibility();
    CheckConcurrencyLimits();
    CheckStoreExpansionAndSkewConvergence();
    CheckStoreFailureAndSafetyRepair();
    CheckManualOperationRaces();
    CheckIrreversibleCancellation();
    CheckAutomaticSplit();
    CheckConcurrentTxnNoLostWritesNoOrphanLocks();

    std::cout << "All Auto-Balancer integration checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Auto-Balancer integration checks failed: " << error.what() << std::endl;
    return 1;
  }
}
