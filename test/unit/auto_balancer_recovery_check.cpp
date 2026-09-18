#include <chrono>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "auto_balancer.h"
#include "metadata_state_machine.h"
#include "topology_operation_coordinator.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

metadataRpcProtocol::MetadataCommand BootstrapCommand() {
  metadataRpcProtocol::MetadataCommand command;
  command.set_mutationid("recovery-bootstrap");
  command.mutable_bootstrap()->set_configdigest("recovery-digest");
  auto* region = command.mutable_bootstrap()->add_regions();
  region->set_regionid(10);
  region->set_startkey("");
  region->set_endkey("");
  region->mutable_epoch()->set_version(1);
  region->mutable_epoch()->set_confversion(1);
  region->set_leaderpeerid(101);
  for (uint64_t storeId = 1; storeId <= 3; ++storeId) {
    auto* peer = region->add_peers();
    peer->set_peerid(100 + storeId);
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

metadataRpcProtocol::SchedulingOperator CreateMoveOperator(
    const std::string& opId, metadataRpcProtocol::SchedulingOperatorPhase phase,
    bool irreversible = false, uint64_t sourceRevision = 1,
    uint64_t epochVersion = 1, uint64_t confVersion = 1) {
  metadataRpcProtocol::SchedulingOperator op;
  op.set_operatorid(opId);
  op.set_type(metadataRpcProtocol::SCHEDULING_OPERATOR_MOVE_PEER);
  op.set_regionid(10);
  op.set_sourcerevision(sourceRevision);
  op.mutable_regionepoch()->set_version(epochVersion);
  op.mutable_regionepoch()->set_confversion(confVersion);
  op.set_sourcestoreid(1);
  op.set_targetstoreid(4);
  op.set_phase(phase);
  op.set_attempts(1);
  op.set_deadlinems(99999999);
  op.set_createdatms(1000);
  op.set_updatedatms(1000);
  op.set_irreversible(irreversible);
  return op;
}

void EnableBalancer(MetadataStateMachine& stateMachine) {
  metadataRpcProtocol::AutoBalancerConfig config = stateMachine.View()->balancerConfig;
  config.set_enabled(true);
  config.set_paused(false);
  config.set_version(config.version() + 1);
  config.set_evaluationintervalms(1000);
  config.set_heartbeattimeoutms(5000);
  config.set_minimumfreebytes(100);
  config.set_maximumdiskusedratio(0.95);
  config.set_replicationfactor(3);
  config.set_imbalancethreshold(0.10);
  config.set_splitsizebytes(1024 * 1024);
  config.set_splitrequestthreshold(100);
  config.set_splitconsecutivewindows(2);
  config.set_regioncooldownms(10000);
  config.set_maximumactiveoperators(2);
  config.set_maximumactiveperstore(1);

  metadataRpcProtocol::MetadataCommand cmd;
  cmd.set_mutationid("enable-balancer");
  *cmd.mutable_updateautobalancerconfig()->mutable_config() = config;
  auto res = stateMachine.Apply(cmd);
  Require(res.error() == metadataRpcProtocol::METADATA_OK, "enable balancer should succeed");
}

// 1. Confirmation that a former leader dispatches no new work
void CheckFormerLeaderDispatchesNoWork() {
  TopologyOperationCoordinator coordinator(nullptr);
  uint32_t preparerCalls = 0;
  uint32_t confChangeCalls = 0;

  coordinator.SetTargetPreparer([&](const RegionMetadata&, uint64_t) {
    preparerCalls++;
    return true;
  });
  coordinator.SetConfChangeSender([&](const RegionMetadata&,
                                      stratakv::region::ConfChangeType,
                                      const RegionPeerLocation&, uint64_t) {
    confChangeCalls++;
    return true;
  });

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  // shouldStop returns true, simulating a former leader losing leadership
  const auto result = coordinator.DriveMovePeer("op-stopped", 10, 1, 4, deadline,
                                                []() { return true; });

  Require(result == CoordinatorStepResult::Stopped,
          "former leader must observe stopped state");
  Require(preparerCalls == 0, "former leader must not dispatch target preparation");
  Require(confChangeCalls == 0, "former leader must not dispatch conf change");
}

// 2. Leader handoff during Pending phase
void CheckLeaderHandoffPendingPhase() {
  MetadataStateMachine stateMachine;
  stateMachine.Apply(BootstrapCommand());
  EnableBalancer(stateMachine);

  // Store 4 must have fresh heartbeat for admission
  metadataRpcProtocol::MetadataCommand hb4;
  hb4.set_mutationid("hb-4");
  auto* hb = hb4.mutable_reportstoreheartbeat()->mutable_heartbeat();
  hb->set_storeid(4);
  hb->set_sequence(1);
  hb->set_observedatms(1000);
  hb->set_expiresatms(100000);
  hb->set_capacitybytes(10000);
  hb->set_availablebytes(8000);
  stateMachine.Apply(hb4);

  metadataRpcProtocol::MetadataCommand addOp;
  addOp.set_mutationid("add-op-pending");
  addOp.set_expectedrevision(stateMachine.View()->revision);
  *addOp.mutable_createschedulingoperator()->mutable_operator_() =
      CreateMoveOperator("op-pending", metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING, false, stateMachine.View()->revision);

  auto res = stateMachine.Apply(addOp);
  if (res.error() != metadataRpcProtocol::METADATA_OK) {
    std::cerr << "Admission failed: error=" << res.error() << " msg=" << res.message() << std::endl;
  }
  Require(res.error() == metadataRpcProtocol::METADATA_OK, "operator should be admitted");

  // Leader A was deposed before dispatch. Leader B steps up and reconciles:
  TopologyOperationCoordinator coordinator(nullptr);
  uint32_t stepsExecuted = 0;
  coordinator.SetTargetPreparer([&](const RegionMetadata&, uint64_t) {
    stepsExecuted++;
    return true;
  });
  coordinator.SetConfChangeSender([&](const RegionMetadata&,
                                      stratakv::region::ConfChangeType,
                                      const RegionPeerLocation&, uint64_t) {
    stepsExecuted++;
    return true;
  });
  coordinator.SetStatusPoller([&](uint64_t, uint64_t, auto, auto, auto) {
    stepsExecuted++;
    return true;
  });
  coordinator.SetSourceRetirer([&](uint64_t, uint64_t, uint64_t) {
    stepsExecuted++;
    return true;
  });

  const auto op = stateMachine.View()->schedulingOperators.at("op-pending");
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  const auto outcome = coordinator.ReconcileOperator(op, deadline, []() { return false; });

  Require(outcome == CoordinatorStepResult::Success, "new leader must succeed forward");
  Require(stepsExecuted >= 4, "new leader must execute all remaining steps");
}

// 3. Leader handoff during Waiting phase (before and after irreversible promotion)
void CheckLeaderHandoffWaitingPhase() {
  // Case A: Before promotion (irreversible == false)
  {
    TopologyOperationCoordinator coordinator(nullptr);
    bool promoted = false;
    coordinator.SetConfChangeSender([&](const RegionMetadata&,
                                        stratakv::region::ConfChangeType type,
                                        const RegionPeerLocation&, uint64_t) {
      if (type == stratakv::region::CONF_CHANGE_PROMOTE_LEARNER) {
        promoted = true;
      }
      return true;
    });
    coordinator.SetStatusPoller([&](uint64_t, uint64_t, auto, auto, auto) {
      return true;
    });

    const auto op = CreateMoveOperator("op-waiting",
                                       metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING,
                                       /*irreversible=*/false);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    const auto result = coordinator.ReconcileOperator(op, deadline, []() { return false; });
    Require(result == CoordinatorStepResult::Success, "new leader must complete promotion");
    Require(promoted, "promote learner must be executed during handoff");
  }

  // Case B: Irreversible point enforcement in state machine
  {
    MetadataStateMachine stateMachine;
    stateMachine.Apply(BootstrapCommand());
    EnableBalancer(stateMachine);

    // Fresh heartbeat for store 4
    metadataRpcProtocol::MetadataCommand hb4;
    hb4.set_mutationid("hb-4");
    auto* hb = hb4.mutable_reportstoreheartbeat()->mutable_heartbeat();
    hb->set_storeid(4);
    hb->set_sequence(1);
    hb->set_observedatms(1000);
    hb->set_expiresatms(100000);
    hb->set_capacitybytes(10000);
    hb->set_availablebytes(8000);
    stateMachine.Apply(hb4);

    metadataRpcProtocol::MetadataCommand addOp;
    addOp.set_mutationid("add-op-irrev");
    addOp.set_expectedrevision(stateMachine.View()->revision);
    *addOp.mutable_createschedulingoperator()->mutable_operator_() =
        CreateMoveOperator("op-irrev", metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING, false, stateMachine.View()->revision);
    auto addRes = stateMachine.Apply(addOp);
    Require(addRes.error() == metadataRpcProtocol::METADATA_OK, "operator should be admitted");

    // Transition PENDING -> DISPATCHING
    metadataRpcProtocol::MetadataCommand toDispatching;
    toDispatching.set_mutationid("to-dispatching");
    auto* upd1 = toDispatching.mutable_updateschedulingoperator();
    upd1->set_operatorid("op-irrev");
    upd1->set_expectedphase(metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING);
    upd1->set_phase(metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING);
    upd1->set_attempts(1);
    upd1->set_updatedatms(1500);
    auto dRes = stateMachine.Apply(toDispatching);
    Require(dRes.error() == metadataRpcProtocol::METADATA_OK, "transition to dispatching should succeed");

    // Transition to WAITING and mark irreversible
    metadataRpcProtocol::MetadataCommand markIrrev;
    markIrrev.set_mutationid("mark-irrev");
    auto* update = markIrrev.mutable_updateschedulingoperator();
    update->set_operatorid("op-irrev");
    update->set_expectedphase(metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING);
    update->set_phase(metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING);
    update->set_attempts(1);
    update->set_irreversible(true);
    update->set_updatedatms(2000);
    auto mRes = stateMachine.Apply(markIrrev);
    Require(mRes.error() == metadataRpcProtocol::METADATA_OK, "transition to waiting should succeed");

    // Attempt cancellation after irreversible point -> MUST fail
    metadataRpcProtocol::MetadataCommand cancelCmd;
    cancelCmd.set_mutationid("cancel-irrev");
    cancelCmd.mutable_cancelschedulingoperator()->set_operatorid("op-irrev");
    auto cancelRes = stateMachine.Apply(cancelCmd);
    Require(cancelRes.error() == metadataRpcProtocol::METADATA_CONFLICT,
            "cancellation after irreversible point must be rejected");
  }
}

// 4. Safe cancellation before promotion
void CheckCancellationBeforePromotion() {
  MetadataStateMachine stateMachine;
  stateMachine.Apply(BootstrapCommand());
  EnableBalancer(stateMachine);

  metadataRpcProtocol::MetadataCommand hb4;
  hb4.set_mutationid("hb-4");
  auto* hb = hb4.mutable_reportstoreheartbeat()->mutable_heartbeat();
  hb->set_storeid(4);
  hb->set_sequence(1);
  hb->set_observedatms(1000);
  hb->set_expiresatms(100000);
  hb->set_capacitybytes(10000);
  hb->set_availablebytes(8000);
  stateMachine.Apply(hb4);

  metadataRpcProtocol::MetadataCommand addOp;
  addOp.set_mutationid("add-op-cancel");
  addOp.set_expectedrevision(stateMachine.View()->revision);
  *addOp.mutable_createschedulingoperator()->mutable_operator_() =
      CreateMoveOperator("op-cancel", metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING, false, stateMachine.View()->revision);
  auto addRes = stateMachine.Apply(addOp);
  Require(addRes.error() == metadataRpcProtocol::METADATA_OK, "operator should be admitted");

  // Transition PENDING -> DISPATCHING so cancel requests enters CANCELLING
  metadataRpcProtocol::MetadataCommand toDispatching;
  toDispatching.set_mutationid("to-dispatching-cancel");
  auto* upd = toDispatching.mutable_updateschedulingoperator();
  upd->set_operatorid("op-cancel");
  upd->set_expectedphase(metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING);
  upd->set_phase(metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING);
  upd->set_attempts(1);
  upd->set_updatedatms(1500);
  auto dRes = stateMachine.Apply(toDispatching);
  Require(dRes.error() == metadataRpcProtocol::METADATA_OK, "transition to dispatching should succeed");

  // Cancel before irreversible point -> succeeds
  metadataRpcProtocol::MetadataCommand cancelCmd;
  cancelCmd.set_mutationid("cancel-ok");
  cancelCmd.mutable_cancelschedulingoperator()->set_operatorid("op-cancel");
  auto cancelRes = stateMachine.Apply(cancelCmd);
  Require(cancelRes.error() == metadataRpcProtocol::METADATA_OK,
          "cancellation before irreversible point must succeed");

  const auto op = stateMachine.View()->schedulingOperators.at("op-cancel");
  Require(op.phase() == metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLING,
          "operator should be in Cancelling phase");

  // Leader reconciles cancelling operator
  TopologyOperationCoordinator coordinator(nullptr);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  const auto result = coordinator.ReconcileOperator(op, deadline, []() { return false; });
  Require(result == CoordinatorStepResult::Cancelled,
          "coordinator should finalize to Cancelled");
}

// 5. AutoBalancer lifecycle and leadership transitions
void CheckAutoBalancerLifecycle() {
  std::atomic<bool> isLeader{false};
  auto view = std::make_shared<MetadataView>();
  view->revision = 1;
  view->balancerConfig.set_enabled(true);
  view->balancerConfig.set_paused(false);

  uint32_t submittedCommands = 0;
  AutoBalancer balancer(
      nullptr, [&]() { return isLeader.load(); }, [&]() { return view; },
      [&](const metadataRpcProtocol::MetadataCommand&, auto) {
        submittedCommands++;
        metadataRpcProtocol::MetadataCommandResult res;
        res.set_error(metadataRpcProtocol::METADATA_OK);
        return res;
      });

  // Non-leader evaluation must be skipped
  balancer.EvaluateOnce();
  Require(balancer.Metrics().evaluationsSkippedNonLeader == 1,
          "non-leader evaluation must be skipped");
  Require(submittedCommands == 0, "non-leader must submit no commands");

  // Leader evaluation runs
  isLeader.store(true);
  balancer.EvaluateOnce();
  Require(balancer.Metrics().evaluationsRun == 2, "evaluation should run");

  // Test start and stop
  balancer.Start();
  Require(balancer.IsRunning(), "balancer should be running");
  balancer.Stop();
  Require(!balancer.IsRunning(), "balancer should be stopped");
}

}  // namespace

int main() {
  try {
    CheckFormerLeaderDispatchesNoWork();
    CheckLeaderHandoffPendingPhase();
    CheckLeaderHandoffWaitingPhase();
    CheckCancellationBeforePromotion();
    CheckAutoBalancerLifecycle();
    std::cout << "Auto-Balancer recovery checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Auto-Balancer recovery checks failed: " << error.what() << std::endl;
    return 1;
  }
}
