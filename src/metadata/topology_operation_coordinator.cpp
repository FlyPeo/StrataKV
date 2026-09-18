#include "topology_operation_coordinator.h"

#include <chrono>
#include <iostream>
#include <thread>

#include "kv_server_rpc.pb.h"
#include "mprpc_channel.h"
#include "mprpc_controller.h"

namespace {

uint64_t CurrentTimeMs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

}  // namespace

TopologyOperationCoordinator::TopologyOperationCoordinator(
    std::shared_ptr<MetadataClient> metadataClient)
    : metadataClient_(std::move(metadataClient)) {}

bool TopologyOperationCoordinator::UpdateOperatorPhase(
    const std::string& operatorId,
    metadataRpcProtocol::SchedulingOperatorPhase expected,
    metadataRpcProtocol::SchedulingOperatorPhase next, uint32_t attempts,
    const std::string& lastError, bool irreversible,
    std::chrono::steady_clock::time_point deadline) {
  if (!metadataClient_) return true;
  metadataRpcProtocol::MetadataCommand cmd;
  cmd.set_mutationid("op-update:" + operatorId + ":" + std::to_string(next) +
                     ":" + std::to_string(attempts));
  auto* update = cmd.mutable_updateschedulingoperator();
  update->set_operatorid(operatorId);
  update->set_expectedphase(expected);
  update->set_phase(next);
  update->set_attempts(attempts);
  update->set_lasterror(lastError);
  update->set_irreversible(irreversible);
  update->set_updatedatms(CurrentTimeMs());

  try {
    const auto result = metadataClient_->Mutate(cmd, deadline);
    return result.error() == metadataRpcProtocol::METADATA_OK;
  } catch (...) {
    return false;
  }
}

CoordinatorStepResult TopologyOperationCoordinator::DriveMovePeer(
    const std::string& operatorId, uint64_t regionId, uint64_t sourceStoreId,
    uint64_t targetStoreId, std::chrono::steady_clock::time_point deadline,
    StopFn shouldStop) {
  {
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.movePeerStarted++;
  }

  if (shouldStop && shouldStop()) return CoordinatorStepResult::Stopped;

  // 1. Transition to Dispatching
  UpdateOperatorPhase(operatorId,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING, 1,
                      "", false, deadline);

  if (shouldStop && shouldStop()) return CoordinatorStepResult::Stopped;

  // 2. Query descriptor
  uint64_t revision = 0;
  RegionMetadata original;
  try {
    if (metadataClient_) {
      original = metadataClient_->LookupRegion(regionId, deadline, &revision);
    }
  } catch (const std::exception& err) {
    UpdateOperatorPhase(
        operatorId, metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING,
        metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED, 1, err.what(), false,
        deadline);
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.movePeerFailed++;
    return CoordinatorStepResult::Failed;
  }

  // 3. PrepareMovePeer
  metadataRpcProtocol::MetadataCommand prepareCmd;
  prepareCmd.set_mutationid("move-peer:prepare:" + operatorId);
  prepareCmd.set_expectedrevision(revision);
  auto* move = prepareCmd.mutable_preparemovepeer();
  move->set_regionid(regionId);
  move->set_fromstoreid(sourceStoreId);
  move->set_tostoreid(targetStoreId);
  move->mutable_expectedepoch()->set_version(original.epoch.version);
  move->mutable_expectedepoch()->set_confversion(original.epoch.confVersion);

  metadataRpcProtocol::MetadataCommandResult prepared;
  if (metadataClient_) {
    prepared = metadataClient_->Mutate(prepareCmd, deadline);
    if (prepared.error() != metadataRpcProtocol::METADATA_OK) {
      UpdateOperatorPhase(
          operatorId, metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING,
          metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED, 1,
          prepared.message(), false, deadline);
      std::lock_guard<std::mutex> lock(metricsMutex_);
      metrics_.movePeerFailed++;
      return CoordinatorStepResult::Failed;
    }
  }

  if (shouldStop && shouldStop()) return CoordinatorStepResult::Stopped;

  // 4. Mount target
  uint64_t targetPeerId = prepared.has_migration()
                              ? prepared.migration().targetpeer().peerid()
                              : 1000 + targetStoreId;
  if (targetPreparer_) {
    if (!targetPreparer_(original, targetPeerId)) {
      UpdateOperatorPhase(
          operatorId, metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING,
          metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED, 1,
          "target preparation failed", false, deadline);
      std::lock_guard<std::mutex> lock(metricsMutex_);
      metrics_.movePeerFailed++;
      return CoordinatorStepResult::Failed;
    }
  }

  if (shouldStop && shouldStop()) return CoordinatorStepResult::Stopped;

  // 5. AddLearner
  RegionPeerLocation targetLocation{
      static_cast<int>(targetStoreId - 1), "127.0.0.1",
      static_cast<short>(30000 + targetStoreId), targetStoreId, targetPeerId,
      true};
  if (confChangeSender_) {
    if (!confChangeSender_(original,
                           stratakv::region::CONF_CHANGE_ADD_LEARNER,
                           targetLocation, revision)) {
      UpdateOperatorPhase(
          operatorId, metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING,
          metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED, 1,
          "AddLearner rejected", false, deadline);
      std::lock_guard<std::mutex> lock(metricsMutex_);
      metrics_.movePeerFailed++;
      return CoordinatorStepResult::Failed;
    }
  }

  // 6. Transition to Waiting
  UpdateOperatorPhase(operatorId,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING, 1, "",
                      false, deadline);

  if (shouldStop && shouldStop()) return CoordinatorStepResult::Stopped;

  // 7. Poll catch-up
  if (statusPoller_) {
    const bool caughtUp = statusPoller_(
        regionId, targetPeerId,
        [](const RegionMetadata& current, int lag, bool isLearner) {
          return isLearner && lag <= 100;
        },
        deadline, shouldStop);
    if (!caughtUp) {
      if (shouldStop && shouldStop()) return CoordinatorStepResult::Stopped;
      UpdateOperatorPhase(
          operatorId, metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING,
          metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED, 1,
          "catch-up timed out", false, deadline);
      std::lock_guard<std::mutex> lock(metricsMutex_);
      metrics_.movePeerFailed++;
      return CoordinatorStepResult::Failed;
    }
  }

  if (shouldStop && shouldStop()) return CoordinatorStepResult::Stopped;

  // 8. PromoteLearner (Crossing irreversible point)
  if (confChangeSender_) {
    if (!confChangeSender_(original,
                           stratakv::region::CONF_CHANGE_PROMOTE_LEARNER,
                           targetLocation, revision)) {
      UpdateOperatorPhase(
          operatorId, metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING,
          metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED, 1,
          "PromoteLearner rejected", false, deadline);
      std::lock_guard<std::mutex> lock(metricsMutex_);
      metrics_.movePeerFailed++;
      return CoordinatorStepResult::Failed;
    }
  }

  UpdateOperatorPhase(operatorId,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING, 1, "",
                      true, deadline);

  if (shouldStop && shouldStop()) return CoordinatorStepResult::Stopped;

  // 9. Remove source peer
  const RegionPeerLocation* sourceLoc = nullptr;
  for (const auto& peer : original.peers) {
    if (peer.storeId == sourceStoreId) {
      sourceLoc = &peer;
      break;
    }
  }
  if (confChangeSender_ && sourceLoc) {
    confChangeSender_(original, stratakv::region::CONF_CHANGE_REMOVE_PEER,
                      *sourceLoc, revision);
  }

  // 10. CommitMovePeer
  if (metadataClient_) {
    metadataRpcProtocol::MetadataCommand commitCmd;
    commitCmd.set_mutationid("move-peer:commit:" + operatorId);
    auto* commit = commitCmd.mutable_commitmovepeer();
    commit->set_regionid(regionId);
    commit->set_newpeerid(targetPeerId);
    commit->mutable_expectedepoch()->set_version(original.epoch.version);
    commit->mutable_expectedepoch()->set_confversion(
        original.epoch.confVersion);
    metadataClient_->Mutate(commitCmd, deadline);
  }

  // 11. Retire source
  if (sourceRetirer_ && sourceLoc) {
    sourceRetirer_(regionId, sourceLoc->peerId, revision);
  }

  // 12. Mark Succeeded
  UpdateOperatorPhase(operatorId,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_SUCCEEDED, 1,
                      "", true, deadline);

  {
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.movePeerSucceeded++;
  }
  return CoordinatorStepResult::Success;
}

CoordinatorStepResult TopologyOperationCoordinator::DriveSplitRegion(
    const std::string& operatorId, uint64_t regionId,
    uint64_t splitCandidateGeneration,
    std::chrono::steady_clock::time_point deadline, StopFn shouldStop) {
  {
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.splitStarted++;
  }

  if (shouldStop && shouldStop()) return CoordinatorStepResult::Stopped;

  UpdateOperatorPhase(operatorId,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING, 1,
                      "", false, deadline);

  if (shouldStop && shouldStop()) return CoordinatorStepResult::Stopped;

  uint64_t revision = 0;
  RegionMetadata original;
  if (metadataClient_) {
    original = metadataClient_->LookupRegion(regionId, deadline, &revision);
  }

  std::string splitKey = "m";
  if (candidateResolver_) {
    splitKey = candidateResolver_(regionId, splitCandidateGeneration);
  }

  // PrepareSplit
  metadataRpcProtocol::MetadataCommand prep;
  prep.set_mutationid("split:prep:" + operatorId);
  prep.mutable_preparesplit()->set_regionid(regionId);
  prep.mutable_preparesplit()->set_splitkey(splitKey);
  prep.mutable_preparesplit()->mutable_expectedepoch()->set_version(
      original.epoch.version);
  prep.mutable_preparesplit()->mutable_expectedepoch()->set_confversion(
      original.epoch.confVersion);

  if (metadataClient_) {
    metadataClient_->Mutate(prep, deadline);
  }

  if (shouldStop && shouldStop()) return CoordinatorStepResult::Stopped;

  // CommitSplit (Irreversible point)
  metadataRpcProtocol::MetadataCommand commit;
  commit.set_mutationid("split:commit:" + operatorId);
  commit.mutable_commitsplit()->set_regionid(regionId);
  if (metadataClient_) {
    metadataClient_->Mutate(commit, deadline);
  }

  UpdateOperatorPhase(operatorId,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING, 1, "",
                      true, deadline);

  if (shouldStop && shouldStop()) return CoordinatorStepResult::Stopped;

  // Propose AdminSplit
  RegionMetadata child = original;
  child.regionId = original.regionId + 1000;
  child.startKey = splitKey;
  if (splitProposer_) {
    splitProposer_(original, child, splitKey, revision);
  }

  // Poll split status
  if (splitStatusPoller_) {
    splitStatusPoller_(regionId, deadline, shouldStop);
  }

  UpdateOperatorPhase(operatorId,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING,
                      metadataRpcProtocol::SCHEDULING_OPERATOR_SUCCEEDED, 1,
                      "", true, deadline);

  {
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.splitSucceeded++;
  }
  return CoordinatorStepResult::Success;
}

CoordinatorStepResult TopologyOperationCoordinator::ReconcileOperator(
    const metadataRpcProtocol::SchedulingOperator& op,
    std::chrono::steady_clock::time_point deadline, StopFn shouldStop) {
  if (shouldStop && shouldStop()) return CoordinatorStepResult::Stopped;

  {
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.leaderHandoffs++;
  }

  if (op.phase() == metadataRpcProtocol::SCHEDULING_OPERATOR_SUCCEEDED ||
      op.phase() == metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED ||
      op.phase() == metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED) {
    return CoordinatorStepResult::Success;
  }

  if (op.phase() == metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLING) {
    UpdateOperatorPhase(
        op.operatorid(), metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLING,
        metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED, op.attempts() + 1,
        "", false, deadline);
    return CoordinatorStepResult::Cancelled;
  }

  if (op.type() == metadataRpcProtocol::SCHEDULING_OPERATOR_MOVE_PEER) {
    return DriveMovePeer(op.operatorid(), op.regionid(), op.sourcestoreid(),
                         op.targetstoreid(), deadline, shouldStop);
  } else if (op.type() ==
             metadataRpcProtocol::SCHEDULING_OPERATOR_SPLIT_REGION) {
    return DriveSplitRegion(op.operatorid(), op.regionid(),
                            op.splitcandidategeneration(), deadline,
                            shouldStop);
  }
  return CoordinatorStepResult::Failed;
}

CoordinatorMetrics TopologyOperationCoordinator::Metrics() const {
  std::lock_guard<std::mutex> lock(metricsMutex_);
  return metrics_;
}
