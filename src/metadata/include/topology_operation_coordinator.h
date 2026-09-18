#ifndef STRATAKV_METADATA_TOPOLOGY_OPERATION_COORDINATOR_H
#define STRATAKV_METADATA_TOPOLOGY_OPERATION_COORDINATOR_H

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "metadata_client.h"
#include "metadata_rpc.pb.h"
#include "region_metadata.h"

enum class CoordinatorStepResult {
  Success,
  Stopped,
  Failed,
  Cancelled,
};

struct CoordinatorMetrics {
  uint64_t movePeerStarted = 0;
  uint64_t movePeerSucceeded = 0;
  uint64_t movePeerFailed = 0;
  uint64_t movePeerCancelled = 0;
  uint64_t splitStarted = 0;
  uint64_t splitSucceeded = 0;
  uint64_t splitFailed = 0;
  uint64_t leaderHandoffs = 0;
  uint64_t stalePlansRejected = 0;
};

class TopologyOperationCoordinator {
 public:
  using StopFn = std::function<bool()>;
  using TargetPreparer =
      std::function<bool(const RegionMetadata& region, uint64_t targetPeerId)>;
  using ConfChangeSender = std::function<bool(
      const RegionMetadata& region, stratakv::region::ConfChangeType type,
      const RegionPeerLocation& target, uint64_t revision)>;
  using StatusPoller = std::function<bool(
      uint64_t regionId, uint64_t peerId,
      std::function<bool(const RegionMetadata&, int lag, bool isLearner)> condition,
      std::chrono::steady_clock::time_point deadline, StopFn shouldStop)>;
  using SourceRetirer =
      std::function<bool(uint64_t regionId, uint64_t peerId, uint64_t revision)>;
  using SplitProposer = std::function<bool(
      const RegionMetadata& parent, const RegionMetadata& child,
      const std::string& splitKey, uint64_t revision)>;
  using SplitStatusPoller = std::function<bool(
      uint64_t regionId, std::chrono::steady_clock::time_point deadline,
      StopFn shouldStop)>;
  using CandidateResolver =
      std::function<std::string(uint64_t regionId, uint64_t generation)>;

  explicit TopologyOperationCoordinator(
      std::shared_ptr<MetadataClient> metadataClient);

  CoordinatorStepResult DriveMovePeer(
      const std::string& operatorId, uint64_t regionId, uint64_t sourceStoreId,
      uint64_t targetStoreId, std::chrono::steady_clock::time_point deadline,
      StopFn shouldStop = nullptr);

  CoordinatorStepResult DriveSplitRegion(
      const std::string& operatorId, uint64_t regionId,
      uint64_t splitCandidateGeneration,
      std::chrono::steady_clock::time_point deadline,
      StopFn shouldStop = nullptr);

  CoordinatorStepResult ReconcileOperator(
      const metadataRpcProtocol::SchedulingOperator& op,
      std::chrono::steady_clock::time_point deadline,
      StopFn shouldStop = nullptr);

  // Test hooks / injection
  void SetTargetPreparer(TargetPreparer fn) { targetPreparer_ = std::move(fn); }
  void SetConfChangeSender(ConfChangeSender fn) {
    confChangeSender_ = std::move(fn);
  }
  void SetStatusPoller(StatusPoller fn) { statusPoller_ = std::move(fn); }
  void SetSourceRetirer(SourceRetirer fn) { sourceRetirer_ = std::move(fn); }
  void SetSplitProposer(SplitProposer fn) { splitProposer_ = std::move(fn); }
  void SetSplitStatusPoller(SplitStatusPoller fn) {
    splitStatusPoller_ = std::move(fn);
  }
  void SetCandidateResolver(CandidateResolver fn) {
    candidateResolver_ = std::move(fn);
  }
  CoordinatorMetrics Metrics() const;

 private:
  bool UpdateOperatorPhase(const std::string& operatorId,
                           metadataRpcProtocol::SchedulingOperatorPhase expected,
                           metadataRpcProtocol::SchedulingOperatorPhase next,
                           uint32_t attempts, const std::string& lastError,
                           bool irreversible,
                           std::chrono::steady_clock::time_point deadline);

  std::shared_ptr<MetadataClient> metadataClient_;
  TargetPreparer targetPreparer_;
  ConfChangeSender confChangeSender_;
  StatusPoller statusPoller_;
  SourceRetirer sourceRetirer_;
  SplitProposer splitProposer_;
  SplitStatusPoller splitStatusPoller_;
  CandidateResolver candidateResolver_;

  mutable std::mutex metricsMutex_;
  CoordinatorMetrics metrics_;
};

#endif  // STRATAKV_METADATA_TOPOLOGY_OPERATION_COORDINATOR_H

