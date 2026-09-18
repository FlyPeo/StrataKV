#ifndef STRATAKV_METADATA_METADATA_STATE_MACHINE_H
#define STRATAKV_METADATA_METADATA_STATE_MACHINE_H

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "metadata_rpc.pb.h"
#include "region_metadata.h"

struct MetadataView {
  uint64_t revision = 0;
  std::shared_ptr<const RegionCatalog> catalog;
  std::unordered_map<uint64_t, stratakv::region::StoreDescriptor> stores;
  uint64_t regionIdHighWater = 0;
  uint64_t peerIdHighWater = 0;
  uint64_t storeIdHighWater = 0;
  std::string bootstrapDigest;
  std::unordered_map<uint64_t, stratakv::region::ReplicaMigrationStatus> migrations;
  std::unordered_map<uint64_t, metadataRpcProtocol::StoreHeartbeat> storeHeartbeats;
  metadataRpcProtocol::AutoBalancerConfig balancerConfig;
  std::unordered_map<std::string, metadataRpcProtocol::SchedulingOperator>
      schedulingOperators;
  std::unordered_map<uint64_t, std::string> activeOperatorByRegion;
};

struct MetadataStateMetrics {
  uint64_t validationRejects = 0;
  uint64_t dedupHits = 0;
  uint64_t snapshotCount = 0;
  uint64_t heartbeatAccepted = 0;
  uint64_t heartbeatIgnored = 0;
  uint64_t operatorAdmissions = 0;
};

class MetadataStateMachine {
 public:
  MetadataStateMachine();

  metadataRpcProtocol::MetadataCommandResult Apply(
      const metadataRpcProtocol::MetadataCommand& command);
  std::shared_ptr<const MetadataView> View() const;
  std::optional<RegionMetadata> LookupKey(const std::string& key) const;
  std::optional<RegionMetadata> LookupRegion(uint64_t regionId) const;
  std::optional<stratakv::region::ReplicaMigrationStatus> LookupMigration(
      uint64_t regionId) const;
  std::optional<metadataRpcProtocol::SchedulingOperator> LookupSchedulingOperator(
      const std::string& operatorId) const;
  std::vector<RegionMetadata> Scan(const std::string& startKey, size_t limit) const;

  std::string Snapshot();
  bool Restore(const std::string& bytes, std::string* error);
  MetadataStateMetrics Metrics() const;

 private:
  static constexpr uint32_t kSnapshotFormatVersion = 1;
  static constexpr size_t kDedupRetention = 4096;

  metadataRpcProtocol::MetadataCommandResult ApplyLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult BootstrapLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult ReplaceRegionsLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult AllocateIdsLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult PutStoreLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult PrepareSplitLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult CommitSplitLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult AckSplitLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult PrepareMovePeerLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult CommitMovePeerLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult CancelMovePeerLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult ReportStoreHeartbeatLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult UpdateAutoBalancerConfigLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult CreateSchedulingOperatorLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult UpdateSchedulingOperatorLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult CancelSchedulingOperatorLocked(
      const metadataRpcProtocol::MetadataCommand& command);
  metadataRpcProtocol::MetadataCommandResult Error(
      metadataRpcProtocol::MetadataErrorCode code, const std::string& message) const;
  void RecordResultLocked(const std::string& mutationId,
                          const metadataRpcProtocol::MetadataCommandResult& result);
  void PublishLocked(std::vector<RegionMetadata> regions);
  void RecomputeHighWaterLocked();

  mutable std::mutex mutex_;
  std::shared_ptr<const MetadataView> published_;
  std::vector<RegionMetadata> regions_;
  std::unordered_map<uint64_t, stratakv::region::StoreDescriptor> stores_;
  uint64_t revision_ = 0;
  uint64_t regionIdHighWater_ = 0;
  uint64_t peerIdHighWater_ = 0;
  uint64_t storeIdHighWater_ = 0;
  std::string bootstrapDigest_;
  std::unordered_map<uint64_t, stratakv::region::ReplicaMigrationStatus> migrations_;
  std::unordered_map<uint64_t, metadataRpcProtocol::StoreHeartbeat> storeHeartbeats_;
  metadataRpcProtocol::AutoBalancerConfig balancerConfig_;
  std::unordered_map<std::string, metadataRpcProtocol::SchedulingOperator>
      schedulingOperators_;
  std::unordered_map<uint64_t, std::string> activeOperatorByRegion_;
  std::unordered_map<std::string, metadataRpcProtocol::MutationRecord> dedup_;
  std::deque<std::string> dedupOrder_;
  std::atomic<uint64_t> validationRejects_{0};
  std::atomic<uint64_t> dedupHits_{0};
  std::atomic<uint64_t> snapshotCount_{0};
  std::atomic<uint64_t> heartbeatAccepted_{0};
  std::atomic<uint64_t> heartbeatIgnored_{0};
  std::atomic<uint64_t> operatorAdmissions_{0};
};

#endif  // STRATAKV_METADATA_METADATA_STATE_MACHINE_H
