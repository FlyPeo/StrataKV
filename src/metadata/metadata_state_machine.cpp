#include "metadata_state_machine.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <unordered_set>

namespace {

using metadataRpcProtocol::MetadataCommandResult;
using metadataRpcProtocol::MetadataErrorCode;

bool SamePeers(const RegionMetadata& lhs, const RegionMetadata& rhs) {
  if (lhs.peers.size() != rhs.peers.size()) return false;
  for (size_t index = 0; index < lhs.peers.size(); ++index) {
    const auto& left = lhs.peers[index];
    const auto& right = rhs.peers[index];
    if (left.peerId != right.peerId || left.storeId != right.storeId ||
        left.host != right.host || left.port != right.port) {
      return false;
    }
  }
  return true;
}

bool DescriptorIdsFit(const stratakv::region::RegionDescriptor& descriptor) {
  return descriptor.regionid() > 0 &&
         descriptor.regionid() <= static_cast<uint64_t>(std::numeric_limits<int>::max());
}

std::vector<RegionMetadata> DecodeRegions(
    const google::protobuf::RepeatedPtrField<stratakv::region::RegionDescriptor>& descriptors) {
  std::vector<RegionMetadata> result;
  result.reserve(static_cast<size_t>(descriptors.size()));
  for (const auto& descriptor : descriptors) {
    if (!DescriptorIdsFit(descriptor)) throw std::invalid_argument("invalid Region ID");
    result.push_back(FromProtoRegion(descriptor));
  }
  return result;
}

std::pair<std::string, std::string> CoveredInterval(std::vector<RegionMetadata> regions) {
  if (regions.empty()) throw std::invalid_argument("replacement set must not be empty");
  std::sort(regions.begin(), regions.end(), [](const RegionMetadata& lhs,
                                               const RegionMetadata& rhs) {
    return RegionBytewiseLess(lhs.startKey, rhs.startKey);
  });
  for (size_t index = 1; index < regions.size(); ++index) {
    if (regions[index - 1].endKey != regions[index].startKey) {
      throw std::invalid_argument("replacement Regions must be contiguous");
    }
  }
  return {regions.front().startKey, regions.back().endKey};
}

uint64_t SafeAdd(uint64_t value, uint64_t count) {
  if (count == 0 || value > std::numeric_limits<uint64_t>::max() - count) {
    throw std::invalid_argument("identifier allocation overflows");
  }
  return value + count;
}

metadataRpcProtocol::AutoBalancerConfig DefaultAutoBalancerConfig() {
  metadataRpcProtocol::AutoBalancerConfig config;
  config.set_enabled(false);
  config.set_paused(false);
  config.set_version(1);
  config.set_evaluationintervalms(5000);
  config.set_heartbeattimeoutms(15000);
  config.set_minimumfreebytes(64ULL * 1024 * 1024);
  config.set_maximumdiskusedratio(0.90);
  config.set_replicationfactor(3);
  config.set_imbalancethreshold(0.15);
  config.set_splitsizebytes(512ULL * 1024 * 1024);
  config.set_splitrequestthreshold(10000);
  config.set_splitconsecutivewindows(3);
  config.set_regioncooldownms(60000);
  config.set_maximumactiveoperators(1);
  config.set_maximumactiveperstore(1);
  return config;
}

bool IsTerminal(metadataRpcProtocol::SchedulingOperatorPhase phase) {
  return phase == metadataRpcProtocol::SCHEDULING_OPERATOR_SUCCEEDED ||
         phase == metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED ||
         phase == metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED;
}

bool EpochOlder(const stratakv::region::RegionEpoch& lhs,
                const stratakv::region::RegionEpoch& rhs) {
  return lhs.version() < rhs.version() ||
         (lhs.version() == rhs.version() && lhs.confversion() < rhs.confversion());
}

bool ValidTransition(metadataRpcProtocol::SchedulingOperatorPhase from,
                     metadataRpcProtocol::SchedulingOperatorPhase to) {
  if (from == to) return true;
  switch (from) {
    case metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING:
      return to == metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING ||
             to == metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLING ||
             to == metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED ||
             to == metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED;
    case metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING:
      return to == metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING ||
             to == metadataRpcProtocol::SCHEDULING_OPERATOR_SUCCEEDED ||
             to == metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLING ||
             to == metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED;
    case metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING:
      return to == metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING ||
             to == metadataRpcProtocol::SCHEDULING_OPERATOR_SUCCEEDED ||
             to == metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLING ||
             to == metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED;
    case metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLING:
      return to == metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED ||
             to == metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED;
    case metadataRpcProtocol::SCHEDULING_OPERATOR_SUCCEEDED:
    case metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED:
    case metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED: return false;
  }
  return false;
}

}  // namespace

MetadataStateMachine::MetadataStateMachine() {
  balancerConfig_ = DefaultAutoBalancerConfig();
  auto initial = std::make_shared<MetadataView>();
  std::atomic_store_explicit(&published_, std::shared_ptr<const MetadataView>(initial),
                             std::memory_order_release);
}

MetadataCommandResult MetadataStateMachine::Apply(
    const metadataRpcProtocol::MetadataCommand& command) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (command.mutationid().empty()) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
                 "metadata mutation ID must not be empty");
  }
  const auto duplicate = dedup_.find(command.mutationid());
  if (duplicate != dedup_.end()) {
    dedupHits_.fetch_add(1, std::memory_order_relaxed);
    return duplicate->second.result();
  }

  MetadataCommandResult result = ApplyLocked(command);
  RecordResultLocked(command.mutationid(), result);
  return result;
}

MetadataCommandResult MetadataStateMachine::ApplyLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  try {
    switch (command.Command_case()) {
      case metadataRpcProtocol::MetadataCommand::kBootstrap:
        return BootstrapLocked(command);
      case metadataRpcProtocol::MetadataCommand::kReplaceRegions:
        return ReplaceRegionsLocked(command);
      case metadataRpcProtocol::MetadataCommand::kAllocateIds:
        return AllocateIdsLocked(command);
      case metadataRpcProtocol::MetadataCommand::kPutStore:
        return PutStoreLocked(command);
      case metadataRpcProtocol::MetadataCommand::kPrepareSplit:
        return PrepareSplitLocked(command);
      case metadataRpcProtocol::MetadataCommand::kCommitSplit:
        return CommitSplitLocked(command);
      case metadataRpcProtocol::MetadataCommand::kAckSplit:
        return AckSplitLocked(command);
      case metadataRpcProtocol::MetadataCommand::kPrepareMovePeer:
        return PrepareMovePeerLocked(command);
      case metadataRpcProtocol::MetadataCommand::kCommitMovePeer:
        return CommitMovePeerLocked(command);
      case metadataRpcProtocol::MetadataCommand::kCancelMovePeer:
        return CancelMovePeerLocked(command);
      case metadataRpcProtocol::MetadataCommand::kReportStoreHeartbeat:
        return ReportStoreHeartbeatLocked(command);
      case metadataRpcProtocol::MetadataCommand::kUpdateAutoBalancerConfig:
        return UpdateAutoBalancerConfigLocked(command);
      case metadataRpcProtocol::MetadataCommand::kCreateSchedulingOperator:
        return CreateSchedulingOperatorLocked(command);
      case metadataRpcProtocol::MetadataCommand::kUpdateSchedulingOperator:
        return UpdateSchedulingOperatorLocked(command);
      case metadataRpcProtocol::MetadataCommand::kCancelSchedulingOperator:
        return CancelSchedulingOperatorLocked(command);
      case metadataRpcProtocol::MetadataCommand::COMMAND_NOT_SET:
        validationRejects_.fetch_add(1, std::memory_order_relaxed);
        return Error(metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
                     "metadata command is missing an operation");
    }
  } catch (const std::exception& error) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(metadataRpcProtocol::METADATA_INVALID_ARGUMENT, error.what());
  }
  return Error(metadataRpcProtocol::METADATA_INVALID_ARGUMENT, "unknown metadata command");
}

MetadataCommandResult MetadataStateMachine::BootstrapLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (revision_ != 0 || !regions_.empty()) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(metadataRpcProtocol::METADATA_ALREADY_BOOTSTRAPPED,
                 "metadata cluster is already bootstrapped");
  }
  if (command.expectedrevision() != 0 || command.bootstrap().configdigest().empty()) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
                 "bootstrap requires revision zero and a configuration digest");
  }

  std::vector<RegionMetadata> candidate = DecodeRegions(command.bootstrap().regions());
  for (auto& region : candidate) {
    // Bootstrap descriptors are not yet part of a committed metadata revision.
    // Normalize them to the revision that this command will publish.
    region.metadataRevision = 1;
  }
  RegionCatalog validated(candidate);
  std::unordered_map<uint64_t, stratakv::region::StoreDescriptor> stores;
  for (const auto& store : command.bootstrap().stores()) {
    if (store.storeid() == 0 || store.host().empty() || store.port() == 0 ||
        store.port() > 65535 || !stores.emplace(store.storeid(), store).second) {
      throw std::invalid_argument("invalid or duplicate Store descriptor");
    }
  }
  for (const auto& region : validated.Regions()) {
    for (const auto& peer : region.peers) {
      const auto store = stores.find(peer.storeId);
      if (store == stores.end() || store->second.host() != peer.host ||
          store->second.port() != static_cast<uint32_t>(peer.port)) {
        throw std::invalid_argument("Region peer does not match a Store descriptor");
      }
    }
  }

  revision_ = 1;
  bootstrapDigest_ = command.bootstrap().configdigest();
  stores_ = std::move(stores);
  candidate = validated.Regions();
  for (auto& region : candidate) region.metadataRevision = revision_;
  regions_ = std::move(candidate);
  RecomputeHighWaterLocked();
  PublishLocked(regions_);

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  return result;
}

MetadataCommandResult MetadataStateMachine::ReplaceRegionsLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (revision_ == 0) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
                 "metadata cluster is not bootstrapped");
  }
  if (command.expectedrevision() != revision_) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "metadata revision precondition failed");
  }
  if (command.replaceregions().removeregionids().empty() ||
      command.replaceregions().addregions().empty()) {
    throw std::invalid_argument("Region replacement requires removed and added Regions");
  }

  std::unordered_set<uint64_t> removedIds;
  for (uint64_t id : command.replaceregions().removeregionids()) {
    if (!removedIds.insert(id).second) throw std::invalid_argument("duplicate removed Region ID");
  }

  std::vector<RegionMetadata> removed;
  std::vector<RegionMetadata> retained;
  for (const auto& region : regions_) {
    if (removedIds.erase(static_cast<uint64_t>(region.regionId)) > 0) removed.push_back(region);
    else retained.push_back(region);
  }
  if (!removedIds.empty()) throw std::invalid_argument("removed Region does not exist");
  std::vector<RegionMetadata> added = DecodeRegions(command.replaceregions().addregions());
  for (auto& region : added) region.metadataRevision = revision_ + 1;
  if (CoveredInterval(removed) != CoveredInterval(added)) {
    throw std::invalid_argument("replacement Regions must exactly cover the removed interval");
  }

  uint64_t maxOldVersion = 0;
  for (const auto& oldRegion : removed) maxOldVersion = std::max(maxOldVersion, oldRegion.epoch.version);
  for (auto& newRegion : added) {
    const auto old = std::find_if(removed.begin(), removed.end(), [&](const RegionMetadata& value) {
      return value.regionId == newRegion.regionId;
    });
    if (old == removed.end()) {
      if (newRegion.epoch.version <= maxOldVersion) {
        throw std::invalid_argument("new Region version must advance past replaced Regions");
      }
    } else {
      const bool rangeChanged =
          old->startKey != newRegion.startKey || old->endKey != newRegion.endKey;
      const bool peersChanged = !SamePeers(*old, newRegion);
      if ((rangeChanged && newRegion.epoch.version <= old->epoch.version) ||
          (!rangeChanged && newRegion.epoch.version < old->epoch.version) ||
          (peersChanged && newRegion.epoch.confVersion <= old->epoch.confVersion) ||
          (!peersChanged && newRegion.epoch.confVersion < old->epoch.confVersion)) {
        throw std::invalid_argument("replacement Region epoch did not advance");
      }
    }
  }

  retained.insert(retained.end(), added.begin(), added.end());
  RegionCatalog validated(std::move(retained));
  ++revision_;
  regions_ = validated.Regions();
  for (auto& region : regions_) region.metadataRevision = revision_;
  RecomputeHighWaterLocked();
  PublishLocked(regions_);

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  return result;
}

MetadataCommandResult MetadataStateMachine::PrepareSplitLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (revision_ == 0 || command.expectedrevision() != revision_) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(command.expectedrevision() != revision_ && revision_ != 0
                     ? metadataRpcProtocol::METADATA_REVISION_MISMATCH
                     : metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
                 "metadata revision precondition failed");
  }
  const auto& prepare = command.preparesplit();
  RegionMetadata* parent = nullptr;
  for (auto& region : regions_) {
    if (region.regionId == static_cast<int>(prepare.regionid())) parent = &region;
  }
  if (parent == nullptr) {
    throw std::invalid_argument("split target Region does not exist");
  }
  if (parent->splitPending) {
    throw std::invalid_argument("Region already has a split in progress");
  }
  if (parent->epoch.version != prepare.expectedepoch().version() ||
      parent->epoch.confVersion != prepare.expectedepoch().confversion()) {
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "split expected epoch does not match the Region descriptor");
  }
  const std::string splitKey = prepare.splitkey();
  if (parent->startKey >= splitKey ||
      (!parent->endKey.empty() && splitKey >= parent->endKey)) {
    throw std::invalid_argument("split key must be strictly inside the Region range");
  }

  // Allocate the child Region and one peer per parent peer from the durable
  // high-water marks, mirroring the parent's placement.
  RegionMetadata child;
  child.regionId = static_cast<int>(SafeAdd(regionIdHighWater_, 1));
  regionIdHighWater_ = SafeAdd(regionIdHighWater_, 1);
  child.startKey = splitKey;
  child.endKey = parent->endKey;
  child.epoch = RegionEpoch{1, 1};
  child.peers.reserve(parent->peers.size());
  for (const auto& peer : parent->peers) {
    RegionPeerLocation childPeer = peer;
    childPeer.peerId = SafeAdd(peerIdHighWater_, 1);
    peerIdHighWater_ = SafeAdd(peerIdHighWater_, 1);
    child.peers.push_back(childPeer);
  }
  child.leaderPeerId = child.peers.front().peerId;

  auto marker = std::make_shared<RegionSplitPending>();
  marker->splitKey = splitKey;
  marker->child = std::make_shared<const RegionMetadata>(child);
  auto marked = std::make_shared<RegionMetadata>(*parent);
  marked->splitPending = marker;

  std::vector<RegionMetadata> updated;
  for (auto& region : regions_) {
    updated.push_back(region.regionId == marked->regionId ? *marked : region);
  }
  ++revision_;
  regions_ = std::move(updated);
  RecomputeHighWaterLocked();
  PublishLocked(regions_);

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  *result.add_regions() = ToProtoRegion(*marked);
  return result;
}

MetadataCommandResult MetadataStateMachine::CommitSplitLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (revision_ == 0 || command.expectedrevision() != revision_) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "metadata revision precondition failed");
  }
  const uint64_t regionId = command.commitsplit().regionid();
  RegionMetadata* parent = nullptr;
  for (auto& region : regions_) {
    if (region.regionId == static_cast<int>(regionId)) parent = &region;
  }
  if (parent == nullptr || !parent->splitPending) {
    throw std::invalid_argument("commit requires a prepared split on the Region");
  }

  auto marked = std::make_shared<RegionMetadata>(*parent);
  const auto pending = marked->splitPending;
  marked->splitPending.reset();

  // Shrunken parent: advance the range-change epoch component.
  RegionMetadata shrunken = *marked;
  shrunken.endKey = pending->splitKey;
  shrunken.epoch.version = marked->epoch.version + 1;

  // Child: fresh epoch, stamped with the revision this commit publishes.
  RegionMetadata child = *pending->child;
  child.endKey = marked->endKey;
  child.metadataRevision = revision_ + 1;

  std::vector<RegionMetadata> updated;
  for (const auto& region : regions_) {
    updated.push_back(region.regionId == shrunken.regionId ? shrunken : region);
  }
  updated.push_back(child);
  RegionCatalog validated(std::move(updated));
  ++revision_;
  regions_ = validated.Regions();
  for (auto& region : regions_) region.metadataRevision = revision_;
  RecomputeHighWaterLocked();
  PublishLocked(regions_);

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  for (const auto& region : regions_) {
    if (region.regionId == shrunken.regionId || region.regionId == child.regionId) {
      *result.add_regions() = ToProtoRegion(region);
    }
  }
  return result;
}

// Node acknowledgment that a split fully materialized locally. Pure
// bookkeeping: idempotent through the mutation dedup table, and validated
// against the committed topology so unknown children are rejected.
MetadataCommandResult MetadataStateMachine::AckSplitLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (revision_ == 0 || command.expectedrevision() != revision_) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "metadata revision precondition failed");
  }
  for (const auto& childId : command.acksplit().childregionids()) {
    const bool exists = std::any_of(regions_.begin(), regions_.end(),
                                    [childId](const RegionMetadata& region) {
                                      return static_cast<uint64_t>(region.regionId) == childId;
                                    });
    if (!exists) {
      throw std::invalid_argument("split ack names an unknown child Region");
    }
  }
  // Bookkeeping only: the ack must not advance the topology revision, or a
  // slow node's ack would invalidate concurrent routing reads' revisions.
  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  return result;
}

MetadataCommandResult MetadataStateMachine::PrepareMovePeerLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (revision_ == 0 || command.expectedrevision() != revision_) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "metadata revision precondition failed");
  }
  const auto& prepare = command.preparemovepeer();
  auto region = std::find_if(regions_.begin(), regions_.end(), [&](const RegionMetadata& value) {
    return static_cast<uint64_t>(value.regionId) == prepare.regionid();
  });
  if (region == regions_.end()) throw std::invalid_argument("migration Region does not exist");
  if (migrations_.find(prepare.regionid()) != migrations_.end()) {
    throw std::invalid_argument("Region already has a migration in progress");
  }
  if (!prepare.has_expectedepoch() ||
      region->epoch.version != prepare.expectedepoch().version() ||
      region->epoch.confVersion != prepare.expectedepoch().confversion()) {
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "migration expected epoch does not match the Region descriptor");
  }
  const auto source = std::find_if(region->peers.begin(), region->peers.end(),
                                   [&](const RegionPeerLocation& peer) {
                                     return peer.storeId == prepare.fromstoreid();
                                   });
  if (source == region->peers.end()) {
    throw std::invalid_argument("migration source Store does not host the Region");
  }
  if (std::any_of(region->peers.begin(), region->peers.end(),
                  [&](const RegionPeerLocation& peer) {
                    return peer.storeId == prepare.tostoreid();
                  })) {
    throw std::invalid_argument("migration target Store already hosts the Region");
  }
  const auto targetStore = stores_.find(prepare.tostoreid());
  if (targetStore == stores_.end()) {
    throw std::invalid_argument("migration target Store does not exist");
  }

  const uint64_t newPeerId = SafeAdd(peerIdHighWater_, 1);
  peerIdHighWater_ = newPeerId;
  stratakv::region::ReplicaMigrationStatus status;
  status.set_regionid(prepare.regionid());
  status.set_sourcepeerid(source->peerId);
  status.set_fromstoreid(prepare.fromstoreid());
  auto* target = status.mutable_targetpeer();
  target->set_peerid(newPeerId);
  target->set_storeid(prepare.tostoreid());
  target->set_host(targetStore->second.host());
  target->set_port(targetStore->second.port());
  target->set_islearner(true);
  status.set_phase(stratakv::region::REPLICA_MIGRATION_PREPARING_TARGET);
  status.set_mutationid(command.mutationid());
  migrations_[prepare.regionid()] = status;

  ++revision_;
  for (auto& descriptor : regions_) descriptor.metadataRevision = revision_;
  PublishLocked(regions_);

  RegionMetadata prepared = *region;
  prepared.metadataRevision = revision_;
  // The target does not receive the AddLearner entry itself; it is mounted at
  // the epoch that existing voters reach after applying that first step.
  ++prepared.epoch.confVersion;
  prepared.peers.push_back(
      {static_cast<int>(prepare.tostoreid() - 1), targetStore->second.host(),
       static_cast<short>(targetStore->second.port()), prepare.tostoreid(), newPeerId, true});

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  *result.mutable_migration() = status;
  *result.add_regions() = ToProtoRegion(prepared);
  return result;
}

MetadataCommandResult MetadataStateMachine::CommitMovePeerLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (revision_ == 0 || command.expectedrevision() != revision_) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "metadata revision precondition failed");
  }
  const auto& commit = command.commitmovepeer();
  const auto migration = migrations_.find(commit.regionid());
  if (migration == migrations_.end() ||
      migration->second.targetpeer().peerid() != commit.newpeerid()) {
    throw std::invalid_argument("migration commit does not match an in-flight move");
  }
  auto region = std::find_if(regions_.begin(), regions_.end(), [&](const RegionMetadata& value) {
    return static_cast<uint64_t>(value.regionId) == commit.regionid();
  });
  if (region == regions_.end()) throw std::invalid_argument("migration Region does not exist");
  if (!commit.has_expectedepoch() ||
      region->epoch.version != commit.expectedepoch().version() ||
      region->epoch.confVersion != commit.expectedepoch().confversion()) {
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "migration expected epoch does not match the Region descriptor");
  }

  auto source = std::find_if(region->peers.begin(), region->peers.end(),
                             [&](const RegionPeerLocation& peer) {
                               return peer.peerId == migration->second.sourcepeerid();
                             });
  if (source == region->peers.end()) {
    throw std::invalid_argument("migration source peer is no longer present");
  }
  const auto& target = migration->second.targetpeer();
  const uint64_t oldLeader = region->leaderPeerId;
  *source = {static_cast<int>(target.storeid() - 1), target.host(),
             static_cast<short>(target.port()), target.storeid(), target.peerid(), false};
  if (oldLeader == migration->second.sourcepeerid()) region->leaderPeerId = target.peerid();
  // AddLearner, PromoteLearner and RemovePeer each advance the data-plane
  // configuration epoch once. Publish the same final epoch to clients.
  region->epoch.confVersion += 3;

  ++revision_;
  for (auto& descriptor : regions_) descriptor.metadataRevision = revision_;
  stratakv::region::ReplicaMigrationStatus completed = migration->second;
  completed.mutable_targetpeer()->set_islearner(false);
  completed.set_phase(stratakv::region::REPLICA_MIGRATION_COMPLETE);
  migrations_.erase(migration);
  PublishLocked(regions_);

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  *result.mutable_migration() = completed;
  *result.add_regions() = ToProtoRegion(*region);
  return result;
}

MetadataCommandResult MetadataStateMachine::CancelMovePeerLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (revision_ == 0 || command.expectedrevision() != revision_) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "metadata revision precondition failed");
  }
  const auto& cancel = command.cancelmovepeer();
  const auto migration = migrations_.find(cancel.regionid());
  if (migration == migrations_.end() ||
      migration->second.targetpeer().peerid() != cancel.newpeerid()) {
    throw std::invalid_argument("migration cancellation does not match an in-flight move");
  }
  stratakv::region::ReplicaMigrationStatus cancelled = migration->second;
  cancelled.set_phase(stratakv::region::REPLICA_MIGRATION_CANCELLED);
  migrations_.erase(migration);
  ++revision_;
  for (auto& descriptor : regions_) descriptor.metadataRevision = revision_;
  PublishLocked(regions_);

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  *result.mutable_migration() = cancelled;
  return result;
}

MetadataCommandResult MetadataStateMachine::ReportStoreHeartbeatLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (revision_ == 0) {
    return Error(metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
                 "metadata cluster is not bootstrapped");
  }
  if (command.expectedrevision() != 0 && command.expectedrevision() != revision_) {
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "heartbeat metadata revision precondition failed");
  }
  const auto& heartbeat = command.reportstoreheartbeat().heartbeat();
  if (heartbeat.storeid() == 0 || heartbeat.sequence() == 0 ||
      heartbeat.observedatms() == 0 || heartbeat.expiresatms() <= heartbeat.observedatms() ||
      heartbeat.capacitybytes() == 0 ||
      heartbeat.availablebytes() > heartbeat.capacitybytes()) {
    throw std::invalid_argument("invalid Store heartbeat");
  }
  if (stores_.find(heartbeat.storeid()) == stores_.end()) {
    throw std::invalid_argument("heartbeat Store is not registered");
  }

  const auto previous = storeHeartbeats_.find(heartbeat.storeid());
  if (previous != storeHeartbeats_.end() &&
      heartbeat.sequence() <= previous->second.sequence()) {
    heartbeatIgnored_.fetch_add(1, std::memory_order_relaxed);
    MetadataCommandResult result;
    result.set_error(metadataRpcProtocol::METADATA_OK);
    result.set_revision(revision_);
    *result.mutable_heartbeat() = previous->second;
    return result;
  }

  std::unordered_map<uint64_t, metadataRpcProtocol::RegionSchedulingObservation>
      previousRegions;
  if (previous != storeHeartbeats_.end()) {
    for (const auto& observation : previous->second.regions()) {
      previousRegions.emplace(observation.regionid(), observation);
    }
  }
  metadataRpcProtocol::StoreHeartbeat merged = heartbeat;
  merged.clear_regions();
  std::unordered_set<uint64_t> seenRegions;
  for (const auto& observation : heartbeat.regions()) {
    if (observation.regionid() == 0 || observation.peerid() == 0 ||
        observation.epoch().version() == 0 ||
        observation.epoch().confversion() == 0 ||
        !seenRegions.insert(observation.regionid()).second) {
      throw std::invalid_argument("invalid or duplicate Region heartbeat observation");
    }
    const auto old = previousRegions.find(observation.regionid());
    if (old != previousRegions.end() && EpochOlder(observation.epoch(), old->second.epoch())) {
      *merged.add_regions() = old->second;
    } else {
      *merged.add_regions() = observation;
    }
  }
  storeHeartbeats_[heartbeat.storeid()] = merged;
  heartbeatAccepted_.fetch_add(1, std::memory_order_relaxed);
  PublishLocked(regions_);

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  *result.mutable_heartbeat() = merged;
  return result;
}

MetadataCommandResult MetadataStateMachine::UpdateAutoBalancerConfigLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (revision_ == 0) {
    return Error(metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
                 "metadata cluster is not bootstrapped");
  }
  if (command.expectedrevision() != 0 && command.expectedrevision() != revision_) {
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "balancer configuration revision precondition failed");
  }
  metadataRpcProtocol::AutoBalancerConfig config =
      command.updateautobalancerconfig().config();
  if (config.version() == 0) config.set_version(balancerConfig_.version() + 1);
  if (config.version() <= balancerConfig_.version() ||
      config.evaluationintervalms() == 0 ||
      config.heartbeattimeoutms() < config.evaluationintervalms() ||
      config.maximumdiskusedratio() <= 0.0 ||
      config.maximumdiskusedratio() > 1.0 || config.replicationfactor() == 0 ||
      config.imbalancethreshold() < 0.0 || config.imbalancethreshold() > 1.0 ||
      config.splitconsecutivewindows() == 0 ||
      config.maximumactiveoperators() == 0 ||
      config.maximumactiveperstore() == 0 ||
      config.maximumactiveperstore() > config.maximumactiveoperators()) {
    throw std::invalid_argument("invalid Auto-Balancer configuration");
  }
  balancerConfig_ = std::move(config);
  PublishLocked(regions_);

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  *result.mutable_balancerconfig() = balancerConfig_;
  return result;
}

MetadataCommandResult MetadataStateMachine::CreateSchedulingOperatorLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (revision_ == 0 || command.expectedrevision() != revision_) {
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "operator source revision is stale");
  }
  const auto& requested = command.createschedulingoperator().operator_();
  if (requested.operatorid().empty() || requested.regionid() == 0 ||
      requested.sourcerevision() != revision_ ||
      requested.phase() != metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING ||
      requested.createdatms() == 0 || requested.updatedatms() < requested.createdatms() ||
      requested.deadlinems() <= requested.createdatms()) {
    throw std::invalid_argument("invalid scheduling operator");
  }
  const auto existing = schedulingOperators_.find(requested.operatorid());
  if (existing != schedulingOperators_.end()) {
    if (existing->second.regionid() != requested.regionid() ||
        existing->second.type() != requested.type()) {
      return Error(metadataRpcProtocol::METADATA_CONFLICT,
                   "operator identifier is already used by different work");
    }
    MetadataCommandResult result;
    result.set_error(metadataRpcProtocol::METADATA_OK);
    result.set_revision(revision_);
    *result.mutable_operator_() = existing->second;
    return result;
  }
  if (!balancerConfig_.enabled() || balancerConfig_.paused()) {
    return Error(metadataRpcProtocol::METADATA_CONFLICT,
                 "Auto-Balancer is disabled or paused");
  }
  if (activeOperatorByRegion_.find(requested.regionid()) !=
      activeOperatorByRegion_.end()) {
    return Error(metadataRpcProtocol::METADATA_CONFLICT,
                 "Region already has an active topology operator");
  }
  const auto region = std::find_if(regions_.begin(), regions_.end(), [&](const auto& value) {
    return static_cast<uint64_t>(value.regionId) == requested.regionid();
  });
  if (region == regions_.end()) throw std::invalid_argument("operator Region does not exist");
  if (!requested.has_regionepoch() ||
      requested.regionepoch().version() != region->epoch.version ||
      requested.regionepoch().confversion() != region->epoch.confVersion) {
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "operator Region epoch is stale");
  }
  if (activeOperatorByRegion_.size() >= balancerConfig_.maximumactiveoperators()) {
    return Error(metadataRpcProtocol::METADATA_CONFLICT,
                 "cluster scheduling concurrency budget is exhausted");
  }

  if (requested.type() == metadataRpcProtocol::SCHEDULING_OPERATOR_MOVE_PEER) {
    if (requested.sourcestoreid() == 0 || requested.targetstoreid() == 0 ||
        requested.sourcestoreid() == requested.targetstoreid()) {
      throw std::invalid_argument("move operator requires distinct source and target Stores");
    }
    const bool sourcePresent = std::any_of(region->peers.begin(), region->peers.end(),
                                           [&](const RegionPeerLocation& peer) {
                                             return peer.storeId == requested.sourcestoreid();
                                           });
    const bool targetPresent = std::any_of(region->peers.begin(), region->peers.end(),
                                           [&](const RegionPeerLocation& peer) {
                                             return peer.storeId == requested.targetstoreid();
                                           });
    const auto targetHeartbeat = storeHeartbeats_.find(requested.targetstoreid());
    if (!sourcePresent || targetPresent || targetHeartbeat == storeHeartbeats_.end() ||
        targetHeartbeat->second.expiresatms() <= requested.createdatms() ||
        targetHeartbeat->second.availablebytes() < balancerConfig_.minimumfreebytes()) {
      return Error(metadataRpcProtocol::METADATA_CONFLICT,
                   "move operator has no safe fresh destination");
    }
  } else if (requested.type() ==
             metadataRpcProtocol::SCHEDULING_OPERATOR_SPLIT_REGION) {
    if (requested.splitcandidategeneration() == 0) {
      throw std::invalid_argument("split operator requires an opaque candidate generation");
    }
  } else {
    throw std::invalid_argument("unknown scheduling operator type");
  }

  std::unordered_map<uint64_t, uint32_t> activeByStore;
  for (const auto& item : schedulingOperators_) {
    const auto& active = item.second;
    if (IsTerminal(active.phase())) continue;
    if (active.sourcestoreid() != 0) ++activeByStore[active.sourcestoreid()];
    if (active.targetstoreid() != 0) ++activeByStore[active.targetstoreid()];
  }
  if ((requested.sourcestoreid() != 0 &&
       activeByStore[requested.sourcestoreid()] >=
           balancerConfig_.maximumactiveperstore()) ||
      (requested.targetstoreid() != 0 &&
       activeByStore[requested.targetstoreid()] >=
           balancerConfig_.maximumactiveperstore())) {
    return Error(metadataRpcProtocol::METADATA_CONFLICT,
                 "Store scheduling concurrency budget is exhausted");
  }

  schedulingOperators_[requested.operatorid()] = requested;
  activeOperatorByRegion_[requested.regionid()] = requested.operatorid();
  operatorAdmissions_.fetch_add(1, std::memory_order_relaxed);
  PublishLocked(regions_);

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  *result.mutable_operator_() = requested;
  return result;
}

MetadataCommandResult MetadataStateMachine::UpdateSchedulingOperatorLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (command.expectedrevision() != 0 && command.expectedrevision() != revision_) {
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "operator update revision precondition failed");
  }
  const auto& update = command.updateschedulingoperator();
  auto found = schedulingOperators_.find(update.operatorid());
  if (found == schedulingOperators_.end()) {
    return Error(metadataRpcProtocol::METADATA_NOT_FOUND, "scheduling operator does not exist");
  }
  auto& current = found->second;
  if (current.phase() != update.expectedphase()) {
    return Error(metadataRpcProtocol::METADATA_CONFLICT,
                 "scheduling operator phase precondition failed");
  }
  if (!ValidTransition(current.phase(), update.phase()) ||
      update.attempts() < current.attempts() ||
      update.updatedatms() < current.updatedatms()) {
    return Error(metadataRpcProtocol::METADATA_CONFLICT,
                 "invalid scheduling operator transition");
  }
  current.set_phase(update.phase());
  current.set_attempts(update.attempts());
  current.set_lasterror(update.lasterror());
  current.set_irreversible(current.irreversible() || update.irreversible());
  current.set_updatedatms(update.updatedatms());
  if (IsTerminal(current.phase())) activeOperatorByRegion_.erase(current.regionid());
  PublishLocked(regions_);

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  *result.mutable_operator_() = current;
  return result;
}

MetadataCommandResult MetadataStateMachine::CancelSchedulingOperatorLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (command.expectedrevision() != 0 && command.expectedrevision() != revision_) {
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "operator cancellation revision precondition failed");
  }
  const auto& cancel = command.cancelschedulingoperator();
  auto found = schedulingOperators_.find(cancel.operatorid());
  if (found == schedulingOperators_.end()) {
    return Error(metadataRpcProtocol::METADATA_NOT_FOUND, "scheduling operator does not exist");
  }
  auto& current = found->second;
  if (IsTerminal(current.phase())) {
    MetadataCommandResult result;
    result.set_error(metadataRpcProtocol::METADATA_OK);
    result.set_revision(revision_);
    *result.mutable_operator_() = current;
    return result;
  }
  if (current.irreversible()) {
    return Error(metadataRpcProtocol::METADATA_CONFLICT,
                 "scheduling operator has crossed its irreversible point");
  }
  current.set_cancelrequested(true);
  current.set_updatedatms(std::max(current.updatedatms(), cancel.updatedatms()));
  if (current.phase() == metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING) {
    current.set_phase(metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED);
    activeOperatorByRegion_.erase(current.regionid());
  } else {
    current.set_phase(metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLING);
  }
  PublishLocked(regions_);

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  *result.mutable_operator_() = current;
  return result;
}

MetadataCommandResult MetadataStateMachine::AllocateIdsLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (revision_ == 0) {
    return Error(metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
                 "metadata cluster is not bootstrapped");
  }
  if (command.expectedrevision() != revision_) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "metadata revision precondition failed");
  }
  const uint64_t count = command.allocateids().count();
  uint64_t* highWater = nullptr;
  switch (command.allocateids().namespace_()) {
    case metadataRpcProtocol::ID_NAMESPACE_REGION: highWater = &regionIdHighWater_; break;
    case metadataRpcProtocol::ID_NAMESPACE_PEER: highWater = &peerIdHighWater_; break;
    case metadataRpcProtocol::ID_NAMESPACE_STORE: highWater = &storeIdHighWater_; break;
  }
  if (highWater == nullptr) throw std::invalid_argument("unknown identifier namespace");
  const uint64_t first = *highWater + 1;
  *highWater = SafeAdd(*highWater, count);
  ++revision_;
  PublishLocked(regions_);

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  result.set_firstallocatedid(first);
  result.set_allocatedcount(count);
  return result;
}

MetadataCommandResult MetadataStateMachine::PutStoreLocked(
    const metadataRpcProtocol::MetadataCommand& command) {
  if (revision_ == 0 || command.expectedrevision() != revision_) {
    validationRejects_.fetch_add(1, std::memory_order_relaxed);
    return Error(metadataRpcProtocol::METADATA_REVISION_MISMATCH,
                 "metadata revision precondition failed");
  }
  const auto& store = command.putstore().store();
  if (store.storeid() == 0 || store.host().empty() || store.port() == 0 ||
      store.port() > 65535) {
    throw std::invalid_argument("invalid Store descriptor");
  }
  stores_[store.storeid()] = store;
  storeIdHighWater_ = std::max(storeIdHighWater_, store.storeid());
  ++revision_;
  PublishLocked(regions_);

  MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_OK);
  result.set_revision(revision_);
  return result;
}

std::shared_ptr<const MetadataView> MetadataStateMachine::View() const {
  return std::atomic_load_explicit(&published_, std::memory_order_acquire);
}

std::optional<RegionMetadata> MetadataStateMachine::LookupKey(const std::string& key) const {
  const auto view = View();
  if (!view->catalog) return std::nullopt;
  try {
    return view->catalog->FindByKey(key);
  } catch (const std::out_of_range&) {
    return std::nullopt;
  }
}

std::optional<RegionMetadata> MetadataStateMachine::LookupRegion(uint64_t regionId) const {
  const auto view = View();
  if (!view->catalog || regionId > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
    return std::nullopt;
  }
  try {
    return view->catalog->FindById(static_cast<int>(regionId));
  } catch (const std::out_of_range&) {
    return std::nullopt;
  }
}

std::optional<stratakv::region::ReplicaMigrationStatus>
MetadataStateMachine::LookupMigration(uint64_t regionId) const {
  const auto view = View();
  const auto found = view->migrations.find(regionId);
  if (found == view->migrations.end()) return std::nullopt;
  return found->second;
}

std::optional<metadataRpcProtocol::SchedulingOperator>
MetadataStateMachine::LookupSchedulingOperator(const std::string& operatorId) const {
  const auto view = View();
  const auto found = view->schedulingOperators.find(operatorId);
  if (found == view->schedulingOperators.end()) return std::nullopt;
  return found->second;
}

std::vector<RegionMetadata> MetadataStateMachine::Scan(const std::string& startKey,
                                                       size_t limit) const {
  std::vector<RegionMetadata> result;
  if (limit == 0) return result;
  const auto view = View();
  if (!view->catalog) return result;
  const auto& regions = view->catalog->Regions();
  auto found = std::find_if(regions.begin(), regions.end(), [&](const RegionMetadata& region) {
    return region.Contains(startKey) || !RegionBytewiseLess(region.startKey, startKey);
  });
  for (; found != regions.end() && result.size() < limit; ++found) result.push_back(*found);
  return result;
}

std::string MetadataStateMachine::Snapshot() {
  std::lock_guard<std::mutex> lock(mutex_);
  metadataRpcProtocol::MetadataSnapshot snapshot;
  snapshot.set_formatversion(kSnapshotFormatVersion);
  snapshot.set_revision(revision_);
  snapshot.set_regionidhighwater(regionIdHighWater_);
  snapshot.set_peeridhighwater(peerIdHighWater_);
  snapshot.set_storeidhighwater(storeIdHighWater_);
  snapshot.set_bootstrapdigest(bootstrapDigest_);
  for (const auto& region : regions_) *snapshot.add_regions() = ToProtoRegion(region);
  std::vector<uint64_t> storeIds;
  storeIds.reserve(stores_.size());
  for (const auto& item : stores_) storeIds.push_back(item.first);
  std::sort(storeIds.begin(), storeIds.end());
  for (uint64_t id : storeIds) *snapshot.add_stores() = stores_.at(id);
  for (const std::string& mutationId : dedupOrder_) {
    const auto found = dedup_.find(mutationId);
    if (found != dedup_.end()) *snapshot.add_recentmutations() = found->second;
  }
  std::vector<uint64_t> migrationIds;
  migrationIds.reserve(migrations_.size());
  for (const auto& migration : migrations_) migrationIds.push_back(migration.first);
  std::sort(migrationIds.begin(), migrationIds.end());
  for (uint64_t regionId : migrationIds) {
    *snapshot.add_migrations() = migrations_.at(regionId);
  }
  std::vector<uint64_t> heartbeatStoreIds;
  heartbeatStoreIds.reserve(storeHeartbeats_.size());
  for (const auto& heartbeat : storeHeartbeats_) heartbeatStoreIds.push_back(heartbeat.first);
  std::sort(heartbeatStoreIds.begin(), heartbeatStoreIds.end());
  for (uint64_t storeId : heartbeatStoreIds) {
    *snapshot.add_storeheartbeats() = storeHeartbeats_.at(storeId);
  }
  *snapshot.mutable_balancerconfig() = balancerConfig_;
  std::vector<std::string> operatorIds;
  operatorIds.reserve(schedulingOperators_.size());
  for (const auto& item : schedulingOperators_) operatorIds.push_back(item.first);
  std::sort(operatorIds.begin(), operatorIds.end());
  for (const auto& operatorId : operatorIds) {
    *snapshot.add_schedulingoperators() = schedulingOperators_.at(operatorId);
  }
  std::string bytes;
  if (!snapshot.SerializeToString(&bytes)) {
    throw std::runtime_error("unable to serialize metadata snapshot");
  }
  snapshotCount_.fetch_add(1, std::memory_order_relaxed);
  return bytes;
}

bool MetadataStateMachine::Restore(const std::string& bytes, std::string* error) {
  metadataRpcProtocol::MetadataSnapshot snapshot;
  if (!snapshot.ParseFromString(bytes) || snapshot.formatversion() != kSnapshotFormatVersion) {
    if (error) *error = "invalid metadata snapshot format";
    return false;
  }
  try {
    std::vector<RegionMetadata> regions = DecodeRegions(snapshot.regions());
    if (snapshot.revision() == 0 && !regions.empty()) {
      throw std::invalid_argument("uncommitted metadata snapshot contains Regions");
    }
    if (snapshot.revision() > 0) {
      const RegionCatalog validatedSnapshot(regions);
      (void)validatedSnapshot;
    }
    std::unordered_map<uint64_t, stratakv::region::StoreDescriptor> stores;
    for (const auto& store : snapshot.stores()) {
      if (store.storeid() == 0 || store.host().empty() || store.port() == 0 ||
          store.port() > 65535 || !stores.emplace(store.storeid(), store).second) {
        throw std::invalid_argument("invalid Store in metadata snapshot");
      }
    }
    if (snapshot.revision() > 0 && snapshot.bootstrapdigest().empty()) {
      throw std::invalid_argument("committed metadata snapshot is missing bootstrap digest");
    }
    for (const auto& region : regions) {
      for (const auto& peer : region.peers) {
        const auto store = stores.find(peer.storeId);
        if (store == stores.end() || store->second.host() != peer.host ||
            store->second.port() != static_cast<uint32_t>(peer.port)) {
          throw std::invalid_argument("snapshot Region peer does not match a Store descriptor");
        }
      }
    }
    std::unordered_map<std::string, metadataRpcProtocol::MutationRecord> dedup;
    std::deque<std::string> order;
    for (const auto& record : snapshot.recentmutations()) {
      if (record.mutationid().empty() || !dedup.emplace(record.mutationid(), record).second) {
        throw std::invalid_argument("invalid mutation record in metadata snapshot");
      }
      order.push_back(record.mutationid());
    }
    std::unordered_map<uint64_t, stratakv::region::ReplicaMigrationStatus> migrations;
    for (const auto& migration : snapshot.migrations()) {
      if (migration.regionid() == 0 || migration.sourcepeerid() == 0 ||
          !migration.has_targetpeer() || migration.targetpeer().peerid() == 0 ||
          !migrations.emplace(migration.regionid(), migration).second) {
        throw std::invalid_argument("invalid migration in metadata snapshot");
      }
    }
    std::unordered_map<uint64_t, metadataRpcProtocol::StoreHeartbeat> heartbeats;
    for (const auto& heartbeat : snapshot.storeheartbeats()) {
      if (heartbeat.storeid() == 0 || heartbeat.sequence() == 0 ||
          heartbeat.expiresatms() <= heartbeat.observedatms() ||
          stores.find(heartbeat.storeid()) == stores.end() ||
          !heartbeats.emplace(heartbeat.storeid(), heartbeat).second) {
        throw std::invalid_argument("invalid Store heartbeat in metadata snapshot");
      }
    }
    metadataRpcProtocol::AutoBalancerConfig balancerConfig =
        snapshot.has_balancerconfig() ? snapshot.balancerconfig()
                                     : DefaultAutoBalancerConfig();
    if (balancerConfig.version() == 0 ||
        balancerConfig.evaluationintervalms() == 0 ||
        balancerConfig.heartbeattimeoutms() < balancerConfig.evaluationintervalms() ||
        balancerConfig.replicationfactor() == 0 ||
        balancerConfig.maximumactiveoperators() == 0 ||
        balancerConfig.maximumactiveperstore() == 0 ||
        balancerConfig.maximumactiveperstore() >
            balancerConfig.maximumactiveoperators()) {
      throw std::invalid_argument("invalid Auto-Balancer configuration in metadata snapshot");
    }
    std::unordered_map<std::string, metadataRpcProtocol::SchedulingOperator> operators;
    std::unordered_map<uint64_t, std::string> activeOperators;
    for (const auto& schedulingOperator : snapshot.schedulingoperators()) {
      if (schedulingOperator.operatorid().empty() ||
          schedulingOperator.regionid() == 0 ||
          schedulingOperator.sourcerevision() > snapshot.revision() ||
          !operators.emplace(schedulingOperator.operatorid(), schedulingOperator).second) {
        throw std::invalid_argument("invalid scheduling operator in metadata snapshot");
      }
      if (!IsTerminal(schedulingOperator.phase()) &&
          !activeOperators.emplace(schedulingOperator.regionid(),
                                   schedulingOperator.operatorid()).second) {
        throw std::invalid_argument("multiple active operators for one Region in snapshot");
      }
    }

    std::lock_guard<std::mutex> lock(mutex_);
    regions_ = std::move(regions);
    stores_ = std::move(stores);
    revision_ = snapshot.revision();
    regionIdHighWater_ = snapshot.regionidhighwater();
    peerIdHighWater_ = snapshot.peeridhighwater();
    storeIdHighWater_ = snapshot.storeidhighwater();
    bootstrapDigest_ = snapshot.bootstrapdigest();
    dedup_ = std::move(dedup);
    dedupOrder_ = std::move(order);
    migrations_ = std::move(migrations);
    storeHeartbeats_ = std::move(heartbeats);
    balancerConfig_ = std::move(balancerConfig);
    schedulingOperators_ = std::move(operators);
    activeOperatorByRegion_ = std::move(activeOperators);
    RecomputeHighWaterLocked();
    PublishLocked(regions_);
    return true;
  } catch (const std::exception& exception) {
    if (error) *error = exception.what();
    return false;
  }
}

MetadataStateMetrics MetadataStateMachine::Metrics() const {
  return {validationRejects_.load(std::memory_order_relaxed),
          dedupHits_.load(std::memory_order_relaxed),
          snapshotCount_.load(std::memory_order_relaxed),
          heartbeatAccepted_.load(std::memory_order_relaxed),
          heartbeatIgnored_.load(std::memory_order_relaxed),
          operatorAdmissions_.load(std::memory_order_relaxed)};
}

MetadataCommandResult MetadataStateMachine::Error(MetadataErrorCode code,
                                                  const std::string& message) const {
  MetadataCommandResult result;
  result.set_error(code);
  result.set_message(message);
  result.set_revision(revision_);
  return result;
}

void MetadataStateMachine::RecordResultLocked(
    const std::string& mutationId, const MetadataCommandResult& result) {
  metadataRpcProtocol::MutationRecord record;
  record.set_mutationid(mutationId);
  *record.mutable_result() = result;
  record.set_appliedrevision(revision_);
  dedup_[mutationId] = std::move(record);
  dedupOrder_.push_back(mutationId);
  while (dedupOrder_.size() > kDedupRetention) {
    dedup_.erase(dedupOrder_.front());
    dedupOrder_.pop_front();
  }
}

void MetadataStateMachine::PublishLocked(std::vector<RegionMetadata> regions) {
  auto view = std::make_shared<MetadataView>();
  view->revision = revision_;
  if (!regions.empty()) view->catalog = std::make_shared<RegionCatalog>(std::move(regions));
  view->stores = stores_;
  view->regionIdHighWater = regionIdHighWater_;
  view->peerIdHighWater = peerIdHighWater_;
  view->storeIdHighWater = storeIdHighWater_;
  view->bootstrapDigest = bootstrapDigest_;
  view->migrations = migrations_;
  view->storeHeartbeats = storeHeartbeats_;
  view->balancerConfig = balancerConfig_;
  view->schedulingOperators = schedulingOperators_;
  view->activeOperatorByRegion = activeOperatorByRegion_;
  std::atomic_store_explicit(&published_, std::shared_ptr<const MetadataView>(view),
                             std::memory_order_release);
}

void MetadataStateMachine::RecomputeHighWaterLocked() {
  for (const auto& region : regions_) {
    regionIdHighWater_ =
        std::max(regionIdHighWater_, static_cast<uint64_t>(region.regionId));
    for (const auto& peer : region.peers) {
      peerIdHighWater_ = std::max(peerIdHighWater_, peer.peerId);
      storeIdHighWater_ = std::max(storeIdHighWater_, peer.storeId);
    }
  }
  for (const auto& store : stores_) storeIdHighWater_ = std::max(storeIdHighWater_, store.first);
}
