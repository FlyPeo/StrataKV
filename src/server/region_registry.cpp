#include "region_registry.h"

#include <stdexcept>

#include "region_metadata.h"

RegionRegistry::RegionRegistry() {
  auto initial = std::make_shared<View>();
  std::atomic_store_explicit(&published_, std::shared_ptr<const View>(initial),
                             std::memory_order_release);
}

std::shared_ptr<const RegionRegistry::View> RegionRegistry::Snapshot() const {
  return std::atomic_load_explicit(&published_, std::memory_order_acquire);
}

bool RegionRegistry::SameDescriptor(const RegionMetadata& lhs, const RegionMetadata& rhs) {
  return ToProtoRegion(lhs).SerializeAsString() == ToProtoRegion(rhs).SerializeAsString();
}

RegionPeerHandle RegionRegistry::Lookup(int regionId) const {
  const auto view = Snapshot();
  const auto found = view->peers.find(regionId);
  if (found == view->peers.end() || !found->second->TryAcquireRequest()) {
    lookupMisses_.fetch_add(1, std::memory_order_relaxed);
    return {};
  }
  return RegionPeerHandle(found->second);
}

std::vector<std::shared_ptr<RegionPeer>> RegionRegistry::Peers() const {
  const auto view = Snapshot();
  std::vector<std::shared_ptr<RegionPeer>> result;
  if (!view) return result;
  result.reserve(view->peers.size());
  for (const auto& item : view->peers) result.push_back(item.second);
  return result;
}

void RegionRegistry::PublishInitial(
    const std::vector<std::shared_ptr<RegionPeer>>& peers, uint64_t revision) {
  if (peers.empty() || revision == 0) {
    throw std::invalid_argument("initial Region registry must be non-empty and versioned");
  }
  auto next = std::make_shared<View>();
  next->revision = revision;
  for (const auto& peer : peers) {
    if (!peer || peer->LifecycleState() != RegionPeerState::Serving ||
        peer->Descriptor().metadataRevision != revision ||
        !next->peers.emplace(peer->RegionId(), peer).second) {
      throw std::invalid_argument("invalid initial Region registry peer set");
    }
  }
  std::lock_guard<std::mutex> lock(publicationMutex_);
  if (!Snapshot()->peers.empty()) throw std::logic_error("Region registry is already published");
  std::atomic_store_explicit(&published_, std::shared_ptr<const View>(next),
                             std::memory_order_release);
}

bool RegionRegistry::Register(const std::shared_ptr<RegionPeer>& peer, uint64_t revision) {
  if (!peer || peer->LifecycleState() != RegionPeerState::Serving || revision == 0 ||
      peer->Descriptor().metadataRevision != revision) {
    throw std::invalid_argument("cannot register an unready or unversioned Region peer");
  }
  std::lock_guard<std::mutex> lock(publicationMutex_);
  const auto current = Snapshot();
  if (revision < current->revision) return false;
  const auto existing = current->peers.find(peer->RegionId());
  if (existing != current->peers.end()) {
    if (existing->second == peer ||
        SameDescriptor(existing->second->Descriptor(), peer->Descriptor())) {
      return true;
    }
    registrationConflicts_.fetch_add(1, std::memory_order_relaxed);
    throw std::logic_error("conflicting Region peer registration");
  }
  auto next = std::make_shared<View>(*current);
  next->revision = revision;
  next->peers.emplace(peer->RegionId(), peer);
  std::atomic_store_explicit(&published_, std::shared_ptr<const View>(next),
                             std::memory_order_release);
  return true;
}

std::vector<std::shared_ptr<RegionPeer>> RegionRegistry::Replace(
    const std::vector<int>& removeRegionIds,
    const std::vector<std::shared_ptr<RegionPeer>>& addPeers, uint64_t revision) {
  if (removeRegionIds.empty() || addPeers.empty() || revision == 0) {
    throw std::invalid_argument("Region registry replacement must remove and add peers");
  }
  std::vector<std::shared_ptr<RegionPeer>> retiring;
  {
    std::lock_guard<std::mutex> lock(publicationMutex_);
    const auto current = Snapshot();
    if (revision <= current->revision) {
      throw std::invalid_argument("Region registry replacement revision must advance");
    }
    auto next = std::make_shared<View>(*current);
    next->revision = revision;
    for (int regionId : removeRegionIds) {
      const auto found = next->peers.find(regionId);
      if (found == next->peers.end()) {
        throw std::invalid_argument("Region registry replacement removes an unknown peer");
      }
      retiring.push_back(found->second);
      next->peers.erase(found);
    }
    for (const auto& peer : addPeers) {
      if (!peer || peer->LifecycleState() != RegionPeerState::Serving ||
          peer->Descriptor().metadataRevision != revision ||
          !next->peers.emplace(peer->RegionId(), peer).second) {
        throw std::invalid_argument("Region registry replacement adds an invalid peer");
      }
    }
    std::atomic_store_explicit(&published_, std::shared_ptr<const View>(next),
                               std::memory_order_release);
    for (const auto& peer : retiring) {
      if (!peer->BeginRetire()) {
        throw std::logic_error("replaced Region peer did not enter retiring state");
      }
    }
  }
  return retiring;
}

RegionRetireResult RegionRegistry::Unregister(
    int regionId, uint64_t revision, std::chrono::steady_clock::time_point deadline) {
  const auto started = std::chrono::steady_clock::now();
  RegionRetireResult result;
  std::shared_ptr<RegionPeer> retiring;
  {
    std::lock_guard<std::mutex> lock(publicationMutex_);
    const auto current = Snapshot();
    if (revision < current->revision) return result;
    const auto found = current->peers.find(regionId);
    if (found == current->peers.end()) return result;
    result.found = true;
    retiring = found->second;
    auto next = std::make_shared<View>(*current);
    next->revision = revision;
    next->peers.erase(regionId);
    std::atomic_store_explicit(&published_, std::shared_ptr<const View>(next),
                               std::memory_order_release);
    if (!retiring->BeginRetire()) {
      throw std::logic_error("Region peer did not enter retiring state");
    }
  }
  result.drained = retiring->WaitForDrain(deadline, &result.remainingRequests);
  result.elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - started);
  if (result.drained) {
    retiring->MarkStopped();
  } else {
    retirementTimeouts_.fetch_add(1, std::memory_order_relaxed);
  }
  return result;
}

void RegionRegistry::RecordValidationReject(stratakv::region::RegionErrorCode code) {
  switch (code) {
    case stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH:
      epochRejects_.fetch_add(1, std::memory_order_relaxed);
      break;
    case stratakv::region::REGION_ERROR_PEER_NOT_FOUND:
      peerRejects_.fetch_add(1, std::memory_order_relaxed);
      break;
    case stratakv::region::REGION_ERROR_KEY_NOT_IN_REGION:
      keyRejects_.fetch_add(1, std::memory_order_relaxed);
      break;
    case stratakv::region::REGION_ERROR_MISSING_HEADER:
      headerRejects_.fetch_add(1, std::memory_order_relaxed);
      break;
    default:
      headerRejects_.fetch_add(1, std::memory_order_relaxed);
      break;
  }
}

RegionRegistryMetrics RegionRegistry::Metrics() const {
  const auto view = Snapshot();
  size_t serving = 0;
  size_t initializing = 0;
  size_t retiring = 0;
  for (const auto& item : view->peers) {
    switch (item.second->LifecycleState()) {
      case RegionPeerState::Serving: ++serving; break;
      case RegionPeerState::Initializing: ++initializing; break;
      case RegionPeerState::Retiring: ++retiring; break;
      case RegionPeerState::Stopped: break;
    }
  }
  return {view->revision,
          serving,
          initializing,
          retiring,
          lookupMisses_.load(std::memory_order_relaxed),
          registrationConflicts_.load(std::memory_order_relaxed),
          retirementTimeouts_.load(std::memory_order_relaxed),
          epochRejects_.load(std::memory_order_relaxed),
          peerRejects_.load(std::memory_order_relaxed),
          keyRejects_.load(std::memory_order_relaxed),
          headerRejects_.load(std::memory_order_relaxed)};
}
