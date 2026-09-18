#include "region_cache.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace {

uint64_t InferRevision(const std::vector<RegionMetadata>& regions) {
  uint64_t revision = 0;
  for (const auto& region : regions) revision = std::max(revision, region.metadataRevision);
  return revision;
}

bool SamePeers(const RegionMetadata& lhs, const RegionMetadata& rhs) {
  if (lhs.peers.size() != rhs.peers.size()) return false;
  for (size_t index = 0; index < lhs.peers.size(); ++index) {
    const auto& left = lhs.peers[index];
    const auto& right = rhs.peers[index];
    if (left.nodeId != right.nodeId || left.host != right.host || left.port != right.port ||
        left.storeId != right.storeId || left.peerId != right.peerId) {
      return false;
    }
  }
  return true;
}

bool Intersects(const RegionMetadata& region, const std::string& start,
                const std::string& end) {
  const bool regionEndsAfterStart =
      region.endKey.empty() || RegionBytewiseLess(start, region.endKey);
  const bool replacementEndsAfterRegionStart =
      end.empty() || RegionBytewiseLess(region.startKey, end);
  return regionEndsAfterStart && replacementEndsAfterRegionStart;
}

void ValidateEpochAdvance(const std::vector<RegionMetadata>& oldRegions,
                          const std::vector<RegionMetadata>& newRegions) {
  uint64_t maximumOldVersion = 0;
  for (const auto& region : oldRegions) {
    maximumOldVersion = std::max(maximumOldVersion, region.epoch.version);
  }
  for (const auto& candidate : newRegions) {
    const auto old = std::find_if(oldRegions.begin(), oldRegions.end(),
                                  [&](const RegionMetadata& region) {
                                    return region.regionId == candidate.regionId;
                                  });
    if (old == oldRegions.end()) {
      // A genuinely new Region id (for example the child of a split) restarts
      // its epoch at {1, 1} by definition. Ordering is guaranteed by the
      // metadata revision gate, and the replacement must still exactly cover
      // the superseded interval.
      continue;
    }
    const bool rangeChanged = old->startKey != candidate.startKey ||
                              old->endKey != candidate.endKey;
    const bool peersChanged = !SamePeers(*old, candidate);
    if (candidate.epoch.version < old->epoch.version ||
        candidate.epoch.confVersion < old->epoch.confVersion ||
        (rangeChanged && candidate.epoch.version == old->epoch.version) ||
        (peersChanged && candidate.epoch.confVersion == old->epoch.confVersion)) {
      throw std::invalid_argument("Region cache update regresses descriptor epoch");
    }
  }
}

}  // namespace

RouteTable::RouteTable(std::vector<RegionMetadata> descriptors,
                       uint64_t metadataRevision)
    : revision(metadataRevision) {
  if (revision == 0) throw std::invalid_argument("RouteTable revision must be positive");
  RegionCatalog validated(std::move(descriptors));
  regions = validated.Regions();
  for (size_t index = 0; index < regions.size(); ++index) {
    if (regions[index].metadataRevision == 0 || regions[index].metadataRevision > revision) {
      throw std::invalid_argument("Region descriptor has an invalid metadata revision");
    }
    if (!byRegionId.emplace(regions[index].regionId, index).second) {
      throw std::invalid_argument("RouteTable contains duplicate Region ID");
    }
  }
}

RegionCache::RegionCache(std::vector<RegionMetadata> regions, uint64_t revision) {
  if (revision == 0) revision = InferRevision(regions);
  auto initial = std::make_shared<RouteTable>(std::move(regions), revision);
  std::atomic_store_explicit(&published_, std::shared_ptr<const RouteTable>(initial),
                             std::memory_order_release);
}

std::shared_ptr<const RouteTable> RegionCache::Snapshot() const {
  return std::atomic_load_explicit(&published_, std::memory_order_acquire);
}

RegionRouteHandle RegionCache::FindKey(const std::shared_ptr<const RouteTable>& table,
                                       const std::string& key) {
  auto found = std::upper_bound(
      table->regions.begin(), table->regions.end(), key,
      [](const std::string& value, const RegionMetadata& region) {
        return RegionBytewiseLess(value, region.startKey);
      });
  if (found == table->regions.begin()) return {};
  --found;
  if (!found->Contains(key)) return {};
  return {table, static_cast<size_t>(std::distance(table->regions.begin(), found))};
}

RegionRouteHandle RegionCache::FindRegion(const std::shared_ptr<const RouteTable>& table,
                                          int regionId) {
  const auto found = table->byRegionId.find(regionId);
  if (found == table->byRegionId.end()) return {};
  return {table, found->second};
}

RegionRouteHandle RegionCache::LookupKey(const std::string& key) const {
  RegionRouteHandle route = FindKey(Snapshot(), key);
  if (!route || IsInvalidated(route.Descriptor().regionId)) {
    misses_.fetch_add(1, std::memory_order_relaxed);
    throw std::out_of_range("Region route is unavailable or stale");
  }
  hits_.fetch_add(1, std::memory_order_relaxed);
  return route;
}

RegionRouteHandle RegionCache::LookupRegion(int regionId) const {
  RegionRouteHandle route = FindRegion(Snapshot(), regionId);
  if (!route || IsInvalidated(regionId)) {
    misses_.fetch_add(1, std::memory_order_relaxed);
    throw std::out_of_range("Region route is unavailable or stale");
  }
  hits_.fetch_add(1, std::memory_order_relaxed);
  return route;
}

bool RegionCache::ReplaceInterval(std::vector<RegionMetadata> descriptors,
                                  uint64_t revision) {
  if (descriptors.empty()) throw std::invalid_argument("Region cache update is empty");
  std::sort(descriptors.begin(), descriptors.end(),
            [](const RegionMetadata& lhs, const RegionMetadata& rhs) {
              return RegionBytewiseLess(lhs.startKey, rhs.startKey);
            });
  for (size_t index = 1; index < descriptors.size(); ++index) {
    if (descriptors[index - 1].endKey != descriptors[index].startKey) {
      throw std::invalid_argument("Region cache update contains a gap or overlap");
    }
  }

  std::lock_guard<std::mutex> publicationLock(publicationMutex_);
  const auto current = Snapshot();
  if (revision < current->revision) {
    staleUpdates_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  const std::string start = descriptors.front().startKey;
  const std::string end = descriptors.back().endKey;
  std::vector<RegionMetadata> replaced;
  std::vector<RegionMetadata> retained;
  for (const auto& region : current->regions) {
    if (Intersects(region, start, end)) replaced.push_back(region);
    else retained.push_back(region);
  }
  if (replaced.empty() || replaced.front().startKey != start ||
      replaced.back().endKey != end) {
    throw std::invalid_argument("Region cache update does not exactly cover old interval");
  }
  ValidateEpochAdvance(replaced, descriptors);
  for (auto& descriptor : descriptors) {
    if (descriptor.metadataRevision != revision) {
      throw std::invalid_argument("Region cache update mixes metadata revisions");
    }
  }
  retained.insert(retained.end(), descriptors.begin(), descriptors.end());
  auto replacement = std::make_shared<RouteTable>(std::move(retained), revision);
  std::atomic_store_explicit(&published_, std::shared_ptr<const RouteTable>(replacement),
                             std::memory_order_release);
  {
    std::lock_guard<std::mutex> refreshLock(refreshMutex_);
    for (const auto& old : replaced) invalidated_.erase(old.regionId);
    for (const auto& descriptor : descriptors) invalidated_.erase(descriptor.regionId);
  }
  return true;
}

void RegionCache::Invalidate(int regionId) {
  std::lock_guard<std::mutex> lock(refreshMutex_);
  invalidated_.insert(regionId);
}

bool RegionCache::IsInvalidated(int regionId) const {
  std::lock_guard<std::mutex> lock(refreshMutex_);
  return invalidated_.find(regionId) != invalidated_.end();
}

std::string RegionCache::RefreshSlotKey(const std::string& key,
                                        const RegionRouteHandle& current) const {
  return current ? "region:" + std::to_string(current.Descriptor().regionId) : "key:" + key;
}

RegionRouteHandle RegionCache::LookupOrRefresh(const std::string& key,
                                               Clock::time_point deadline,
                                               const RefreshFunction& refresh) {
  RegionRouteHandle current = FindKey(Snapshot(), key);
  if (current && !IsInvalidated(current.Descriptor().regionId)) {
    hits_.fetch_add(1, std::memory_order_relaxed);
    return current;
  }
  misses_.fetch_add(1, std::memory_order_relaxed);
  const std::string slotKey = RefreshSlotKey(key, current);
  std::shared_ptr<RefreshSlot> slot;
  bool leader = false;
  {
    std::lock_guard<std::mutex> lock(refreshMutex_);
    const auto found = refreshSlots_.find(slotKey);
    if (found == refreshSlots_.end()) {
      slot = std::make_shared<RefreshSlot>();
      refreshSlots_.emplace(slotKey, slot);
      leader = true;
      refreshLeaders_.fetch_add(1, std::memory_order_relaxed);
    } else {
      slot = found->second;
      refreshWaiters_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  if (leader) {
    try {
      RefreshResult response = refresh(key, deadline);
      ReplaceInterval(std::move(response.regions), response.revision);
      refreshSuccesses_.fetch_add(1, std::memory_order_relaxed);
    } catch (...) {
      slot->error = std::current_exception();
      refreshFailures_.fetch_add(1, std::memory_order_relaxed);
    }
    {
      std::lock_guard<std::mutex> lock(refreshMutex_);
      slot->done = true;
      refreshSlots_.erase(slotKey);
    }
    slot->condition.notify_all();
  } else {
    std::unique_lock<std::mutex> lock(refreshMutex_);
    if (!slot->condition.wait_until(lock, deadline, [&] { return slot->done; })) {
      waiterTimeouts_.fetch_add(1, std::memory_order_relaxed);
      throw std::runtime_error("Region refresh waiter deadline exhausted");
    }
  }
  if (slot->error) std::rethrow_exception(slot->error);
  RegionRouteHandle result = FindKey(Snapshot(), key);
  if (!result || IsInvalidated(result.Descriptor().regionId)) {
    throw std::out_of_range("metadata refresh did not produce a usable Region route");
  }
  return result;
}

RegionCacheMetrics RegionCache::Metrics() const {
  return {hits_.load(std::memory_order_relaxed),
          misses_.load(std::memory_order_relaxed),
          refreshLeaders_.load(std::memory_order_relaxed),
          refreshWaiters_.load(std::memory_order_relaxed),
          refreshSuccesses_.load(std::memory_order_relaxed),
          refreshFailures_.load(std::memory_order_relaxed),
          waiterTimeouts_.load(std::memory_order_relaxed),
          staleUpdates_.load(std::memory_order_relaxed)};
}
