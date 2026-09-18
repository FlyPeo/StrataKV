#ifndef STRATAKV_TRANSACTION_REGION_CACHE_H
#define STRATAKV_TRANSACTION_REGION_CACHE_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "region_metadata.h"

struct RouteTable {
  RouteTable(std::vector<RegionMetadata> descriptors, uint64_t metadataRevision);

  std::vector<RegionMetadata> regions;
  std::unordered_map<int, size_t> byRegionId;
  uint64_t revision = 0;
};

struct RegionRouteHandle {
  std::shared_ptr<const RouteTable> table;
  size_t index = 0;

  const RegionMetadata& Descriptor() const { return table->regions.at(index); }
  explicit operator bool() const { return table && index < table->regions.size(); }
};

struct RegionCacheMetrics {
  uint64_t hits = 0;
  uint64_t misses = 0;
  uint64_t refreshLeaders = 0;
  uint64_t refreshWaiters = 0;
  uint64_t refreshSuccesses = 0;
  uint64_t refreshFailures = 0;
  uint64_t waiterTimeouts = 0;
  uint64_t staleUpdates = 0;
};

class RegionCache {
 public:
  using Clock = std::chrono::steady_clock;

  struct RefreshResult {
    std::vector<RegionMetadata> regions;
    uint64_t revision = 0;
  };
  using RefreshFunction =
      std::function<RefreshResult(const std::string& key, Clock::time_point deadline)>;

  explicit RegionCache(std::vector<RegionMetadata> regions, uint64_t revision = 0);

  std::shared_ptr<const RouteTable> Snapshot() const;
  RegionRouteHandle LookupKey(const std::string& key) const;
  RegionRouteHandle LookupRegion(int regionId) const;

  // Replaces exactly the old interval covered by descriptors. Returns false
  // for an out-of-order response and throws if the response is malformed.
  bool ReplaceInterval(std::vector<RegionMetadata> descriptors, uint64_t revision);

  void Invalidate(int regionId);
  RegionRouteHandle LookupOrRefresh(const std::string& key, Clock::time_point deadline,
                                    const RefreshFunction& refresh);
  RegionCacheMetrics Metrics() const;

 private:
  struct RefreshSlot {
    std::condition_variable condition;
    bool done = false;
    std::exception_ptr error;
  };

  static RegionRouteHandle FindKey(const std::shared_ptr<const RouteTable>& table,
                                   const std::string& key);
  static RegionRouteHandle FindRegion(const std::shared_ptr<const RouteTable>& table,
                                      int regionId);
  bool IsInvalidated(int regionId) const;
  std::string RefreshSlotKey(const std::string& key,
                             const RegionRouteHandle& current) const;

  mutable std::shared_ptr<const RouteTable> published_;
  mutable std::mutex publicationMutex_;
  mutable std::mutex refreshMutex_;
  std::unordered_map<std::string, std::shared_ptr<RefreshSlot>> refreshSlots_;
  std::unordered_set<int> invalidated_;

  mutable std::atomic<uint64_t> hits_{0};
  mutable std::atomic<uint64_t> misses_{0};
  std::atomic<uint64_t> refreshLeaders_{0};
  std::atomic<uint64_t> refreshWaiters_{0};
  std::atomic<uint64_t> refreshSuccesses_{0};
  std::atomic<uint64_t> refreshFailures_{0};
  std::atomic<uint64_t> waiterTimeouts_{0};
  std::atomic<uint64_t> staleUpdates_{0};
};

#endif  // STRATAKV_TRANSACTION_REGION_CACHE_H
