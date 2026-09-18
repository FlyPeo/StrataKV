#include <atomic>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "region_cache.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

RegionMetadata Region(int id, std::string start, std::string end, uint64_t version,
                      uint64_t revision) {
  RegionMetadata region;
  region.regionId = id;
  region.startKey = std::move(start);
  region.endKey = std::move(end);
  region.epoch = {version, 1};
  region.metadataRevision = revision;
  region.peers = {{0, "127.0.0.1", 26000, static_cast<uint64_t>(id + 1),
                   static_cast<uint64_t>(id * 10 + 1)}};
  region.leaderPeerId = region.peers.front().peerId;
  return region;
}

void CheckBoundariesAndReplacement() {
  RegionCache cache({Region(10, "", "m", 1, 1), Region(20, "m", "", 1, 1)}, 1);
  Require(cache.LookupKey("").Descriptor().regionId == 10,
          "empty lower-bound key must route left");
  Require(cache.LookupKey("m").Descriptor().regionId == 20,
          "half-open boundary must route right");
  Require(cache.LookupRegion(20).Descriptor().startKey == "m",
          "Region ID lookup must use the immutable index");

  Require(cache.ReplaceInterval(
              {Region(11, "", "g", 2, 2), Region(12, "g", "m", 2, 2)}, 2),
          "newer split response must publish");
  Require(cache.LookupKey("g").Descriptor().regionId == 12 &&
              cache.Snapshot()->regions.size() == 3,
          "split replacement must atomically expose both children");
  Require(!cache.ReplaceInterval(
              {Region(10, "", "m", 1, 1)}, 1),
          "older metadata response must be discarded");
  Require(cache.LookupKey("g").Descriptor().regionId == 12,
          "discarded response must not roll back routes");

  bool rejectedGap = false;
  try {
    cache.ReplaceInterval({Region(13, "", "f", 3, 3),
                           Region(14, "g", "m", 3, 3)}, 3);
  } catch (const std::invalid_argument&) {
    rejectedGap = true;
  }
  Require(rejectedGap, "gapped refresh must be rejected");

  bool rejectedOverlap = false;
  try {
    cache.ReplaceInterval({Region(13, "", "h", 3, 3),
                           Region(14, "g", "m", 3, 3)}, 3);
  } catch (const std::invalid_argument&) {
    rejectedOverlap = true;
  }
  Require(rejectedOverlap, "overlapping refresh must be rejected");
}

void CheckConcurrentReaders() {
  RegionCache cache({Region(10, "", "m", 1, 1), Region(20, "m", "", 1, 1)}, 1);
  std::atomic<bool> stop{false};
  std::atomic<bool> failed{false};
  std::vector<std::thread> readers;
  for (int index = 0; index < 8; ++index) {
    readers.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        const auto snapshot = cache.Snapshot();
        const size_t size = snapshot->regions.size();
        if (size != 2 && size != 3) failed.store(true, std::memory_order_relaxed);
        try {
          const int regionId = cache.LookupKey("g").Descriptor().regionId;
          if (regionId != 10 && regionId != 12) failed.store(true, std::memory_order_relaxed);
        } catch (...) {
          failed.store(true, std::memory_order_relaxed);
        }
      }
    });
  }
  cache.ReplaceInterval({Region(11, "", "g", 2, 2), Region(12, "g", "m", 2, 2)}, 2);
  stop.store(true, std::memory_order_relaxed);
  for (auto& reader : readers) reader.join();
  Require(!failed.load(std::memory_order_relaxed),
          "concurrent reader observed a partial route table");
}

void CheckRefreshCoalescingAndDeadlines() {
  RegionCache cache({Region(10, "", "m", 1, 1), Region(20, "m", "", 1, 1)}, 1);
  cache.Invalidate(10);
  std::atomic<int> fetches{0};
  std::atomic<bool> waiterTimedOut{false};
  const auto refresh = [&](const std::string&, RegionCache::Clock::time_point) {
    fetches.fetch_add(1, std::memory_order_relaxed);
    // Re-enter both read and invalidation paths. This would deadlock if the
    // cache held its refresh/publication locks across metadata I/O.
    (void)cache.Snapshot();
    cache.Invalidate(10);
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    return RegionCache::RefreshResult{
        {Region(11, "", "g", 2, 2), Region(12, "g", "m", 2, 2)}, 2};
  };
  std::thread owner([&] {
    const auto route = cache.LookupOrRefresh(
        "a", RegionCache::Clock::now() + std::chrono::seconds(2), refresh);
    if (route.Descriptor().regionId != 11) throw std::runtime_error("refresh routed wrong child");
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(15));
  std::thread waiter([&] {
    try {
      (void)cache.LookupOrRefresh(
          "b", RegionCache::Clock::now() + std::chrono::milliseconds(30), refresh);
    } catch (const std::runtime_error&) {
      waiterTimedOut.store(true, std::memory_order_relaxed);
    }
  });
  waiter.join();
  owner.join();
  Require(fetches.load(std::memory_order_relaxed) == 1,
          "equivalent stale-range misses must share one metadata lookup");
  Require(waiterTimedOut.load(std::memory_order_relaxed),
          "refresh waiters must retain independent deadlines");
  Require(cache.LookupKey("b").Descriptor().regionId == 11,
          "successful refresh must remain usable after waiter timeout");

  cache.Invalidate(11);
  bool failedRefresh = false;
  try {
    (void)cache.LookupOrRefresh(
        "b", RegionCache::Clock::now() + std::chrono::seconds(1),
        [](const std::string&, RegionCache::Clock::time_point)
            -> RegionCache::RefreshResult {
          throw std::runtime_error("metadata unavailable");
        });
  } catch (const std::runtime_error&) {
    failedRefresh = true;
  }
  Require(failedRefresh, "metadata failure must reach the refresh caller");
  const auto recovered = cache.LookupOrRefresh(
      "b", RegionCache::Clock::now() + std::chrono::seconds(1),
      [](const std::string&, RegionCache::Clock::time_point) {
        return RegionCache::RefreshResult{
            {Region(11, "", "g", 2, 2), Region(12, "g", "m", 2, 2)}, 2};
      });
  Require(recovered.Descriptor().regionId == 11,
          "refresh slot must be reusable after metadata failure");
}

}  // namespace

int main() {
  try {
    CheckBoundariesAndReplacement();
    CheckConcurrentReaders();
    CheckRefreshCoalescingAndDeadlines();
    std::cout << "Region cache checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Region cache checks failed: " << error.what() << std::endl;
    return 1;
  }
}
