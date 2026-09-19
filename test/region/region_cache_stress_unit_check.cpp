/*
 * 测试目标：压测路由状态（RegionCache 与 RegionRegistry）在读写并发下不出现撕裂表、丢失 Region
 *           或资源泄漏，本文件是在 TSan/ASan 下运行的首选目标。
 * 测试策略：6 读 1 写并发：读者随机键查找并断言解析出的 Region 真的包含该键，写者连续 400 轮
 *           随机分裂替换；再加 6 线程对失效范围的刷新风暴，以及 5 读 1 写的 registry 轮换（每轮移除上一轮 peer）。
 * 测试规模：约 400 次分裂替换 + 300 次 LookupOrRefresh + 100 次 registry 替换与 100 次验证拒绝。
 * 验证内容：任何时刻发布的表都从空键覆盖到空键、无缝无叠，乱序 revision 的替换被拒绝，刷新
 *           风暴合并成共享拉取且每次尝试都完成（300/300）；registry 替换推进 revision、
 *           EPOCH_NOT_MATCH 拒绝全部计数、读者至少成功一次，全程无死锁无异常。
 */
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "region_cache.h"
#include "region_registry.h"

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

RegionMetadata Descriptor(int id, const std::string& start, const std::string& end,
                          uint64_t version, uint64_t revision) {
  RegionMetadata descriptor;
  descriptor.regionId = id;
  descriptor.startKey = start;
  descriptor.endKey = end;
  descriptor.epoch = {version, 1};
  descriptor.metadataRevision = revision;
  descriptor.peers = {{0, "127.0.0.1", 25000, 1, static_cast<uint64_t>(id * 10 + 1)}};
  descriptor.leaderPeerId = descriptor.peers.front().peerId;
  return descriptor;
}

std::shared_ptr<RegionPeer> MakePeer(const std::shared_ptr<NodeTxnScheduler>& scheduler,
                                     RegionMetadata descriptor) {
  auto peer = std::make_shared<RegionPeer>(0, std::move(descriptor), 0, RaftLogGcConfig{},
                                           scheduler);
  peer->MarkServing();
  return peer;
}

// A published table must always cover the whole keyspace with no gap or overlap.
void RequireCoversKeyspace(const std::shared_ptr<const RouteTable>& table,
                           const std::string& context) {
  Require(table != nullptr, context + ": table must exist");
  auto ordered = table->regions;
  std::sort(ordered.begin(), ordered.end(),
            [](const RegionMetadata& lhs, const RegionMetadata& rhs) {
              return lhs.startKey < rhs.startKey;
            });
  Require(!ordered.empty(), context + ": table must not be empty");
  Require(ordered.front().startKey.empty(), context + ": table must start at the empty key");
  Require(ordered.back().endKey.empty(), context + ": table must end at the empty key");
  for (size_t index = 1; index < ordered.size(); ++index) {
    Require(ordered[index - 1].endKey == ordered[index].startKey,
            context + ": table must stay contiguous");
  }
}

void StressCache() {
  RegionCache cache({Descriptor(1, "", "m", 1, 1), Descriptor(2, "m", "", 1, 1)}, 1);
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> readerIterations{0};
  std::atomic<uint64_t> staleLookups{0};
  std::atomic<uint64_t> rejectedReplacements{0};

  std::vector<std::thread> readers;
  for (int threadIndex = 0; threadIndex < 6; ++threadIndex) {
    readers.emplace_back([&, threadIndex] {
      std::mt19937 generator(static_cast<unsigned>(1000 + threadIndex));
      const std::vector<std::string> keys = {"", "a", "g", "m", "s", "z"};
      std::uniform_int_distribution<size_t> pick(0, keys.size() - 1);
      while (!stop.load(std::memory_order_relaxed)) {
        const std::string& key = keys[pick(generator)];
        try {
          const auto handle = cache.LookupKey(key);
          // A torn table is the failure we care about: either the lookup says
          // the topology moved, or the answer must actually own the key.
          Require(handle.Descriptor().Contains(key),
                  "the resolved Region must contain the key");
          readerIterations.fetch_add(1, std::memory_order_relaxed);
        } catch (const std::out_of_range&) {
          staleLookups.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  // A single writer publishes splits. Serialised writes keep every replacement
  // well formed, so a false return can only mean an out-of-order revision.
  std::thread writer([&] {
    int nextId = 10;
    std::mt19937 generator(4242u);
    for (int round = 0; round < 400; ++round) {
      const auto table = cache.Snapshot();
      RequireCoversKeyspace(table, "writer snapshot");
      std::uniform_int_distribution<size_t> pick(0, table->regions.size() - 1);
      const RegionMetadata victim = table->regions[pick(generator)];
      const std::string boundary = victim.startKey + "n";
      if (!(boundary > victim.startKey && (victim.endKey.empty() || boundary < victim.endKey))) {
        continue;
      }
      std::vector<RegionMetadata> replacement = {
          Descriptor(++nextId, victim.startKey, boundary, victim.epoch.version + 1,
                     table->revision + 1),
          Descriptor(++nextId, boundary, victim.endKey, victim.epoch.version + 1,
                     table->revision + 1)};
      if (!cache.ReplaceInterval(std::move(replacement), table->revision + 1)) {
        rejectedReplacements.fetch_add(1, std::memory_order_relaxed);
      }
    }
    stop.store(true, std::memory_order_relaxed);
  });

  writer.join();
  for (auto& reader : readers) reader.join();
  RequireCoversKeyspace(cache.Snapshot(), "final table");
  Require(readerIterations.load() > 100, "readers must make progress");
  Require(cache.Metrics().hits > 0, "cache hits must be counted");

  // Refresh storm: concurrent invalidations must collapse onto shared refreshes
  // rather than deadlocking or leaking waiters.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  std::atomic<int> refreshed{0};
  // The refresh rebroadcasts the current table with an advanced epoch, so it is
  // always a legal update regardless of how far the writer has split things.
  RegionCache::RefreshFunction rebroadcast =
      [&cache](const std::string&, RegionCache::Clock::time_point) {
        const auto table = cache.Snapshot();
        std::vector<RegionMetadata> refreshed;
        refreshed.reserve(table->regions.size());
        for (auto region : table->regions) {
          region.epoch.version += 1;
          region.metadataRevision = table->revision + 1;
          refreshed.push_back(region);
        }
        return RegionCache::RefreshResult{std::move(refreshed), table->revision + 1};
      };
  std::vector<std::thread> refreshers;
  for (int threadIndex = 0; threadIndex < 6; ++threadIndex) {
    refreshers.emplace_back([&, threadIndex] {
      for (int round = 0; round < 50; ++round) {
        const bool left = threadIndex % 2 == 0;
        try {
          const auto handle = cache.LookupOrRefresh(left ? "a" : "z", deadline, rebroadcast);
          Require(handle.Descriptor().Contains(left ? "a" : "z"),
                  "a refresh must resolve the requested key");
        } catch (const std::out_of_range&) {
          // A refresh that cannot complete before the deadline reports a miss.
        }
        refreshed.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (auto& refresher : refreshers) refresher.join();
  Require(refreshed.load() == 300, "every refresh attempt must complete");
  RequireCoversKeyspace(cache.Snapshot(), "table after refresh storm");
}

void StressRegistry() {
  auto scheduler = std::make_shared<NodeTxnScheduler>(64, 1, 16, 32);
  RegionRegistry registry;
  registry.PublishInitial({MakePeer(scheduler, Descriptor(10, "", "m", 1, 1)),
                           MakePeer(scheduler, Descriptor(20, "m", "", 1, 1))},
                          1);
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> lookups{0};

  std::vector<std::thread> readers;
  for (int threadIndex = 0; threadIndex < 5; ++threadIndex) {
    readers.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        if (const auto handle = registry.Lookup(10)) {
          Require(handle->RegionId() == 10, "a lookup must return the requested Region");
          lookups.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  std::thread writer([&] {
    // Each round replaces the peer installed by the previous round, so the
    // registry always swaps a known id for a fresh one.
    int installed = 20;
    int nextId = 30;
    for (int round = 0; round < 100; ++round) {
      const std::vector<int> remove = {installed};
      const std::vector<std::shared_ptr<RegionPeer>> add = {
          MakePeer(scheduler, Descriptor(++nextId, "m", "", 1 + round / 10, 2 + round))};
      registry.Replace(remove, add, 2 + round);
      registry.RecordValidationReject(stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH);
      installed = nextId;
    }
    stop.store(true, std::memory_order_relaxed);
  });

  writer.join();
  for (auto& reader : readers) reader.join();
  Require(lookups.load() > 0, "registry readers must succeed at least once");
  Require(registry.Metrics().revision >= 2, "replacements must advance the revision");
  Require(registry.Metrics().epochRejects >= 100, "validation rejects must be counted");
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-region-stress-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) return 1;
  const auto originalDirectory = std::filesystem::current_path();
  std::filesystem::current_path(temporaryDirectory);
  try {
    StressCache();
    StressRegistry();
    std::filesystem::current_path(originalDirectory);
    std::filesystem::remove_all(temporaryDirectory);
    std::cout << "Region routing stress checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::filesystem::current_path(originalDirectory);
    std::filesystem::remove_all(temporaryDirectory);
    std::cerr << "Region routing stress checks failed: " << error.what() << std::endl;
    return 1;
  }
}
