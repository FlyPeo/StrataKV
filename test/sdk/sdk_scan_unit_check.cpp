/*
 * 测试目标:范围扫描(Scan)的快照一致性、跨 Region 聚合、limit/续读与冲突语义。
 * 测试策略:进程内构造三个按 key range 划分的 MvccStorage Region + ShardRouter
 *           + DistributedTransactionCoordinator,不启动网络与 Raft。
 * 验证内容:跨 Region 有序聚合、limit 截断与 last-key 续读无缝、快照后提交
 *           不可见、已删除键不出现、未完成锁整批返回可重试 LockConflict。
 */
#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "distributed_transaction_coordinator.h"
#include "mvcc_storage.h"
#include "shard_router.h"
#include "timestamp_oracle.h"

namespace {

class MemoryEngine final : public IKVEngine {
 public:
  bool Put(const std::string& key, const std::string& value) override {
    return WriteBatch({{KVBatchOpType::Put, key, value}});
  }
  bool Get(const std::string& key, std::string* value) override {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = data_.find(key);
    if (found == data_.end()) return false;
    *value = found->second;
    return true;
  }
  bool Append(const std::string& key, const std::string& value) override {
    std::lock_guard<std::mutex> lock(mutex_);
    data_[key] += value;
    return true;
  }
  bool Delete(const std::string& key) override {
    return WriteBatch({{KVBatchOpType::Delete, key, {}}});
  }
  bool WriteBatch(const std::vector<KVBatchOp>& ops) override {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& op : ops) {
      if (op.type == KVBatchOpType::Delete) data_.erase(op.key);
      else data_[op.key] = op.value;
    }
    return true;
  }
  std::vector<std::pair<std::string, std::string>> ScanPrefix(
      const std::string& prefix) override {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::pair<std::string, std::string>> values;
    for (const auto& item : data_) {
      if (item.first.rfind(prefix, 0) == 0) values.push_back(item);
    }
    return values;
  }
  std::string Dump() override { return {}; }
  bool Load(const std::string&) override { return false; }
  void DebugPrint() override {}

 private:
  std::mutex mutex_;
  std::map<std::string, std::string> data_;
};

class CountingOracle final : public TimestampOracle {
 public:
  uint64_t Next() override { return ++ts_; }
  uint64_t Peek() override { return ts_; }
  void Observe(uint64_t ts) override { ts_ = std::max(ts_, ts_); }

 private:
  uint64_t ts_ = 100;
};

struct Fixture {
  std::vector<std::shared_ptr<MvccStorage>> storages;
  std::shared_ptr<ShardRouter> router;
  std::shared_ptr<DistributedTransactionCoordinator> coordinator;

  Fixture() {
    const std::vector<std::pair<std::string, std::string>> ranges = {
        {"", "h"}, {"h", "p"}, {"p", ""}};
    std::vector<ShardRouter::RegionRoute> routes;
    int regionId = 100;
    for (const auto& [start, end] : ranges) {
      RegionMetadata region;
      region.regionId = regionId++;
      region.startKey = start;
      region.endKey = end;
      region.epoch.version = 1;
      region.epoch.confVersion = 1;
      region.metadataRevision = 1;
      region.peers.push_back(RegionPeerLocation{static_cast<int>(regionId), "127.0.0.1",
                                                static_cast<short>(20000 + regionId),
                                                static_cast<uint64_t>(regionId),
                                                static_cast<uint64_t>(regionId)});
      auto storage = std::make_shared<MvccStorage>(std::make_shared<MemoryEngine>());
      storages.push_back(storage);
      routes.push_back({region, std::move(storage)});
    }
    router = std::make_shared<ShardRouter>(std::move(routes));
    coordinator = std::make_shared<DistributedTransactionCoordinator>(
        router, std::make_shared<CountingOracle>());
  }
};

std::string JoinKeys(const std::vector<std::pair<std::string, std::string>>& entries) {
  std::string joined;
  for (const auto& [key, value] : entries) {
    if (!joined.empty()) joined += ',';
    joined += key + '=' + value;
  }
  return joined;
}

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

}  // namespace

int main() {
  try {
    Fixture fx;

    // 三个 Region 各提交两个键。
    for (const char* key : {"alpha", "echo", "hotel", "kilo", "papa", "romeo"}) {
      auto tx = fx.coordinator->Begin();
      tx.Put(key, "v-" + std::string(key));
      Require(fx.coordinator->Commit(&tx, {}) == TxnStatus::Ok, "seed put must commit");
    }

    // 三个 Region 全空间扫描:有序聚合。
    {
      auto tx = fx.coordinator->Begin();
      std::vector<std::pair<std::string, std::string>> entries;
      Require(fx.coordinator->Scan(&tx, "", "", 0, &entries, {}) == TxnStatus::Ok,
              "full scan must succeed");
      Require(entries.size() == 6, "full scan must see all six entries: " + JoinKeys(entries));
      Require(std::is_sorted(entries.begin(), entries.end()),
              "scan output must be ascending");
    }

    // limit 截断 + last-key 后继续读无缝。
    {
      auto tx = fx.coordinator->Begin();
      std::vector<std::pair<std::string, std::string>> first;
      Require(fx.coordinator->Scan(&tx, "", "", 2, &first, {}) == TxnStatus::Ok,
              "limited scan must succeed");
      Require(first.size() == 2, "limit must cap entries");
      std::vector<std::pair<std::string, std::string>> rest;
      Require(fx.coordinator->Scan(&tx, first.back().first + "\x01", "", 0, &rest, {}) ==
                  TxnStatus::Ok,
              "continuation scan must succeed");
      Require(rest.size() == 4, "continuation must see remaining entries");
      Require(first.front().first < rest.front().first, "no overlap between pages");
    }

    // 快照后提交不可见。
    {
      auto tx = fx.coordinator->Begin();
      auto later = fx.coordinator->Begin();
      later.Put("alpha", "changed");
      Require(fx.coordinator->Commit(&later, {}) == TxnStatus::Ok, "later put must commit");
      std::vector<std::pair<std::string, std::string>> entries;
      Require(fx.coordinator->Scan(&tx, "", "", 0, &entries, {}) == TxnStatus::Ok,
              "snapshot scan must succeed");
      const auto it = std::find_if(entries.begin(), entries.end(), [](const auto& e) {
        return e.first == "alpha";
      });
      Require(it != entries.end() && it->second == "v-alpha",
              "snapshot must hide later commits");
    }

    // 已删除键不出现。
    {
      auto tx = fx.coordinator->Begin();
      tx.Delete("echo");
      Require(fx.coordinator->Commit(&tx, {}) == TxnStatus::Ok, "delete must commit");
      auto readTx = fx.coordinator->Begin();
      std::vector<std::pair<std::string, std::string>> entries;
      const TxnStatus st = fx.coordinator->Scan(&readTx, "", "", 0, &entries, {});
      Require(st == TxnStatus::Ok,
              "post-delete scan must succeed, got " + std::to_string(static_cast<int>(st)));
      Require(std::none_of(entries.begin(), entries.end(), [](const auto& e) {
        return e.first == "echo";
      }), "deleted key must not appear");
    }

    // 未完成锁:整批 LockConflict,可重试,不返回部分结果。
    {
      const auto conflicting = fx.storages[0];
      // Region 100 内直接挂一个未完成的 prewrite 锁,不进入 2PC 提交。
      Require(conflicting->Prewrite("bravo", "locked-value", "bravo", 50, 120000) ==
                  TxnStatus::Ok,
              "manual prewrite must succeed");
      auto tx = fx.coordinator->Begin();
      std::vector<std::pair<std::string, std::string>> entries;
      Require(fx.coordinator->Scan(&tx, "", "", 0, &entries, {}) == TxnStatus::LockConflict,
              "locked key must fail the whole scan");
      Require(entries.empty(), "conflicted scan must not return partial entries");
      std::vector<std::pair<std::string, std::string>> afterLock;
      Require(fx.coordinator->Scan(&tx, "bravo\x01", "", 0, &afterLock, {}) == TxnStatus::Ok,
              "range after the locked key must scan");
    }

    std::cout << "SDK scan unit checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "SDK scan unit checks failed: " << error.what() << std::endl;
    return 1;
  }
}
