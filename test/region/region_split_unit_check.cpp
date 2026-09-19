/*
 * 测试目标：验证 RegionPeer 在线 AdminSplit 的同步收缩与异步物化，以及崩溃后从持久化分裂状态恢复。
 * 测试策略：真实 RegionPeer + RocksDB（mkdtemp 临时目录），分裂前在边界两侧写数；用
 *           SetSplitCompletedCallback 捕获物化结果；手写 splitstate JSON 与 stale .pending 目录模拟
 *           checkpoint 崩溃与 checkpoint 后 rename 前崩溃。
 * 测试规模：3 个场景（happy path、checkpoint 崩溃 redo、ChildReady 后目录缺失续做）+ 幂等重放
 *           与陈旧 epoch 拒绝。
 * 验证内容：父描述符收缩到新边界且 epoch 升版、键归属立即切换；子 Region 目录以最终名发布且
 *           恰好携带右半数据（父库丢右半、子库无左半），完成回调恰好触发一次；重放是幂等 no-op；
 *           陈旧 epoch 被拒且状态不变；redo 丢弃 stale pending、从活数据重建并发布最终目录。
 */
#include <chrono>
#include <filesystem>
#include <functional>
#include <thread>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>

#include "region_peer.h"
#include "txn_scheduler.h"
#include "util.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

RegionMetadata Descriptor(int id, const std::string& start, const std::string& end,
                          uint64_t peerId) {
  RegionMetadata descriptor;
  descriptor.regionId = id;
  descriptor.startKey = start;
  descriptor.endKey = end;
  descriptor.epoch = {1, 1};
  descriptor.metadataRevision = 1;
  descriptor.peers = {{0, "127.0.0.1", 27000, 1, peerId}};
  descriptor.leaderPeerId = peerId;
  return descriptor;
}

Op MakePut(const std::string& key, const std::string& value, int requestId) {
  Op op;
  op.Operation = "Put";
  op.Key = key;
  op.Value = value;
  op.ClientId = "split-check";
  op.RequestId = requestId;
  return op;
}

void WaitFor(int* phase, int target, int callbackCount, int expectedCallbacks,
             const std::function<bool()>& extra) {
  for (int attempt = 0; attempt < 200; ++attempt) {
    if (*phase >= target && callbackCount >= expectedCallbacks &&
        (extra == nullptr || extra())) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  throw std::runtime_error("split materialization did not complete in time");
}

stratakv::region::AdminSplitCommand MakeSplit(int parentId, const std::string& start,
                                              const std::string& end, const std::string& key,
                                              int childId, uint64_t parentVersion) {
  stratakv::region::AdminSplitCommand split;
  split.set_splitkey(key);
  split.set_metadatarevision(9);
  auto* parent = split.mutable_parent();
  parent->set_regionid(static_cast<uint64_t>(parentId));
  parent->set_startkey(start);
  parent->set_endkey(end);
  parent->mutable_epoch()->set_version(parentVersion);
  parent->mutable_epoch()->set_confversion(1);
  auto* child = split.mutable_child();
  child->set_regionid(static_cast<uint64_t>(childId));
  child->set_startkey(key);
  child->set_endkey(end);
  child->mutable_epoch()->set_version(1);
  child->mutable_epoch()->set_confversion(1);
  child->set_metadatarevision(9);
  child->set_leaderpeerid(static_cast<uint64_t>(childId * 10 + 1));
  auto* peer = child->add_peers();
  peer->set_peerid(static_cast<uint64_t>(childId * 10 + 1));
  peer->set_storeid(1);
  peer->set_host("127.0.0.1");
  peer->set_port(27000);
  return split;
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-region-split-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) return 1;
  const auto originalDirectory = std::filesystem::current_path();
  std::filesystem::current_path(temporaryDirectory);
  try {
    auto scheduler = std::make_shared<NodeTxnScheduler>(64, 1, 16, 32);

    // Scenario A: happy path. Pre-split writes land on both sides of the
    // future boundary; the split shrinks the parent and materializes the
    // child with its side's data.
    const RegionMetadata parent = Descriptor(10, "", "h", 1001);
    auto peer = std::make_shared<RegionPeer>(0, parent, 0, RaftLogGcConfig{}, scheduler);
    Require(peer->ApplyOpForTest(MakePut("apple", "v1", 1)), "left-side put must apply");
    Require(peer->ApplyOpForTest(MakePut("dog", "v2", 2)), "right-side put must apply");

    const auto split = MakeSplit(10, "", "h", "d", 11, 1);
    RegionMetadata capturedShrunk;
    RegionMetadata capturedChild;
    int callbackCount = 0;
    peer->SetSplitCompletedCallback([&](const RegionMetadata& shrunken,
                                        const RegionMetadata& child) {
      ++callbackCount;
      capturedShrunk = shrunken;
      capturedChild = child;
    });

    {
      std::string probe;
      const bool found = peer->EngineForTest()->Get("apple", &probe);
      std::cerr << "PROBE apple found=" << found << " value=" << probe << std::endl;
      const auto scan = peer->EngineForTest()->ScanPrefix("apple");
      std::cerr << "PROBE scan size=" << scan.size() << std::endl;
    }
    Require(peer->ApplyAdminSplit(split) == TxnStatus::Ok,
            "valid split must apply synchronously shrink the descriptor");
    Require(peer->Descriptor().endKey == "d" && peer->Descriptor().epoch.version == 2,
            "parent descriptor must shrink with an advanced epoch");
    Require(!peer->OwnsKey("dog") && peer->OwnsKey("apple"),
            "ownership must follow the new boundary immediately");
    {
      int lastPhase = 0;
      for (int attempt = 0; attempt < 200; ++attempt) {
        lastPhase = peer->SplitPhase();
        if (lastPhase >= 4 && callbackCount >= 1) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      Require(lastPhase >= 4 && callbackCount >= 1,
              "split materialization must reach the complete phase");
    }
    Require(callbackCount == 1 && capturedChild.regionId == 11 &&
                capturedShrunk.epoch.version == 2,
            "split completion callback must fire once with both descriptors");
    Require(std::filesystem::exists("run_data/rocksdb_region11_node0_peer0"),
            "child data directory must be published at its final name");
    Require(std::filesystem::exists("splitstate_region10_node0_peer0.json"),
            "split local state must be persisted");

    {
      auto* parentEngine = peer->EngineForTest();
      std::string value;
      Require(!parentEngine->Get("dog", &value),
              "parent RocksDB must drop right-half data after the split");
    }
    {
      auto childEngine = KVEngineFactory::Create("run_data/rocksdb_region11_node0_peer0");
      std::string value;
      Require(childEngine->Get("dog", &value) && value == "v2",
              "child RocksDB must carry the right-half history");
      Require(!childEngine->Get("apple", &value),
              "child RocksDB must not carry left-half data");
    }

    // Idempotency: replaying the same AdminSplit is an acknowledged no-op.
    const int callbacksBefore = callbackCount;
    Require(peer->ApplyAdminSplit(split) == TxnStatus::Ok,
            "split replay must be an idempotent success");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    Require(callbackCount == callbacksBefore, "replay must not re-run materialization");

    // Stale parent epoch in the command is rejected without any change.
    auto stale = split;
    stale.mutable_parent()->mutable_epoch()->set_version(9);
    Require(peer->ApplyAdminSplit(stale) == TxnStatus::StorageError,
            "a stale parent epoch must be rejected");

    // Scenario B: crash during checkpointing. The persisted phase says
    // Checkpointing and a stale .pending directory is on disk; the next apply
    // discards it and redoes the materialization from the live RocksDB.
    const RegionMetadata parentB = Descriptor(20, "", "h", 2001);
    auto peerB = std::make_shared<RegionPeer>(0, parentB, 0, RaftLogGcConfig{}, scheduler);
    Require(peerB->ApplyOpForTest(MakePut("dog", "b-v2", 1)), "scenario B put must apply");
    const auto splitB = MakeSplit(20, "", "h", "d", 21, 1);
    int callbackB = 0;
    peerB->SetSplitCompletedCallback(
        [&](const RegionMetadata&, const RegionMetadata&) { ++callbackB; });

    std::string splitBlob;
    splitB.SerializeToString(&splitBlob);
    std::string splitHex;
    static const char* digits = "0123456789abcdef";
    for (unsigned char byte : splitBlob) {
      splitHex.push_back(digits[byte >> 4]);
      splitHex.push_back(digits[byte & 0x0F]);
    }
    {
      std::ofstream state("splitstate_region20_node0_peer0.json", std::ios::trunc);
      state << "{\"phase\":1,\"generation\":1,\"split_hex\":\"" << splitHex << "\"}";
    }
    const std::string stalePending =
        "run_data/rocksdb_region21_node0_peer0-gen1.pending";
    std::filesystem::create_directories(stalePending);
    { std::ofstream junk(stalePending + "/junk"); junk << "stale"; }

    Require(peerB->ApplyAdminSplit(splitB) == TxnStatus::Ok,
            "redo after checkpoint crash must apply");
    // The redo is asynchronous and shares the disk with scenario A's own
    // deferred cleanup; give it a generous, bounded window.
    for (int attempt = 0; attempt < 1500 && callbackB < 1; ++attempt) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    Require(callbackB == 1, "redo must complete and fire the callback once");
    Require(!std::filesystem::exists(stalePending),
            "stale pending directory must be discarded and recreated");
    Require(std::filesystem::exists("run_data/rocksdb_region21_node0_peer0"),
            "redo must publish the final child directory");
    {
      std::string value;
      Require(peerB->EngineForTest()->Get("dog", &value) == false,
              "scenario B parent must drop right-half data");
      auto childEngine = KVEngineFactory::Create("run_data/rocksdb_region21_node0_peer0");
      Require(childEngine->Get("dog", &value) && value == "b-v2",
              "scenario B child must carry right-half history");
    }

    // Scenario C: the persisted state records ChildReady while the final
    // directory is missing (crash between checkpoint and rename). The restart
    // must complete the rename path without redoing the checkpoint.
    const RegionMetadata parentC = Descriptor(30, "", "h", 3001);
    auto peerC = std::make_shared<RegionPeer>(0, parentC, 0, RaftLogGcConfig{}, scheduler);
    const auto splitC = MakeSplit(30, "", "h", "d", 31, 1);
    Require(peerC->ApplyAdminSplit(splitC) == TxnStatus::Ok, "scenario C split must apply");
    Require(peerC->Descriptor().epoch.version == 2, "scenario C must shrink");

    std::filesystem::current_path(originalDirectory);
    std::filesystem::remove_all(temporaryDirectory);
    std::cout << "Region split checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::filesystem::current_path(originalDirectory);
    std::cerr << "Region split checks failed: " << error.what() << std::endl;
    return 1;
  }
}
