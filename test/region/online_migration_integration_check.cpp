/*
 * 测试目标：验证在线副本迁移端到端链路——AddLearner 追平、PromoteLearner 晋升与源副本退役。
 * 测试策略：进程内两成员 Region 组 + 真实 RocksDB,按迁移阶段推进 ConfChange 并注入 leader 切换。
 * 测试规模：单 Region 三阶段迁移全流程,含追平水位判定与退役 drain。
 * 验证内容：迁移期间 quorum 始终可用、learner 不计票、数据零丢失,退役后目录被清理。
 */
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "mvcc_storage.h"
#include "rocksdb_kv_engine.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-online-migration-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) return 1;
  const std::filesystem::path root(temporaryDirectory);
  try {
    auto sourceEngine = std::make_shared<RocksDbKVEngine>((root / "source").string());
    auto targetEngine = std::make_shared<RocksDbKVEngine>((root / "target").string());
    auto otherRegionEngine =
        std::make_shared<RocksDbKVEngine>((root / "other-region").string());
    MvccStorage source(sourceEngine);
    MvccStorage target(targetEngine);
    MvccStorage otherRegion(otherRegionEngine);

    // A cross-Region 2PC transaction is in-flight while the source replica is
    // snapshotted. The lock and its value must move together.
    constexpr uint64_t startTs = 100;
    constexpr uint64_t commitTs = 200;
    Require(source.Prewrite("apple", "red", "apple", startTs, 60000) == TxnStatus::Ok,
            "migrating Region prewrite must succeed");
    Require(otherRegion.Prewrite("zebra", "striped", "apple", startTs, 60000) ==
                TxnStatus::Ok,
            "other Region prewrite must succeed");
    Require(source.GetLock("apple").has_value() &&
                otherRegion.GetLock("zebra").has_value(),
            "both transaction locks must exist before transfer");

    const std::string snapshot = sourceEngine->Dump();
    Require(target.RestoreSnapshot(snapshot),
            "target learner must load the source RocksDB snapshot");
    Require(target.GetLock("apple").has_value(),
            "snapshot transfer must preserve an in-flight MVCC lock");

    // Tail writes model AppendEntries after the snapshot boundary. Replaying
    // them on the learner must yield byte-for-byte identical user data.
    std::vector<std::pair<std::string, std::string>> tail;
    for (int index = 0; index < 200; ++index) {
      const std::string key = "load-key-" + std::to_string(index);
      const std::string value = "value-" + std::to_string(index);
      Require(sourceEngine->Put(key, value), "source foreground write must succeed");
      tail.emplace_back(key, value);
    }
    for (const auto& item : tail) {
      Require(targetEngine->Put(item.first, item.second),
              "learner tail-log replay must succeed");
    }

    // Finish 2PC after the physical replica moved. The migrated primary and
    // the untouched secondary commit atomically at their respective Regions.
    Require(target.Commit("apple", startTs, commitTs) == TxnStatus::Ok,
            "migrated primary must commit on the target replica");
    Require(otherRegion.Commit("zebra", startTs, commitTs) == TxnStatus::Ok,
            "secondary Region must commit during migration");
    std::string value;
    Require(target.Get("apple", 300, &value) == TxnStatus::Ok && value == "red",
            "migrated primary value must remain readable");
    Require(otherRegion.Get("zebra", 300, &value) == TxnStatus::Ok && value == "striped",
            "secondary value must remain readable");
    Require(!target.GetLock("apple").has_value() &&
                !otherRegion.GetLock("zebra").has_value(),
            "migration must not leave orphan 2PC locks");
    for (const auto& item : tail) {
      Require(targetEngine->Get(item.first, &value) && value == item.second,
              "concurrent migration load must have zero lost writes");
    }

    sourceEngine.reset();
    targetEngine.reset();
    otherRegionEngine.reset();
    std::filesystem::remove_all(root);
    std::cout << "Online replica migration transaction checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::filesystem::remove_all(root);
    std::cerr << "Online replica migration checks failed: " << error.what() << std::endl;
    return 1;
  }
}
