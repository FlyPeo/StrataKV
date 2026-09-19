/*
 * 测试目标：验证同一 Region 内批量 MVCC 命令的确定性编码、原子落盘和失败重试语义。
 * 测试策略：使用可计数、可注入一次失败的内存 KVEngine，执行多键 batch prewrite/commit，
 *           并对序列化命令进行 round-trip 后模拟存储批写失败与重试。
 * 测试规模：固定 2 个顶层场景；正常路径覆盖 3 个键的 prewrite/commit，失败路径覆盖
 *           2 个键的一次失败和一次重试。
 * 验证内容：确认键顺序稳定、一次 Raft apply 只产生一次原子 WriteBatch、值全部可见；
 *           失败时不得暴露部分锁或推进 applied index，随后重试必须成功。
 */
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "mvcc_storage.h"

namespace {

class CountingEngine final : public IKVEngine {
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
  bool WriteBatch(const std::vector<KVBatchOp>& operations) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++writeBatches_;
    if (failNext_) {
      failNext_ = false;
      return false;
    }
    auto next = data_;
    for (const auto& operation : operations) {
      if (operation.type == KVBatchOpType::Delete) next.erase(operation.key);
      else next[operation.key] = operation.value;
    }
    data_.swap(next);
    return true;
  }
  std::vector<std::pair<std::string, std::string>> ScanPrefix(const std::string&) override { return {}; }
  std::string Dump() override { return {}; }
  bool Load(const std::string&) override { return false; }
  void DebugPrint() override {}

  void FailNextBatch() {
    std::lock_guard<std::mutex> lock(mutex_);
    failNext_ = true;
  }
  size_t WriteBatches() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return writeBatches_;
  }

 private:
  mutable std::mutex mutex_;
  std::map<std::string, std::string> data_;
  size_t writeBatches_ = 0;
  bool failNext_ = false;
};

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

void CheckAtomicRegionBatch() {
  auto engine = std::make_shared<CountingEngine>();
  MvccStorage storage(engine);
  const uint64_t startTs = 100;
  const uint64_t commitTs = 200;
  const std::vector<MvccMutation> mutations{{"c", "3", false, false},
                                            {"a", "1", false, false},
                                            {"b", "2", false, false}};

  PreparedMvccBatch prewrite = storage.PrepareBatchPrewrite(mutations, "a", startTs, 120000);
  Require(prewrite.status == TxnStatus::Ok && prewrite.items.size() == 3 &&
              prewrite.items[0].key == "a" && prewrite.items[2].key == "c",
          "batch prepare must sort keys deterministically");
  PreparedMvccBatch decoded;
  Require(decoded.Parse(prewrite.Serialize()), "batch Raft payload must round-trip");
  const size_t beforePrewrite = engine->WriteBatches();
  Require(storage.ApplyPreparedBatch(decoded, 7) == TxnStatus::Ok,
          "batch prewrite Apply must succeed");
  Require(engine->WriteBatches() == beforePrewrite + 1 && storage.Stats().appliedRaftIndex == 7,
          "all prewrites and progress must share one engine batch");

  PreparedMvccBatch commit = storage.PrepareBatchCommit({"c", "a", "b"}, startTs, commitTs);
  const size_t beforeCommit = engine->WriteBatches();
  Require(commit.status == TxnStatus::Ok && storage.ApplyPreparedBatch(commit, 8) == TxnStatus::Ok,
          "batch commit Apply must succeed");
  Require(engine->WriteBatches() == beforeCommit + 1 && storage.Stats().appliedRaftIndex == 8,
          "all commits and progress must share one engine batch");
  for (const auto& expected : std::vector<std::pair<std::string, std::string>>{{"a", "1"}, {"b", "2"}, {"c", "3"}}) {
    std::string value;
    Require(storage.Get(expected.first, commitTs, &value) == TxnStatus::Ok && value == expected.second,
            "committed batch value must be visible");
  }
}

void CheckFailureDoesNotAdvanceProgress() {
  auto engine = std::make_shared<CountingEngine>();
  MvccStorage storage(engine);
  PreparedMvccBatch batch = storage.PrepareBatchPrewrite(
      {{"a", "1", false, false}, {"b", "2", false, false}}, "a", 100, 120000);
  engine->FailNextBatch();
  Require(storage.ApplyPreparedBatch(batch, 11) == TxnStatus::StorageError,
          "failed engine batch must fail Apply");
  Require(storage.Stats().appliedRaftIndex == 0 && !storage.GetLock("a").has_value() &&
              !storage.GetLock("b").has_value(),
          "failed batch must not expose partial data or progress");
  Require(storage.ApplyPreparedBatch(batch, 11) == TxnStatus::Ok &&
              storage.Stats().appliedRaftIndex == 11,
          "same committed command must remain retryable after storage failure");
}

}  // namespace

int main() {
  try {
    CheckAtomicRegionBatch();
    CheckFailureDoesNotAdvanceProgress();
    return 0;
  } catch (const std::exception&) {
    return 1;
  }
}
