/*
 * 测试目标：验证 RocksDB 捕获快照的钉住语义：捕获后的延迟序列化必须还原捕获时刻的完整状态。
 * 测试策略：用真实 RocksDbKVEngine 在临时目录写入、CaptureSnapshot、继续写入并 Load 更新状态，
 *           再把序列化字节 Load 到同一引擎与全新目录，比较恢复前后的键值集合。
 * 测试规模：1 个场景（rocksdb_deferred_snapshot），2 个引擎实例、3 次写入、2 次安装、1 次跨目录还原。
 * 验证内容：捕获后再写入或安装新状态都不会使已钉住的快照失效（序列化字节保持不变），安装立即
 *           生效且清掉快照之外的键，Load 快照还原出的恰是捕获时的值（before=v1），绝不包含捕获之后的写入。
 */
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include "rocksdb_kv_engine.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

}  // namespace

int main() {
  const auto suffix = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  const auto root = std::filesystem::temp_directory_path() /
                    ("stratakv-rocks-snapshot-" + suffix);
  const auto sourcePath = root / "source";
  const auto restoredPath = root / "restored";

  try {
    std::filesystem::create_directories(root);
    std::string encoded;
    {
      RocksDbKVEngine source(sourcePath.string());
      Require(source.Put("before", "v1"), "initial write failed");
      auto snapshot = source.CaptureSnapshot();
      Require(source.Put("before", "v2"), "update after capture failed");
      Require(source.Put("after", "new"), "new write after capture failed");
      encoded = snapshot->Serialize();
      const std::string newerState = source.Dump();
      Require(source.Load(newerState), "install while an older snapshot is pinned failed");
      Require(snapshot->Serialize() == encoded,
              "install invalidated the older pinned snapshot");
      std::string value;
      Require(source.Get("before", &value) && value == "v2",
              "installed state was not published");
      Require(source.Load(encoded), "replacing the current keyspace failed");
      Require(!source.Get("after", &value), "install retained keys absent from snapshot");
    }

    {
      RocksDbKVEngine restored(restoredPath.string());
      Require(restored.Load(encoded), "captured snapshot did not restore");
      std::string value;
      Require(restored.Get("before", &value) && value == "v1",
              "snapshot did not retain the captured value");
      Require(!restored.Get("after", &value),
              "snapshot included a write made after capture");
    }

    std::filesystem::remove_all(root);
    std::cout << "PASS rocksdb_deferred_snapshot\n";
    return 0;
  } catch (const std::exception& error) {
    std::filesystem::remove_all(root);
    std::cerr << "rocksdb snapshot check failed: " << error.what() << '\n';
    return 1;
  }
}
