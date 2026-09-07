/*
 * Test target: verify that a captured RocksDB view stays pinned while live
 * writes continue, and that its deferred serialization restores exactly the
 * captured state.
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
