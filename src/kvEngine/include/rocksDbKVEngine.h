#ifndef ROCKSDB_KV_ENGINE_H
#define ROCKSDB_KV_ENGINE_H

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <string>
#include <utility>
#include <vector>

#include "kvEngine.h"

class RocksDbKVEngine : public IKVEngine {
 public:
  explicit RocksDbKVEngine(std::string dbPath);
  ~RocksDbKVEngine() override;

  bool Put(const std::string& key, const std::string& value) override;
  bool Get(const std::string& key, std::string* value) override;
  bool Append(const std::string& key, const std::string& value) override;
  bool Delete(const std::string& key) override;
  bool WriteBatch(const std::vector<KVBatchOp>& ops) override;
  std::vector<std::pair<std::string, std::string>> ScanPrefix(const std::string& prefix) override;

  std::string Dump() override;
  bool Load(const std::string& snapshot) override;
  void DebugPrint() override;

 private:
  struct SnapshotData {
    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
      ar &keys;
      ar &values;
    }

    std::vector<std::string> keys;
    std::vector<std::string> values;
  };

  bool Open();
  bool ResetDatabase();

 private:
  std::string dbPath_;
  rocksdb::DB* db_;
  rocksdb::Options options_;
};

#endif  // ROCKSDB_KV_ENGINE_H
