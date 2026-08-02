#ifndef SKIPLIST_KV_ENGINE_H
#define SKIPLIST_KV_ENGINE_H

#include "kvEngine.h"
#include "skipList.h"

#include <utility>
#include <vector>

class SkipListKVEngine : public IKVEngine {
 public:
  explicit SkipListKVEngine(int maxLevel = 6);
  ~SkipListKVEngine() override = default;

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
  SkipList<std::string, std::string> skipList_;
};

#endif  // SKIPLIST_KV_ENGINE_H
