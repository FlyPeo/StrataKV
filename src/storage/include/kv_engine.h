#ifndef KV_ENGINE_H
#define KV_ENGINE_H

#include <string>
#include <utility>
#include <vector>

enum class KVBatchOpType {
  Put,
  Delete,
};

struct KVBatchOp {
  KVBatchOpType type = KVBatchOpType::Put;
  std::string key;
  std::string value;
};

class IKVEngine {
 public:
  virtual ~IKVEngine() = default;

  virtual bool Put(const std::string& key, const std::string& value) = 0;
  virtual bool Get(const std::string& key, std::string* value) = 0;
  virtual bool Append(const std::string& key, const std::string& value) = 0;
  virtual bool Delete(const std::string& key) = 0;
  virtual bool WriteBatch(const std::vector<KVBatchOp>& ops) = 0;
  virtual std::vector<std::pair<std::string, std::string>> ScanPrefix(const std::string& prefix) = 0;

  virtual std::string Dump() = 0;
  virtual bool Load(const std::string& snapshot) = 0;
  virtual void DebugPrint() = 0;
};

#endif  // KV_ENGINE_H
