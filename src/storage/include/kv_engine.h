#ifndef KV_ENGINE_H
#define KV_ENGINE_H

#include <memory>
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

// A stable point-in-time view whose potentially expensive serialization can
// happen after the state-machine apply lock has been released.
class IKVSnapshot {
 public:
  virtual ~IKVSnapshot() = default;
  virtual std::string Serialize() = 0;
};

class MaterializedKVSnapshot final : public IKVSnapshot {
 public:
  explicit MaterializedKVSnapshot(std::string data) : data_(std::move(data)) {}
  std::string Serialize() override { return data_; }

 private:
  std::string data_;
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
  virtual std::unique_ptr<IKVSnapshot> CaptureSnapshot() {
    return std::make_unique<MaterializedKVSnapshot>(Dump());
  }
  virtual bool Load(const std::string& snapshot) = 0;
  virtual void DebugPrint() = 0;
};

#endif  // KV_ENGINE_H
