#include "skipListKVEngine.h"

#include <boost/archive/text_iarchive.hpp>
#include <sstream>

SkipListKVEngine::SkipListKVEngine(int maxLevel) : skipList_(maxLevel) {}

bool SkipListKVEngine::Put(const std::string& key, const std::string& value) {
  std::string mutableKey = key;
  std::string mutableValue = value;
  skipList_.insert_set_element(mutableKey, mutableValue);
  return true;
}

bool SkipListKVEngine::Get(const std::string& key, std::string* value) {
  if (value == nullptr) {
    return false;
  }
  return skipList_.search_element(key, *value);
}

bool SkipListKVEngine::Append(const std::string& key, const std::string& value) {
  std::string oldValue;
  if (skipList_.search_element(key, oldValue)) {
    std::string mutableKey = key;
    std::string newValue = oldValue + value;
    skipList_.insert_set_element(mutableKey, newValue);
    return true;
  }
  std::string mutableKey = key;
  std::string mutableValue = value;
  skipList_.insert_set_element(mutableKey, mutableValue);
  return true;
}

bool SkipListKVEngine::Delete(const std::string& key) {
  skipList_.delete_element(key);
  return true;
}

bool SkipListKVEngine::WriteBatch(const std::vector<KVBatchOp>& ops) {
  for (const auto& op : ops) {
    if (op.type == KVBatchOpType::Delete) {
      if (!Delete(op.key)) {
        return false;
      }
      continue;
    }
    if (!Put(op.key, op.value)) {
      return false;
    }
  }
  return true;
}

std::vector<std::pair<std::string, std::string>> SkipListKVEngine::ScanPrefix(const std::string& prefix) {
  std::vector<std::pair<std::string, std::string>> items;
  const std::string snapshot = Dump();
  if (snapshot.empty()) {
    return items;
  }

  SkipListDump<std::string, std::string> dump;
  std::stringstream ss(snapshot);
  boost::archive::text_iarchive ia(ss);
  ia >> dump;
  for (size_t i = 0; i < dump.keyDumpVt_.size(); ++i) {
    if (dump.keyDumpVt_[i].rfind(prefix, 0) == 0) {
      items.emplace_back(dump.keyDumpVt_[i], dump.valDumpVt_[i]);
    }
  }
  return items;
}

std::string SkipListKVEngine::Dump() { return skipList_.dump_file(); }

bool SkipListKVEngine::Load(const std::string& snapshot) {
  skipList_.load_file(snapshot);
  return true;
}

void SkipListKVEngine::DebugPrint() { skipList_.display_list(); }
