#include "rocksdb_kv_engine.h"

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>

#include <iostream>
#include <sstream>
#include <stdexcept>

namespace {

struct SerializableSnapshotData {
  template <class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar &keys;
    ar &values;
  }

  std::vector<std::string> keys;
  std::vector<std::string> values;
};

class RocksDbSnapshot final : public IKVSnapshot {
 public:
  RocksDbSnapshot(rocksdb::DB* db, const rocksdb::Snapshot* snapshot)
      : db_(db), snapshot_(snapshot) {}

  ~RocksDbSnapshot() override {
    if (snapshot_ != nullptr) db_->ReleaseSnapshot(snapshot_);
  }

  std::string Serialize() override {
    SerializableSnapshotData snapshotData;
    rocksdb::ReadOptions readOptions;
    readOptions.snapshot = snapshot_;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(readOptions));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      snapshotData.keys.emplace_back(it->key().ToString());
      snapshotData.values.emplace_back(it->value().ToString());
    }
    if (!it->status().ok()) {
      throw std::runtime_error("failed to iterate RocksDB snapshot: " +
                               it->status().ToString());
    }

    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << snapshotData;
    return ss.str();
  }

 private:
  rocksdb::DB* db_;
  const rocksdb::Snapshot* snapshot_;
};

}  // namespace

RocksDbKVEngine::RocksDbKVEngine(std::string dbPath) : dbPath_(std::move(dbPath)), db_(nullptr) {
  options_.create_if_missing = true;
  if (!Open()) {
    throw std::runtime_error("failed to open rocksdb at " + dbPath_);
  }
}

RocksDbKVEngine::~RocksDbKVEngine() {
  delete db_;
  db_ = nullptr;
}

bool RocksDbKVEngine::Put(const std::string& key, const std::string& value) {
  return db_->Put(rocksdb::WriteOptions(), key, value).ok();
}

bool RocksDbKVEngine::Get(const std::string& key, std::string* value) {
  if (value == nullptr) {
    return false;
  }
  const rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, value);
  return status.ok();
}

bool RocksDbKVEngine::Append(const std::string& key, const std::string& value) {
  std::string oldValue;
  if (Get(key, &oldValue)) {
    oldValue += value;
    return Put(key, oldValue);
  }
  return Put(key, value);
}

bool RocksDbKVEngine::Delete(const std::string& key) {
  return db_->Delete(rocksdb::WriteOptions(), key).ok();
}

bool RocksDbKVEngine::WriteBatch(const std::vector<KVBatchOp>& ops) {
  rocksdb::WriteBatch batch;
  for (const auto& op : ops) {
    if (op.type == KVBatchOpType::Delete) {
      batch.Delete(op.key);
    } else {
      batch.Put(op.key, op.value);
    }
  }
  return db_->Write(rocksdb::WriteOptions(), &batch).ok();
}

std::vector<std::pair<std::string, std::string>> RocksDbKVEngine::ScanPrefix(const std::string& prefix) {
  std::vector<std::pair<std::string, std::string>> items;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
  for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
    items.emplace_back(it->key().ToString(), it->value().ToString());
  }
  return items;
}

std::string RocksDbKVEngine::Dump() {
  return CaptureSnapshot()->Serialize();
}

std::unique_ptr<IKVSnapshot> RocksDbKVEngine::CaptureSnapshot() {
  return std::make_unique<RocksDbSnapshot>(db_, db_->GetSnapshot());
}

bool RocksDbKVEngine::Load(const std::string& snapshot) {
  if (snapshot.empty()) {
    return true;
  }

  SnapshotData snapshotData;
  std::stringstream ss(snapshot);
  boost::archive::text_iarchive ia(ss);
  ia >> snapshotData;

  if (snapshotData.keys.size() != snapshotData.values.size()) return false;

  // An asynchronous GC snapshot may still pin this DB's old sequence. Replacing
  // the DB object here would invalidate its iterator and ReleaseSnapshot call.
  // Atomically replace the keyspace in the existing DB instead: pinned readers
  // retain the old sequence and new readers see the complete installed state.
  rocksdb::WriteBatch batch;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
  for (it->SeekToFirst(); it->Valid(); it->Next()) batch.Delete(it->key());
  if (!it->status().ok()) return false;
  for (size_t i = 0; i < snapshotData.keys.size(); ++i) {
    batch.Put(snapshotData.keys[i], snapshotData.values[i]);
  }
  return db_->Write(rocksdb::WriteOptions(), &batch).ok();
}

void RocksDbKVEngine::DebugPrint() {
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
  std::cout << "\n*****RocksDB KV*****" << std::endl;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString() << ":" << it->value().ToString() << ";" << std::endl;
  }
}

bool RocksDbKVEngine::Open() {
  if (db_ != nullptr) {
    return true;
  }
  return rocksdb::DB::Open(options_, dbPath_, &db_).ok();
}
