#include "kv_engine_factory.h"

#include "rocksdb_kv_engine.h"

std::unique_ptr<IKVEngine> KVEngineFactory::Create(const std::string& dbPath) {
  return std::make_unique<RocksDbKVEngine>(dbPath);
}
