#include "kvEngineFactory.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <stdexcept>

#include "rocksDbKVEngine.h"
#include "skipListKVEngine.h"

std::unique_ptr<IKVEngine> KVEngineFactory::Create(KVEngineType engineType, const std::string& dbPath) {
  switch (engineType) {
    case KVEngineType::SkipList:
      return std::make_unique<SkipListKVEngine>();
    case KVEngineType::RocksDB:
      return std::make_unique<RocksDbKVEngine>(dbPath);
  }
  throw std::invalid_argument("unknown kv engine type");
}

KVEngineType KVEngineFactory::ResolveFromEnvOrDefault() {
  const char* backend = std::getenv("KV_ENGINE_BACKEND");
  if (backend == nullptr) {
    return KVEngineType::RocksDB;
  }

  std::string value = backend;
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  if (value == "skiplist") {
    return KVEngineType::SkipList;
  }
  if (value == "rocksdb") {
    return KVEngineType::RocksDB;
  }
  return KVEngineType::RocksDB;
}
