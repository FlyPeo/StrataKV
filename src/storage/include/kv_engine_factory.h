#ifndef KV_ENGINE_FACTORY_H
#define KV_ENGINE_FACTORY_H

#include <memory>
#include <string>

#include "kv_engine.h"

class KVEngineFactory {
 public:
  static std::unique_ptr<IKVEngine> Create(const std::string& dbPath);
};

#endif  // KV_ENGINE_FACTORY_H
