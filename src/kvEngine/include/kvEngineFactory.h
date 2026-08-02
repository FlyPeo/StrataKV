#ifndef KV_ENGINE_FACTORY_H
#define KV_ENGINE_FACTORY_H

#include <memory>
#include <string>

#include "kvEngine.h"

class KVEngineFactory {
 public:
  static std::unique_ptr<IKVEngine> Create(KVEngineType engineType, const std::string& dbPath = "");
  static KVEngineType ResolveFromEnvOrDefault();
};

#endif  // KV_ENGINE_FACTORY_H
