#ifndef STRATAKV_SERVER_STRUCTURED_REGION_LOG_H
#define STRATAKV_SERVER_STRUCTURED_REGION_LOG_H

#include <cstdint>
#include <ostream>
#include <string>

// Structured diagnostics for topology events. The field set deliberately
// contains only Region, Store, peer, epoch and revision identifiers: user keys,
// values and transaction payloads must never be logged.
struct RegionLogFields {
  const char* event = "";
  int regionId = -1;
  uint64_t peerId = 0;
  uint64_t storeId = 0;
  uint64_t epochVersion = 0;
  uint64_t epochConfVersion = 0;
  uint64_t metadataRevision = 0;
  const char* errorCode = "";
  const char* component = "";
  std::string message;
};

std::string FormatRegionLog(const RegionLogFields& fields);
void EmitRegionLog(std::ostream& stream, const RegionLogFields& fields);

#endif  // STRATAKV_SERVER_STRUCTURED_REGION_LOG_H
