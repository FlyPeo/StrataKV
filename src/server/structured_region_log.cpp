#include "structured_region_log.h"

#include <sstream>

std::string FormatRegionLog(const RegionLogFields& fields) {
  std::ostringstream line;
  line << "event=" << fields.event << " region_id=" << fields.regionId
       << " peer_id=" << fields.peerId << " store_id=" << fields.storeId
       << " epoch=" << fields.epochVersion << "." << fields.epochConfVersion
       << " metadata_revision=" << fields.metadataRevision;
  if (fields.errorCode != nullptr && fields.errorCode[0] != '\0') {
    line << " error_code=" << fields.errorCode;
  }
  if (fields.component != nullptr && fields.component[0] != '\0') {
    line << " component=" << fields.component;
  }
  if (!fields.message.empty()) line << " message=" << fields.message;
  return line.str();
}

void EmitRegionLog(std::ostream& stream, const RegionLogFields& fields) {
  stream << FormatRegionLog(fields) << std::endl;
}
