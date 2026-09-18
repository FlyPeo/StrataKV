#ifndef STRATAKV_SERVER_REGION_REQUEST_VALIDATOR_H
#define STRATAKV_SERVER_REGION_REQUEST_VALIDATOR_H

#include <string>
#include <vector>

#include "region.pb.h"
#include "region_metadata.h"

struct RegionValidationResult {
  stratakv::region::RegionErrorCode code = stratakv::region::REGION_ERROR_NONE;
  std::string message;
  bool ok() const { return code == stratakv::region::REGION_ERROR_NONE; }
};

RegionValidationResult ValidateRegionRequest(
    const RegionMetadata& descriptor, uint64_t localPeerId, int legacyRegionId,
    const stratakv::region::RegionRequestHeader* header,
    const std::vector<std::string>& keys, bool requireHeader);

RegionValidationResult ValidateRaftPeerRequest(
    const RegionMetadata& descriptor, uint64_t localPeerId, int legacyRegionId,
    const stratakv::region::RaftPeerHeader* header, bool requireHeader);

void PopulateRegionError(const RegionValidationResult& validation,
                         const RegionMetadata* current,
                         stratakv::region::RegionResponseHeader* response);

#endif  // STRATAKV_SERVER_REGION_REQUEST_VALIDATOR_H
