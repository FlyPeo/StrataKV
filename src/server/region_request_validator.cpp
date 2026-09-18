#include "region_request_validator.h"

namespace {

template <typename Header>
RegionValidationResult ValidateIdentity(const RegionMetadata& descriptor,
                                        uint64_t localPeerId, int legacyRegionId,
                                        const Header* header, bool requireHeader) {
  if (header == nullptr) {
    if (requireHeader) {
      return {stratakv::region::REGION_ERROR_MISSING_HEADER,
              "dynamic topology request is missing a Region header"};
    }
    if (legacyRegionId != descriptor.regionId) {
      return {stratakv::region::REGION_ERROR_REGION_NOT_FOUND,
              "legacy Region ID does not match the target peer"};
    }
    return {};
  }
  if (header->regionid() != static_cast<uint64_t>(descriptor.regionId) ||
      legacyRegionId != descriptor.regionId) {
    return {stratakv::region::REGION_ERROR_REGION_NOT_FOUND,
            "request Region ID does not match the target peer"};
  }
  const uint64_t targetPeerId = [&] {
    if constexpr (std::is_same_v<Header, stratakv::region::RegionRequestHeader>) {
      return header->peerid();
    } else {
      return header->topeerid();
    }
  }();
  if (targetPeerId != localPeerId) {
    return {stratakv::region::REGION_ERROR_PEER_NOT_FOUND,
            "request target peer ID is not served by this process"};
  }
  if (!header->has_epoch() || header->epoch().version() != descriptor.epoch.version ||
      header->epoch().confversion() != descriptor.epoch.confVersion) {
    return {stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH,
            "request epoch " + std::to_string(header->epoch().version()) + "." +
                std::to_string(header->epoch().confversion()) +
                " vs local " + std::to_string(descriptor.epoch.version) + "." +
                std::to_string(descriptor.epoch.confVersion)};
  }
  return {};
}

}  // namespace

RegionValidationResult ValidateRegionRequest(
    const RegionMetadata& descriptor, uint64_t localPeerId, int legacyRegionId,
    const stratakv::region::RegionRequestHeader* header,
    const std::vector<std::string>& keys, bool requireHeader) {
  RegionValidationResult identity =
      ValidateIdentity(descriptor, localPeerId, legacyRegionId, header, requireHeader);
  if (!identity.ok()) return identity;
  for (const auto& key : keys) {
    if (!descriptor.Contains(key)) {
      return {stratakv::region::REGION_ERROR_KEY_NOT_IN_REGION,
              "request key is outside the target Region range"};
    }
  }
  return {};
}

RegionValidationResult ValidateRaftPeerRequest(
    const RegionMetadata& descriptor, uint64_t localPeerId, int legacyRegionId,
    const stratakv::region::RaftPeerHeader* header, bool requireHeader) {
  if (header == nullptr) {
    if (requireHeader) {
      return {stratakv::region::REGION_ERROR_MISSING_HEADER,
              "dynamic topology request is missing a Region header"};
    }
    if (legacyRegionId != descriptor.regionId) {
      return {stratakv::region::REGION_ERROR_REGION_NOT_FOUND,
              "legacy Region ID does not match the target peer"};
    }
    return {};
  }
  if (header->regionid() != static_cast<uint64_t>(descriptor.regionId) ||
      legacyRegionId != descriptor.regionId) {
    return {stratakv::region::REGION_ERROR_REGION_NOT_FOUND,
            "request Region ID does not match the target peer"};
  }
  if (header->topeerid() != localPeerId) {
    return {stratakv::region::REGION_ERROR_PEER_NOT_FOUND,
            "request target peer ID is not served by this process"};
  }
  if (descriptor.FindPeer(header->frompeerid()) == nullptr) {
    return {stratakv::region::REGION_ERROR_PEER_NOT_FOUND,
            "Raft source peer ID is not in the current descriptor"};
  }
  if (!header->has_epoch() || header->epoch().confversion() != descriptor.epoch.confVersion) {
    return {stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH,
            "request epoch confversion " +
                (header->has_epoch() ? std::to_string(header->epoch().confversion()) : "none") +
                " vs local " + std::to_string(descriptor.epoch.confVersion)};
  }
  // During a Region split, replicas apply the split at slightly different log positions.
  // Replicas in the same Raft group can communicate if their epoch version matches or
  // is in split transition (differing by at most 1).
  const uint64_t reqVer = header->epoch().version();
  const uint64_t locVer = descriptor.epoch.version;
  if (reqVer != locVer && reqVer + 1 != locVer && reqVer != locVer + 1) {
    return {stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH,
            "request epoch " + std::to_string(reqVer) + "." +
                std::to_string(header->epoch().confversion()) +
                " vs local " + std::to_string(locVer) + "." +
                std::to_string(descriptor.epoch.confVersion)};
  }
  return {};
}

void PopulateRegionError(const RegionValidationResult& validation,
                         const RegionMetadata* current,
                         stratakv::region::RegionResponseHeader* response) {
  auto* error = response->mutable_error();
  error->set_code(validation.code);
  error->set_message(validation.message);
  if (current != nullptr) {
    error->set_metadatarevision(current->metadataRevision);
    if (validation.code == stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH ||
        validation.code == stratakv::region::REGION_ERROR_KEY_NOT_IN_REGION) {
      *error->add_currentregions() = ToProtoRegion(*current);
    }
    if (const auto* leader = current->FindPeer(current->leaderPeerId)) {
      auto* hint = error->mutable_leader();
      hint->set_peerid(leader->peerId);
      hint->set_storeid(leader->storeId);
      hint->set_host(leader->host);
      hint->set_port(static_cast<uint32_t>(leader->port));
    }
  }
}
