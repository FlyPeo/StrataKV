#include "node_bootstrap.h"

#include <algorithm>
#include <sstream>
#include <unordered_map>

std::string NodeBootstrapFailure::Structured() const {
  std::ostringstream line;
  line << "region_id=" << regionId << " component=" << component << " message=" << message;
  return line.str();
}

std::string NodeBootstrapReport::Summary() const {
  std::ostringstream summary;
  summary << "metadata_revision=" << revision << " initialized_regions=" << peers.size()
          << " failures=" << failures.size();
  for (const auto& failure : failures) {
    summary << " [" << failure.Structured() << "]";
  }
  return summary.str();
}

NodeBootstrapReport BootstrapNodeRegions(int nodeId, const RegionCatalog& localCatalog,
                                        const std::vector<RegionMetadata>& metadataRegions,
                                        uint64_t metadataRevision,
                                        const RegionPeerFactory& factory) {
  NodeBootstrapReport report;
  report.revision = metadataRevision;
  if (metadataRevision == 0) {
    report.failures.push_back({-1, "metadata", "bootstrap metadata revision must be positive"});
    return report;
  }

  const auto assignments = localCatalog.PeersOnNode(nodeId);
  if (assignments.empty()) {
    report.failures.push_back({-1, "config", "local Region configuration assigns no peer"});
    return report;
  }

  std::unordered_map<int, const RegionMetadata*> byRegionId;
  for (const auto& region : metadataRegions) {
    byRegionId.emplace(region.regionId, &region);
  }

  // One physical node exposes one RPC listener, so every local peer must agree
  // on the port before any peer is created.
  short sharedPort = 0;
  for (const auto& assignment : assignments) {
    if (assignment.peerIndex >= assignment.region.peers.size()) {
      report.failures.push_back({assignment.region.regionId, "config",
                                 "local peer index is outside the configured peer set"});
      return report;
    }
    const auto& local = assignment.region.peers[assignment.peerIndex];
    if (sharedPort == 0) {
      sharedPort = local.port;
    } else if (sharedPort != local.port) {
      report.failures.push_back({assignment.region.regionId, "config",
                                 "local peers must share one RPC port"});
      return report;
    }
  }

  for (const auto& assignment : assignments) {
    const int regionId = assignment.region.regionId;
    const auto found = byRegionId.find(regionId);
    if (found == byRegionId.end()) {
      report.failures.push_back({regionId, "metadata",
                                 "Region is absent from the bootstrap metadata revision"});
      continue;
    }
    const RegionMetadata& descriptor = *found->second;
    if (descriptor.metadataRevision != metadataRevision) {
      report.failures.push_back({regionId, "metadata",
                                 "Region descriptor revision does not match the bootstrap view"});
      continue;
    }
    if (assignment.peerIndex >= descriptor.peers.size()) {
      report.failures.push_back({regionId, "descriptor",
                                 "metadata descriptor has no peer at the local index"});
      continue;
    }
    try {
      auto peer = factory(descriptor, assignment.peerIndex);
      if (!peer) {
        report.failures.push_back({regionId, "peer", "Region peer factory returned no peer"});
        continue;
      }
      report.peers.push_back(std::move(peer));
    } catch (const std::exception& error) {
      report.failures.push_back({regionId, "peer", error.what()});
    }
  }

  if (!report.failures.empty()) {
    // Nothing is published unless every assigned Region initialized.
    report.peers.clear();
  }
  return report;
}
