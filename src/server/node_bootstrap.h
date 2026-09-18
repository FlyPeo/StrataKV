#ifndef STRATAKV_SERVER_NODE_BOOTSTRAP_H
#define STRATAKV_SERVER_NODE_BOOTSTRAP_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "region_metadata.h"
#include "region_peer.h"

// One failed local assignment. component names the failing subsystem so an
// operator can tell a metadata gap from a RocksDB or Raft initialization error.
struct NodeBootstrapFailure {
  int regionId = -1;
  std::string component;
  std::string message;

  std::string Structured() const;
};

// Result of initializing every Region peer assigned to one physical node from
// one validated metadata revision. `peers` stays empty unless the complete
// assignment initialized, so a partial registry is never published.
struct NodeBootstrapReport {
  uint64_t revision = 0;
  std::vector<std::shared_ptr<RegionPeer>> peers;
  std::vector<NodeBootstrapFailure> failures;

  bool ok() const { return failures.empty() && !peers.empty(); }
  std::string Summary() const;
};

using RegionPeerFactory =
    std::function<std::shared_ptr<RegionPeer>(const RegionMetadata& descriptor, size_t peerIndex)>;

// Reconciles the local static assignment with one metadata revision and builds
// the peers. Any failure aborts with an empty peer set and a full report.
NodeBootstrapReport BootstrapNodeRegions(int nodeId, const RegionCatalog& localCatalog,
                                        const std::vector<RegionMetadata>& metadataRegions,
                                        uint64_t metadataRevision,
                                        const RegionPeerFactory& factory);

// Reads the complete Region set of one metadata revision. Returns false when no
// consistent view can be loaded, in which case nothing may be published.
struct MetadataBootstrapView {
  std::vector<RegionMetadata> regions;
  uint64_t revision = 0;
};

#endif  // STRATAKV_SERVER_NODE_BOOTSTRAP_H
