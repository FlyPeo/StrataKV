#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include "node_server.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

RegionMetadata Descriptor(int id, const std::string& start, const std::string& end,
                          uint64_t peerId, uint64_t revision) {
  RegionMetadata descriptor;
  descriptor.regionId = id;
  descriptor.startKey = start;
  descriptor.endKey = end;
  descriptor.epoch = {1, 1};
  descriptor.metadataRevision = revision;
  descriptor.peers = {{0, "127.0.0.1", 27500, 1, peerId}};
  descriptor.leaderPeerId = peerId;
  return descriptor;
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-node-split-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) return 1;
  const auto originalDirectory = std::filesystem::current_path();
  std::filesystem::current_path(temporaryDirectory);
  try {
    RegionMetadata parent = Descriptor(10, "", "h", 1001, 1);
    RegionMetadata region20 = Descriptor(20, "h", "", 2001, 1);
    NodeServer server(0, RaftLogGcConfig{}, RegionCatalog({parent, region20}), "");
    for (const auto& peer : server.PeersForTest()) peer->MarkServing();
    server.PublishInitialRegistryForTest();
    Require(server.RegistryMetrics().serving == 2, "initial registry must hold two Regions");

    // The parent applied its AdminSplit: descriptor shrinks and the child
    // arrives. NodeServer materializes the child and publishes both.
    RegionMetadata shrunken = parent;
    shrunken.endKey = "d";
    shrunken.epoch.version = 2;
    shrunken.metadataRevision = 2;

    RegionMetadata child = Descriptor(30, "d", "h", 3001, 2);
    std::cerr << "TRACE before materialize" << std::endl;
    server.MaterializeSplitChild(0, shrunken, child);
    std::cerr << "TRACE after materialize" << std::endl;

    auto childHandle = server.RegistryForTest().Lookup(30);
    std::cerr << "TRACE test: child=" << (childHandle ? int(childHandle->LifecycleState()) : -1)
              << std::endl;
    Require(childHandle && childHandle->LifecycleState() == RegionPeerState::Serving,
            "the materialized child must be published and serving");
    auto parentHandle = server.RegistryForTest().Lookup(10);
    Require(parentHandle && parentHandle.operator->() == server.PeersForTest()[0].get(),
            "the parent handle must stay the same in-place updated peer");
    Require(server.RegistryMetrics().serving == 3,
            "post-split registry must hold parent, sibling and child");

    bool schedulerHasChild = false;
    for (const auto& region : server.TxnSchedulerForTest()->Regions()) {
      if (region->TxnRegionId() == 30) schedulerHasChild = true;
    }
    Require(schedulerHasChild, "the child must be registered with the node scheduler");

    // Idempotent materialization (retry after a lost ack) must not duplicate.
    server.MaterializeSplitChild(0, shrunken, child);
    Require(server.RegistryMetrics().serving == 3,
            "a repeated materialization must not duplicate the child");

    std::filesystem::current_path(originalDirectory);
    std::filesystem::remove_all(temporaryDirectory);
    std::cout << "Node split materialization checks passed" << std::endl;
    // Started child Raft tickers are detached by design (process-lifetime
    // model); a normal exit would interact with them during destruction.
    std::cout.flush();
    std::cerr.flush();
    std::_Exit(0);
  } catch (const std::exception& error) {
    std::filesystem::current_path(originalDirectory);
    std::cerr << "Node split materialization checks failed: " << error.what() << std::endl;
    return 1;
  }
}
