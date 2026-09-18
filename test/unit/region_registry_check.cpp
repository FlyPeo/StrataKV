#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "region_registry.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

RegionMetadata Descriptor(int id, std::string start, std::string end,
                          uint64_t version, uint64_t revision) {
  RegionMetadata descriptor;
  descriptor.regionId = id;
  descriptor.startKey = std::move(start);
  descriptor.endKey = std::move(end);
  descriptor.epoch = {version, 1};
  descriptor.metadataRevision = revision;
  descriptor.peers = {{0, "127.0.0.1", 25000, 1,
                       static_cast<uint64_t>(id * 10 + 1)}};
  descriptor.leaderPeerId = descriptor.peers.front().peerId;
  return descriptor;
}

std::shared_ptr<RegionPeer> MakePeer(
    const std::shared_ptr<NodeTxnScheduler>& scheduler, RegionMetadata descriptor,
    int physicalNodeId = 0) {
  auto peer = std::make_shared<RegionPeer>(physicalNodeId, std::move(descriptor), 0,
                                           RaftLogGcConfig{}, scheduler);
  peer->MarkServing();
  return peer;
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-region-registry-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) return 1;
  const auto originalDirectory = std::filesystem::current_path();
  std::filesystem::current_path(temporaryDirectory);
  try {
    auto scheduler = std::make_shared<NodeTxnScheduler>(64, 1, 16, 32);
    auto parent = MakePeer(scheduler, Descriptor(10, "", "m", 1, 1));
    auto right = MakePeer(scheduler, Descriptor(20, "m", "", 1, 1));
    RegionRegistry registry;
    registry.PublishInitial({parent, right}, 1);
    Require(registry.Metrics().revision == 1 && registry.Metrics().serving == 2,
            "initial registry must publish atomically");
    Require(registry.Register(parent, 1), "identical registration must be idempotent");

    RegionPeerHandle retained = registry.Lookup(10);
    Require(retained && parent->InFlightRequests() == 1,
            "lookup must retain one lifecycle request lease");

    auto leftChild = MakePeer(scheduler, Descriptor(11, "", "g", 2, 2));
    auto middleChild = MakePeer(scheduler, Descriptor(12, "g", "m", 2, 2));
    const auto retired = registry.Replace({10}, {leftChild, middleChild}, 2);
    Require(retired.size() == 1 && parent->LifecycleState() == RegionPeerState::Retiring,
            "replacement must retire the removed peer");
    Require(!registry.Lookup(10) && registry.Lookup(11) && registry.Lookup(12),
            "replacement must become visible as one complete map");
    Require(!parent->WaitForDrain(std::chrono::steady_clock::now() +
                                      std::chrono::milliseconds(20)),
            "retained request must keep retiring peer alive");
    retained = RegionPeerHandle{};
    Require(parent->WaitForDrain(std::chrono::steady_clock::now() +
                                     std::chrono::seconds(1)),
            "retiring peer must drain after the request releases its handle");
    parent->MarkStopped();
    Require(parent->LifecycleState() == RegionPeerState::Stopped,
            "drained peer must reach Stopped state");

    auto conflicting = MakePeer(scheduler, Descriptor(20, "x", "", 3, 2), 1);
    bool conflictRejected = false;
    try {
      registry.Register(conflicting, 2);
    } catch (const std::logic_error&) {
      conflictRejected = true;
    }
    Require(conflictRejected && registry.Metrics().registrationConflicts == 1,
            "conflicting registration must be counted and rejected");

    RegionPeerHandle draining = registry.Lookup(12);
    const auto timedOut = registry.Unregister(
        12, 3, std::chrono::steady_clock::now() + std::chrono::milliseconds(20));
    Require(timedOut.found && !timedOut.drained && timedOut.remainingRequests == 1 &&
                registry.Metrics().retirementTimeouts == 1,
            "unregister must report bounded drain timeout diagnostics");
    Require(!registry.Lookup(12), "retiring peer must reject new requests");
    draining = RegionPeerHandle{};
    Require(middleChild->WaitForDrain(std::chrono::steady_clock::now() +
                                          std::chrono::seconds(1)),
            "timed-out retirement must still drain safely later");
    middleChild->MarkStopped();

    std::filesystem::current_path(originalDirectory);
    scheduler.reset();
    std::filesystem::remove_all(temporaryDirectory);
    std::cout << "Region registry and lifecycle checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::filesystem::current_path(originalDirectory);
    std::cerr << "Region registry checks failed: " << error.what() << std::endl;
    return 1;
  }
}
