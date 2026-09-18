#ifndef STRATAKV_SERVER_REGION_REGISTRY_H
#define STRATAKV_SERVER_REGION_REGISTRY_H

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "region_peer.h"

class RegionPeerHandle {
 public:
  RegionPeerHandle() = default;
  explicit RegionPeerHandle(std::shared_ptr<RegionPeer> peer) : peer_(std::move(peer)) {}
  ~RegionPeerHandle() { Reset(); }
  RegionPeerHandle(const RegionPeerHandle&) = delete;
  RegionPeerHandle& operator=(const RegionPeerHandle&) = delete;
  RegionPeerHandle(RegionPeerHandle&& other) noexcept : peer_(std::move(other.peer_)) {}
  RegionPeerHandle& operator=(RegionPeerHandle&& other) noexcept {
    if (this != &other) {
      Reset();
      peer_ = std::move(other.peer_);
    }
    return *this;
  }

  RegionPeer* operator->() const { return peer_.get(); }
  RegionPeer& operator*() const { return *peer_; }
  explicit operator bool() const { return static_cast<bool>(peer_); }

 private:
  void Reset() {
    if (peer_) {
      peer_->ReleaseRequest();
      peer_.reset();
    }
  }
  std::shared_ptr<RegionPeer> peer_;
};

struct RegionRetireResult {
  bool found = false;
  bool drained = false;
  uint64_t remainingRequests = 0;
  std::chrono::microseconds elapsed{0};
};

struct RegionRegistryMetrics {
  uint64_t revision = 0;
  size_t serving = 0;
  size_t initializing = 0;
  size_t retiring = 0;
  uint64_t lookupMisses = 0;
  uint64_t registrationConflicts = 0;
  uint64_t retirementTimeouts = 0;
  uint64_t epochRejects = 0;
  uint64_t peerRejects = 0;
  uint64_t keyRejects = 0;
  uint64_t headerRejects = 0;
};

class RegionRegistry {
 public:
  RegionRegistry();

  RegionPeerHandle Lookup(int regionId) const;
  // Snapshot of every currently published peer. The returned shared_ptrs keep the
  // peers alive even if the registry later unregisters them.
  std::vector<std::shared_ptr<RegionPeer>> Peers() const;
  void PublishInitial(const std::vector<std::shared_ptr<RegionPeer>>& peers,
                      uint64_t revision);
  bool Register(const std::shared_ptr<RegionPeer>& peer, uint64_t revision);
  std::vector<std::shared_ptr<RegionPeer>> Replace(
      const std::vector<int>& removeRegionIds,
      const std::vector<std::shared_ptr<RegionPeer>>& addPeers, uint64_t revision);
  RegionRetireResult Unregister(int regionId, uint64_t revision,
                                std::chrono::steady_clock::time_point deadline);
  RegionRegistryMetrics Metrics() const;
  // Counts a header-validation rejection so operators can separate stale
  // epochs, wrong peers, out-of-range keys and missing headers.
  void RecordValidationReject(stratakv::region::RegionErrorCode code);

 private:
  struct View {
    uint64_t revision = 0;
    std::unordered_map<int, std::shared_ptr<RegionPeer>> peers;
  };

  static bool SameDescriptor(const RegionMetadata& lhs, const RegionMetadata& rhs);
  std::shared_ptr<const View> Snapshot() const;

  mutable std::shared_ptr<const View> published_;
  mutable std::mutex publicationMutex_;
  mutable std::atomic<uint64_t> lookupMisses_{0};
  std::atomic<uint64_t> registrationConflicts_{0};
  std::atomic<uint64_t> retirementTimeouts_{0};
  std::atomic<uint64_t> epochRejects_{0};
  std::atomic<uint64_t> peerRejects_{0};
  std::atomic<uint64_t> keyRejects_{0};
  std::atomic<uint64_t> headerRejects_{0};
};

#endif  // STRATAKV_SERVER_REGION_REGISTRY_H
