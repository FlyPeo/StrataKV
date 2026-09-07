#ifndef STRATAKV_RAFT_LOG_GC_H
#define STRATAKV_RAFT_LOG_GC_H

#include <algorithm>
#include <chrono>
#include <cstdint>

struct RaftLogGcConfig {
  // A snapshot serializes the complete Region state. Compacting every few
  // dozen entries turns a bulk load into quadratic scan/write amplification.
  uint64_t threshold = 10000;
  uint64_t countLimit = 196608;
  uint64_t sizeLimitBytes = 192ULL * 1024ULL * 1024ULL;
  std::chrono::milliseconds tickInterval{3000};
};

struct RaftLogGcState {
  int truncatedIndex = 0;
  int appliedIndex = 0;
  int replicatedIndex = 0;
  uint64_t logCount = 0;
  uint64_t approximateSizeBytes = 0;
};

struct RaftLogGcDecision {
  int compactIndex = 0;
  uint64_t reclaimableCount = 0;
  bool softThreshold = false;
  bool countLimit = false;
  bool sizeLimit = false;

  bool ShouldGc() const { return compactIndex > 0; }
  bool Forced() const { return countLimit || sizeLimit; }
};

// Soft GC waits until every peer has replicated enough reclaimable entries.
// Both paths must snapshot at the local applied index: the storage snapshot is
// a view of the current state, not of an earlier follower match index. A peer
// behind that exact boundary catches up through InstallSnapshot.
inline RaftLogGcDecision EvaluateRaftLogGc(const RaftLogGcConfig& config,
                                           const RaftLogGcState& state) {
  RaftLogGcDecision decision;
  const int appliedIndex = std::max(state.truncatedIndex, state.appliedIndex);
  if (appliedIndex <= state.truncatedIndex) return decision;

  decision.countLimit = config.countLimit > 0 && state.logCount >= config.countLimit;
  decision.sizeLimit = config.sizeLimitBytes > 0 &&
                       state.approximateSizeBytes >= config.sizeLimitBytes;
  if (decision.Forced()) {
    decision.compactIndex = appliedIndex;
    decision.reclaimableCount = static_cast<uint64_t>(appliedIndex - state.truncatedIndex);
    return decision;
  }

  const int replicatedIndex = std::clamp(state.replicatedIndex, state.truncatedIndex,
                                         appliedIndex);
  const uint64_t reclaimable =
      static_cast<uint64_t>(replicatedIndex - state.truncatedIndex);
  if (config.threshold > 0 && reclaimable >= config.threshold) {
    decision.compactIndex = appliedIndex;
    decision.reclaimableCount = static_cast<uint64_t>(appliedIndex - state.truncatedIndex);
    decision.softThreshold = true;
  }
  return decision;
}

#endif  // STRATAKV_RAFT_LOG_GC_H
