#ifndef STRATAKV_RAFT_LOG_GC_H
#define STRATAKV_RAFT_LOG_GC_H

#include <algorithm>
#include <chrono>
#include <cstdint>

struct RaftLogGcConfig {
  uint64_t threshold = 50;
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

// A soft collection stays behind the least replicated peer so a healthy
// follower can continue with AppendEntries. Hard limits prioritize bounding
// local log growth and compact to the applied state; a lagging follower then
// catches up through InstallSnapshot.
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
    decision.compactIndex = replicatedIndex;
    decision.reclaimableCount = reclaimable;
    decision.softThreshold = true;
  }
  return decision;
}

#endif  // STRATAKV_RAFT_LOG_GC_H
