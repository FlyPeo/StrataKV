#ifndef STRATAKV_TRANSACTION_TIMESTAMP_ORACLE_H
#define STRATAKV_TRANSACTION_TIMESTAMP_ORACLE_H

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>

namespace HlcTimestamp {

constexpr uint32_t kLogicalBits = 18;
constexpr uint64_t kLogicalMask = (uint64_t{1} << kLogicalBits) - 1;
constexpr uint64_t kMaxPhysicalMs = UINT64_MAX >> kLogicalBits;

uint64_t PhysicalMs(uint64_t timestamp);
uint32_t Logical(uint64_t timestamp);
uint64_t Compose(uint64_t physicalMs, uint32_t logical);
uint64_t WallClockMs();

}  // namespace HlcTimestamp

// Shared contract used by the transaction coordinator. Production clients use
// RemoteTimestampOracle; only TSO control-plane members own persistent
// allocators.
class TimestampOracle {
 public:
  virtual ~TimestampOracle() = default;
  virtual uint64_t Next() = 0;
  virtual uint64_t Peek() = 0;
  virtual void Observe(uint64_t ts) = 0;
};

// Durable single-process allocator. TsoConsensusNode uses it only as a
// per-member crash-safe candidate floor; the cluster-wide authoritative
// high-water and publish decision belong to the TSO Raft state machine.
class PersistentTimestampOracle final : public TimestampOracle {
 public:
  using Clock = std::function<uint64_t()>;

  explicit PersistentTimestampOracle(std::string statePath, uint64_t segmentSize = 4096,
                                     Clock clock = HlcTimestamp::WallClockMs);
  ~PersistentTimestampOracle() override;

  PersistentTimestampOracle(const PersistentTimestampOracle&) = delete;
  PersistentTimestampOracle& operator=(const PersistentTimestampOracle&) = delete;

  uint64_t Next() override;
  uint64_t Peek() override;
  void Observe(uint64_t ts) override;

 private:
  struct PersistedState {
    uint64_t limit = 0;
    bool exists = false;
    bool legacy = false;
  };

  PersistedState ReadPersistedState() const;
  uint64_t NextCandidateLocked() const;
  void ReserveThroughLocked(uint64_t required);
  void PersistLimitLocked(uint64_t limit);

  const std::string statePath_;
  const uint64_t segmentSize_;
  const Clock clock_;
  int lockFd_ = -1;
  std::mutex mutex_;
  uint64_t nextTs_ = 1;
  uint64_t segmentLimit_ = 0;
};

#endif  // STRATAKV_TRANSACTION_TIMESTAMP_ORACLE_H
