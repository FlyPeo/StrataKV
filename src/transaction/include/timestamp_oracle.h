#ifndef STRATAKV_TRANSACTION_TIMESTAMP_ORACLE_H
#define STRATAKV_TRANSACTION_TIMESTAMP_ORACLE_H

#include <cstdint>
#include <mutex>
#include <string>

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

// Single-process allocator used by stratakv-tso. Before timestamps from a new
// segment become visible, the segment's upper bound is durably persisted.
class PersistentTimestampOracle final : public TimestampOracle {
 public:
  explicit PersistentTimestampOracle(std::string statePath, uint64_t segmentSize = 4096);
  ~PersistentTimestampOracle() override;

  PersistentTimestampOracle(const PersistentTimestampOracle&) = delete;
  PersistentTimestampOracle& operator=(const PersistentTimestampOracle&) = delete;

  uint64_t Next() override;
  uint64_t Peek() override;
  void Observe(uint64_t ts) override;

 private:
  uint64_t ReadPersistedLimit() const;
  void ReserveThroughLocked(uint64_t required);
  void PersistLimitLocked(uint64_t limit);

  const std::string statePath_;
  const uint64_t segmentSize_;
  int lockFd_ = -1;
  std::mutex mutex_;
  uint64_t nextTs_ = 1;
  uint64_t segmentLimit_ = 0;
};

#endif  // STRATAKV_TRANSACTION_TIMESTAMP_ORACLE_H
