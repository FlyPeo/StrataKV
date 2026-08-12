#ifndef STRATAKV_TRANSACTION_TXN_SCHEDULER_H
#define STRATAKV_TRANSACTION_TXN_SCHEDULER_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

// Region-local short-lived concurrency control.  These latches are not MVCC
// locks: they are neither persisted nor replicated, and only serialize the
// read/prepare/propose/apply lifecycle on the current Region leader.
class TxnScheduler {
 public:
  struct Stats {
    uint64_t acquisitions = 0;
    uint64_t waits = 0;
    uint64_t waitMicros = 0;
    uint64_t currentWaiters = 0;
    uint64_t maxWaiters = 0;
  };

  class Guard {
   public:
    Guard() = default;
    Guard(Guard&&) noexcept = default;
    Guard& operator=(Guard&&) noexcept = default;
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
    ~Guard() = default;

    explicit operator bool() const { return regionReadLock_ != nullptr || regionWriteLock_ != nullptr; }

   private:
    friend class TxnScheduler;
    std::unique_ptr<std::shared_lock<std::shared_mutex>> regionReadLock_;
    std::unique_ptr<std::unique_lock<std::shared_mutex>> regionWriteLock_;
    std::vector<std::unique_lock<std::mutex>> keyLocks_;
  };

  explicit TxnScheduler(size_t latchSlots = 4096);

  // Sorts and deduplicates key hash slots before blocking, so multi-key
  // commands cannot deadlock even if callers provide keys in reverse order.
  Guard Acquire(const std::vector<std::string>& keys);
  Guard Acquire(const std::string& key) { return Acquire(std::vector<std::string>{key}); }

  // Maintenance commands whose read/write set covers the whole Region use an
  // exclusive gate; ordinary key commands hold the gate in shared mode.
  Guard AcquireRegionExclusive();
  Stats GetStats() const;

 private:
  struct Slot {
    std::mutex mutex;
  };

  void BeginWait();
  void EndWait(std::chrono::steady_clock::time_point started);

  std::shared_mutex regionGate_;
  std::vector<std::unique_ptr<Slot>> slots_;
  std::atomic<uint64_t> acquisitions_{0};
  std::atomic<uint64_t> waits_{0};
  std::atomic<uint64_t> waitMicros_{0};
  std::atomic<uint64_t> currentWaiters_{0};
  std::atomic<uint64_t> maxWaiters_{0};
};

#endif  // STRATAKV_TRANSACTION_TXN_SCHEDULER_H
