// Transaction subsystem: monotonic timestamp allocation.
#include "timestamp_oracle.h"

// 初始化全局时间戳发号器，为整个事务系统提供单调递增的起点和水位线。
TimestampOracle::TimestampOracle(uint64_t segmentSize)
    : segmentSize_(segmentSize == 0 ? 1 : segmentSize), nextTs_(1), segmentLimit_(0), persistedMaxTs_(0) {}

// 向全局事务系统分配一个新的开始时间戳或提交时间戳，保证所有事务拥有统一顺序。
uint64_t TimestampOracle::Next() {
  uint64_t ts = nextTs_.fetch_add(1);
  if (ts <= segmentLimit_.load()) {
    return ts;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (ts > segmentLimit_.load()) {
    RefillLocked(ts);
  }
  return ts;
}

// 提供当前全局时间戳进度的可见值，便于调试或恢复时检查发号器状态。
uint64_t TimestampOracle::Peek() const { return nextTs_.load(); }

// 当系统从存储或其他节点观察到更大的时间戳时，推进全局水位线避免重复发号。
void TimestampOracle::Observe(uint64_t ts) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (ts < nextTs_.load()) {
    return;
  }
  nextTs_.store(ts + 1);
  RefillLocked(ts + 1);
}

// 在全局水位线发生推进时，重新划定可分配区间，减少后续发号时的锁竞争。
void TimestampOracle::RefillLocked(uint64_t observed) {
  const uint64_t newLimit = observed + segmentSize_;
  persistedMaxTs_ = newLimit;
  segmentLimit_.store(persistedMaxTs_);
}
