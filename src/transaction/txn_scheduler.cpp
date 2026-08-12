#include "txn_scheduler.h"

#include <algorithm>
#include <functional>
#include <stdexcept>

TxnScheduler::TxnScheduler(size_t latchSlots) {
  if (latchSlots == 0) {
    throw std::invalid_argument("TxnScheduler requires at least one latch slot");
  }
  slots_.reserve(latchSlots);
  for (size_t index = 0; index < latchSlots; ++index) {
    slots_.push_back(std::make_unique<Slot>());
  }
}

void TxnScheduler::BeginWait() {
  const uint64_t waiters = currentWaiters_.fetch_add(1, std::memory_order_relaxed) + 1;
  uint64_t maximum = maxWaiters_.load(std::memory_order_relaxed);
  while (waiters > maximum &&
         !maxWaiters_.compare_exchange_weak(maximum, waiters, std::memory_order_relaxed)) {
  }
}

void TxnScheduler::EndWait(std::chrono::steady_clock::time_point started) {
  currentWaiters_.fetch_sub(1, std::memory_order_relaxed);
  const uint64_t micros = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                    std::chrono::steady_clock::now() - started)
                                                    .count());
  if (micros > 0) {
    waits_.fetch_add(1, std::memory_order_relaxed);
    waitMicros_.fetch_add(micros, std::memory_order_relaxed);
  }
}

TxnScheduler::Guard TxnScheduler::Acquire(const std::vector<std::string>& keys) {
  Guard guard;
  bool contended = false;
  std::chrono::steady_clock::time_point waitStarted;
  const auto markContended = [&]() {
    if (contended) return;
    contended = true;
    waitStarted = std::chrono::steady_clock::now();
    BeginWait();
  };

  guard.regionReadLock_ =
      std::make_unique<std::shared_lock<std::shared_mutex>>(regionGate_, std::defer_lock);
  if (!guard.regionReadLock_->try_lock()) {
    markContended();
    guard.regionReadLock_->lock();
  }

  std::vector<size_t> slots;
  slots.reserve(keys.size());
  for (const auto& key : keys) {
    slots.push_back(std::hash<std::string>{}(key) % slots_.size());
  }
  std::sort(slots.begin(), slots.end());
  slots.erase(std::unique(slots.begin(), slots.end()), slots.end());
  guard.keyLocks_.reserve(slots.size());
  for (const size_t slot : slots) {
    std::unique_lock<std::mutex> keyLock(slots_[slot]->mutex, std::defer_lock);
    if (!keyLock.try_lock()) {
      markContended();
      keyLock.lock();
    }
    guard.keyLocks_.push_back(std::move(keyLock));
  }
  if (contended) EndWait(waitStarted);
  acquisitions_.fetch_add(1, std::memory_order_relaxed);
  return guard;
}

TxnScheduler::Guard TxnScheduler::AcquireRegionExclusive() {
  Guard guard;
  guard.regionWriteLock_ =
      std::make_unique<std::unique_lock<std::shared_mutex>>(regionGate_, std::defer_lock);
  if (!guard.regionWriteLock_->try_lock()) {
    const auto started = std::chrono::steady_clock::now();
    BeginWait();
    guard.regionWriteLock_->lock();
    EndWait(started);
  }
  acquisitions_.fetch_add(1, std::memory_order_relaxed);
  return guard;
}

TxnScheduler::Stats TxnScheduler::GetStats() const {
  return {acquisitions_.load(std::memory_order_relaxed), waits_.load(std::memory_order_relaxed),
          waitMicros_.load(std::memory_order_relaxed), currentWaiters_.load(std::memory_order_relaxed),
          maxWaiters_.load(std::memory_order_relaxed)};
}
