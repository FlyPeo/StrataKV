#ifndef STRATAKV_COMMON_BOUNDED_THREAD_POOL_H
#define STRATAKV_COMMON_BOUNDED_THREAD_POOL_H

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

// A fixed-size executor for code that can block in RPC, storage, or native
// synchronization. The bounded queue is intentional: callers can reject work
// at an ingress boundary instead of turning overload into unbounded memory and
// latency growth.
class BoundedThreadPool {
 public:
  BoundedThreadPool(size_t workerCount, size_t queueCapacity)
      : workerCount_(workerCount), queueCapacity_(queueCapacity) {
    if (workerCount_ == 0 || queueCapacity_ == 0) {
      throw std::invalid_argument("bounded thread pool sizes must be positive");
    }
    workers_.reserve(workerCount_);
    for (size_t index = 0; index < workerCount_; ++index) {
      workers_.emplace_back([this]() { WorkerLoop(); });
    }
  }

  ~BoundedThreadPool() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_ = true;
    }
    workAvailable_.notify_all();
    queueSpaceAvailable_.notify_all();
    for (std::thread& worker : workers_) {
      if (worker.joinable()) worker.join();
    }
  }

  BoundedThreadPool(const BoundedThreadPool&) = delete;
  BoundedThreadPool& operator=(const BoundedThreadPool&) = delete;

  // Non-blocking submission for network ingress. A full queue is reported to
  // the caller so it can apply explicit overload backpressure.
  template <typename Function>
  bool TrySchedule(Function&& function) {
    std::function<void()> task(std::forward<Function>(function));
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stopping_ || tasks_.size() >= queueCapacity_) {
        rejected_.fetch_add(1, std::memory_order_relaxed);
        return false;
      }
      tasks_.emplace_back(std::move(task));
    }
    workAvailable_.notify_one();
    return true;
  }

  // Blocking submission is used inside a native request worker. Waiting for
  // queue capacity is safe there and keeps cross-Region fan-out bounded.
  template <typename Function>
  auto Submit(Function&& function)
      -> std::future<std::invoke_result_t<std::decay_t<Function>>> {
    using Result = std::invoke_result_t<std::decay_t<Function>>;
    auto task = std::make_shared<std::packaged_task<Result()>>(
        std::forward<Function>(function));
    std::future<Result> future = task->get_future();
    {
      std::unique_lock<std::mutex> lock(mutex_);
      queueSpaceAvailable_.wait(lock, [this]() {
        return stopping_ || tasks_.size() < queueCapacity_;
      });
      if (stopping_) throw std::runtime_error("bounded thread pool is stopping");
      tasks_.emplace_back([task]() { (*task)(); });
    }
    workAvailable_.notify_one();
    return future;
  }

  size_t workers() const { return workerCount_; }
  size_t queueCapacity() const { return queueCapacity_; }

  size_t queued() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return tasks_.size();
  }

  size_t active() const { return active_.load(std::memory_order_relaxed); }
  uint64_t rejected() const { return rejected_.load(std::memory_order_relaxed); }

 private:
  void WorkerLoop() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        workAvailable_.wait(lock, [this]() { return stopping_ || !tasks_.empty(); });
        if (stopping_ && tasks_.empty()) return;
        task = std::move(tasks_.front());
        tasks_.pop_front();
        active_.fetch_add(1, std::memory_order_relaxed);
      }
      queueSpaceAvailable_.notify_one();
      try {
        task();
      } catch (...) {
        // TrySchedule has no result channel. Service boundaries are expected
        // to translate failures, while Submit stores them in its future.
      }
      active_.fetch_sub(1, std::memory_order_relaxed);
    }
  }

  const size_t workerCount_;
  const size_t queueCapacity_;
  mutable std::mutex mutex_;
  std::condition_variable workAvailable_;
  std::condition_variable queueSpaceAvailable_;
  std::deque<std::function<void()>> tasks_;
  std::vector<std::thread> workers_;
  bool stopping_ = false;
  std::atomic<size_t> active_{0};
  std::atomic<uint64_t> rejected_{0};
};

#endif  // STRATAKV_COMMON_BOUNDED_THREAD_POOL_H
