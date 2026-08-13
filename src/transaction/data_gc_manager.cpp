// Transaction subsystem: obsolete MVCC data collection.
#include "data_gc_manager.h"

#include <exception>

namespace {
uint64_t NowMs() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}
}  // namespace

DataGcManager::DataGcManager(std::shared_ptr<MvccStorage> storage, std::shared_ptr<TimestampOracle> tso,
                             std::chrono::milliseconds gcRetention, std::chrono::milliseconds checkInterval)
    : storage_(std::move(storage)),
      tso_(std::move(tso)),
      gcRetention_(gcRetention),
      checkInterval_(checkInterval),
      stopped_(true) {}

DataGcManager::~DataGcManager() { Stop(); }

void DataGcManager::Start() {
  std::lock_guard<std::mutex> lock(stateMutex_);
  bool expected = true;
  if (!stopped_.compare_exchange_strong(expected, false)) {
    return;
  }
  worker_ = std::thread([this]() {
    while (!stopped_.load()) {
      try {
        ScanOnce();
      } catch (const std::exception&) {
        // A temporary TSO outage must not terminate the client process. GC can
        // safely skip a round because retaining old MVCC versions is harmless.
      }
      std::this_thread::sleep_for(checkInterval_);
    }
  });
}

void DataGcManager::Stop() {
  std::lock_guard<std::mutex> lock(stateMutex_);
  stopped_.store(true);
  if (worker_.joinable()) {
    worker_.join();
  }
}

void DataGcManager::ScanOnce() {
  uint64_t currentMs = NowMs();
  uint64_t currentTs = tso_->Peek();

  std::lock_guard<std::mutex> lock(stateMutex_);
  // 将当前物理时间和逻辑 TSO 的映射存入队列尾部
  timeToTsQueue_.push_back({currentMs, currentTs});

  // 寻找可以作为 safePointTs 的 TSO
  // 要求该映射点的物理时间加上保留时长必须小于等于当前物理时间
  uint64_t safePointTs = 0;
  while (!timeToTsQueue_.empty()) {
    const auto& front = timeToTsQueue_.front();
    if (front.first + static_cast<uint64_t>(gcRetention_.count()) <= currentMs) {
      safePointTs = front.second;
      timeToTsQueue_.pop_front();
    } else {
      break;
    }
  }

  if (safePointTs > 0) {
    storage_->GarbageCollect(safePointTs);
  }
}
