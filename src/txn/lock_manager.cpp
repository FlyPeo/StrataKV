#include "lock_manager.h"

namespace {
uint64_t NowMs() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}
}  // namespace

LockManager::LockManager(std::shared_ptr<MvccStorage> storage, RollbackFn rollbackFn,
                         std::chrono::milliseconds checkInterval)
    : storage_(std::move(storage)),
      rollbackFn_(std::move(rollbackFn)),
      checkInterval_(checkInterval),
      stopped_(true) {}

LockManager::~LockManager() { Stop(); }

void LockManager::Start() {
  std::lock_guard<std::mutex> lock(stateMutex_);
  bool expected = true;
  if (!stopped_.compare_exchange_strong(expected, false)) {
    return;
  }
  worker_ = std::thread([this]() {
    while (!stopped_.load()) {
      ScanOnce();
      std::this_thread::sleep_for(checkInterval_);
    }
  });
}

void LockManager::Stop() {
  std::lock_guard<std::mutex> lock(stateMutex_);
  stopped_.store(true);
  if (worker_.joinable()) {
    worker_.join();
  }
}

void LockManager::ScanOnce() {
  for (const auto& item : storage_->ExpiredLocks(NowMs())) {
    rollbackFn_(item.first, item.second);
  }
}
