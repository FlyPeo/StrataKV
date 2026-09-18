// Transaction subsystem: per-Store RPC channel pool.
#include "region_channel_pool.h"

#include <algorithm>

RegionChannelPool::RegionChannelPool(size_t channelsPerStore)
    : channelsPerStore_(channelsPerStore == 0 ? 8 : channelsPerStore) {}

std::string RegionChannelPool::Key(const std::string& host, short port) {
  return host + ":" + std::to_string(port);
}

std::shared_ptr<RegionChannelPool::SlotList> RegionChannelPool::SlotsFor(
    const std::string& host, short port) {
  const std::string key = Key(host, port);
  // Readers share this lock; only first creation takes it exclusively,
  // so the steady-state data path never serializes here.
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto found = stores_.find(key);
  if (found != stores_.end()) return found->second;
  lock.unlock();
  std::unique_lock<std::shared_mutex> exclusive(mutex_);
  found = stores_.find(key);
  if (found != stores_.end()) return found->second;
  auto slots = std::make_shared<SlotList>();
  slots->reserve(channelsPerStore_);
  for (size_t index = 0; index < channelsPerStore_; ++index) {
    auto slot = std::make_unique<Slot>();
    slot->channel = std::make_unique<MprpcChannel>(host, port, false);
    slot->stub = std::make_unique<raftKVRpcProctoc::kvServerRpc_Stub>(slot->channel.get());
    stubToSlot_[slot->stub.get()] = slot.get();
    slots->push_back(std::move(slot));
  }
  stores_.emplace(key, slots);
  return slots;
}

raftKVRpcProctoc::kvServerRpc_Stub* RegionChannelPool::Acquire(const std::string& host,
                                                               short port) {
  auto slots = SlotsFor(host, port);
  acquisitions_.fetch_add(1, std::memory_order_relaxed);
  // Round-robin start index so concurrent workers do not all pile onto slot 0.
  const size_t start = nextSlot_.fetch_add(1, std::memory_order_relaxed) % slots->size();
  for (size_t offset = 0; offset < slots->size(); ++offset) {
    auto& slot = *(*slots)[(start + offset) % slots->size()];
    if (slot.mutex.try_lock()) {
      slot.busy = true;
      return slot.stub.get();
    }
  }
  // Every pooled channel is in use: wait for one rather than failing a request
  // that a healthy Store could serve.
  auto& slot = *(*slots)[start % slots->size()];
  slot.mutex.lock();
  slot.busy = true;
  return slot.stub.get();
}

void RegionChannelPool::Release(raftKVRpcProctoc::kvServerRpc_Stub* stub) {
  if (stub == nullptr) return;
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto found = stubToSlot_.find(stub);
  if (found != stubToSlot_.end() && found->second->busy) {
    found->second->busy = false;
    found->second->mutex.unlock();
    return;
  }
  for (auto& store : stores_) {
    for (auto& slot : *store.second) {
      if (slot->stub.get() == stub && slot->busy) {
        slot->busy = false;
        slot->mutex.unlock();
        return;
      }
    }
  }
}

RegionChannelPool::Borrowed RegionChannelPool::Borrow(const std::string& host, short port) {
  auto slots = SlotsFor(host, port);
  acquisitions_.fetch_add(1, std::memory_order_relaxed);
  // Round-robin start index so concurrent workers do not all pile onto slot 0.
  const size_t start = nextSlot_.fetch_add(1, std::memory_order_relaxed) % slots->size();
  for (size_t offset = 0; offset < slots->size(); ++offset) {
    auto& slot = *(*slots)[(start + offset) % slots->size()];
    if (slot.mutex.try_lock()) {
      slot.busy = true;
      return Borrowed(
          slot.stub.get(), &slot,
          [](void*, void* s) {
            auto* slotPtr = static_cast<Slot*>(s);
            slotPtr->busy = false;
            slotPtr->mutex.unlock();
          },
          nullptr, slots);
    }
  }
  // Every pooled channel is in use: wait for one rather than failing a request
  // that a healthy Store could serve.
  auto& slot = *(*slots)[start % slots->size()];
  slot.mutex.lock();
  slot.busy = true;
  return Borrowed(
      slot.stub.get(), &slot,
      [](void*, void* s) {
        auto* slotPtr = static_cast<Slot*>(s);
        slotPtr->busy = false;
        slotPtr->mutex.unlock();
      },
      nullptr, slots);
}

size_t RegionChannelPool::StoreCount() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return stores_.size();
}

size_t RegionChannelPool::ChannelCount() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  size_t total = 0;
  for (const auto& store : stores_) total += store.second->size();
  return total;
}

uint64_t RegionChannelPool::Acquisitions() const {
  return acquisitions_.load(std::memory_order_relaxed);
}
