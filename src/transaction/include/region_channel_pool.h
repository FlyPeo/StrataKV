#ifndef STRATAKV_TRANSACTION_REGION_CHANNEL_POOL_H
#define STRATAKV_TRANSACTION_REGION_CHANNEL_POOL_H

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "kv_server_rpc.pb.h"
#include "mprpc_channel.h"

// Source of per-Store RPC channels. The indirection exists so routing tests can
// inject scripted endpoints without binding sockets.
class RegionChannelSource {
 public:
  virtual ~RegionChannelSource() = default;

  class Borrowed {
   public:
    using Releaser = void (*)(void* context, void* slot);

    Borrowed() = default;
    Borrowed(raftKVRpcProctoc::kvServerRpc_Stub* stub, void* slot, Releaser releaser,
             void* context = nullptr, std::shared_ptr<void> owner = nullptr)
        : stub(stub),
          slot_(slot),
          releaser_(releaser),
          context_(context),
          owner_(std::move(owner)) {}
    ~Borrowed() { Release(); }

    Borrowed(Borrowed&& other) noexcept
        : stub(other.stub),
          slot_(other.slot_),
          releaser_(other.releaser_),
          context_(other.context_),
          owner_(std::move(other.owner_)) {
      other.stub = nullptr;
      other.slot_ = nullptr;
      other.releaser_ = nullptr;
      other.context_ = nullptr;
    }

    Borrowed& operator=(Borrowed&& other) noexcept {
      if (this != &other) {
        Release();
        stub = other.stub;
        slot_ = other.slot_;
        releaser_ = other.releaser_;
        context_ = other.context_;
        owner_ = std::move(other.owner_);
        other.stub = nullptr;
        other.slot_ = nullptr;
        other.releaser_ = nullptr;
        other.context_ = nullptr;
      }
      return *this;
    }

    Borrowed(const Borrowed&) = delete;
    Borrowed& operator=(const Borrowed&) = delete;

    raftKVRpcProctoc::kvServerRpc_Stub* stub = nullptr;
    explicit operator bool() const { return stub != nullptr; }
    raftKVRpcProctoc::kvServerRpc_Stub* operator->() const { return stub; }

    void Release() {
      if (releaser_ != nullptr && slot_ != nullptr) {
        releaser_(context_, slot_);
        releaser_ = nullptr;
        slot_ = nullptr;
        context_ = nullptr;
      }
      owner_.reset();
      stub = nullptr;
    }

   private:
    void* slot_ = nullptr;
    Releaser releaser_ = nullptr;
    void* context_ = nullptr;
    std::shared_ptr<void> owner_;
  };

  virtual Borrowed Borrow(const std::string& host, short port) {
    auto* s = Acquire(host, port);
    if (s == nullptr) return Borrowed();
    return Borrowed(
        s, s,
        [](void* ctx, void* stubPtr) {
          if (ctx != nullptr && stubPtr != nullptr) {
            static_cast<RegionChannelSource*>(ctx)->Release(
                static_cast<raftKVRpcProctoc::kvServerRpc_Stub*>(stubPtr));
          }
        },
        this, nullptr);
  }

  // Returns nullptr when the endpoint has no usable channel right now.
  virtual raftKVRpcProctoc::kvServerRpc_Stub* Acquire(const std::string& host, short port) = 0;
  virtual void Release(raftKVRpcProctoc::kvServerRpc_Stub* stub) = 0;
};

// Owns MPRPC channels keyed by advertised Store address. A stub is not safe for
// concurrent use, so callers borrow one slot at a time and return it.
class RegionChannelPool : public RegionChannelSource {
 public:
  using Borrowed = RegionChannelSource::Borrowed;

  explicit RegionChannelPool(size_t channelsPerStore = 8);

  raftKVRpcProctoc::kvServerRpc_Stub* Acquire(const std::string& host, short port) override;
  void Release(raftKVRpcProctoc::kvServerRpc_Stub* stub) override;
  Borrowed Borrow(const std::string& host, short port) override;

  size_t StoreCount() const;
  size_t ChannelCount() const;
  uint64_t Acquisitions() const;

 private:
  struct Slot {
    std::mutex mutex;
    std::unique_ptr<MprpcChannel> channel;
    std::unique_ptr<raftKVRpcProctoc::kvServerRpc_Stub> stub;
    bool busy = false;
  };

  using SlotList = std::vector<std::unique_ptr<Slot>>;
  static std::string Key(const std::string& host, short port);
  // Returns a stable shared slot list, so borrowed channels stay valid even if
  // another Store is registered concurrently.
  std::shared_ptr<SlotList> SlotsFor(const std::string& host, short port);

  const size_t channelsPerStore_;
  std::atomic<size_t> nextSlot_{0};
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<SlotList>> stores_;
  std::unordered_map<raftKVRpcProctoc::kvServerRpc_Stub*, Slot*> stubToSlot_;
  std::atomic<uint64_t> acquisitions_{0};
};

#endif  // STRATAKV_TRANSACTION_REGION_CHANNEL_POOL_H
