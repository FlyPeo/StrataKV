#ifndef STRATAKV_TRANSACTION_REGION_REQUEST_SENDER_H
#define STRATAKV_TRANSACTION_REGION_REQUEST_SENDER_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "kv_server_rpc.pb.h"
#include "region.pb.h"
#include "region_cache.h"
#include "region_channel_pool.h"

// Typed routing outcome. Only routing causes trigger a retry: storage, MVCC
// conflict and protocol results are definitive answers for the caller.
enum class RouteErrorKind {
  None,
  Transport,
  NotLeader,
  EpochNotMatch,
  RegionNotFound,
  PeerNotFound,
  KeyNotInRegion,
  ServerOverloaded,
  Timeout,
  Storage,
};

const char* RouteErrorName(RouteErrorKind kind);

// What one attempt produced: the routing cause plus the leader the receiver
// pointed at, so a retry can skip the follower round trip.
struct DispatchOutcome {
  RouteErrorKind kind = RouteErrorKind::None;
  uint64_t leaderPeerId = 0;

  DispatchOutcome() = default;
  DispatchOutcome(RouteErrorKind errorKind, uint64_t leader = 0)
      : kind(errorKind), leaderPeerId(leader) {}
};

// Maps a structured Region response to a routing cause. A reply without a
// header is a legacy static-mode answer and is always definitive.
template <typename Reply>
DispatchOutcome ClassifyRegionReply(const Reply& reply) {
  if (!reply.has_header() || !reply.header().has_error()) return {};
  const auto code = reply.header().error().code();
  if (code == stratakv::region::REGION_ERROR_NOT_LEADER && reply.header().error().has_leader()) {
    return {RouteErrorKind::NotLeader, reply.header().error().leader().peerid()};
  }
  switch (code) {
    case stratakv::region::REGION_ERROR_NOT_LEADER: return RouteErrorKind::NotLeader;
    case stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH: return RouteErrorKind::EpochNotMatch;
    case stratakv::region::REGION_ERROR_REGION_NOT_FOUND: return RouteErrorKind::RegionNotFound;
    case stratakv::region::REGION_ERROR_PEER_NOT_FOUND: return RouteErrorKind::PeerNotFound;
    case stratakv::region::REGION_ERROR_KEY_NOT_IN_REGION: return RouteErrorKind::KeyNotInRegion;
    case stratakv::region::REGION_ERROR_SERVER_OVERLOADED: return RouteErrorKind::ServerOverloaded;
    case stratakv::region::REGION_ERROR_TIMEOUT: return RouteErrorKind::Timeout;
    case stratakv::region::REGION_ERROR_STORAGE: return RouteErrorKind::Storage;
    default: return RouteErrorKind::None;
  }
}

// One caller deadline, attempt ceiling and backoff state shared by transport,
// leader and topology retries. Backoff never sleeps past the deadline.
struct RetryContext {
  std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max();
  int maxAttempts = 8;
  std::chrono::milliseconds initialBackoff{5};
  std::chrono::milliseconds maxBackoff{200};
  uint64_t RemainingMs() const;
  bool Expired() const;
};

struct RegionSenderMetrics {
  uint64_t sends = 0;
  uint64_t attempts = 0;
  uint64_t transportRetries = 0;
  uint64_t leaderRetries = 0;
  uint64_t epochRefreshes = 0;
  uint64_t regionNotFoundRefreshes = 0;
  uint64_t refreshFailures = 0;
  uint64_t leaderHintUpdates = 0;
  uint64_t regroupRequired = 0;
  uint64_t deadlineExhausted = 0;
  uint64_t definitiveFailures = 0;
};

struct SendResult {
  RouteErrorKind kind = RouteErrorKind::None;
  // True when the last attempt produced a definitive answer (success or a
  // non-routing error). False means the caller must surface a retryable error.
  bool completed = false;
  // True when a refresh changed Region boundaries under a key batch, so the
  // coordinator must regroup before retrying.
  bool regroupRequired = false;
  int attempts = 0;
  int regionId = -1;
  std::string message;

  bool ok() const { return completed && kind == RouteErrorKind::None; }
};

// Central data-path sender: routes a key or key batch, picks a peer, classifies
// the structured response, and retries only within the caller's deadline while
// preserving whatever identity the dispatch callback puts on the wire.
class RegionRequestSender {
 public:
    using Dispatch = std::function<DispatchOutcome(raftKVRpcProctoc::kvServerRpc_Stub& stub,
                                                 const RegionMetadata& descriptor,
                                                 const RegionPeerLocation& peer,
                                                 uint64_t remainingBudgetMs)>;
    using BatchDispatch =
    std::function<DispatchOutcome(raftKVRpcProctoc::kvServerRpc_Stub& stub,
                                    const RegionMetadata& descriptor,
                                    const RegionPeerLocation& peer,
                                    const std::vector<std::string>& keys,
                                    uint64_t remainingBudgetMs)>;

  // Structured routing diagnostics. Fields are Region id, epoch, metadata
  // revision, cause and attempt count only: never keys, values or payloads.
  using LogSink = std::function<void(const std::string& event, int regionId, uint64_t epochVersion,
                                     uint64_t metadataRevision, const char* cause, int attempts)>;
  RegionRequestSender(std::shared_ptr<RegionCache> cache,
                      std::shared_ptr<RegionChannelSource> channels,
                      RegionCache::RefreshFunction refresh);
  // The sink is installed once before traffic; it must be safe to call from
  // multiple request threads.
  void SetLogSink(LogSink sink) { logSink_ = std::move(sink); }

  // Shares this sender's metadata refresh with the router layer: a coordinator
  // lookup that misses or hits an invalidated Region refreshes once (network
  // I/O outside any cache or router lock) before failing. The router owns the
  // cache and performs the LookupOrRefresh itself.
  RegionCache::RefreshFunction RefreshFunctionForRouter() const { return refresh_; }

  SendResult Send(const std::string& key, const RetryContext& context,
                  const Dispatch& dispatch);
  SendResult SendBatch(const std::vector<std::string>& keys, const RetryContext& context,
                       const BatchDispatch& dispatch);

  RegionSenderMetrics Metrics() const;
  // Peer ordering prefers the last known leader so the steady state avoids a
  // follower round trip after a leadership change.
  std::vector<RegionPeerLocation> OrderedPeers(const RegionMetadata& descriptor) const;
  void ObserveLeader(int regionId, uint64_t peerId);
  uint64_t LeaderHint(int regionId) const;

 private:
  bool Resolve(const std::string& key, const RetryContext& context,
               RegionRouteHandle* route, SendResult* result);
  bool RefreshRoute(const std::string& key, const RetryContext& context,
                    RegionRouteHandle* route, SendResult* result, RouteErrorKind cause);
  bool SleepBeforeRetry(int attempt, const RetryContext& context);
  void Record(RouteErrorKind kind, SendResult* result);
  void Emit(const char* event, const RegionMetadata& descriptor, RouteErrorKind cause,
            int attempts) const;

  std::shared_ptr<RegionCache> cache_;
  std::shared_ptr<RegionChannelSource> channels_;
  RegionCache::RefreshFunction refresh_;
  LogSink logSink_;

  mutable std::mutex hintMutex_;
  std::unordered_map<int, uint64_t> leaderHints_;

  mutable std::atomic<uint64_t> sends_{0};
  mutable std::atomic<uint64_t> attempts_{0};
  mutable std::atomic<uint64_t> transportRetries_{0};
  mutable std::atomic<uint64_t> leaderRetries_{0};
  mutable std::atomic<uint64_t> epochRefreshes_{0};
  mutable std::atomic<uint64_t> regionNotFoundRefreshes_{0};
  mutable std::atomic<uint64_t> refreshFailures_{0};
  mutable std::atomic<uint64_t> leaderHintUpdates_{0};
  mutable std::atomic<uint64_t> regroupRequired_{0};
  mutable std::atomic<uint64_t> deadlineExhausted_{0};
  mutable std::atomic<uint64_t> definitiveFailures_{0};
};

#endif  // STRATAKV_TRANSACTION_REGION_REQUEST_SENDER_H
