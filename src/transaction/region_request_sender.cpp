// Transaction subsystem: topology-aware data RPC routing and retry.
#include "region_request_sender.h"

#include <algorithm>
#include <random>
#include <thread>

namespace {

bool NeedsRefresh(RouteErrorKind kind) {
  return kind == RouteErrorKind::EpochNotMatch || kind == RouteErrorKind::RegionNotFound ||
         kind == RouteErrorKind::PeerNotFound || kind == RouteErrorKind::KeyNotInRegion;
}

}  // namespace

const char* RouteErrorName(RouteErrorKind kind) {
  switch (kind) {
    case RouteErrorKind::None: return "none";
    case RouteErrorKind::Transport: return "transport";
    case RouteErrorKind::NotLeader: return "not_leader";
    case RouteErrorKind::EpochNotMatch: return "epoch_not_match";
    case RouteErrorKind::RegionNotFound: return "region_not_found";
    case RouteErrorKind::PeerNotFound: return "peer_not_found";
    case RouteErrorKind::KeyNotInRegion: return "key_not_in_region";
    case RouteErrorKind::ServerOverloaded: return "server_overloaded";
    case RouteErrorKind::Timeout: return "timeout";
    case RouteErrorKind::Storage: return "storage";
  }
  return "unknown";
}

uint64_t RetryContext::RemainingMs() const {
  if (deadline == std::chrono::steady_clock::time_point::max()) return 0;
  const auto now = std::chrono::steady_clock::now();
  if (now >= deadline) return 0;
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
}

bool RetryContext::Expired() const {
  return deadline != std::chrono::steady_clock::time_point::max() &&
         std::chrono::steady_clock::now() >= deadline;
}

RegionRequestSender::RegionRequestSender(std::shared_ptr<RegionCache> cache,
                                         std::shared_ptr<RegionChannelSource> channels,
                                         RegionCache::RefreshFunction refresh)
    : cache_(std::move(cache)),
      channels_(std::move(channels)),
      refresh_(std::move(refresh)) {
  if (!cache_) throw std::invalid_argument("RegionRequestSender needs a Region cache");
  if (!channels_) throw std::invalid_argument("RegionRequestSender needs a channel source");
}

void RegionRequestSender::Record(RouteErrorKind kind, SendResult* result) {
  switch (kind) {
    case RouteErrorKind::Transport: transportRetries_.fetch_add(1, std::memory_order_relaxed); break;
    case RouteErrorKind::NotLeader: leaderRetries_.fetch_add(1, std::memory_order_relaxed); break;
    case RouteErrorKind::EpochNotMatch:
    case RouteErrorKind::KeyNotInRegion:
      epochRefreshes_.fetch_add(1, std::memory_order_relaxed);
      break;
    case RouteErrorKind::RegionNotFound:
    case RouteErrorKind::PeerNotFound:
      regionNotFoundRefreshes_.fetch_add(1, std::memory_order_relaxed);
      break;
    default: definitiveFailures_.fetch_add(1, std::memory_order_relaxed); break;
  }
  result->kind = kind;
}

void RegionRequestSender::Emit(const char* event, const RegionMetadata& descriptor,
                               RouteErrorKind cause, int attempts) const {
  if (!logSink_) return;
  logSink_(event, descriptor.regionId, descriptor.epoch.version, descriptor.metadataRevision,
           RouteErrorName(cause), attempts);
}

bool RegionRequestSender::SleepBeforeRetry(int attempt, const RetryContext& context) {
  if (context.Expired()) return false;
  int64_t backoffMs = context.initialBackoff.count() * (1LL << std::min(attempt, 5));
  backoffMs = std::min<int64_t>(backoffMs, context.maxBackoff.count());
  static thread_local std::mt19937 generator(std::random_device{}());
  std::uniform_int_distribution<int64_t> jitter(0, std::max<int64_t>(1, backoffMs));
  const auto delay = std::chrono::milliseconds(jitter(generator));
  const auto now = std::chrono::steady_clock::now();
  if (context.deadline != std::chrono::steady_clock::time_point::max() &&
      now + delay >= context.deadline) {
    std::this_thread::sleep_until(context.deadline);
    return !context.Expired();
  }
  std::this_thread::sleep_for(delay);
  return !context.Expired();
}

std::vector<RegionPeerLocation> RegionRequestSender::OrderedPeers(
    const RegionMetadata& descriptor) const {
  std::vector<RegionPeerLocation> peers = descriptor.peers;
  // The learned hint reflects the real leader; the descriptor's hint is only
  // the bootstrap guess (first peer) and must never override it.
  uint64_t preferred = LeaderHint(descriptor.regionId);
  if (preferred == 0) preferred = descriptor.leaderPeerId;
  if (preferred != 0) {
    std::stable_sort(peers.begin(), peers.end(), [preferred](const RegionPeerLocation& lhs,
                                                             const RegionPeerLocation& rhs) {
      return (lhs.peerId == preferred) && (rhs.peerId != preferred);
    });
  }
  return peers;
}

void RegionRequestSender::ObserveLeader(int regionId, uint64_t peerId) {
  if (peerId == 0) return;
  std::lock_guard<std::mutex> lock(hintMutex_);
  const auto inserted = leaderHints_.emplace(regionId, peerId);
  if (!inserted.second) {
    if (inserted.first->second == peerId) return;
    inserted.first->second = peerId;
  }
  leaderHintUpdates_.fetch_add(1, std::memory_order_relaxed);
}

uint64_t RegionRequestSender::LeaderHint(int regionId) const {
  std::lock_guard<std::mutex> lock(hintMutex_);
  const auto found = leaderHints_.find(regionId);
  return found == leaderHints_.end() ? 0 : found->second;
}

bool RegionRequestSender::Resolve(const std::string& key, const RetryContext& context,
                                  RegionRouteHandle* route, SendResult* result) {
  try {
    *route = cache_->LookupKey(key);
    return true;
  } catch (const std::out_of_range&) {
    // Cache miss or invalidation: refresh inside the caller's deadline.
    return RefreshRoute(key, context, route, result, RouteErrorKind::RegionNotFound);
  }
}

bool RegionRequestSender::RefreshRoute(const std::string& key, const RetryContext& context,
                                       RegionRouteHandle* route, SendResult* result,
                                       RouteErrorKind cause) {
  Record(cause, result);
  if (!refresh_) {
    refreshFailures_.fetch_add(1, std::memory_order_relaxed);
    result->message = "no metadata refresh function is configured";
    result->kind = RouteErrorKind::RegionNotFound;
    return false;
  }
  try {
    *route = cache_->LookupOrRefresh(key, context.deadline, refresh_);
    return true;
  } catch (const std::exception& error) {
    refreshFailures_.fetch_add(1, std::memory_order_relaxed);
    result->message = error.what();
    result->kind = RouteErrorKind::RegionNotFound;
    if (context.Expired()) {
      deadlineExhausted_.fetch_add(1, std::memory_order_relaxed);
      result->kind = RouteErrorKind::Timeout;
    }
    return false;
  }
}

SendResult RegionRequestSender::Send(const std::string& key, const RetryContext& context,
                                     const Dispatch& dispatch) {
  SendResult result;
  const RegionMetadata* lastDescriptor = nullptr;
  sends_.fetch_add(1, std::memory_order_relaxed);
  for (int attempt = 0; attempt < context.maxAttempts; ++attempt) {
    if (context.Expired()) {
      deadlineExhausted_.fetch_add(1, std::memory_order_relaxed);
      result.kind = RouteErrorKind::Timeout;
      result.message = "Region request deadline exhausted";
      return result;
    }
    RegionRouteHandle route;
    if (!Resolve(key, context, &route, &result)) return result;
    const RegionMetadata& descriptor = route.Descriptor();
    lastDescriptor = &descriptor;
    result.regionId = descriptor.regionId;

    RouteErrorKind kind = RouteErrorKind::Transport;
    for (const auto& peer : OrderedPeers(descriptor)) {
      auto channel = channels_->Borrow(peer.host, peer.port);
      if (!channel) {
        kind = RouteErrorKind::Transport;
        transportRetries_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      attempts_.fetch_add(1, std::memory_order_relaxed);
      const auto outcome = dispatch(*channel.stub, descriptor, peer, context.RemainingMs());
      channel.Release();
      kind = outcome.kind;
      if (kind == RouteErrorKind::None) break;
      if (kind == RouteErrorKind::NotLeader) {
        // A hint to an unknown peer means membership changed underneath this
        // cache entry. Refresh topology instead of repeatedly rotating through
        // peers that have all been decommissioned.
        if (outcome.leaderPeerId != 0) ObserveLeader(descriptor.regionId, outcome.leaderPeerId);
        if (outcome.leaderPeerId != 0 && descriptor.FindPeer(outcome.leaderPeerId) == nullptr) {
          kind = RouteErrorKind::EpochNotMatch;
          break;
        }
        continue;
      }
      if (kind == RouteErrorKind::Transport) continue;
      // Epoch/Region/Peer/Key errors are descriptor-wide: every other peer
      // would answer the same, so rotating only burns the retry budget.
      break;
    }

    result.attempts = attempt + 1;
    if (kind == RouteErrorKind::None) {
      result.completed = true;
      result.kind = RouteErrorKind::None;
      return result;
    }
    Record(kind, &result);
    if (kind == RouteErrorKind::Transport || kind == RouteErrorKind::NotLeader) {
      if (!SleepBeforeRetry(attempt, context)) {
        deadlineExhausted_.fetch_add(1, std::memory_order_relaxed);
        result.kind = RouteErrorKind::Timeout;
        result.message = "Region request deadline exhausted during leader retry";
      }
      continue;
    }
    if (NeedsRefresh(kind)) {
      cache_->Invalidate(descriptor.regionId);
      Emit("region_route_refreshed", descriptor, kind, attempt + 1);
      if (kind == RouteErrorKind::RegionNotFound) {
        if (!SleepBeforeRetry(attempt, context)) {
          deadlineExhausted_.fetch_add(1, std::memory_order_relaxed);
          result.kind = RouteErrorKind::Timeout;
          result.message = "Region request deadline exhausted during materialization retry";
          Emit("region_request_deadline", descriptor, RouteErrorKind::Timeout, attempt + 1);
          return result;
        }
      } else if (context.Expired()) {
        deadlineExhausted_.fetch_add(1, std::memory_order_relaxed);
        result.kind = RouteErrorKind::Timeout;
        result.message = "Region request deadline exhausted during topology refresh";
        Emit("region_request_deadline", descriptor, RouteErrorKind::Timeout, attempt + 1);
        return result;
      }
      continue;
    }
    // Storage, overload and protocol results are definitive for the caller.
    result.completed = true;
    result.message = RouteErrorName(kind);
    Emit("region_request_definitive", descriptor, kind, attempt + 1);
    return result;
  }
  if (result.kind == RouteErrorKind::None) {
    result.kind = RouteErrorKind::Timeout;
    result.message = "Region request retry budget exhausted";
  }
  deadlineExhausted_.fetch_add(1, std::memory_order_relaxed);
  if (lastDescriptor != nullptr) {
    Emit("region_request_exhausted", *lastDescriptor, result.kind, result.attempts);
  }
  return result;
}

SendResult RegionRequestSender::SendBatch(const std::vector<std::string>& keys,
                                          const RetryContext& context,
                                          const BatchDispatch& dispatch) {
  SendResult result;
  if (keys.empty()) {
    result.completed = true;
    return result;
  }
  sends_.fetch_add(1, std::memory_order_relaxed);
  for (int attempt = 0; attempt < context.maxAttempts; ++attempt) {
    if (context.Expired()) {
      deadlineExhausted_.fetch_add(1, std::memory_order_relaxed);
      result.kind = RouteErrorKind::Timeout;
      result.message = "Region batch deadline exhausted";
      return result;
    }
    RegionRouteHandle route;
    if (!Resolve(keys.front(), context, &route, &result)) return result;
    const RegionMetadata& descriptor = route.Descriptor();

    // A batch is only valid while every key still belongs to the same Region.
    // Once a refresh splits the range the coordinator must regroup instead of
    // letting one peer silently reject part of the work.
    std::vector<RegionRouteHandle> routes;
    routes.reserve(keys.size());
    bool sameRegion = true;
    for (const auto& key : keys) {
      RegionRouteHandle keyRoute;
      if (!Resolve(key, context, &keyRoute, &result)) return result;
      if (keyRoute.Descriptor().regionId != descriptor.regionId) sameRegion = false;
      routes.push_back(keyRoute);
    }
    if (!sameRegion) {
      regroupRequired_.fetch_add(1, std::memory_order_relaxed);
      result.regroupRequired = true;
      result.completed = false;
      result.kind = RouteErrorKind::None;
      result.message = "refreshed Region boundaries no longer contain the whole batch";
      Emit("region_batch_regroup", descriptor, RouteErrorKind::None, attempt + 1);
      return result;
    }
    result.regionId = descriptor.regionId;

    RouteErrorKind kind = RouteErrorKind::Transport;
    for (const auto& peer : OrderedPeers(descriptor)) {
      auto channel = channels_->Borrow(peer.host, peer.port);
      if (!channel) {
        kind = RouteErrorKind::Transport;
        transportRetries_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      attempts_.fetch_add(1, std::memory_order_relaxed);
      const auto outcome = dispatch(*channel.stub, descriptor, peer, keys, context.RemainingMs());
      channel.Release();
      kind = outcome.kind;
      if (kind == RouteErrorKind::None) break;
      if (kind == RouteErrorKind::NotLeader) {
        if (outcome.leaderPeerId != 0) ObserveLeader(descriptor.regionId, outcome.leaderPeerId);
        if (outcome.leaderPeerId != 0 && descriptor.FindPeer(outcome.leaderPeerId) == nullptr) {
          kind = RouteErrorKind::EpochNotMatch;
          break;
        }
        continue;
      }
      if (kind == RouteErrorKind::Transport) continue;
      break;
    }

    result.attempts = attempt + 1;
    if (kind == RouteErrorKind::None) {
      result.completed = true;
      result.kind = RouteErrorKind::None;
      return result;
    }
    Record(kind, &result);
    if (kind == RouteErrorKind::Transport || kind == RouteErrorKind::NotLeader) {
      if (!SleepBeforeRetry(attempt, context)) {
        deadlineExhausted_.fetch_add(1, std::memory_order_relaxed);
        result.kind = RouteErrorKind::Timeout;
        result.message = "Region batch deadline exhausted during leader retry";
      }
      continue;
    }
    if (NeedsRefresh(kind)) {
      cache_->Invalidate(descriptor.regionId);
      Emit("region_route_refreshed", descriptor, kind, attempt + 1);
      if (kind == RouteErrorKind::RegionNotFound) {
        if (!SleepBeforeRetry(attempt, context)) {
          deadlineExhausted_.fetch_add(1, std::memory_order_relaxed);
          result.kind = RouteErrorKind::Timeout;
          result.message = "Region batch deadline exhausted during materialization retry";
          Emit("region_request_deadline", descriptor, RouteErrorKind::Timeout, attempt + 1);
          return result;
        }
      } else if (context.Expired()) {
        deadlineExhausted_.fetch_add(1, std::memory_order_relaxed);
        result.kind = RouteErrorKind::Timeout;
        result.message = "Region batch deadline exhausted during topology refresh";
        Emit("region_request_deadline", descriptor, RouteErrorKind::Timeout, attempt + 1);
        return result;
      }
      continue;
    }
    result.completed = true;
    result.message = RouteErrorName(kind);
    Emit("region_request_definitive", descriptor, kind, attempt + 1);
    return result;
  }
  if (result.kind == RouteErrorKind::None) {
    result.kind = RouteErrorKind::Timeout;
    result.message = "Region batch retry budget exhausted";
  }
  deadlineExhausted_.fetch_add(1, std::memory_order_relaxed);
  return result;
}

RegionSenderMetrics RegionRequestSender::Metrics() const {
  return {sends_.load(std::memory_order_relaxed),
          attempts_.load(std::memory_order_relaxed),
          transportRetries_.load(std::memory_order_relaxed),
          leaderRetries_.load(std::memory_order_relaxed),
          epochRefreshes_.load(std::memory_order_relaxed),
          regionNotFoundRefreshes_.load(std::memory_order_relaxed),
          refreshFailures_.load(std::memory_order_relaxed),
          leaderHintUpdates_.load(std::memory_order_relaxed),
          regroupRequired_.load(std::memory_order_relaxed),
          deadlineExhausted_.load(std::memory_order_relaxed),
          definitiveFailures_.load(std::memory_order_relaxed)};
}
