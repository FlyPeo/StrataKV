#include "store_heartbeat_reporter.h"

#include <chrono>
#include <iostream>

namespace {

stratakv::region::RegionEpoch ToProtoEpoch(const RegionEpoch& epoch) {
  stratakv::region::RegionEpoch proto;
  proto.set_version(epoch.version);
  proto.set_confversion(epoch.confVersion);
  return proto;
}

}  // namespace

StoreHeartbeatReporter::StoreHeartbeatReporter(
    StoreHeartbeatReporterConfig config, const RegionRegistry* registry,
    std::shared_ptr<MetadataClient> metadataClient)
    : config_(std::move(config)),
      registry_(registry),
      metadataClient_(std::move(metadataClient)) {}

StoreHeartbeatReporter::~StoreHeartbeatReporter() { Stop(); }

void StoreHeartbeatReporter::Start() {
  if (running_.exchange(true)) return;
  stopRequested_.store(false);
  workerThread_ = std::thread(&StoreHeartbeatReporter::WorkerLoop, this);
}

void StoreHeartbeatReporter::Stop() {
  if (!running_.exchange(false)) return;
  stopRequested_.store(true);
  cv_.notify_all();
  if (workerThread_.joinable()) {
    workerThread_.join();
  }
}

metadataRpcProtocol::StoreHeartbeat StoreHeartbeatReporter::CollectHeartbeat() {
  metadataRpcProtocol::StoreHeartbeat heartbeat;
  heartbeat.set_storeid(config_.storeId);
  const uint64_t seq = sequence_.fetch_add(1, std::memory_order_relaxed) + 1;
  heartbeat.set_sequence(seq);

  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  heartbeat.set_observedatms(static_cast<uint64_t>(now));
  heartbeat.set_expiresatms(static_cast<uint64_t>(now + config_.timeout.count()));

  {
    std::lock_guard<std::mutex> lock(metricsMutex_);
    heartbeat.set_capacitybytes(config_.capacityBytes);
    heartbeat.set_availablebytes(config_.availableBytes);
  }
  heartbeat.set_requestcount(totalRequests_.load(std::memory_order_relaxed));

  if (customCollector_) {
    auto observations = customCollector_();
    for (auto& obs : observations) {
      *heartbeat.add_regions() = std::move(obs);
    }
  } else if (registry_) {
    // Registry::Peers() returns a snapshot copy of std::shared_ptr<RegionPeer>
    // without holding registry or peer locks across RPC.
    const auto peers = registry_->Peers();
    for (const auto& peer : peers) {
      if (!peer) continue;
      auto* obs = heartbeat.add_regions();
      obs->set_regionid(static_cast<uint64_t>(peer->RegionId()));
      obs->set_peerid(peer->LocalPeerDescriptor().peerId);
      obs->mutable_epoch()->set_version(peer->Descriptor().epoch.version);
      obs->mutable_epoch()->set_confversion(peer->Descriptor().epoch.confVersion);
      obs->set_approximatebytes(peer->ApproximateBytes());
      obs->set_requestcount(peer->RequestCount());
      obs->set_isleader(peer->IsTxnLeader());
      obs->set_topologyoperationactive(
          peer->SplitPhase() != 0 ||
          peer->LifecycleState() != RegionPeerState::Serving);
      obs->set_splitcandidategeneration(peer->SplitCandidateGeneration());
    }
  }

  {
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.samplesCollected++;
  }
  return heartbeat;
}

void StoreHeartbeatReporter::TriggerOnce(bool collectNewSample) {
  bool shouldCollect = false;
  {
    std::lock_guard<std::mutex> lock(pendingMutex_);
    shouldCollect = collectNewSample || !pendingHeartbeat_;
  }
  if (shouldCollect) {
    metadataRpcProtocol::StoreHeartbeat sample = CollectHeartbeat();
    std::lock_guard<std::mutex> lock(pendingMutex_);
    if (pendingHeartbeat_) {
      std::lock_guard<std::mutex> mlock(metricsMutex_);
      metrics_.retriesCoalesced++;
    }
    pendingHeartbeat_ =
        std::make_unique<metadataRpcProtocol::StoreHeartbeat>(std::move(sample));
  }

  metadataRpcProtocol::StoreHeartbeat toSend;
  {
    std::lock_guard<std::mutex> lock(pendingMutex_);
    toSend = *pendingHeartbeat_;
    if (lastSendFailed_) {
      std::lock_guard<std::mutex> mlock(metricsMutex_);
      metrics_.duplicateRetries++;
    }
  }

  {
    std::lock_guard<std::mutex> mlock(metricsMutex_);
    metrics_.heartbeatsSent++;
  }

  const bool ok = SendHeartbeat(toSend);
  {
    std::lock_guard<std::mutex> lock(pendingMutex_);
    if (ok) {
      if (pendingHeartbeat_ &&
          pendingHeartbeat_->sequence() == toSend.sequence()) {
        pendingHeartbeat_.reset();
      }
      lastSendFailed_ = false;
      std::lock_guard<std::mutex> mlock(metricsMutex_);
      metrics_.heartbeatsSucceeded++;
    } else {
      lastSendFailed_ = true;
      std::lock_guard<std::mutex> mlock(metricsMutex_);
      metrics_.heartbeatsFailed++;
    }
  }
}

void StoreHeartbeatReporter::WorkerLoop() {
  while (!stopRequested_.load(std::memory_order_acquire)) {
    TriggerOnce();
    std::unique_lock<std::mutex> lock(cvMutex_);
    cv_.wait_for(lock, config_.interval, [this] {
      return stopRequested_.load(std::memory_order_acquire);
    });
  }
}

bool StoreHeartbeatReporter::SendHeartbeat(
    const metadataRpcProtocol::StoreHeartbeat& heartbeat) {
  if (customSender_) {
    try {
      const auto result = customSender_(heartbeat);
      return result.error() == metadataRpcProtocol::METADATA_OK;
    } catch (...) {
      return false;
    }
  }
  if (!metadataClient_) {
    return false;
  }
  try {
    metadataRpcProtocol::MetadataCommand command;
    command.set_mutationid("heartbeat:" + std::to_string(heartbeat.storeid()) +
                           ":" + std::to_string(heartbeat.sequence()));
    *command.mutable_reportstoreheartbeat()->mutable_heartbeat() = heartbeat;
    const auto deadline = std::chrono::steady_clock::now() + config_.timeout;
    const auto result = metadataClient_->Mutate(command, deadline);
    return result.error() == metadataRpcProtocol::METADATA_OK;
  } catch (...) {
    return false;
  }
}

HeartbeatReporterMetrics StoreHeartbeatReporter::Metrics() const {
  std::lock_guard<std::mutex> lock(metricsMutex_);
  return metrics_;
}
