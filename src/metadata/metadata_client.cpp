#include "metadata_client.h"

#include <algorithm>
#include <limits>
#include <random>
#include <stdexcept>
#include <thread>

#include "mprpc_controller.h"

namespace {

constexpr int kMaxAttemptsWithoutDeadline = 12;

bool Retryable(metadataRpcProtocol::MetadataErrorCode code) {
  return code == metadataRpcProtocol::METADATA_NOT_LEADER ||
         code == metadataRpcProtocol::METADATA_TIMEOUT ||
         code == metadataRpcProtocol::METADATA_UNAVAILABLE;
}

void ValidateScan(const std::vector<RegionMetadata>& regions) {
  if (regions.empty()) return;
  for (size_t index = 0; index < regions.size(); ++index) {
    const auto& region = regions[index];
    if (region.regionId <= 0 || region.peers.empty() || region.epoch.version == 0 ||
        region.epoch.confVersion == 0 || region.metadataRevision == 0 ||
        (!region.endKey.empty() && !RegionBytewiseLess(region.startKey, region.endKey))) {
      throw std::invalid_argument("metadata returned an invalid Region descriptor");
    }
    if (index > 0 && regions[index - 1].endKey != region.startKey) {
      throw std::invalid_argument("metadata scan contains a gap or overlap");
    }
  }
}

}  // namespace

MetadataClient::EndpointClient::EndpointClient(const std::string& host, short port)
    : channel(std::make_unique<MprpcChannel>(host, port, false)),
      stub(std::make_unique<metadataRpcProtocol::metadataRpc_Stub>(channel.get())) {}

MetadataClient::MetadataClient(const std::string& endpoints) {
  for (const auto& endpoint : ParseServiceEndpoints(endpoints, "metadata")) {
    endpoints_.push_back(std::make_unique<EndpointClient>(endpoint.first, endpoint.second));
  }
}

metadataRpcProtocol::MetadataCommandResult MetadataClient::Mutate(
    const metadataRpcProtocol::MetadataCommand& command,
    std::chrono::steady_clock::time_point deadline) {
  if (command.mutationid().empty()) {
    throw std::invalid_argument("metadata mutation ID must not be empty");
  }
  calls_.fetch_add(1, std::memory_order_relaxed);
  int attempt = 0;
  for (;;) {
    const size_t endpoint = (StartingEndpoint() + static_cast<size_t>(attempt)) % endpoints_.size();
    metadataRpcProtocol::MutateRequest request;
    *request.mutable_command() = command;
    request.set_timeoutms(RemainingMs(deadline));
    metadataRpcProtocol::MutateReply reply;
    MprpcController controller;
    {
      std::lock_guard<std::mutex> lock(endpoints_[endpoint]->mutex);
      endpoints_[endpoint]->stub->Mutate(&controller, &request, &reply, nullptr);
    }
    if (!controller.Failed()) {
      ObserveLeader(reply.leaderid());
      if (!Retryable(reply.result().error())) return reply.result();
    }
    if (std::chrono::steady_clock::now() >= deadline ||
        (++attempt >= kMaxAttemptsWithoutDeadline &&
         deadline == std::chrono::steady_clock::time_point::max())) {
      timeouts_.fetch_add(1, std::memory_order_relaxed);
      metadataRpcProtocol::MetadataCommandResult result;
      result.set_error(metadataRpcProtocol::METADATA_TIMEOUT);
      result.set_message(controller.Failed() ? controller.ErrorText()
                                             : "metadata mutation retry budget exhausted");
      return result;
    }
    retries_.fetch_add(1, std::memory_order_relaxed);
    Backoff(attempt, deadline);
  }
}

RegionMetadata MetadataClient::LookupKey(const std::string& key,
                                         std::chrono::steady_clock::time_point deadline,
                                         uint64_t* revision) {
  calls_.fetch_add(1, std::memory_order_relaxed);
  int attempt = 0;
  for (;;) {
    const size_t endpoint = (StartingEndpoint() + static_cast<size_t>(attempt)) % endpoints_.size();
    metadataRpcProtocol::LookupKeyRequest request;
    request.set_key(key);
    request.set_timeoutms(RemainingMs(deadline));
    metadataRpcProtocol::RegionReply reply;
    MprpcController controller;
    {
      std::lock_guard<std::mutex> lock(endpoints_[endpoint]->mutex);
      endpoints_[endpoint]->stub->LookupKey(&controller, &request, &reply, nullptr);
    }
    if (!controller.Failed()) {
      ObserveLeader(reply.leaderid());
      if (reply.error() == metadataRpcProtocol::METADATA_OK) {
        try {
          RegionMetadata region = FromProtoRegion(reply.region());
          if (region.metadataRevision != reply.revision()) {
            throw std::invalid_argument("Region revision differs from response revision");
          }
          if (revision) *revision = reply.revision();
          return region;
        } catch (...) {
          validationFailures_.fetch_add(1, std::memory_order_relaxed);
          throw;
        }
      }
      if (!Retryable(reply.error())) throw std::out_of_range(reply.message());
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      timeouts_.fetch_add(1, std::memory_order_relaxed);
      throw std::runtime_error("metadata key lookup deadline exhausted");
    }
    ++attempt;
    retries_.fetch_add(1, std::memory_order_relaxed);
    Backoff(attempt, deadline);
  }
}

RegionMetadata MetadataClient::LookupRegion(uint64_t regionId,
                                            std::chrono::steady_clock::time_point deadline,
                                            uint64_t* revision) {
  calls_.fetch_add(1, std::memory_order_relaxed);
  int attempt = 0;
  for (;;) {
    const size_t endpoint = (StartingEndpoint() + static_cast<size_t>(attempt)) % endpoints_.size();
    metadataRpcProtocol::LookupRegionRequest request;
    request.set_regionid(regionId);
    request.set_timeoutms(RemainingMs(deadline));
    metadataRpcProtocol::RegionReply reply;
    MprpcController controller;
    {
      std::lock_guard<std::mutex> lock(endpoints_[endpoint]->mutex);
      endpoints_[endpoint]->stub->LookupRegion(&controller, &request, &reply, nullptr);
    }
    if (!controller.Failed()) {
      ObserveLeader(reply.leaderid());
      if (reply.error() == metadataRpcProtocol::METADATA_OK) {
        try {
          RegionMetadata region = FromProtoRegion(reply.region());
          if (region.metadataRevision != reply.revision()) {
            throw std::invalid_argument("Region revision differs from response revision");
          }
          if (revision) *revision = reply.revision();
          return region;
        } catch (...) {
          validationFailures_.fetch_add(1, std::memory_order_relaxed);
          throw;
        }
      }
      if (!Retryable(reply.error())) throw std::out_of_range(reply.message());
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      timeouts_.fetch_add(1, std::memory_order_relaxed);
      throw std::runtime_error("metadata Region lookup deadline exhausted");
    }
    ++attempt;
    retries_.fetch_add(1, std::memory_order_relaxed);
    Backoff(attempt, deadline);
  }
}

std::vector<RegionMetadata> MetadataClient::Scan(
    const std::string& startKey, size_t limit,
    std::chrono::steady_clock::time_point deadline, uint64_t* revision) {
  if (limit == 0 || limit > static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
    throw std::invalid_argument("metadata scan limit is invalid");
  }
  calls_.fetch_add(1, std::memory_order_relaxed);
  int attempt = 0;
  for (;;) {
    const size_t endpoint = (StartingEndpoint() + static_cast<size_t>(attempt)) % endpoints_.size();
    metadataRpcProtocol::ScanRegionsRequest request;
    request.set_startkey(startKey);
    request.set_limit(static_cast<uint32_t>(limit));
    request.set_timeoutms(RemainingMs(deadline));
    metadataRpcProtocol::ScanRegionsReply reply;
    MprpcController controller;
    {
      std::lock_guard<std::mutex> lock(endpoints_[endpoint]->mutex);
      endpoints_[endpoint]->stub->ScanRegions(&controller, &request, &reply, nullptr);
    }
    if (!controller.Failed()) {
      ObserveLeader(reply.leaderid());
      if (reply.error() == metadataRpcProtocol::METADATA_OK) {
        try {
          std::vector<RegionMetadata> regions;
          regions.reserve(static_cast<size_t>(reply.regions_size()));
          for (const auto& descriptor : reply.regions()) {
            RegionMetadata region = FromProtoRegion(descriptor);
            if (region.metadataRevision != reply.revision()) {
              throw std::invalid_argument("scan Region revision differs from response revision");
            }
            regions.push_back(std::move(region));
          }
          ValidateScan(regions);
          if (revision) *revision = reply.revision();
          return regions;
        } catch (...) {
          validationFailures_.fetch_add(1, std::memory_order_relaxed);
          throw;
        }
      }
      if (!Retryable(reply.error())) throw std::out_of_range(reply.message());
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      timeouts_.fetch_add(1, std::memory_order_relaxed);
      throw std::runtime_error("metadata Region scan deadline exhausted");
    }
    ++attempt;
    retries_.fetch_add(1, std::memory_order_relaxed);
    Backoff(attempt, deadline);
  }
}

metadataRpcProtocol::BalancerStatusReply MetadataClient::BalancerStatus(
    std::chrono::steady_clock::time_point deadline) {
  calls_.fetch_add(1, std::memory_order_relaxed);
  int attempt = 0;
  for (;;) {
    const size_t endpoint = (StartingEndpoint() + static_cast<size_t>(attempt)) % endpoints_.size();
    metadataRpcProtocol::BalancerStatusRequest request;
    request.set_timeoutms(RemainingMs(deadline));
    metadataRpcProtocol::BalancerStatusReply reply;
    MprpcController controller;
    {
      std::lock_guard<std::mutex> lock(endpoints_[endpoint]->mutex);
      endpoints_[endpoint]->stub->BalancerStatus(&controller, &request, &reply, nullptr);
    }
    if (!controller.Failed()) {
      ObserveLeader(reply.leaderid());
      if (reply.error() == metadataRpcProtocol::METADATA_OK) return reply;
      if (!Retryable(reply.error())) {
        throw std::runtime_error(reply.message());
      }
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      timeouts_.fetch_add(1, std::memory_order_relaxed);
      throw std::runtime_error("metadata balancer status deadline exhausted");
    }
    ++attempt;
    retries_.fetch_add(1, std::memory_order_relaxed);
    Backoff(attempt, deadline);
  }
}

MetadataClientMetrics MetadataClient::Metrics() const {
  return {calls_.load(std::memory_order_relaxed),
          retries_.load(std::memory_order_relaxed),
          leaderRedirects_.load(std::memory_order_relaxed),
          timeouts_.load(std::memory_order_relaxed),
          validationFailures_.load(std::memory_order_relaxed)};
}

uint64_t MetadataClient::RemainingMs(std::chrono::steady_clock::time_point deadline) const {
  if (deadline == std::chrono::steady_clock::time_point::max()) return 0;
  const auto now = std::chrono::steady_clock::now();
  if (now >= deadline) return 1;
  return std::max<uint64_t>(
      1, static_cast<uint64_t>(
             std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count()));
}

void MetadataClient::Backoff(int attempt, std::chrono::steady_clock::time_point deadline) {
  const int cap = std::min(250, 5 * (1 << std::min(attempt, 5)));
  static thread_local std::mt19937 generator(std::random_device{}());
  std::uniform_int_distribution<int> jitter(0, cap);
  auto delay = std::chrono::milliseconds(jitter(generator));
  if (deadline != std::chrono::steady_clock::time_point::max()) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) return;
    delay = std::min(delay, std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now));
  }
  std::this_thread::sleep_for(delay);
}

size_t MetadataClient::StartingEndpoint() const {
  const int hint = leaderHint_.load(std::memory_order_relaxed);
  return hint >= 0 && hint < static_cast<int>(endpoints_.size()) ? static_cast<size_t>(hint) : 0;
}

void MetadataClient::ObserveLeader(int leaderId) {
  if (leaderId >= 0 && leaderId < static_cast<int>(endpoints_.size())) {
    const int previous = leaderHint_.exchange(leaderId, std::memory_order_relaxed);
    if (previous != leaderId) leaderRedirects_.fetch_add(1, std::memory_order_relaxed);
  }
}
