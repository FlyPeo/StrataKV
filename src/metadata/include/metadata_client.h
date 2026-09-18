#ifndef STRATAKV_METADATA_METADATA_CLIENT_H
#define STRATAKV_METADATA_METADATA_CLIENT_H

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "metadata_rpc.pb.h"
#include "mprpc_channel.h"
#include "region_metadata.h"
#include "topology_config.h"

struct MetadataClientMetrics {
  uint64_t calls = 0;
  uint64_t retries = 0;
  uint64_t leaderRedirects = 0;
  uint64_t timeouts = 0;
  uint64_t validationFailures = 0;
};

class MetadataClient {
 public:
  explicit MetadataClient(const std::string& endpoints);

  metadataRpcProtocol::MetadataCommandResult Mutate(
      const metadataRpcProtocol::MetadataCommand& command,
      std::chrono::steady_clock::time_point deadline);
  RegionMetadata LookupKey(const std::string& key,
                           std::chrono::steady_clock::time_point deadline,
                           uint64_t* revision = nullptr);
  RegionMetadata LookupRegion(uint64_t regionId,
                              std::chrono::steady_clock::time_point deadline,
                              uint64_t* revision = nullptr);
  std::vector<RegionMetadata> Scan(const std::string& startKey, size_t limit,
                                   std::chrono::steady_clock::time_point deadline,
                                   uint64_t* revision = nullptr);
  metadataRpcProtocol::BalancerStatusReply BalancerStatus(
      std::chrono::steady_clock::time_point deadline);
  MetadataClientMetrics Metrics() const;

 private:
  struct EndpointClient {
    EndpointClient(const std::string& host, short port);
    std::unique_ptr<MprpcChannel> channel;
    std::unique_ptr<metadataRpcProtocol::metadataRpc_Stub> stub;
    std::mutex mutex;
  };

  uint64_t RemainingMs(std::chrono::steady_clock::time_point deadline) const;
  void Backoff(int attempt, std::chrono::steady_clock::time_point deadline);
  size_t StartingEndpoint() const;
  void ObserveLeader(int leaderId);

  std::vector<std::unique_ptr<EndpointClient>> endpoints_;
  std::atomic<int> leaderHint_{0};
  std::atomic<uint64_t> calls_{0};
  std::atomic<uint64_t> retries_{0};
  std::atomic<uint64_t> leaderRedirects_{0};
  std::atomic<uint64_t> timeouts_{0};
  std::atomic<uint64_t> validationFailures_{0};
};

#endif  // STRATAKV_METADATA_METADATA_CLIENT_H
