#ifndef STRATAKV_TRANSACTION_REMOTE_TIMESTAMP_ORACLE_H
#define STRATAKV_TRANSACTION_REMOTE_TIMESTAMP_ORACLE_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "timestamp_oracle.h"

struct TsoEndpoint {
  std::string host;
  uint16_t port = 0;
};

std::vector<TsoEndpoint> ParseTsoEndpoints(const std::string& endpoints);

// Cluster-aware RPC proxy. It caches the last successful leader endpoint and
// rotates through all configured members during a transport failure or Raft
// leadership change; it never generates or leases timestamps locally.
class RemoteTimestampOracle final : public TimestampOracle {
 public:
  RemoteTimestampOracle(std::string host, uint16_t port);
  explicit RemoteTimestampOracle(std::vector<TsoEndpoint> endpoints);
  explicit RemoteTimestampOracle(const std::string& endpoints);
  ~RemoteTimestampOracle() override;

  uint64_t Next() override;
  uint64_t Peek() override;
  void Observe(uint64_t ts) override;

 private:
  struct EndpointClient;
  std::vector<std::unique_ptr<EndpointClient>> endpoints_;
  std::atomic<size_t> preferredEndpoint_{0};
};

#endif  // STRATAKV_TRANSACTION_REMOTE_TIMESTAMP_ORACLE_H
