#include "remote_timestamp_oracle.h"

#include <chrono>
#include <stdexcept>
#include <thread>
#include <utility>

#include "mprpc_channel.h"
#include "mprpc_controller.h"
#include "tso_rpc.pb.h"

namespace {
constexpr int kFailoverRounds = 20;
constexpr std::chrono::milliseconds kElectionRetryDelay{50};

uint16_t ParsePort(const std::string& text, const std::string& endpoint) {
  try {
    size_t consumed = 0;
    const unsigned long value = std::stoul(text, &consumed);
    if (consumed != text.size() || value == 0 || value > 65535) throw std::out_of_range("port");
    return static_cast<uint16_t>(value);
  } catch (const std::exception&) {
    throw std::invalid_argument("invalid TSO endpoint: " + endpoint);
  }
}
}  // namespace

struct RemoteTimestampOracle::EndpointClient {
  EndpointClient(std::string host, uint16_t port)
      : endpoint{std::move(host), port},
        channel(std::make_unique<MprpcChannel>(endpoint.host, static_cast<short>(endpoint.port), false)),
        stub(std::make_unique<tsoRpcProtocol::timestampOracleRpc_Stub>(channel.get())) {}

  TsoEndpoint endpoint;
  std::unique_ptr<MprpcChannel> channel;
  std::unique_ptr<tsoRpcProtocol::timestampOracleRpc_Stub> stub;
};

std::vector<TsoEndpoint> ParseTsoEndpoints(const std::string& text) {
  std::vector<TsoEndpoint> endpoints;
  size_t begin = 0;
  while (begin <= text.size()) {
    const size_t end = text.find(',', begin);
    const std::string endpoint = text.substr(begin, end == std::string::npos ? end : end - begin);
    const size_t colon = endpoint.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 == endpoint.size()) {
      throw std::invalid_argument("invalid TSO endpoint: " + endpoint);
    }
    endpoints.push_back(TsoEndpoint{endpoint.substr(0, colon), ParsePort(endpoint.substr(colon + 1), endpoint)});
    if (end == std::string::npos) break;
    begin = end + 1;
  }
  if (endpoints.empty()) throw std::invalid_argument("TSO endpoint list is empty");
  return endpoints;
}

RemoteTimestampOracle::RemoteTimestampOracle(std::string host, uint16_t port)
    : RemoteTimestampOracle(std::vector<TsoEndpoint>{{std::move(host), port}}) {}

RemoteTimestampOracle::RemoteTimestampOracle(const std::string& endpoints)
    : RemoteTimestampOracle(ParseTsoEndpoints(endpoints)) {}

RemoteTimestampOracle::RemoteTimestampOracle(std::vector<TsoEndpoint> endpoints) {
  if (endpoints.empty()) throw std::invalid_argument("TSO endpoint list is empty");
  endpoints_.reserve(endpoints.size());
  for (auto& endpoint : endpoints) {
    if (endpoint.host.empty() || endpoint.port == 0) throw std::invalid_argument("invalid TSO endpoint");
    endpoints_.push_back(std::make_unique<EndpointClient>(std::move(endpoint.host), endpoint.port));
  }
}

RemoteTimestampOracle::~RemoteTimestampOracle() = default;

uint64_t RemoteTimestampOracle::Next() {
  std::string lastError = "no TSO leader available";
  for (int round = 0; round < kFailoverRounds; ++round) {
    const size_t preferred = preferredEndpoint_.load(std::memory_order_relaxed) % endpoints_.size();
    for (size_t offset = 0; offset < endpoints_.size(); ++offset) {
      const size_t index = (preferred + offset) % endpoints_.size();
      tsoRpcProtocol::TimestampRequest request;
      tsoRpcProtocol::TimestampReply response;
      MprpcController controller;
      endpoints_[index]->stub->Next(&controller, &request, &response, nullptr);
      if (controller.Failed()) {
        lastError = controller.ErrorText();
        continue;
      }
      if (response.notleader()) {
        lastError = response.err();
        continue;
      }
      if (!response.err().empty()) throw std::runtime_error("TSO Next failed: " + response.err());
      if (response.timestamp() == 0) throw std::runtime_error("TSO returned timestamp zero");
      preferredEndpoint_.store(index, std::memory_order_relaxed);
      return response.timestamp();
    }
    std::this_thread::sleep_for(kElectionRetryDelay);
  }
  throw std::runtime_error("TSO Next RPC failed: " + lastError);
}

uint64_t RemoteTimestampOracle::Peek() {
  std::string lastError = "no TSO leader available";
  for (int round = 0; round < kFailoverRounds; ++round) {
    const size_t preferred = preferredEndpoint_.load(std::memory_order_relaxed) % endpoints_.size();
    for (size_t offset = 0; offset < endpoints_.size(); ++offset) {
      const size_t index = (preferred + offset) % endpoints_.size();
      tsoRpcProtocol::TimestampRequest request;
      tsoRpcProtocol::TimestampReply response;
      MprpcController controller;
      endpoints_[index]->stub->Peek(&controller, &request, &response, nullptr);
      if (controller.Failed()) {
        lastError = controller.ErrorText();
        continue;
      }
      if (response.notleader()) {
        lastError = response.err();
        continue;
      }
      if (!response.err().empty()) throw std::runtime_error("TSO Peek failed: " + response.err());
      preferredEndpoint_.store(index, std::memory_order_relaxed);
      return response.timestamp();
    }
    std::this_thread::sleep_for(kElectionRetryDelay);
  }
  throw std::runtime_error("TSO Peek RPC failed: " + lastError);
}

void RemoteTimestampOracle::Observe(uint64_t timestamp) {
  std::string lastError = "no TSO leader available";
  for (int round = 0; round < kFailoverRounds; ++round) {
    const size_t preferred = preferredEndpoint_.load(std::memory_order_relaxed) % endpoints_.size();
    for (size_t offset = 0; offset < endpoints_.size(); ++offset) {
      const size_t index = (preferred + offset) % endpoints_.size();
      tsoRpcProtocol::ObserveTimestampRequest request;
      tsoRpcProtocol::ObserveTimestampReply response;
      request.set_timestamp(timestamp);
      MprpcController controller;
      endpoints_[index]->stub->Observe(&controller, &request, &response, nullptr);
      if (controller.Failed()) {
        lastError = controller.ErrorText();
        continue;
      }
      if (response.notleader()) {
        lastError = response.err();
        continue;
      }
      if (!response.err().empty()) throw std::runtime_error("TSO Observe failed: " + response.err());
      preferredEndpoint_.store(index, std::memory_order_relaxed);
      return;
    }
    std::this_thread::sleep_for(kElectionRetryDelay);
  }
  throw std::runtime_error("TSO Observe RPC failed: " + lastError);
}
