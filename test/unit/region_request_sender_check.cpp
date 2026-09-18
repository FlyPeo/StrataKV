// Scripted routing tests for RegionRequestSender: transport failure, NotLeader,
// EpochNotMatch, RegionNotFound, storage failure, lost response, deadline
// exhaustion, identity preservation and batch regrouping.
#include <chrono>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "kv_server_rpc.pb.h"
#include "mprpc_controller.h"
#include "region.pb.h"
#include "region_cache.h"
#include "region_metadata.h"
#include "region_request_sender.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

RegionMetadata Descriptor(int id, const char* start, const char* end, uint64_t basePeerId,
                          uint64_t epochVersion, uint64_t revision) {
  RegionMetadata descriptor;
  descriptor.regionId = id;
  descriptor.startKey = start;
  descriptor.endKey = end;
  descriptor.epoch = {epochVersion, 1};
  descriptor.metadataRevision = revision;
  descriptor.peers = {
      {0, "127.0.0.1", 26001, 1, basePeerId},
      {1, "127.0.0.1", 26002, 2, basePeerId + 1},
      {2, "127.0.0.1", 26003, 3, basePeerId + 2},
  };
  descriptor.leaderPeerId = basePeerId;
  return descriptor;
}

struct ScriptStep {
  bool transportFailure = false;
  bool loseResponse = false;
  stratakv::region::RegionErrorCode code = stratakv::region::REGION_ERROR_NONE;
  uint64_t leaderPeerId = 0;
};

// A stub whose replies are fully scripted, so routing behaviour is testable
// without sockets, Raft or RocksDB.
class ScriptedStub final : public raftKVRpcProctoc::kvServerRpc_Stub {
 public:
  explicit ScriptedStub(std::vector<ScriptStep> script)
      : raftKVRpcProctoc::kvServerRpc_Stub(nullptr), script_(std::move(script)) {}

  void TxnPrewrite(google::protobuf::RpcController* controller,
                   const raftKVRpcProctoc::TxnPrewriteArgs* request,
                   raftKVRpcProctoc::TxnPrewriteReply* response,
                   google::protobuf::Closure*) override {
    observedClientIds.push_back(request->clientid());
    observedRequestIds.push_back(request->requestid());
    Apply(controller, response);
  }

  void TxnBatchPrewrite(google::protobuf::RpcController* controller,
                        const raftKVRpcProctoc::TxnBatchPrewriteArgs* request,
                        raftKVRpcProctoc::TxnBatchPrewriteReply* response,
                        google::protobuf::Closure*) override {
    observedClientIds.push_back(request->clientid());
    observedRequestIds.push_back(request->requestid());
    for (const auto& mutation : request->mutations()) observedKeys.push_back(mutation.key());
    Apply(controller, response);
  }

  std::vector<std::string> observedClientIds;
  std::vector<int> observedRequestIds;
  std::vector<std::string> observedKeys;
  size_t calls = 0;

 private:
  void Apply(google::protobuf::RpcController* controller,
             google::protobuf::Message* response) {
    auto* rpcController = static_cast<MprpcController*>(controller);
    const ScriptStep step = script_.empty() ? ScriptStep{} : script_[std::min(calls, script_.size() - 1)];
    ++calls;
    if (step.transportFailure) {
      rpcController->SetFailed("connect refused");
      return;
    }
    if (step.loseResponse) {
      rpcController->SetFailed("response lost");
      return;
    }
    auto* reply = static_cast<raftKVRpcProctoc::TxnPrewriteReply*>(response);
    if (step.code != stratakv::region::REGION_ERROR_NONE) {
      auto* error = reply->mutable_header()->mutable_error();
      error->set_code(step.code);
      if (step.leaderPeerId != 0) error->mutable_leader()->set_peerid(step.leaderPeerId);
      reply->set_err("storage_error");
      return;
    }
    reply->set_err("ok");
  }

  std::vector<ScriptStep> script_;
};

class ScriptedChannels final : public RegionChannelSource {
 public:
  ScriptedChannels(std::vector<ScriptStep> script, const std::vector<short>& unreachablePorts)
      : script_(std::move(script)), unreachablePorts_(unreachablePorts) {}

  raftKVRpcProctoc::kvServerRpc_Stub* Acquire(const std::string&, short port) override {
    for (short blocked : unreachablePorts_) {
      if (blocked == port) return nullptr;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto& stub = stubs_[port];
    if (!stub) stub = std::make_unique<ScriptedStub>(script_);
    return stub.get();
  }
  void Release(raftKVRpcProctoc::kvServerRpc_Stub*) override {}

  ScriptedStub* Stub(short port) {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<ScriptedStub*>(stubs_[port].get());
  }

 private:
  std::vector<ScriptStep> script_;
  std::vector<short> unreachablePorts_;
  std::mutex mutex_;
  std::unordered_map<short, std::unique_ptr<ScriptedStub>> stubs_;
};

RegionCache::RefreshFunction ScriptedRefresh(std::vector<RegionMetadata> replacement,
                                             uint64_t revision, size_t* invocations) {
  return [replacement, revision, invocations](const std::string&,
                                              RegionCache::Clock::time_point) {
    if (invocations != nullptr) ++(*invocations);
    return RegionCache::RefreshResult{replacement, revision};
  };
}

RegionRequestSender::Dispatch Prewrite(const std::string& clientId, int requestId) {
  return [clientId, requestId](raftKVRpcProctoc::kvServerRpc_Stub& stub,
                               const RegionMetadata& descriptor,
                               const RegionPeerLocation& peer,
                               uint64_t remainingBudgetMs) -> DispatchOutcome {
    raftKVRpcProctoc::TxnPrewriteArgs args;
    args.set_regionid(descriptor.regionId);
    args.set_key("apple");
    args.set_value("v");
    args.set_primarykey("apple");
    args.set_startts(100);
    args.set_clientid(clientId);
    args.set_requestid(requestId);
    args.set_remainingbudgetms(remainingBudgetMs);
    auto* header = args.mutable_header();
    header->set_regionid(static_cast<uint64_t>(descriptor.regionId));
    header->mutable_epoch()->set_version(descriptor.epoch.version);
    header->mutable_epoch()->set_confversion(descriptor.epoch.confVersion);
    header->set_clientid(clientId);
    header->set_requestid(static_cast<uint64_t>(requestId));
    raftKVRpcProctoc::TxnPrewriteReply reply;
    MprpcController controller;
    stub.TxnPrewrite(&controller, &args, &reply, nullptr);
    if (controller.Failed()) return RouteErrorKind::Transport;
    return ClassifyRegionReply(reply);
  };
}

struct Fixture {
  std::shared_ptr<RegionCache> cache;
  std::shared_ptr<ScriptedChannels> channels;
  std::shared_ptr<RegionRequestSender> sender;
};

Fixture MakeSender(std::vector<ScriptStep> script,
                   const std::vector<short>& unreachablePorts = {},
                   RegionCache::RefreshFunction refresh = nullptr, size_t* refreshes = nullptr) {
  Fixture fixture;
  fixture.cache = std::make_shared<RegionCache>(
      std::vector<RegionMetadata>{Descriptor(1, "", "", 1000, 1, 1)}, 1);
  fixture.channels = std::make_shared<ScriptedChannels>(std::move(script), unreachablePorts);
  if (refresh == nullptr) {
    refresh = ScriptedRefresh({Descriptor(1, "", "", 1000, 2, 2)}, 2, refreshes);
  }
  fixture.sender = std::make_shared<RegionRequestSender>(fixture.cache, fixture.channels,
                                                         std::move(refresh));
  return fixture;
}

}  // namespace

int main() {
  try {
    // 1. A healthy leader answers on the first attempt.
    {
      auto fixture = MakeSender({ScriptStep{}});
      RetryContext context;
      context.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      const auto result = fixture.sender->Send("apple", context, Prewrite("client-1", 7));
      Require(result.ok() && result.attempts == 1, "a current route must succeed immediately");
      Require(fixture.sender->Metrics().attempts == 1, "one attempt must be issued");
    }

    // 2. Transport failure moves to another peer without touching the cache.
    {
      auto fixture = MakeSender({ScriptStep{}}, {26001});
      RetryContext context;
      context.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      const auto result = fixture.sender->Send("apple", context, Prewrite("client-2", 7));
      Require(result.ok(), "a transport failure must be retried on another peer");
      Require(fixture.sender->Metrics().transportRetries >= 1,
              "transport failures must be counted");
      Require(fixture.cache->Metrics().hits >= 1 && fixture.cache->Metrics().misses == 0,
              "a transport retry must not invalidate a current route");
    }

    // 3. NotLeader updates only the hint and keeps request identity.
    {
      auto fixture = MakeSender({{false, false, stratakv::region::REGION_ERROR_NOT_LEADER, 1001},
                                 ScriptStep{}});
      RetryContext context;
      context.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      const auto result = fixture.sender->Send("apple", context, Prewrite("client-3", 42));
      Require(result.ok(), "NotLeader must be retried within the deadline");
      Require(fixture.sender->LeaderHint(1) == 1001, "NotLeader must update the leader hint");
      // The retry must go to the hinted leader (26002), not repeat on the
      // stale descriptor hint (26001).
      const auto* stale = fixture.channels->Stub(26001);
      const auto* hinted = fixture.channels->Stub(26002);
      Require(stale != nullptr && stale->observedClientIds.size() == 1,
              "the stale descriptor hint must be tried exactly once");
      // Each scripted stub replays the same step list, so the hinted peer
      // answers NotLeader once (its step 0) and succeeds on its step 1.
      Require(hinted != nullptr && hinted->observedClientIds.size() == 2,
              "the retry must be observed on the hinted leader");
      for (const auto& clientId : hinted->observedClientIds) {
        Require(clientId == "client-3", "retries must preserve the client identity");
      }
      for (int requestId : hinted->observedRequestIds) {
        Require(requestId == 42, "retries must preserve the request identity");
      }
    }

    // 4. EpochNotMatch invalidates the Region and refreshes from metadata.
    {
      size_t refreshes = 0;
      auto fixture =
          MakeSender({{false, false, stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH, 0},
                      ScriptStep{}},
                     {}, ScriptedRefresh({Descriptor(1, "", "", 1000, 2, 2)}, 2, &refreshes),
                     &refreshes);
      RetryContext context;
      context.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      const auto result = fixture.sender->Send("apple", context, Prewrite("client-4", 9));
      Require(result.ok(), "an epoch mismatch must refresh and retry");
      Require(refreshes >= 1, "an epoch mismatch must consult metadata");
      Require(fixture.sender->Metrics().epochRefreshes >= 1,
              "epoch refreshes must be counted separately");
      Require(fixture.cache->Snapshot()->regions.front().epoch.version == 2,
              "the refreshed descriptor must be published");
    }

    // A migration can return NotLeader with a newly promoted peer that is not
    // present in the cached descriptor. Treat that hint as topology evidence
    // and refresh before retrying.
    {
      size_t refreshes = 0;
      RegionMetadata migrated = Descriptor(1, "", "", 2000, 1, 2);
      migrated.epoch.confVersion = 2;
      auto fixture = MakeSender(
          {{false, false, stratakv::region::REGION_ERROR_NOT_LEADER, 2000}, ScriptStep{}},
          {}, ScriptedRefresh({migrated}, 2, &refreshes), &refreshes);
      RetryContext context;
      context.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      const auto result = fixture.sender->Send("apple", context, Prewrite("client-move", 91));
      Require(result.ok() && refreshes >= 1,
              "NotLeader pointing at a new migration peer must refresh and reroute");
      Require(fixture.cache->Snapshot()->regions.front().FindPeer(2000) != nullptr,
              "migration refresh must publish the new peer set");
    }

    // 5. RegionNotFound refreshes the affected range and reroutes.
    {
      size_t refreshes = 0;
      auto fixture =
          MakeSender({{false, false, stratakv::region::REGION_ERROR_REGION_NOT_FOUND, 0},
                      ScriptStep{}},
                     {}, ScriptedRefresh({Descriptor(1, "", "", 1000, 2, 2)}, 2, &refreshes),
                     &refreshes);
      RetryContext context;
      context.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      const auto result = fixture.sender->Send("apple", context, Prewrite("client-5", 11));
      Require(result.ok(), "a missing Region must refresh and retry");
      Require(fixture.sender->Metrics().regionNotFoundRefreshes >= 1,
              "missing-Region refreshes must be counted");
    }

    // 6. A storage failure is definitive: no retry, no cache invalidation.
    {
      auto fixture = MakeSender({{false, false, stratakv::region::REGION_ERROR_STORAGE, 0}});
      RetryContext context;
      context.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      const auto result = fixture.sender->Send("apple", context, Prewrite("client-6", 3));
      Require(result.completed && result.kind == RouteErrorKind::Storage,
              "storage failures must be returned to the caller");
      Require(fixture.channels->Stub(26001)->calls == 1,
              "a storage failure must not be retried");
    }

    // 7. A lost response is a transport error and is retried with the same
    // identity until the deadline.
    {
      auto fixture = MakeSender({{false, true, stratakv::region::REGION_ERROR_NONE, 0}});
      RetryContext context;
      context.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(120);
      context.maxAttempts = 64;
      const auto result = fixture.sender->Send("apple", context, Prewrite("client-7", 5));
      Require(!result.ok() && result.kind == RouteErrorKind::Timeout,
              "a permanently lost response must exhaust the deadline");
      Require(fixture.sender->Metrics().deadlineExhausted >= 1,
              "deadline exhaustion must be counted");
      const auto* stub = fixture.channels->Stub(26001);
      Require(stub->calls >= 2, "a lost response must be retried");
      for (const auto& clientId : stub->observedClientIds) {
        Require(clientId == "client-7", "retries must keep the original client identity");
      }
    }

    // 8. A batch whose refreshed boundaries change ownership regroups instead
    // of repeating work against a Region that no longer owns every key.
    {
      size_t refreshes = 0;
      auto fixture = MakeSender(
          {{false, false, stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH, 0}}, {},
          ScriptedRefresh({Descriptor(1, "", "m", 1000, 2, 2), Descriptor(2, "m", "", 2000, 2, 2)},
                          2, &refreshes),
          &refreshes);
      RetryContext context;
      context.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      auto dispatch = [](raftKVRpcProctoc::kvServerRpc_Stub& stub,
                         const RegionMetadata& descriptor, const RegionPeerLocation& peer,
                         const std::vector<std::string>& keys, uint64_t) -> DispatchOutcome {
        raftKVRpcProctoc::TxnBatchPrewriteArgs args;
        args.set_regionid(descriptor.regionId);
        args.set_primarykey(keys.front());
        args.set_startts(100);
        args.set_clientid("client-8");
        args.set_requestid(77);
        for (const auto& key : keys) {
          auto* mutation = args.add_mutations();
          mutation->set_key(key);
          mutation->set_value("v");
        }
        raftKVRpcProctoc::TxnBatchPrewriteReply reply;
        MprpcController controller;
        stub.TxnBatchPrewrite(&controller, &args, &reply, nullptr);
        if (controller.Failed()) return RouteErrorKind::Transport;
        return ClassifyRegionReply(reply);
      };
      const auto result =
          fixture.sender->SendBatch({"apple", "zebra"}, context, dispatch);
      Require(result.regroupRequired && !result.completed,
              "a split batch must require regrouping");
      Require(fixture.sender->Metrics().regroupRequired == 1, "regrouping must be counted");
      Require(refreshes >= 1, "regrouping must follow a metadata refresh");
    }

    std::cout << "Region request sender routing checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Region request sender checks failed: " << error.what() << std::endl;
    return 1;
  }
}
