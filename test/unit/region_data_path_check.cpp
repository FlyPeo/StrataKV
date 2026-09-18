// Topology-aware data path checks: the cache-backed ShardRouter and the
// sender-driven RaftMvccStorage (header propagation, leader retry, definitive
// storage errors, and regroup signalling).
#include <chrono>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "kv_server_rpc.pb.h"
#include "mprpc_controller.h"
#include "raft_mvcc_storage.h"
#include "region.pb.h"
#include "region_cache.h"
#include "region_channel_pool.h"
#include "region_request_sender.h"
#include "shard_router.h"

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
  descriptor.peers = {{0, "127.0.0.1", 26001, 1, basePeerId}};
  descriptor.leaderPeerId = basePeerId;
  return descriptor;
}

struct ScriptStep {
  stratakv::region::RegionErrorCode code = stratakv::region::REGION_ERROR_NONE;
  uint64_t leaderPeerId = 0;
  const char* err = "0";
};

// Records the header every attempt puts on the wire, so tests can prove the
// epoch and idempotency identity survive retries.
class ScriptedStub final : public raftKVRpcProctoc::kvServerRpc_Stub {
 public:
  explicit ScriptedStub(std::vector<ScriptStep> script)
      : raftKVRpcProctoc::kvServerRpc_Stub(nullptr), script_(std::move(script)) {}

  void TxnBatchPrewrite(google::protobuf::RpcController* controller,
                        const raftKVRpcProctoc::TxnBatchPrewriteArgs* request,
                        raftKVRpcProctoc::TxnBatchPrewriteReply* response,
                        google::protobuf::Closure*) override {
    headers.push_back(request->header());
    keys.clear();
    for (const auto& mutation : request->mutations()) keys.push_back(mutation.key());
    const auto step = script_[std::min(calls, script_.size() - 1)];
    ++calls;
    if (step.code != stratakv::region::REGION_ERROR_NONE) {
      auto* error = response->mutable_header()->mutable_error();
      error->set_code(step.code);
      if (step.leaderPeerId != 0) error->mutable_leader()->set_peerid(step.leaderPeerId);
    }
    response->set_err(step.err);
    (void)controller;
  }

  std::vector<stratakv::region::RegionRequestHeader> headers;
  std::vector<std::string> keys;
  size_t calls = 0;

 private:
  std::vector<ScriptStep> script_;
};

class ScriptedChannels final : public RegionChannelSource {
 public:
  explicit ScriptedChannels(std::vector<ScriptStep> script) : script_(std::move(script)) {}

  raftKVRpcProctoc::kvServerRpc_Stub* Acquire(const std::string&, short) override {
    if (!stub_) stub_ = std::make_unique<ScriptedStub>(script_);
    return stub_.get();
  }
  void Release(raftKVRpcProctoc::kvServerRpc_Stub*) override {}

  ScriptedStub* Stub() const { return stub_.get(); }

 private:
  std::vector<ScriptStep> script_;
  std::unique_ptr<ScriptedStub> stub_;
};

struct Fixture {
  std::shared_ptr<RegionCache> cache;
  std::shared_ptr<ScriptedChannels> channels;
  std::shared_ptr<RegionRequestSender> sender;
  std::shared_ptr<RaftMvccStorage> storage;
};

Fixture MakeStorage(std::vector<ScriptStep> script,
                    RegionCache::RefreshFunction refresh = nullptr) {
  Fixture fixture;
  fixture.cache = std::make_shared<RegionCache>(
      std::vector<RegionMetadata>{Descriptor(1, "", "", 1000, 1, 1)}, 1);
  fixture.channels = std::make_shared<ScriptedChannels>(std::move(script));
  if (refresh == nullptr) {
    refresh = [](const std::string&, RegionCache::Clock::time_point) {
      return RegionCache::RefreshResult{{Descriptor(1, "", "", 1000, 2, 2)}, 2};
    };
  }
  fixture.sender =
      std::make_shared<RegionRequestSender>(fixture.cache, fixture.channels, std::move(refresh));
  fixture.storage = std::make_shared<RaftMvccStorage>(1, std::vector<std::pair<std::string, short>>{
                                                             {"127.0.0.1", 26001}});
  fixture.storage->AttachRequestSender(fixture.sender);
  return fixture;
}

}  // namespace

int main() {
  try {
    // 1. The cache-backed router follows a split without being rebuilt.
    {
      auto cache = std::make_shared<RegionCache>(
          std::vector<RegionMetadata>{Descriptor(1, "", "", 1000, 1, 1)}, 1);
      std::unordered_map<int, int> built;
      ShardRouter::StorageFactory factory =
          [&built](const RegionMetadata& descriptor) -> std::shared_ptr<MvccStorage> {
        ++built[descriptor.regionId];
        return std::make_shared<MvccStorage>(nullptr);
      };
      ShardRouter router(cache, factory);
      const auto* firstStorage = router.Route("apple").get();
      Require(router.RegionId("apple") == 1, "routing must resolve the initial Region");
      Require(built[1] == 1, "a Region's storage must be built once");
      Require(router.Route("zebra").get() == firstStorage,
              "keys in one Region share one storage before the split");

      cache->ReplaceInterval(
          {Descriptor(1, "", "m", 1000, 2, 2), Descriptor(2, "m", "", 2000, 2, 2)}, 2);
      Require(router.RegionId("apple") == 1 && router.RegionId("zebra") == 2,
              "a published split must move the affected keys");
      Require(router.Route("apple").get() == firstStorage,
              "storage for an unchanged Region must be reused");
      Require(router.Route("zebra").get() != firstStorage,
              "the new Region must receive its own storage");
      Require(built[2] == 1, "the new Region's storage must be built lazily");
      Require(router.Shards().size() == 2, "the router must expose both Regions");
    }

    // 2. A dynamic batch carries the typed header and the lane identity.
    {
      auto fixture = MakeStorage({ScriptStep{}});
      const auto status = fixture.storage->BatchPrewrite(
          {{"apple", "v", false, false}}, "apple", 100, 3000);
      Require(status == TxnStatus::Ok, "a current Region must accept the batch");
      const auto* stub = fixture.channels->Stub();
      Require(stub != nullptr && stub->calls == 1, "one attempt must be issued");
      Require(stub->headers.front().regionid() == 1 &&
                  stub->headers.front().epoch().version() == 1,
              "the batch must carry the resolved Region id and epoch");
      Require(!stub->headers.front().clientid().empty() &&
                  stub->headers.front().requestid() != 0,
              "the batch must carry an idempotency identity");
    }

    // 3. NotLeader is retried with the same request identity.
    {
      auto fixture = MakeStorage(
          {{stratakv::region::REGION_ERROR_NOT_LEADER, 1001, "0"}, {}});
      const auto status = fixture.storage->BatchPrewrite(
          {{"apple", "v", false, false}}, "apple", 100, 3000);
      Require(status == TxnStatus::Ok, "NotLeader must be retried");
      Require(fixture.sender->LeaderHint(1) == 1001, "the leader hint must follow the reply");
      const auto* stub = fixture.channels->Stub();
      Require(stub->calls == 2, "the retry must be observable");
      Require(stub->headers.front().requestid() == stub->headers.back().requestid(),
              "retries must reuse the same request id");
    }

    // 4. A storage error is definitive and is not retried.
    {
      auto fixture =
          MakeStorage({{stratakv::region::REGION_ERROR_STORAGE, 0, "9"}});
      const auto status = fixture.storage->BatchPrewrite(
          {{"apple", "v", false, false}}, "apple", 100, 3000);
      Require(status == TxnStatus::StorageError, "a storage error must reach the caller");
      Require(fixture.channels->Stub()->calls == 1, "a storage error must not be retried");
    }

    // 5. A split discovered mid-batch regroups instead of repeating work.
    {
      auto cache = std::make_shared<RegionCache>(
          std::vector<RegionMetadata>{Descriptor(1, "", "", 1000, 1, 1)}, 1);
      auto channels = std::make_shared<ScriptedChannels>(std::vector<ScriptStep>{
          {stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH, 0, "0"}});
      RegionCache::RefreshFunction refresh =
          [](const std::string&, RegionCache::Clock::time_point) {
            return RegionCache::RefreshResult{
                {Descriptor(1, "", "m", 1000, 2, 2), Descriptor(2, "m", "", 2000, 2, 2)}, 2};
          };
      auto sender = std::make_shared<RegionRequestSender>(cache, channels, refresh);
      auto storage = std::make_shared<RaftMvccStorage>(
          1, std::vector<std::pair<std::string, short>>{{"127.0.0.1", 26001}});
      storage->AttachRequestSender(sender);
      const auto status = storage->BatchPrewrite({{"apple", "v", false, false},
                                                  {"zebra", "v", false, false}},
                                                 "apple", 100, 3000);
      Require(status == TxnStatus::ResultUnknown, "a split batch must not report success");
      Require(storage->LastRegroupRequired(), "a split batch must ask the caller to regroup");
      Require(sender->Metrics().regroupRequired == 1, "regrouping must be counted");
      Require(cache->Snapshot()->regions.size() == 2,
              "the refreshed topology must be published");
    }

    std::cout << "Region data path checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Region data path checks failed: " << error.what() << std::endl;
    return 1;
  }
}
