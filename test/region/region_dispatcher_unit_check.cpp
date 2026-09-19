/*
 * 测试目标：验证 KvServiceDispatcher 的请求围栏：header 校验、epoch/Region/peer/键范围检查与
 *           结构化错误回带。
 * 测试策略：在 mkdtemp 临时目录用真实 NodeServer 分别以静态与动态拓扑模式构建 dispatcher，直接
 *           下发带缺陷 header 的 PutAppend/TxnBatchPrewrite 与 Raft AppendEntries，检查响应 header、
 *           错误码与调度器 accepted 计数。
 * 测试规模：静态模式 8 个用例 + 动态模式 4 个用例，覆盖 6 种 Region 错误码。
 * 验证内容：缺失 Region、陈旧 epoch（回带当前描述符与 leader hint）、错误 peer、越界键、跨边界
 *           批次都被结构化拒绝且 rejected 请求绝不进入节点调度器、不保留 peer 句柄；动态模式强制
 *           header，陈旧/未知 peer 的 Raft 消息在进入 Raft 执行前被围栏。
 */
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "kv_server_rpc.pb.h"
#include "mvcc_storage.h"
#include "node_server.h"
#include "raft_rpc.pb.h"
#include "region.pb.h"
#include "region_request_validator.h"
#include "txn_scheduler.h"
#include "util.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

RegionMetadata Descriptor(int id, const char* start, const char* end, uint64_t basePeerId,
                          short localPort) {
  RegionMetadata descriptor;
  descriptor.regionId = id;
  descriptor.startKey = start;
  descriptor.endKey = end;
  descriptor.epoch = {1, 1};
  descriptor.metadataRevision = 1;
  descriptor.peers = {
      {0, "127.0.0.1", localPort, 1, basePeerId},
      {1, "127.0.0.1", static_cast<short>(localPort + 1), 2, basePeerId + 1},
      {2, "127.0.0.1", static_cast<short>(localPort + 2), 3, basePeerId + 2},
  };
  descriptor.leaderPeerId = basePeerId;
  return descriptor;
}

RegionCatalog TwoRegionCatalog(int firstId, uint64_t basePeerId, short localPort) {
  // One shared listener per physical node: both Regions reuse the same three
  // stores/ports, only the Region- and peer-IDs differ.
  return RegionCatalog({Descriptor(firstId, "", "m", basePeerId, localPort),
                        Descriptor(firstId + 10, "m", "", basePeerId + 10, localPort)});
}

class SyncDone final : public google::protobuf::Closure {
 public:
  void Run() override { finished = true; }
  bool finished = false;
};

stratakv::region::RegionRequestHeader Header(int regionId, uint64_t peerId, uint64_t version,
                                             uint64_t confVersion) {
  stratakv::region::RegionRequestHeader header;
  header.set_regionid(static_cast<uint64_t>(regionId));
  header.set_peerid(peerId);
  header.mutable_epoch()->set_version(version);
  header.mutable_epoch()->set_confversion(confVersion);
  header.set_clientid("dispatcher-check");
  header.set_requestid(1);
  return header;
}

void DispatchPutAppend(KvServiceDispatcher& dispatcher, int regionId, const std::string& key,
                       const stratakv::region::RegionRequestHeader* header,
                       raftKVRpcProctoc::PutAppendReply* reply) {
  raftKVRpcProctoc::PutAppendArgs request;
  request.set_key(key);
  request.set_value("v");
  request.set_op("Put");
  request.set_clientid("dispatcher-check");
  request.set_requestid(1);
  request.set_regionid(regionId);
  if (header != nullptr) request.mutable_header()->CopyFrom(*header);
  SyncDone done;
  dispatcher.PutAppend(nullptr, &request, reply, &done);
  Require(done.finished, "dispatcher must complete the protobuf closure");
}

void DispatchTxnBatchPrewrite(KvServiceDispatcher& dispatcher, int regionId,
                              const std::vector<std::string>& keys,
                              const stratakv::region::RegionRequestHeader* header,
                              raftKVRpcProctoc::TxnBatchPrewriteReply* reply) {
  raftKVRpcProctoc::TxnBatchPrewriteArgs request;
  request.set_primarykey(keys.front());
  request.set_startts(100);
  request.set_clientid("dispatcher-check");
  request.set_requestid(2);
  request.set_regionid(regionId);
  for (const auto& key : keys) {
    auto* mutation = request.add_mutations();
    mutation->set_key(key);
    mutation->set_value("v");
  }
  if (header != nullptr) request.mutable_header()->CopyFrom(*header);
  SyncDone done;
  dispatcher.TxnBatchPrewrite(nullptr, &request, reply, &done);
  Require(done.finished, "dispatcher must complete the protobuf closure");
}

stratakv::region::RegionErrorCode ResponseErrorCode(
    const stratakv::region::RegionResponseHeader& header) {
  return header.error().code();
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-region-dispatcher-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) return 1;
  const auto originalDirectory = std::filesystem::current_path();
  std::filesystem::current_path(temporaryDirectory);
  try {
    // Static compatibility mode: headers are optional but validated when present.
    auto catalog = TwoRegionCatalog(10, 1000000, 25010);
    NodeServer server(0, RaftLogGcConfig{}, catalog, "");
    for (const auto& peer : server.PeersForTest()) peer->MarkServing();
    server.PublishInitialRegistryForTest();
    KvServiceDispatcher dispatcher(&server);
    const auto* scheduler = server.TxnSchedulerForTest();
    const uint64_t localPeerId = 1000000;

    const uint64_t acceptedBefore = scheduler->GetStats().accepted;

    raftKVRpcProctoc::PutAppendReply missingRegion;
    DispatchPutAppend(dispatcher, 999, "apple", nullptr, &missingRegion);
    Require(ResponseErrorCode(missingRegion.header()) ==
                stratakv::region::REGION_ERROR_REGION_NOT_FOUND,
            "missing Region must return a structured missing-Region error");
    Require(missingRegion.err() ==
                std::to_string(static_cast<int>(TxnStatus::StorageError)),
            "legacy replies must still observe a failure status");

    raftKVRpcProctoc::PutAppendReply staleEpoch;
    const auto staleHeader = Header(10, localPeerId, 2, 1);
    DispatchPutAppend(dispatcher, 10, "apple", &staleHeader, &staleEpoch);
    Require(ResponseErrorCode(staleEpoch.header()) ==
                stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH,
            "stale epoch must return EpochNotMatch");
    Require(staleEpoch.header().error().metadatarevision() == 1 &&
                staleEpoch.header().error().currentregions().size() == 1,
            "epoch errors must return the observed current descriptor");
    Require(staleEpoch.header().error().leader().peerid() == localPeerId,
            "epoch errors must include the current leader hint");

    const auto wrongPeerHeader = Header(10, localPeerId + 1, 1, 1);
    raftKVRpcProctoc::PutAppendReply wrongPeer;
    DispatchPutAppend(dispatcher, 10, "apple", &wrongPeerHeader, &wrongPeer);
    Require(ResponseErrorCode(wrongPeer.header()) ==
                stratakv::region::REGION_ERROR_PEER_NOT_FOUND,
            "request targeting another peer must be rejected");

    const auto outOfRangeHeader = Header(10, localPeerId, 1, 1);
    raftKVRpcProctoc::PutAppendReply outOfRange;
    DispatchPutAppend(dispatcher, 10, "zzz", &outOfRangeHeader, &outOfRange);
    Require(ResponseErrorCode(outOfRange.header()) ==
                stratakv::region::REGION_ERROR_KEY_NOT_IN_REGION,
            "out-of-range keys must be rejected before Region side effects");

    raftKVRpcProctoc::PutAppendReply legacyAccepted;
    DispatchPutAppend(dispatcher, 10, "apple", nullptr, &legacyAccepted);
    Require(legacyAccepted.err() == ErrWrongLeader &&
                ResponseErrorCode(legacyAccepted.header()) == stratakv::region::REGION_ERROR_NONE,
            "legacy static requests must still reach the Region service");

    raftKVRpcProctoc::PutAppendReply headerAccepted;
    const auto validHeader = Header(10, localPeerId, 1, 1);
    DispatchPutAppend(dispatcher, 10, "apple", &validHeader, &headerAccepted);
    Require(headerAccepted.err() == ErrWrongLeader,
            "matching headers must reach the Region service");

    // A batch whose keys span a Region boundary is rejected whole, before the
    // node scheduler enqueues any transaction work.
    raftKVRpcProctoc::TxnBatchPrewriteReply spanningBatch;
    DispatchTxnBatchPrewrite(dispatcher, 10, {"apple", "zzz"}, &outOfRangeHeader,
                             &spanningBatch);
    Require(ResponseErrorCode(spanningBatch.header()) ==
                stratakv::region::REGION_ERROR_KEY_NOT_IN_REGION,
            "batches crossing a boundary must be rejected before grouping");
    Require(scheduler->GetStats().accepted == acceptedBefore,
            "rejected requests must never reach the node scheduler");

    for (const auto& peer : server.PeersForTest()) {
      Require(peer->InFlightRequests() == 0,
              "rejected dispatch must release the retained peer handle");
    }

    // Dynamic topology mode: the header is mandatory for KV dispatch, and Raft
    // messages with headers are identity- and epoch-checked.
    auto dynamicCatalog = TwoRegionCatalog(30, 3000000, 26010);
    NodeServer dynamicServer(0, RaftLogGcConfig{}, dynamicCatalog, "", true);
    for (const auto& peer : dynamicServer.PeersForTest()) peer->MarkServing();
    dynamicServer.PublishInitialRegistryForTest();
    KvServiceDispatcher dynamicDispatcher(&dynamicServer);
    RaftServiceDispatcher raftDispatcher(&dynamicServer);
    const uint64_t dynamicPeerId = 3000000;
    const auto dynamicScheduler = dynamicServer.TxnSchedulerForTest();
    const uint64_t dynamicAcceptedBefore = dynamicScheduler->GetStats().accepted;

    raftKVRpcProctoc::PutAppendReply headerless;
    DispatchPutAppend(dynamicDispatcher, 30, "apple", nullptr, &headerless);
    Require(ResponseErrorCode(headerless.header()) ==
                stratakv::region::REGION_ERROR_MISSING_HEADER,
            "dynamic mode must reject requests without a Region header");
    Require(dynamicScheduler->GetStats().accepted == dynamicAcceptedBefore,
            "headerless dynamic requests must never reach the node scheduler");

    raftRpcProctoc::AppendEntriesArgs append;
    append.set_regionid(30);
    append.set_term(5);
    append.set_leaderid(1);
    auto* raftHeader = append.mutable_header();
    raftHeader->set_regionid(30);
    raftHeader->set_frompeerid(dynamicPeerId + 1);
    raftHeader->set_topeerid(dynamicPeerId);
    raftHeader->mutable_epoch()->set_version(1);
    raftHeader->mutable_epoch()->set_confversion(2);
    raftRpcProctoc::AppendEntriesReply appendReply;
    SyncDone appendDone;
    raftDispatcher.AppendEntries(nullptr, &append, &appendReply, &appendDone);
    Require(appendDone.finished && !appendReply.success() && appendReply.term() == 0,
            "stale-epoch Raft messages must be fenced before Raft execution");
    Require(ResponseErrorCode(appendReply.header()) ==
                stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH,
            "stale-epoch Raft messages must return a structured epoch error");

    raftHeader->mutable_epoch()->set_confversion(1);
    raftHeader->set_frompeerid(9999);
    raftRpcProctoc::AppendEntriesReply unknownPeerReply;
    SyncDone unknownPeerDone;
    raftDispatcher.AppendEntries(nullptr, &append, &unknownPeerReply, &unknownPeerDone);
    Require(!unknownPeerReply.success() &&
                ResponseErrorCode(unknownPeerReply.header()) ==
                    stratakv::region::REGION_ERROR_PEER_NOT_FOUND,
            "Raft messages from unknown peers must be rejected");

    std::filesystem::current_path(originalDirectory);
    std::filesystem::remove_all(temporaryDirectory);
    std::cout << "Region dispatcher validation checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::filesystem::current_path(originalDirectory);
    std::cerr << "Region dispatcher checks failed: " << error.what() << std::endl;
    return 1;
  }
}
