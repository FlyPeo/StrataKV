/*
 * 测试目标：验证请求信封（Region header）的兼容矩阵：新旧客户端在静态/动态模式下的接受与拒绝规则。
 * 测试策略：直接调用纯函数 ValidateRegionRequest/ValidateRaftPeerRequest/PopulateRegionError，
 *           用同一 Region 描述符分别以带 header、无 header、陈旧 epoch、错误 Region、越界键等输入
 *           遍历矩阵，不启动任何服务器。
 * 测试规模：6 组用例：typed header 双模式接受、legacy 无 header、动态模式拒绝、3 类错误码、
 *           Raft peer 围栏、结构化错误降级。
 * 验证内容：typed header 在两种模式都被接受，静态模式容忍无 header 而动态模式以 MISSING_HEADER
 *           拒绝，陈旧 epoch/错 Region id/越界键返回互不相同的错误码，Raft peer 消息同样按
 *           header/peer 围栏；结构化错误携带 code、说明、metadata revision、leader hint，epoch 错误
 *           还回带当前描述符以便客户端免二次往返刷新。
 */
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "region.pb.h"
#include "region_metadata.h"
#include "region_request_validator.h"

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

RegionMetadata Descriptor(int id, const char* start, const char* end, uint64_t version,
                          uint64_t peerId) {
  RegionMetadata descriptor;
  descriptor.regionId = id;
  descriptor.startKey = start;
  descriptor.endKey = end;
  descriptor.epoch = {version, 1};
  descriptor.metadataRevision = 7;
  descriptor.peers = {{0, "127.0.0.1", 26001, 1, peerId},
                      {1, "127.0.0.1", 26002, 2, peerId + 1}};
  descriptor.leaderPeerId = peerId;
  return descriptor;
}

stratakv::region::RegionRequestHeader Header(int regionId, uint64_t peerId, uint64_t version,
                                             const std::string& clientId, int requestId) {
  stratakv::region::RegionRequestHeader header;
  header.set_regionid(static_cast<uint64_t>(regionId));
  header.set_peerid(peerId);
  header.mutable_epoch()->set_version(version);
  header.mutable_epoch()->set_confversion(1);
  header.set_clientid(clientId);
  header.set_requestid(static_cast<uint64_t>(requestId));
  return header;
}

}  // namespace

int main() {
  try {
    const RegionMetadata descriptor = Descriptor(5, "a", "m", 3, 501);

    // New client -> new server: the typed header matches and is accepted in
    // both modes.
    {
      const auto header = Header(5, 501, 3, "client-1", 1);
      const auto dynamic =
          ValidateRegionRequest(descriptor, 501, 5, &header, {"apple"}, true);
      const auto legacy =
          ValidateRegionRequest(descriptor, 501, 5, &header, {"apple"}, false);
      Require(dynamic.ok(), "a typed header must be accepted in dynamic mode");
      Require(legacy.ok(), "a typed header must be accepted in static mode");
    }

    // Legacy client -> static server: a headerless request is still served.
    {
      const auto legacy = ValidateRegionRequest(descriptor, 501, 5, nullptr, {"apple"}, false);
      Require(legacy.ok(), "static mode must accept a legacy headerless request");
    }

    // Any client -> dynamic server: a headerless request is rejected with a
    // typed, countable error instead of being guessed at.
    {
      const auto missing = ValidateRegionRequest(descriptor, 501, 5, nullptr, {"apple"}, true);
      Require(!missing.ok() && missing.code == stratakv::region::REGION_ERROR_MISSING_HEADER,
              "dynamic mode must reject a headerless request");
    }

    // Stale epoch, wrong Region and out-of-range keys produce distinct codes.
    {
      const auto staleEpoch = Header(5, 501, 2, "client-1", 2);
      const auto epochResult =
          ValidateRegionRequest(descriptor, 501, 5, &staleEpoch, {"apple"}, true);
      Require(epochResult.code == stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH,
              "an older epoch must be rejected");

      const auto wrongRegion = Header(9, 501, 3, "client-1", 3);
      const auto regionResult =
          ValidateRegionRequest(descriptor, 501, 5, &wrongRegion, {"apple"}, true);
      Require(regionResult.code == stratakv::region::REGION_ERROR_REGION_NOT_FOUND,
              "a mismatched Region id must be rejected");

      const auto header = Header(5, 501, 3, "client-1", 4);
      const auto keyResult =
          ValidateRegionRequest(descriptor, 501, 5, &header, {"zebra"}, true);
      Require(keyResult.code == stratakv::region::REGION_ERROR_KEY_NOT_IN_REGION,
              "a key outside the Region must be rejected");
    }

    // Raft peer transport is fenced the same way in dynamic mode.
    {
      stratakv::region::RaftPeerHeader peer;
      peer.set_regionid(5);
      peer.set_frompeerid(501);
      peer.set_topeerid(502);
      peer.mutable_epoch()->set_version(3);
      peer.mutable_epoch()->set_confversion(1);
      Require(ValidateRaftPeerRequest(descriptor, 502, 5, &peer, true).ok(),
              "a current Raft peer header must be accepted");
      stratakv::region::RaftPeerHeader wrongTarget = peer;
      wrongTarget.set_topeerid(999);
      Require(ValidateRaftPeerRequest(descriptor, 502, 5, &wrongTarget, true).code ==
                  stratakv::region::REGION_ERROR_PEER_NOT_FOUND,
              "a Raft message for another peer must be rejected");
      Require(ValidateRaftPeerRequest(descriptor, 502, 5, nullptr, true).code ==
                  stratakv::region::REGION_ERROR_MISSING_HEADER,
              "dynamic mode must reject a headerless Raft message");
      Require(ValidateRaftPeerRequest(descriptor, 502, 5, nullptr, false).ok(),
              "static mode must accept a headerless Raft message");
    }

    // Structured errors degrade to a legacy string path: the response still
    // carries the current descriptor so a client can refresh.
    {
      const auto missing =
          ValidateRegionRequest(descriptor, 501, 5, nullptr, {"apple"}, true);
      stratakv::region::RegionResponseHeader response;
      PopulateRegionError(missing, &descriptor, &response);
      Require(response.error().code() == stratakv::region::REGION_ERROR_MISSING_HEADER,
              "the structured error must carry its code");
      Require(!response.error().message().empty(), "the structured error must explain itself");
      Require(response.error().metadatarevision() == 7,
              "the structured error must carry the metadata revision");
      Require(response.error().has_leader(), "the structured error must carry a leader hint");

      // Stale-epoch errors additionally carry the current descriptor so a
      // client can refresh without a second round trip.
      const auto staleEpoch = Header(5, 501, 2, "client-1", 9);
      const auto stale = ValidateRegionRequest(descriptor, 501, 5, &staleEpoch, {"apple"}, true);
      stratakv::region::RegionResponseHeader staleResponse;
      PopulateRegionError(stale, &descriptor, &staleResponse);
      Require(staleResponse.error().currentregions_size() == 1,
              "an epoch error must carry the current descriptor");
      Require(staleResponse.error().currentregions(0).regionid() == 5 &&
                  staleResponse.error().currentregions(0).epoch().version() == 3,
              "an epoch error must describe the current epoch");
    }

    std::cout << "Region envelope compatibility checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Region envelope compatibility checks failed: " << error.what() << std::endl;
    return 1;
  }
}
