/*
 * 测试目标：验证 RegionMetadata/RegionCatalog 的键空间边界路由、protobuf 往返确定性与拓扑合法性校验。
 * 测试策略：纯内存构造 2 Region 目录，探测空键、分裂键与高位 0xff 字节的归属；ToProto 后经
 *           FromProto 还原比对 Digest 与 epoch；对 5 种非法拓扑逐个断言抛 invalid_argument；再验证
 *           旧格式记录的 ID 归一化。
 * 测试规模：3 组场景、8 个合法断言 + 5 个非法构造 + 3 个旧 ID 检查。
 * 验证内容：[start, end) 半开边界路由正确（分裂键归右 Region、空键被覆盖），往返后 Digest 一致
 *           且 epoch 不变；非空下界、键空间 gap、range 重叠、重复 Region id、重复 peer id 一律被
 *           拒绝；旧记录的 store/peer/leader ID 归一化结果是确定的且 leader 指向存在的 peer。
 */
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "region_metadata.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

template <typename Function>
void RequireInvalid(Function&& function, const char* message) {
  try {
    function();
  } catch (const std::invalid_argument&) {
    return;
  }
  throw std::runtime_error(message);
}

RegionMetadata Region(int id, std::string start, std::string end, uint64_t peerId,
                      uint64_t storeId = 1) {
  RegionMetadata region;
  region.regionId = id;
  region.startKey = std::move(start);
  region.endKey = std::move(end);
  region.peers.push_back({static_cast<int>(storeId - 1), "127.0.0.1",
                          static_cast<short>(22000 + storeId), storeId, peerId});
  region.leaderPeerId = peerId;
  return region;
}

void CheckRoundTripAndBoundaries() {
  RegionCatalog catalog({Region(10, "", "m", 101), Region(20, "m", "", 201)});
  Require(catalog.FindByKey("").regionId == 10, "empty lower key must be covered");
  Require(catalog.FindByKey("m").regionId == 20, "split key belongs to right Region");
  const std::string highByte(1, static_cast<char>(0xff));
  Require(catalog.FindByKey(highByte).regionId == 20, "bytewise high key must be covered");

  const auto encoded = catalog.ToProto();
  google::protobuf::RepeatedPtrField<stratakv::region::RegionDescriptor> repeated;
  for (const auto& descriptor : encoded) *repeated.Add() = descriptor;
  const RegionCatalog restored = RegionCatalog::FromProto(repeated);
  Require(restored.Digest() == catalog.Digest(), "protobuf round trip must be deterministic");
  Require(restored.FindById(20).epoch == RegionEpoch{1, 1}, "epoch must round trip");
}

void CheckInvalidTopologies() {
  RequireInvalid([] { RegionCatalog({Region(10, "a", "", 101)}); },
                 "non-empty lower bound must fail");
  RequireInvalid(
      [] { RegionCatalog({Region(10, "", "m", 101), Region(20, "n", "", 201)}); },
      "range gap must fail");
  RequireInvalid(
      [] { RegionCatalog({Region(10, "", "n", 101), Region(20, "m", "", 201)}); },
      "range overlap must fail");
  RequireInvalid(
      [] { RegionCatalog({Region(10, "", "m", 101), Region(10, "m", "", 201)}); },
      "duplicate Region ID must fail");
  RequireInvalid(
      [] { RegionCatalog({Region(10, "", "m", 101), Region(20, "m", "", 101)}); },
      "duplicate peer ID must fail");
}

void CheckLegacyIds() {
  RegionMetadata region;
  region.regionId = 7;
  region.peers = {{2, "127.0.0.1", 22100}};
  RegionCatalog catalog({region});
  const auto& normalized = catalog.Regions().front();
  Require(normalized.peers.front().storeId == 3, "legacy Store ID must be deterministic");
  Require(normalized.peers.front().peerId != 0, "legacy peer ID must be deterministic");
  Require(normalized.leaderPeerId == normalized.peers.front().peerId,
          "legacy leader must reference a peer");
}

}  // namespace

int main() {
  try {
    CheckRoundTripAndBoundaries();
    CheckInvalidTopologies();
    CheckLegacyIds();
    std::cout << "Region metadata checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Region metadata checks failed: " << error.what() << std::endl;
    return 1;
  }
}
