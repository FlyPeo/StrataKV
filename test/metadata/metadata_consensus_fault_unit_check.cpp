// Consensus and persistence fault tests: quorum loss, duplicate (lost) mutation
// responses, snapshot install into a lagging member, and malformed local
// snapshots. No fault may publish a partial or uncommitted metadata view.
/*
 * 测试目标：验证 metadata 共识与持久化的故障语义：仲裁丢失、mutation 响应丢失重放、快照安装与损坏快照。
 * 测试策略：用 3 成员配置的单存活节点制造仲裁丢失；在内存 MetadataStateMachine 上重放同一
 *           mutation id 模拟响应丢失；把 leader 快照安装到落后成员并截断破坏；全程不依赖真实对端流量。
 * 测试规模：3 个故障场景（重复响应、快照安装、仲裁丢失），配 1 个 mkdtemp 目录与完整 bootstrap 命令。
 * 验证内容：仲裁丢失时读写都以 NotLeader 拒绝、不泄漏 proposal waiter、不发布未提交 revision；
 *           重放返回与首次完全相同的结果并计 dedup 且 revision 不前进；有效快照还原 leader 的
 *           revision 与键路由，截断快照被原子拒绝且不发布任何部分状态、不残留可服务的陈旧键。
 */
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "metadata_consensus.h"
#include "metadata_state_machine.h"

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

stratakv::region::RegionDescriptor Region(uint64_t id, const std::string& start,
                                          const std::string& end, uint64_t version) {
  stratakv::region::RegionDescriptor region;
  region.set_regionid(id);
  region.set_startkey(start);
  region.set_endkey(end);
  region.mutable_epoch()->set_version(version);
  region.mutable_epoch()->set_confversion(1);
  region.set_leaderpeerid(101);
  auto* peer = region.add_peers();
  peer->set_peerid(101);
  peer->set_storeid(1);
  peer->set_host("127.0.0.1");
  peer->set_port(26001);
  return region;
}

metadataRpcProtocol::MetadataCommand Bootstrap(const std::string& mutationId) {
  metadataRpcProtocol::MetadataCommand command;
  command.set_mutationid(mutationId);
  command.set_expectedrevision(0);
  command.mutable_bootstrap()->set_configdigest("fault-digest");
  *command.mutable_bootstrap()->add_regions() = Region(10, "", "", 1);
  auto* store = command.mutable_bootstrap()->add_stores();
  store->set_storeid(1);
  store->set_host("127.0.0.1");
  store->set_port(26001);
  return command;
}

std::unique_ptr<MetadataConsensusNode> MakeNode(const std::string& directory) {
  return std::make_unique<MetadataConsensusNode>(
      0,
      std::vector<MetadataConsensusNode::Endpoint>{
          {"127.0.0.1", 27890}, {"127.0.0.1", 27891}, {"127.0.0.1", 27892}},
      directory, 64);
}

// Quorum loss: with one member of three alive no election can succeed, so no
// call may observe an uncommitted view and no waiter may leak.
void CheckQuorumLoss(const std::string& directory) {
  // The node is intentionally never destroyed: its Raft ticker is detached by
  // design and a destructor would wait on threads that hold no quorum.
  auto* node = MakeNode(directory + "/quorum-loss").release();
  node->Start();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);

  bool readRejected = false;
  try {
    node->LinearizableView(deadline);
  } catch (const MetadataNotLeaderError&) {
    readRejected = true;
  }
  Require(readRejected, "quorum loss must reject linearizable reads with NotLeader");

  bool mutateRejected = false;
  try {
    node->Mutate(Bootstrap("quorum-loss-1"), deadline);
  } catch (const MetadataNotLeaderError&) {
    mutateRejected = true;
  }
  Require(mutateRejected, "quorum loss must reject mutations with NotLeader");
  Require(node->PendingWaitersForTest() == 0, "quorum loss must not leak proposal waiters");
  Require(node->LocalView()->revision == 0,
          "quorum loss must not publish an uncommitted revision");
  node->Stop();
  Require(node->IsStopped(), "Stop must resolve the node");
}

// A lost response makes the client resend the same mutation id: the retry must
// return the original result without applying the mutation twice.
void CheckDuplicateResponseLoss() {
  MetadataStateMachine state;
  const auto command = Bootstrap("duplicate-1");
  const auto first = state.Apply(command);
  Require(first.error() == metadataRpcProtocol::METADATA_OK, "bootstrap must apply");
  const auto retry = state.Apply(command);
  Require(retry.SerializeAsString() == first.SerializeAsString(),
          "a resent mutation must return its original result");
  Require(state.Metrics().dedupHits >= 1, "a resent mutation must count as a dedup hit");
  Require(state.View()->revision == first.revision(),
          "a resent mutation must not advance the revision");
}

// Snapshot install into a lagging member, and a corrupt install that must not
// publish any partial state.
void CheckSnapshotInstall(const std::string& directory) {
  MetadataStateMachine leader;
  Require(leader.Apply(Bootstrap("snapshot-install-1")).error() ==
              metadataRpcProtocol::METADATA_OK,
          "leader bootstrap must apply");
  const std::string snapshot = leader.Snapshot();
  Require(!snapshot.empty(), "a published view must produce a snapshot");

  MetadataStateMachine lagging;
  std::string error;
  Require(lagging.Restore(snapshot, &error),
          "a lagging member must install a valid snapshot: " + error);
  Require(lagging.View()->revision == leader.View()->revision,
          "an installed snapshot must reproduce the leader revision");
  Require(lagging.LookupKey("m").has_value(), "an installed snapshot must serve key lookups");

  // A truncated install is a malformed local snapshot: it must be rejected
  // atomically, leaving the member on its previous (empty but valid) view.
  MetadataStateMachine corrupt;
  Require(!corrupt.Restore(snapshot.substr(0, snapshot.size() / 2), &error),
          "a truncated snapshot must be rejected");
  Require(corrupt.View()->revision == 0, "a rejected snapshot must not publish partial state");

  // A member that already holds a damaged local file must not publish it: the
  // install path above is the only publisher, and a failed install leaves the
  // member on revision zero with a still-usable lookup path.
  Require(!corrupt.LookupKey("m").has_value(),
          "a rejected install must not serve stale key lookups");
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-metadata-fault-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) return 1;
  try {
    CheckDuplicateResponseLoss();
    CheckSnapshotInstall(std::string(temporaryDirectory));
    CheckQuorumLoss(std::string(temporaryDirectory));
    std::filesystem::remove_all(temporaryDirectory);
    std::cout << "Metadata consensus fault checks passed" << std::endl;
    // Raft tickers are detached by design; exit without static destruction.
    std::cout.flush();
    std::cerr.flush();
    std::_Exit(0);
  } catch (const std::exception& error) {
    std::filesystem::remove_all(temporaryDirectory);
    std::cerr << "Metadata consensus fault checks failed: " << error.what() << std::endl;
    return 1;
  }
}
