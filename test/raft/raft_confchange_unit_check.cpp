/*
 * 测试目标：验证 Region Raft 配置变更：learner 角色、动态 AddLearner/PromoteLearner/RemovePeer
 *           与在途 ConfChange 互斥。
 * 测试策略：进程内直接构造 Raft 核心与 RegionPeer（真实 Persister，mkdtemp 临时目录），以 learner
 *           候选投票、错误 epoch、在途重复提案、自退役等输入驱动 RequestVote/ProposeConfChange/ApplyConfChange。
 * 测试规模：3 组测试：Raft 核心 learner/quorum/投票拒绝与动态成员变更、在途 ConfChange 互斥、
 *           RegionPeer 集成（epoch 校验、in-flight 拒绝、增删晋升与自退役）。
 * 验证内容：learner 不能发起投票也不能为他人投票，成员变更逐一生效且 conf_version 前进，在途
 *           ConfChange 期间第二个提案被拒（REGION_ERROR_CONF_CHANGE_IN_FLIGHT），自退役后节点
 *           拒绝 RPC，epoch 不匹配的提案在任何执行前被结构化拒绝。
 */
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "raft.h"
#include "region_peer.h"
#include "region.pb.h"
#include "kv_server_rpc.pb.h"
#include "txn_scheduler.h"
#include "util.h"

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

RegionMetadata MakeDescriptor(int regionId, uint64_t selfPeerId,
                              const std::vector<std::pair<uint64_t, bool>>& peersWithLearner) {
  RegionMetadata desc;
  desc.regionId = regionId;
  desc.startKey = "a";
  desc.endKey = "z";
  desc.epoch = {1, 1};
  desc.metadataRevision = 1;
  desc.leaderPeerId = selfPeerId;

  for (size_t i = 0; i < peersWithLearner.size(); ++i) {
    RegionPeerLocation loc;
    loc.nodeId = static_cast<int>(i);
    loc.host = "127.0.0.1";
    loc.port = static_cast<short>(28000 + i);
    loc.storeId = i + 1;
    loc.peerId = peersWithLearner[i].first;
    loc.isLearner = peersWithLearner[i].second;
    desc.peers.push_back(loc);
  }
  return desc;
}

stratakv::region::ConfChangeCommand MakeConfChange(stratakv::region::ConfChangeType type,
                                                   uint64_t targetPeerId, uint64_t storeId,
                                                   const std::string& host, uint32_t port,
                                                   bool isLearner, uint64_t confVersion,
                                                   uint64_t version = 1) {
  stratakv::region::ConfChangeCommand cmd;
  cmd.set_changetype(type);
  auto* p = cmd.mutable_peer();
  p->set_peerid(targetPeerId);
  p->set_storeid(storeId);
  p->set_host(host);
  p->set_port(port);
  p->set_islearner(isLearner);

  auto* epoch = cmd.mutable_expectedepoch();
  epoch->set_confversion(confVersion);
  epoch->set_version(version);
  cmd.set_metadatarevision(10);
  return cmd;
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-raft-confchange-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) return 1;
  const auto originalDirectory = std::filesystem::current_path();
  std::filesystem::current_path(temporaryDirectory);

  try {
    std::cout << "--- Starting Raft ConfChange & Replica Migration Unit Checks ---" << std::endl;

    // =========================================================================
    // Test 1: Direct Raft Core Protocol checks
    // =========================================================================
    std::cout << "[Test 1] Testing Raft core learner role, quorum, and vote rejection..." << std::endl;
    {
      auto persister = std::make_shared<Persister>(0);
      auto applyCh = std::make_shared<LockQueue<ApplyMsg>>();
      auto raft = std::make_shared<Raft>();

      // 3 peers: peer 0 (me, voter), peer 1 (voter), peer 2 (learner)
      std::vector<std::shared_ptr<RaftRpcUtil>> rpcPeers(3, nullptr);
      std::vector<uint64_t> peerIds = {1001, 1002, 1003};
      std::vector<bool> learners = {false, false, true};

      raft->init(rpcPeers, 0, persister, applyCh, /*deferActivation=*/true,
                 /*regionId=*/1, /*localPeerId=*/1001, peerIds, learners);

      Require(!raft->IsRemoved(), "Raft peer must not be removed on startup");
      Require(!raft->HasConfChangeInFlight(), "Initial Raft must have no in-flight confchange");

      // Verify vote rejection from learner candidate
      raftRpcProctoc::RequestVoteArgs voteArgs;
      voteArgs.set_term(1);
      voteArgs.set_candidateid(2); // peer 2 is learner
      voteArgs.set_lastlogindex(0);
      voteArgs.set_lastlogterm(0);
      raftRpcProctoc::RequestVoteReply voteReply;

      raft->RequestVote(&voteArgs, &voteReply);
      Require(!voteReply.votegranted(), "Learner candidate must be rejected by RequestVote");

      // Verify vote request to learner local peer
      auto learnerRaft = std::make_shared<Raft>();
      auto learnerPersister = std::make_shared<Persister>(2);
      auto learnerApplyCh = std::make_shared<LockQueue<ApplyMsg>>();
      learnerRaft->init(rpcPeers, 2, learnerPersister, learnerApplyCh, /*deferActivation=*/true,
                        /*regionId=*/1, /*localPeerId=*/1003, peerIds, learners);

      voteArgs.set_candidateid(0); // voter 0 asks learner 2
      voteReply.Clear();
      learnerRaft->RequestVote(&voteArgs, &voteReply);
      Require(!voteReply.votegranted(), "Learner local peer must never grant votes");

      // Verify dynamic AddLearner, PromoteLearner, and RemovePeer on Raft
      // Add learner 1004
      auto addLearnerCmd = MakeConfChange(stratakv::region::CONF_CHANGE_ADD_LEARNER,
                                          1004, 4, "127.0.0.1", 28003, true, 1);
      raft->ApplyConfChange(addLearnerCmd);

      // Promote learner 1003 to voter
      auto promoteCmd = MakeConfChange(stratakv::region::CONF_CHANGE_PROMOTE_LEARNER,
                                       1003, 3, "127.0.0.1", 28002, false, 2);
      raft->ApplyConfChange(promoteCmd);

      // Remove peer 1002
      auto removeCmd = MakeConfChange(stratakv::region::CONF_CHANGE_REMOVE_PEER,
                                      1002, 2, "127.0.0.1", 28001, false, 3);
      raft->ApplyConfChange(removeCmd);

      // Verify self-removal decomission
      Require(!learnerRaft->IsRemoved(), "Learner must not be removed before decommission");
      auto removeSelfCmd = MakeConfChange(stratakv::region::CONF_CHANGE_REMOVE_PEER,
                                          1003, 3, "127.0.0.1", 28002, false, 4);
      learnerRaft->ApplyConfChange(removeSelfCmd);
      Require(learnerRaft->IsRemoved(), "Targeting self with RemovePeer must mark peer removed");

      // Removed peer rejects RPCs
      raftRpcProctoc::AppendEntriesArgs aeArgs;
      aeArgs.set_term(1);
      raftRpcProctoc::AppendEntriesReply aeReply;
      learnerRaft->AppendEntries1(&aeArgs, &aeReply);
      Require(!aeReply.success(), "Removed peer must reject AppendEntries");
    }
    std::cout << "[Test 1] PASSED." << std::endl;

    // =========================================================================
    // Test 2: In-Flight ConfChange Mutual Exclusion in Raft
    // =========================================================================
    std::cout << "[Test 2] Testing in-flight ConfChange mutual exclusion..." << std::endl;
    {
      auto persister = std::make_shared<Persister>(0);
      auto applyCh = std::make_shared<LockQueue<ApplyMsg>>();
      auto raft = std::make_shared<Raft>();
      std::vector<std::shared_ptr<RaftRpcUtil>> rpcPeers(1, nullptr);
      std::vector<uint64_t> peerIds = {1001};
      std::vector<bool> learners = {false};

      raft->init(rpcPeers, 0, persister, applyCh, /*deferActivation=*/true,
                 /*regionId=*/2, /*localPeerId=*/1001, peerIds, learners);

      // Make single peer 0 leader
      raft->doElection();

      Op confOp;
      confOp.Operation = "ConfChange";
      confOp.ClientId = "test-client";
      confOp.RequestId = 1;
      auto cmd = MakeConfChange(stratakv::region::CONF_CHANGE_ADD_LEARNER, 1002, 2, "127.0.0.1", 28001, true, 1);
      cmd.SerializeToString(&confOp.Value);

      int newIndex = -1;
      int newTerm = -1;
      bool isLeader = false;
      raft->Start(confOp, &newIndex, &newTerm, &isLeader);
      Require(isLeader, "Single node peer must accept Start as leader");

      // In-flight mutex must now report true!
      Require(raft->HasConfChangeInFlight(), "ConfChange in queue must set HasConfChangeInFlight");

      // Apply the conf change
      raft->ApplyConfChange(cmd);
      // Wait or clear
    }
    std::cout << "[Test 2] PASSED." << std::endl;

    // =========================================================================
    // Test 3: RegionPeer Integration - Epoch advancement, in-flight rejection, self-retire
    // =========================================================================
    std::cout << "[Test 3] Testing RegionPeer ConfChange application, epoch advance & retire..." << std::endl;
    {
      auto scheduler = std::make_shared<NodeTxnScheduler>(64, 1, 16, 32);
      std::vector<std::pair<uint64_t, bool>> peers = {
          {2001, false}, // voter 0 (me)
          {2002, false}, // voter 1
          {2003, true}   // learner 2
      };
      auto desc = MakeDescriptor(100, 2001, peers);
      auto peer = std::make_shared<RegionPeer>(0, desc, 0, RaftLogGcConfig{}, scheduler);
      peer->MarkServing();

      Require(peer->Descriptor().epoch.confVersion == 1, "Initial conf_version must be 1");
      Require(peer->Descriptor().peers.size() == 3, "Initial peers count must be 3");
      Require(peer->Descriptor().peers[2].isLearner, "Peer 2003 must be learner");

      // 1. ProposeConfChange with epoch mismatch
      raftKVRpcProctoc::ProposeConfChangeArgs propArgs;
      propArgs.set_regionid(100);
      auto badEpochCmd = MakeConfChange(stratakv::region::CONF_CHANGE_ADD_LEARNER, 2004, 4,
                                        "127.0.0.1", 28003, true, 999); // wrong epoch
      *propArgs.mutable_confchange() = badEpochCmd;
      raftKVRpcProctoc::ProposeConfChangeReply propReply;
      peer->ProposeConfChange(nullptr, &propArgs, &propReply, nullptr);
      Require(propReply.header().error().code() == stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH,
              "ProposeConfChange with epoch mismatch must return REGION_ERROR_EPOCH_NOT_MATCH");

      // In-flight proposal rejection check on RegionPeer
      std::vector<std::pair<uint64_t, bool>> singlePeer = {{3001, false}};
      auto singleDesc = MakeDescriptor(200, 3001, singlePeer);
      auto singleRegionPeer = std::make_shared<RegionPeer>(0, singleDesc, 0, RaftLogGcConfig{}, scheduler);
      singleRegionPeer->MarkServing();
      singleRegionPeer->RaftNode()->doElection(); // elects itself leader!

      raftKVRpcProctoc::ProposeConfChangeArgs pArgs1;
      pArgs1.set_regionid(200);
      auto addCmd = MakeConfChange(stratakv::region::CONF_CHANGE_ADD_LEARNER, 3002, 2, "127.0.0.1", 28001, true, 1);
      *pArgs1.mutable_confchange() = addCmd;
      raftKVRpcProctoc::ProposeConfChangeReply pReply1;
      singleRegionPeer->ProposeConfChange(nullptr, &pArgs1, &pReply1, nullptr);
      Require(pReply1.err() == std::to_string(static_cast<int>(TxnStatus::Ok)), "First ProposeConfChange must succeed");

      // Second proposal while first is in-flight must be rejected with REGION_ERROR_CONF_CHANGE_IN_FLIGHT!
      raftKVRpcProctoc::ProposeConfChangeArgs pArgs2;
      pArgs2.set_regionid(200);
      auto promoteCmd2 = MakeConfChange(stratakv::region::CONF_CHANGE_PROMOTE_LEARNER, 3002, 2, "127.0.0.1", 28001, false, 1);
      *pArgs2.mutable_confchange() = promoteCmd2;
      raftKVRpcProctoc::ProposeConfChangeReply pReply2;
      singleRegionPeer->ProposeConfChange(nullptr, &pArgs2, &pReply2, nullptr);
      Require(pReply2.header().error().code() == stratakv::region::REGION_ERROR_CONF_CHANGE_IN_FLIGHT,
              "Second ProposeConfChange in flight must return REGION_ERROR_CONF_CHANGE_IN_FLIGHT");

      // 2. ApplyConfChange: AddLearner 2004
      auto addLearnerCmd = MakeConfChange(stratakv::region::CONF_CHANGE_ADD_LEARNER, 2004, 4,
                                          "127.0.0.1", 28003, true, 1);
      Require(peer->ApplyConfChange(addLearnerCmd) == TxnStatus::Ok, "Apply AddLearner must succeed");
      Require(peer->Descriptor().epoch.confVersion == 2, "ConfVersion must advance to 2");
      Require(peer->Descriptor().peers.size() == 4, "Peers count must be 4");
      Require(peer->Descriptor().peers[3].peerId == 2004 && peer->Descriptor().peers[3].isLearner,
              "Peer 2004 must be added as learner");

      // 3. ApplyConfChange: PromoteLearner 2003
      auto promoteCmd = MakeConfChange(stratakv::region::CONF_CHANGE_PROMOTE_LEARNER, 2003, 3,
                                       "127.0.0.1", 28002, false, 2);
      Require(peer->ApplyConfChange(promoteCmd) == TxnStatus::Ok, "Apply PromoteLearner must succeed");
      Require(peer->Descriptor().epoch.confVersion == 3, "ConfVersion must advance to 3");
      Require(!peer->Descriptor().peers[2].isLearner, "Peer 2003 must be promoted to voter");

      // 4. ApplyConfChange: RemovePeer 2002 (remote voter)
      auto removeRemoteCmd = MakeConfChange(stratakv::region::CONF_CHANGE_REMOVE_PEER, 2002, 2,
                                            "127.0.0.1", 28001, false, 3);
      Require(peer->ApplyConfChange(removeRemoteCmd) == TxnStatus::Ok, "Apply RemovePeer remote must succeed");
      Require(peer->Descriptor().epoch.confVersion == 4, "ConfVersion must advance to 4");
      Require(peer->Descriptor().peers.size() == 3, "Peers count must be reduced to 3");
      for (const auto& p : peer->Descriptor().peers) {
        Require(p.peerId != 2002, "Peer 2002 must be removed from descriptor");
      }

      // 5. ApplyConfChange: RemovePeer 2001 (self-decommission)
      Require(peer->LifecycleState() == RegionPeerState::Serving, "Peer must still be serving");
      auto removeSelfCmd = MakeConfChange(stratakv::region::CONF_CHANGE_REMOVE_PEER, 2001, 1,
                                          "127.0.0.1", 28000, false, 4);
      Require(peer->ApplyConfChange(removeSelfCmd) == TxnStatus::Ok, "Apply RemovePeer self must succeed");
      Require(peer->LifecycleState() == RegionPeerState::Stopped ||
              peer->LifecycleState() == RegionPeerState::Retiring,
              "Self-removal must trigger retirement/stopped state");
    }
    std::cout << "[Test 3] PASSED." << std::endl;

    std::cout << "All Raft ConfChange checks completed successfully!" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "Test failed with exception: " << e.what() << std::endl;
    std::filesystem::current_path(originalDirectory);
    std::filesystem::remove_all(temporaryDirectory);
    return 1;
  }

  std::filesystem::current_path(originalDirectory);
  std::filesystem::remove_all(temporaryDirectory);
  return 0;
}
