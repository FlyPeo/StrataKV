/*
 * 测试目标：验证 learner 副本增量追赶水位、阈值触发晋升与多块流式 InstallSnapshot 的端到端正确性。
 * 测试策略：进程内构造 leader+learner 的 Raft/RegionPeer，手动追加日志并回报 AE 成功推进
 *           matchIndex；用 600KB 校验模式 payload 按 256KB 分块喂 InstallSnapshot；最后用真实
 *           RocksDB 快照做序列化、流式安装与 learner 晋升的全链路验证。
 * 测试规模：4 组测试：matchIndex/lag 水位计算、MaybePromoteLearner 晋升、3 块流式快照重组、
 *           RocksDB 快照安装 + learner 晋升。
 * 验证内容：learner 的 matchIndex 随应答推进、lag 水位正确，追平 commitIndex 后晋升提案可被
 *           应用为 voter 配置，未收完的快照块不产出任何 ApplyMsg，重组后快照与 persister 持久化
 *           内容一致，恢复出的 learner 可读到 leader 写入并成功晋升。
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
    loc.port = static_cast<short>(29000 + i);
    loc.storeId = i + 1;
    loc.peerId = peersWithLearner[i].first;
    loc.isLearner = peersWithLearner[i].second;
    desc.peers.push_back(loc);
  }
  return desc;
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-replica-catchup-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) return 1;
  const auto originalDirectory = std::filesystem::current_path();
  std::filesystem::current_path(temporaryDirectory);

  try {
    std::cout << "--- Starting Learner Replica Catch-Up & Streaming Snapshot Unit Checks ---" << std::endl;

    // =========================================================================
    // Test 1: Incremental Log Catch-up & Watermark Lag Detection
    // =========================================================================
    std::cout << "[Test 1] Testing Learner lag watermark calculation and match index tracking..." << std::endl;
    {
      auto persister = std::make_shared<Persister>(0);
      auto applyCh = std::make_shared<LockQueue<ApplyMsg>>();
      auto raft = std::make_shared<Raft>();

      // 2 peers: peer 0 (me, voter), peer 1 (learner)
      std::vector<std::shared_ptr<RaftRpcUtil>> rpcPeers(2, nullptr);
      std::vector<uint64_t> peerIds = {1001, 1002};
      std::vector<bool> learners = {false, true};

      raft->init(rpcPeers, 0, persister, applyCh, /*deferActivation=*/true,
                 /*regionId=*/1, /*localPeerId=*/1001, peerIds, learners);

      // Elect peer 0 as leader (1 voter -> quorum = 1 -> elects immediately)
      raft->doElection();
      int term = 0;
      bool isLeader = false;
      raft->GetState(&term, &isLeader);
      Require(isLeader, "Peer 0 must be leader after election");

      // Initial commitIndex is 1 (term 1 no-op committed)
      Require(raft->GetPeerMatchIndex(1002) == 0, "Learner initial matchIndex must be 0");
      int initialLag = raft->GetPeerLag(1002);
      Require(initialLag >= 1, "Learner must lag by at least 1 at start");
      Require(!raft->IsPeerCaughtUp(1002, /*maxLag=*/0), "Learner must not be caught up with maxLag=0");
      Require(raft->IsPeerCaughtUp(1002, /*maxLag=*/initialLag), "Learner must be caught up when maxLag >= initialLag");

      // Simulate appending entries and receiving AE success from learner
      raftRpcProctoc::AppendEntriesArgs aeArgs;
      aeArgs.set_term(1);
      aeArgs.set_leaderid(0);
      aeArgs.set_prevlogindex(0);
      aeArgs.set_prevlogterm(0);
      auto* entry = aeArgs.add_entries();
      entry->set_logindex(1);
      entry->set_logterm(1);
      entry->set_command("noop");

      raftRpcProctoc::AppendEntriesReply aeReply;
      aeReply.set_term(1);
      aeReply.set_success(true);
      aeReply.set_appstate(AppNormal);

      raft->HandleAppendEntriesReply(1, &aeArgs, &aeReply);

      // Now matchIndex for learner is 1
      Require(raft->GetPeerMatchIndex(1002) == 1, "Learner matchIndex must advance to 1 after AE reply");
      Require(raft->GetPeerLag(1002) == 0, "Learner lag must be 0 after catching up to commitIndex");
      Require(raft->IsPeerCaughtUp(1002, /*maxLag=*/0), "Learner must be caught up with maxLag=0");
    }
    std::cout << "[Test 1] PASSED." << std::endl;

    // =========================================================================
    // Test 2: Triggered Promotion via MaybePromoteLearner in RegionPeer
    // =========================================================================
    std::cout << "[Test 2] Testing RegionPeer MaybePromoteLearner threshold trigger..." << std::endl;
    {
      auto scheduler = std::make_shared<NodeTxnScheduler>(64, 1, 16, 32);
      std::vector<std::pair<uint64_t, bool>> peers = {
          {2001, false}, // voter 0 (me)
          {2002, true}   // learner 1
      };
      auto desc = MakeDescriptor(100, 2001, peers);
      auto peer = std::make_shared<RegionPeer>(0, desc, 0, RaftLogGcConfig{}, scheduler);
      peer->MarkServing();
      auto applyCh = std::make_shared<LockQueue<ApplyMsg>>();
      peer->RaftNode()->init(std::vector<std::shared_ptr<RaftRpcUtil>>(2, nullptr), 0,
                             std::make_shared<Persister>(0), applyCh,
                             /*deferActivation=*/true, /*regionId=*/100,
                             /*localPeerId=*/2001, {2001, 2002}, {false, true});
      peer->RaftNode()->doElection(); // Make 2001 leader

      Require(peer->Descriptor().peers[1].isLearner, "Peer 2002 must be learner initially");
      Require(peer->Descriptor().epoch.confVersion == 1, "Initial confVersion must be 1");

      std::cout << "Test 2: isLeader=" << peer->RaftNode()->GetStatus().isLeader
                << " commitIndex=" << peer->RaftNode()->GetStatus().commitIndex
                << " matchIndex=" << peer->RaftNode()->GetPeerMatchIndex(2002)
                << " lag=" << peer->RaftNode()->GetPeerLag(2002)
                << " isCaughtUp=" << peer->IsPeerCaughtUp(2002, 0)
                << std::endl;
      Require(!peer->IsPeerCaughtUp(2002, /*maxLag=*/0), "Learner 2002 must lag initially");
      std::cout << "Step 2.1: check promotedWhileLagging" << std::endl;
      bool promotedWhileLagging = peer->MaybePromoteLearner(2002, /*maxLag=*/0);
      Require(!promotedWhileLagging, "MaybePromoteLearner must refuse to promote lagging learner");
      Require(peer->Descriptor().peers[1].isLearner, "Peer 2002 must remain learner when lagging");

      // Simulate learner catching up to leader
      std::cout << "Step 2.2: SetPeerMatchIndexForTest" << std::endl;
      peer->SetPeerMatchIndexForTest(2002, 10);
      // Ensure commitIndex <= 10
      Require(peer->IsPeerCaughtUp(2002, /*maxLag=*/10), "Learner 2002 must be caught up with maxLag=10");

      // Now trigger MaybePromoteLearner
      std::cout << "Step 2.3: MaybePromoteLearner" << std::endl;
      bool promoted = peer->MaybePromoteLearner(2002, /*maxLag=*/10);
      Require(promoted, "MaybePromoteLearner must succeed once learner is caught up");

      // The production apply loop consumes the committed command. This unit
      // test deliberately keeps perpetual RegionPeer workers stopped, so drive
      // the same apply hook directly after verifying the promotion proposal.
      std::cout << "Step 2.4: Apply promoted configuration" << std::endl;
      stratakv::region::ConfChangeCommand promote;
      promote.set_changetype(stratakv::region::CONF_CHANGE_PROMOTE_LEARNER);
      promote.mutable_peer()->set_peerid(2002);
      promote.mutable_peer()->set_storeid(2);
      promote.mutable_peer()->set_host("127.0.0.1");
      promote.mutable_peer()->set_port(29001);
      promote.mutable_expectedepoch()->set_version(1);
      promote.mutable_expectedepoch()->set_confversion(1);
      Require(peer->ApplyConfChange(promote) == TxnStatus::Ok,
              "PromoteLearner must apply successfully");
      Require(peer->Descriptor().peers[1].peerId == 2002, "Promoted peer ID must be 2002");
      Require(!peer->Descriptor().peers[1].isLearner, "Promoted peer isLearner flag must be false");
      Require(peer->Descriptor().epoch.confVersion == 2, "ConfVersion must advance to 2");
    }
    std::cout << "[Test 2] PASSED." << std::endl;

    // =========================================================================
    // Test 3: Multi-Chunk Streaming InstallSnapshot Assembly & Verification
    // =========================================================================
    std::cout << "[Test 3] Testing multi-chunk streaming InstallSnapshot assembly..." << std::endl;
    {
      auto persister = std::make_shared<Persister>(1);
      auto applyCh = std::make_shared<LockQueue<ApplyMsg>>();
      auto raft = std::make_shared<Raft>();

      std::vector<std::shared_ptr<RaftRpcUtil>> rpcPeers(2, nullptr);
      std::vector<uint64_t> peerIds = {3001, 3002};
      std::vector<bool> learners = {false, true};

      raft->init(rpcPeers, 1, persister, applyCh, /*deferActivation=*/true,
                 /*regionId=*/2, /*localPeerId=*/3002, peerIds, learners);

      // Generate a 600 KB payload with a distinct verification pattern
      const size_t payloadSize = 600 * 1024;
      std::string payload;
      payload.resize(payloadSize);
      for (size_t i = 0; i < payloadSize; ++i) {
        payload[i] = static_cast<char>('A' + (i % 26));
      }

      // Chunk 0: 256 KB, chunkDone = false
      const size_t chunkSize = 256 * 1024;
      raftRpcProctoc::InstallSnapshotRequest req0;
      req0.set_leaderid(0);
      req0.set_term(2);
      req0.set_lastsnapshotincludeindex(50);
      req0.set_lastsnapshotincludeterm(2);
      req0.set_data(payload.data(), chunkSize);
      req0.set_chunkoffset(0);
      req0.set_chunkdone(false);

      raftRpcProctoc::InstallSnapshotResponse resp0;
      raft->InstallSnapshot(&req0, &resp0);
      Require(resp0.term() == 2, "Response term must match snapshot term");

      // Verify that no ApplyMsg was emitted yet for incomplete chunk 0
      ApplyMsg pendingMsg;
      Require(!applyCh->timeOutPop(20, &pendingMsg), "Apply channel must not receive message before ChunkDone");

      // Chunk 1: 256 KB, chunkDone = false
      raftRpcProctoc::InstallSnapshotRequest req1;
      req1.set_leaderid(0);
      req1.set_term(2);
      req1.set_lastsnapshotincludeindex(50);
      req1.set_lastsnapshotincludeterm(2);
      req1.set_data(payload.data() + chunkSize, chunkSize);
      req1.set_chunkoffset(chunkSize);
      req1.set_chunkdone(false);

      raftRpcProctoc::InstallSnapshotResponse resp1;
      raft->InstallSnapshot(&req1, &resp1);
      Require(resp1.term() == 2, "Response term must match snapshot term");
      Require(!applyCh->timeOutPop(20, &pendingMsg), "Apply channel must not receive message before ChunkDone");

      // Chunk 2: Remaining 88 KB, chunkDone = true
      const size_t remainingSize = payloadSize - (2 * chunkSize);
      raftRpcProctoc::InstallSnapshotRequest req2;
      req2.set_leaderid(0);
      req2.set_term(2);
      req2.set_lastsnapshotincludeindex(50);
      req2.set_lastsnapshotincludeterm(2);
      req2.set_data(payload.data() + 2 * chunkSize, remainingSize);
      req2.set_chunkoffset(2 * chunkSize);
      req2.set_chunkdone(true);

      raftRpcProctoc::InstallSnapshotResponse resp2;
      raft->InstallSnapshot(&req2, &resp2);
      Require(resp2.term() == 2, "Response term must match snapshot term");

      // Now applyChan MUST have the reassembled snapshot!
      ApplyMsg finalMsg;
      Require(applyCh->timeOutPop(500, &finalMsg), "Apply channel must receive message once ChunkDone is true");
      Require(finalMsg.SnapshotValid, "Delivered message must have SnapshotValid = true");
      Require(finalMsg.SnapshotIndex == 50, "SnapshotIndex must match included index 50");
      Require(finalMsg.SnapshotTerm == 2, "SnapshotTerm must match included term 2");
      Require(finalMsg.Snapshot.size() == payloadSize, "Reassembled snapshot size must match original payload size");
      Require(finalMsg.Snapshot == payload, "Reassembled snapshot must be byte-for-byte identical to payload");

      // Verify persister stored snapshot
      Require(persister->ReadSnapshot() == payload, "Persister must save byte-for-byte identical snapshot");
    }
    std::cout << "[Test 3] PASSED." << std::endl;

    // =========================================================================
    // Test 4: End-to-End RocksDB Snapshot Serialization, Loading & Learner Promotion
    // =========================================================================
    std::cout << "[Test 4] Testing end-to-end RocksDB snapshot restore and promotion..." << std::endl;
    {
      auto scheduler = std::make_shared<NodeTxnScheduler>(64, 1, 16, 32);
      std::vector<std::pair<uint64_t, bool>> peers = {
          {4001, false}, // voter 0 (leader)
          {4002, true}   // learner 1
      };
      auto descLeader = MakeDescriptor(200, 4001, peers);
      auto descLearner = MakeDescriptor(200, 4002, peers);

      auto leaderPeer = std::make_shared<RegionPeer>(0, descLeader, 0, RaftLogGcConfig{}, scheduler);
      auto learnerPeer = std::make_shared<RegionPeer>(1, descLearner, 1, RaftLogGcConfig{}, scheduler);
      leaderPeer->MarkServing();
      learnerPeer->MarkServing();
      auto leaderApplyCh = std::make_shared<LockQueue<ApplyMsg>>();
      auto learnerApplyCh = std::make_shared<LockQueue<ApplyMsg>>();
      leaderPeer->RaftNode()->init(std::vector<std::shared_ptr<RaftRpcUtil>>(2, nullptr), 0,
                                   std::make_shared<Persister>("catchup-leader"), leaderApplyCh,
                                   /*deferActivation=*/true, /*regionId=*/200,
                                   /*localPeerId=*/4001, {4001, 4002}, {false, true});
      learnerPeer->RaftNode()->init(std::vector<std::shared_ptr<RaftRpcUtil>>(2, nullptr), 1,
                                    std::make_shared<Persister>("catchup-learner"), learnerApplyCh,
                                    /*deferActivation=*/true, /*regionId=*/200,
                                    /*localPeerId=*/4002, {4001, 4002}, {false, true});
      leaderPeer->RaftNode()->doElection();

      // Put initial KV data into leader
      Require(leaderPeer->EngineForTest()->Put("key-alpha", "value-alpha"),
              "Leader test key must be written before snapshot capture");

      // Capture leader RocksDB snapshot
      std::string capturedSnapshot = leaderPeer->MakeSnapShot();
      Require(!capturedSnapshot.empty(), "Captured snapshot from leader must not be empty");

      // Stream the snapshot to learner through Raft InstallSnapshot
      raftRpcProctoc::InstallSnapshotRequest snapReq;
      snapReq.set_leaderid(0);
      snapReq.set_term(1);
      snapReq.set_lastsnapshotincludeindex(10);
      snapReq.set_lastsnapshotincludeterm(1);
      snapReq.set_data(capturedSnapshot);
      snapReq.set_chunkoffset(0);
      snapReq.set_chunkdone(true);

      raftRpcProctoc::InstallSnapshotResponse snapResp;
      learnerPeer->RaftNode()->InstallSnapshot(&snapReq, &snapResp);

      ApplyMsg installed;
      Require(learnerApplyCh->timeOutPop(500, &installed),
              "Learner must publish the installed snapshot to its state machine");
      learnerPeer->GetSnapShotFromRaft(std::move(installed));
      std::string restoredValue;
      Require(learnerPeer->EngineForTest()->Get("key-alpha", &restoredValue) &&
                  restoredValue == "value-alpha",
              "Learner must restore keys from streamed RocksDB snapshot");

      // Promote learner once caught up
      leaderPeer->SetPeerMatchIndexForTest(4002, 10);
      Require(leaderPeer->IsPeerCaughtUp(4002, /*maxLag=*/0), "Learner 4002 must be caught up");
      Require(leaderPeer->MaybePromoteLearner(4002, /*maxLag=*/0), "Leader must succeed in proposing learner promotion");
    }
    std::cout << "[Test 4] PASSED." << std::endl;

    std::filesystem::current_path(originalDirectory);
    std::filesystem::remove_all(temporaryDirectory);

    std::cout << "\nALL 4 REPLICA CATCHUP & STREAMING SNAPSHOT CHECKS PASSED!\n";
    return 0;
  } catch (const std::exception& error) {
    std::filesystem::current_path(originalDirectory);
    std::filesystem::remove_all(temporaryDirectory);
    std::cerr << "\nERROR: Replica catch-up check failed: " << error.what() << std::endl;
    return 1;
  }
}
