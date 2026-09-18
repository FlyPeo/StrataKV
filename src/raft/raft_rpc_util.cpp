#include "raft_rpc_util.h"

#include <mprpc_channel.h>
#include <mprpc_controller.h>

namespace {

template <typename Request>
void PopulateHeader(Request* request, int regionId, uint64_t fromPeerId,
                    uint64_t toPeerId, const RegionEpoch& epoch) {
  if (fromPeerId == 0 || toPeerId == 0) return;
  auto* header = request->mutable_header();
  header->set_regionid(static_cast<uint64_t>(regionId));
  header->set_frompeerid(fromPeerId);
  header->set_topeerid(toPeerId);
  header->mutable_epoch()->set_version(epoch.version);
  header->mutable_epoch()->set_confversion(epoch.confVersion);
}

}  // namespace

bool RaftRpcUtil::AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response) {
  args->set_regionid(regionId_);
  PopulateHeader(args, regionId_, fromPeerId_, toPeerId_, epoch_);
  RegionEpoch currentEpoch;
  {
    std::lock_guard<std::mutex> lock(epochMutex_);
    currentEpoch = epoch_;
  }
  PopulateHeader(args, regionId_, fromPeerId_, toPeerId_, currentEpoch);
  MprpcController controller;
  stub_->AppendEntries(&controller, args, response, nullptr);
  return !controller.Failed();
}

bool RaftRpcUtil::InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args,
                                  raftRpcProctoc::InstallSnapshotResponse *response) {
  args->set_regionid(regionId_);
  PopulateHeader(args, regionId_, fromPeerId_, toPeerId_, epoch_);
  RegionEpoch currentEpoch;
  {
    std::lock_guard<std::mutex> lock(epochMutex_);
    currentEpoch = epoch_;
  }
  PopulateHeader(args, regionId_, fromPeerId_, toPeerId_, currentEpoch);
  MprpcController controller;
  stub_->InstallSnapshot(&controller, args, response, nullptr);
  return !controller.Failed();
}

bool RaftRpcUtil::RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response) {
  args->set_regionid(regionId_);
  PopulateHeader(args, regionId_, fromPeerId_, toPeerId_, epoch_);
  RegionEpoch currentEpoch;
  {
    std::lock_guard<std::mutex> lock(epochMutex_);
    currentEpoch = epoch_;
  }
  PopulateHeader(args, regionId_, fromPeerId_, toPeerId_, currentEpoch);
  MprpcController controller;
  stub_->RequestVote(&controller, args, response, nullptr);
  return !controller.Failed();
}

void RaftRpcUtil::SetEpoch(const RegionEpoch& epoch) {
  std::lock_guard<std::mutex> lock(epochMutex_);
  epoch_ = epoch;
}

//先开启服务器，再尝试连接其他的节点，中间给一个间隔时间，等待其他的rpc服务器节点启动

RaftRpcUtil::RaftRpcUtil(std::string ip, short port, int regionId) : regionId_(regionId) {
  //*********************************************  */
  //发送rpc设置
  channel_ = new MprpcChannel(ip, port, true);
  stub_ = new raftRpcProctoc::raftRpc_Stub(channel_);
}

RaftRpcUtil::RaftRpcUtil(std::string ip, short port, int regionId,
                         uint64_t fromPeerId, uint64_t toPeerId, RegionEpoch epoch)
    : regionId_(regionId),
      fromPeerId_(fromPeerId),
      toPeerId_(toPeerId),
      epoch_(epoch) {
  channel_ = new MprpcChannel(ip, port, true);
  stub_ = new raftRpcProctoc::raftRpc_Stub(channel_);
}

RaftRpcUtil::~RaftRpcUtil() {
  delete stub_;
  delete channel_;
}
