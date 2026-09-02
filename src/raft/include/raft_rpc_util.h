#ifndef STRATAKV_RAFT_RAFT_RPC_UTIL_H
#define STRATAKV_RAFT_RAFT_RPC_UTIL_H

#include "raft_rpc.pb.h"

class MprpcChannel;

/// Owns the RPC channel and generated stub used to contact one remote Raft peer.
class RaftRpcUtil {
 private:
  MprpcChannel* channel_;
  raftRpcProctoc::raftRpc_Stub *stub_;
  int regionId_;

 public:
  // Outbound Raft RPCs are issued through the generated Protobuf stub.
  bool AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response);
  bool InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args, raftRpcProctoc::InstallSnapshotResponse *response);
  bool RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response);
  //响应其他节点的方法
  /**
   *
   * @param ip  远端ip
   * @param port  远端端口
   */
  RaftRpcUtil(std::string ip, short port, int regionId);
  ~RaftRpcUtil();
};

#endif  // STRATAKV_RAFT_RAFT_RPC_UTIL_H
