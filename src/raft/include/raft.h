#ifndef RAFT_H
#define RAFT_H

#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
#include "apply_msg.h"
#include "persister.h"
#include "boost/serialization/access.hpp"
#include "config.h"
#include <pulsar/pulsar.h>
#include "raft_rpc_util.h"
#include "util.h"
// Transport status used to distinguish an RPC failure from a Raft rejection.
constexpr int Disconnected =
    0;  // 方便网络分区的时候debug，网络异常的时候为disconnected，只要网络正常就为AppNormal，防止matchIndex[]数组异常减小
constexpr int AppNormal = 1;

///////////////投票状态

constexpr int Voted = 1;   //本轮已经投过票了
constexpr int Expire = 2;  //投票（消息、竞选者）过期
constexpr int Normal = 3;

class Raft : public raftRpcProctoc::raftRpc {
 public:
  enum class ReadIndexStatus { Ok, NotLeader, Timeout };

  struct ReadIndexResult {
    ReadIndexStatus status = ReadIndexStatus::NotLeader;
    int term = -1;
    int readIndex = -1;
    uint64_t context = 0;
    bool ok() const { return status == ReadIndexStatus::Ok; }
  };

  struct NodeStatus {
    int term;
    bool isLeader;
    int commitIndex;
    int lastApplied;
    int lastLogIndex;
    uint64_t readIndexRequests;
    uint64_t readIndexRounds;
    uint64_t readIndexCompleted;
    uint64_t appendEntriesSent;
    uint64_t persistCount;
  };

 private:
  // The RPC service is registered before init() finishes so that peers can
  // establish connections during startup. Reject inbound Raft RPCs until all
  // persistent state and the apply channel have been initialized.
  std::atomic<bool> m_initialized{false};
  std::mutex m_mtx;
  std::condition_variable m_applyCond;
  std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;
  std::shared_ptr<Persister> m_persister;
  int m_me;
  int m_currentTerm;
  int m_votedFor;
  std::vector<raftRpcProctoc::LogEntry> m_logs;  //// 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号
                                                 // 这两个状态所有结点都在维护，易失
  int m_commitIndex;
  int m_lastApplied;  // 已经汇报给状态机（上层应用）的log 的index

  // 这两个状态是由服务器来维护，易失
  std::vector<int>
      m_nextIndex;  // 这两个状态的下标1开始，因为通常commitIndex和lastApplied从0开始，应该是一个无效的index，因此下标从1开始
  std::vector<int> m_matchIndex;
  enum Status { Follower, Candidate, Leader };
  // 身份
  Status m_status;

  // Delivers committed entries and snapshots to the Region state machine.
  std::shared_ptr<LockQueue<ApplyMsg>> applyChan;

  // 选举超时

  std::chrono::steady_clock::time_point m_lastResetElectionTime;
  // Last valid AppendEntries/InstallSnapshot from the recognized leader.
  // Followers use this separately from the election timer to avoid granting a
  // competing higher-term vote inside an active leader-lease window.
  std::chrono::steady_clock::time_point m_lastLeaderContactTime;
  // 心跳超时，用于leader
  std::chrono::steady_clock::time_point m_lastResetHearBeatTime;

  // 2D中用于传入快照点
  // 储存了快照中的最后一个日志的Index和Term
  int m_lastSnapshotIncludeIndex;
  int m_lastSnapshotIncludeTerm;

  LockQueue<Op> m_proposeQueue; // MPSC queue for batching requests without locking m_mtx

  enum class ReplicationTaskType { AppendEntries, Snapshot };

  struct ReplicationTask {
    ReplicationTaskType type = ReplicationTaskType::AppendEntries;
    std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs;
    std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply;
  };

  std::vector<std::shared_ptr<LockQueue<ReplicationTask>>> m_replicationQueues;

  struct ReadWaiter {
    std::chrono::steady_clock::time_point deadline;
    bool completed = false;
    ReadIndexResult result;
  };

  struct ReadRound {
    uint64_t context = 0;
    int term = -1;
    int readIndex = -1;
    std::unordered_set<int> acknowledgedPeers;
    std::vector<std::shared_ptr<ReadWaiter>> waiters;
  };

  uint64_t m_nextReadContext = 0;
  std::vector<std::shared_ptr<ReadWaiter>> m_pendingReadWaiters;
  std::shared_ptr<ReadRound> m_activeReadRound;
  std::condition_variable m_readCond;
  uint64_t m_readIndexRequests = 0;
  uint64_t m_readIndexRounds = 0;
  uint64_t m_readIndexCompleted = 0;
  std::atomic<uint64_t> m_appendEntriesSent{0};
  std::atomic<uint64_t> m_persistCount{0};

  // 协程
  std::unique_ptr<pulsar::IOManager> m_ioManager = nullptr;

 public:
  void AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply);
  void applierTicker();
  bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot);
  void doElection();
  /**
   * \brief 发起心跳，只有leader才需要发起心跳
   */
  void doHeartBeat();
  // 每隔一段时间检查睡眠时间内有没有重置定时器，没有则说明超时了
  // 如果有则设置合适睡眠时间：睡眠到重置时间+超时时间
  void electionTimeOutTicker();
  void raftPollerTicker();
  std::vector<ApplyMsg> getApplyLogs();
  int getNewCommandIndex();
  void getPrevLogInfo(int server, int *preIndex, int *preTerm);
  void GetState(int *term, bool *isLeader);
  NodeStatus GetStatus();
  std::vector<raftRpcProctoc::LogEntry> GetLogEntries(size_t maxEntries);
  void InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                       raftRpcProctoc::InstallSnapshotResponse *reply);
  void leaderHearBeatTicker();
  void leaderSendSnapShot(int server);
  void replicationWorker(int server);
  void leaderUpdateCommitIndex();
  bool matchLog(int logIndex, int logTerm);
  void persist();
  void RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply);
  bool UpToDate(int index, int term);
  int getLastLogIndex();
  int getLastLogTerm();
  void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);
  int getLogTermFromLogIndex(int logIndex);
  int GetRaftStateSize();
  int getSlicesIndexFromLogIndex(int logIndex);

  bool sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                       std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);
  bool sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                         std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply);

  // Deliver an apply message without holding the Raft state mutex.
  void pushMsgToRegionPeer(ApplyMsg msg);
  void readPersist(std::string data);
  std::string persistData();

  void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);
  ReadIndexResult ReadIndex(std::chrono::steady_clock::time_point deadline);
  bool IsLeaderInTerm(int term);

  // Persist a service snapshot through index and compact the covered log prefix.
  void Snapshot(int index, std::string snapshot);

 public:
  // 重写基类方法,因为rpc远程调用真正调用的是这个方法
  //序列化，反序列化等操作rpc框架都已经做完了，因此这里只需要获取值然后真正调用本地方法即可。
  void AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProctoc::AppendEntriesArgs *request,
                     ::raftRpcProctoc::AppendEntriesReply *response, ::google::protobuf::Closure *done) override;
  void InstallSnapshot(google::protobuf::RpcController *controller,
                       const ::raftRpcProctoc::InstallSnapshotRequest *request,
                       ::raftRpcProctoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done) override;
  void RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProctoc::RequestVoteArgs *request,
                   ::raftRpcProctoc::RequestVoteReply *response, ::google::protobuf::Closure *done) override;

 public:
  void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

 private:
  bool hasCommittedEntryInCurrentTermLocked();
  void completeReadRoundIfQuorumLocked();
  void abortReadIndexLocked(ReadIndexStatus status);
  void removeReadWaiterLocked(const std::shared_ptr<ReadWaiter>& waiter);

  // for persist

  class BoostPersistRaftNode {
   public:
    friend class boost::serialization::access;
    // When the class Archive corresponds to an output archive, the
    // & operator is defined similar to <<.  Likewise, when the class Archive
    // is a type of input archive the & operator is defined similar to >>.
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version) {
      ar &m_currentTerm;
      ar &m_votedFor;
      ar &m_lastSnapshotIncludeIndex;
      ar &m_lastSnapshotIncludeTerm;
      ar &m_logs;
    }
    int m_currentTerm;
    int m_votedFor;
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;
    std::vector<std::string> m_logs;
    std::unordered_map<std::string, int> umap;

   public:
  };
};

#endif  // RAFT_H
