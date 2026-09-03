#include "raft.h"  // Raft consensus implementation.
#include <algorithm>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <cstring>
#include <memory>
#include "config.h"
#include "util.h"

namespace {
constexpr char kRaftPersistMagic[] = "TTRF2";
constexpr size_t kRaftPersistMagicSize = sizeof(kRaftPersistMagic) - 1;

void AppendInt32(std::string* out, int value) {
  const int32_t encoded = static_cast<int32_t>(value);
  out->append(reinterpret_cast<const char*>(&encoded), sizeof(encoded));
}

void AppendUint64(std::string* out, uint64_t value) {
  out->append(reinterpret_cast<const char*>(&value), sizeof(value));
}

bool ReadInt32(const std::string& data, size_t* offset, int* value) {
  if (*offset + sizeof(int32_t) > data.size()) {
    return false;
  }
  int32_t encoded = 0;
  std::memcpy(&encoded, data.data() + *offset, sizeof(encoded));
  *offset += sizeof(encoded);
  *value = static_cast<int>(encoded);
  return true;
}

bool ReadUint64(const std::string& data, size_t* offset, uint64_t* value) {
  if (*offset + sizeof(uint64_t) > data.size()) {
    return false;
  }
  std::memcpy(value, data.data() + *offset, sizeof(*value));
  *offset += sizeof(*value);
  return true;
}

bool HasBinaryPersistMagic(const std::string& data) {
  return data.size() >= kRaftPersistMagicSize &&
         std::memcmp(data.data(), kRaftPersistMagic, kRaftPersistMagicSize) == 0;
}
}  // namespace

void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args, raftRpcProctoc::AppendEntriesReply* reply) {
  std::lock_guard<std::mutex> locker(m_mtx);
  bool persistentStateChanged = false;
  reply->set_appstate(AppNormal);  // 能接收到代表网络是正常的
  if (args->term() < m_currentTerm) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(-100);  // 论文中：让领导人可以及时更新自己
        DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%d}< rf{%d}.term{%d}\n", m_me, args->leaderid(),
          args->term(), m_me, m_currentTerm);
    return;  // 注意从过期的领导人收到消息不要重设定时器
  }
  // currentTerm/votedFor/log entries are persistent Raft state. commitIndex,
  // role and election timers are volatile. Persisting unconditionally here
  // rewrote the complete log on every 25 ms heartbeat and eventually starved
  // RPC handling behind disk I/O.
  DEFER {
    if (persistentStateChanged) {
      persist();
    }
  };
  if (args->term() > m_currentTerm) {
    // A higher term invalidates the current role, vote and pending reads.
    abortReadIndexLocked(ReadIndexStatus::NotLeader);
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;  // 这里设置成-1有意义，如果突然宕机然后上线理论上是可以投票的
    persistentStateChanged = true;
  }
  myAssert(args->term() == m_currentTerm, "AppendEntries term differs from current term");
  // A valid AppendEntries establishes the leader for this term.
  if (m_status == Leader) abortReadIndexLocked(ReadIndexStatus::NotLeader);
  m_status = Follower;
  m_lastResetElectionTime = now();
  m_lastLeaderContactTime = m_lastResetElectionTime;
  if (args->readcontext() != 0) {
    reply->set_readcontextack(args->readcontext());
    reply->set_readcontextaccepted(true);
  }
  // Validate the prefix before applying entries from a possibly delayed RPC.
  if (args->prevlogindex() > getLastLogIndex()) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(getLastLogIndex() + 1);
    return;
  } else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(
        m_lastSnapshotIncludeIndex +
        1);  // Continue immediately after the compacted prefix.
    // The referenced entry has already been compacted. Do not fall through to
    // matchLog(), which requires logIndex >= lastSnapshotIncludeIndex and used
    // to abort the node during follower catch-up after a snapshot.
    return;
  }
  if (matchLog(args->prevlogindex(), args->prevlogterm())) {
    for (int i = 0; i < args->entries_size(); i++) {
      auto log = args->entries(i);
      if (log.logindex() > getLastLogIndex()) {
        m_logs.push_back(log);
        persistentStateChanged = true;
      } else {
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
            m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command()) {
          // Equal index and term must identify the same log entry.
          myAssert(false, format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
                                 " {%d:%d}却不同！！\n",
                                 m_me, log.logindex(), log.logterm(), m_me,
                                 m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), args->leaderid(),
                                 log.command()));
        }
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm()) {
          // Delete the conflicting entry and its complete suffix before
          // appending the leader's entry. Replacing a single slot leaves a
          // stale leader suffix attached to the accepted log.
          const int sliceIndex = getSlicesIndexFromLogIndex(log.logindex());
          m_logs.erase(m_logs.begin() + sliceIndex, m_logs.end());
          m_logs.push_back(log);
          persistentStateChanged = true;
        }
      }
    }

    myAssert(
        getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
        format("[AppendEntries raft{%d}] lastLogIndex{%d} < prevLogIndex{%d}+entriesSize{%d}",
               m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));
    if (args->leadercommit() > m_commitIndex) {
      const int newCommitIndex = std::min(args->leadercommit(), getLastLogIndex());
      if (newCommitIndex > m_commitIndex) {
        m_commitIndex = newCommitIndex;
        m_applyCond.notify_one();
      }
    }

    myAssert(getLastLogIndex() >= m_commitIndex,
             format("[AppendEntries raft{%d}] lastLogIndex{%d} < commitIndex{%d}", m_me,
                    getLastLogIndex(), m_commitIndex));
    reply->set_success(true);
    reply->set_term(m_currentTerm);

    return;
  } else {
    // Return the first index of the conflicting term to accelerate backtracking.
    reply->set_updatenextindex(args->prevlogindex());

    for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
      if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {
        reply->set_updatenextindex(index + 1);
        break;
      }
    }
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    return;
  }
}

void Raft::applierTicker() {
  while (true) {
    std::vector<ApplyMsg> applyMsgs;
    {
      std::unique_lock<std::mutex> lock(m_mtx);
      m_applyCond.wait(lock, [this]() { return m_lastApplied < m_commitIndex; });
      if (m_status == Leader) {
        DPrintf("[Raft::applierTicker() - raft{%d}]  m_lastApplied{%d}   m_commitIndex{%d}", m_me, m_lastApplied,
                m_commitIndex);
      }
      applyMsgs = getApplyLogs();
    }

    // The single applier thread preserves commit order without holding m_mtx
    // while the Region queue may block.
    if (!applyMsgs.empty()) {
      DPrintf("[func- Raft::applierTicker()-raft{%d}] 向kvserver報告的applyMsgs長度爲：{%d}", m_me, applyMsgs.size());
    }
    for (auto& message : applyMsgs) {
      applyChan->Push(message);
    }
  }
}

bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot) {
  std::lock_guard<std::mutex> lock(m_mtx);
  
  if (lastIncludedIndex <= m_commitIndex) {
    return false;
  }
  
  auto lastLogIndex = getLastLogIndex();
  if (lastIncludedIndex > lastLogIndex) {
    m_logs.clear();
  } else {
    m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(lastIncludedIndex) + 1);
  }
  
  m_commitIndex = lastIncludedIndex;
  m_lastApplied = lastIncludedIndex;
  m_lastSnapshotIncludeIndex = lastIncludedIndex;
  m_lastSnapshotIncludeTerm = lastIncludedTerm;
  
  m_persister->Save(persistData(), snapshot);
  return true;
}

void Raft::doElection() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status != Leader) {
    abortReadIndexLocked(ReadIndexStatus::NotLeader);
    DPrintf("[       ticker-func-rf(%d)              ]  选举定时器到期且不是leader，开始选举 \n", m_me);
    m_status = Candidate;
    m_currentTerm += 1;
    m_votedFor = m_me;
    persist();
    std::shared_ptr<int> votedNum = std::make_shared<int>(1);
    m_lastResetElectionTime = now();
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      int lastLogIndex = -1, lastLogTerm = -1;
      getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);  //获取最后一个log的term和下标

      std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
          std::make_shared<raftRpcProctoc::RequestVoteArgs>();
      requestVoteArgs->set_term(m_currentTerm);
      requestVoteArgs->set_candidateid(m_me);
      requestVoteArgs->set_lastlogindex(lastLogIndex);
      requestVoteArgs->set_lastlogterm(lastLogTerm);
      auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();

      std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs, requestVoteReply, votedNum);
      t.detach();
    }
  }
}

void Raft::doHeartBeat() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader) {
    DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了且拿到mutex，开始发送AE\n", m_me);
    if (!m_activeReadRound && !m_pendingReadWaiters.empty() && hasCommittedEntryInCurrentTermLocked()) {
      auto round = std::make_shared<ReadRound>();
      round->context = ++m_nextReadContext;
      if (round->context == 0) round->context = ++m_nextReadContext;
      round->term = m_currentTerm;
      round->readIndex = m_commitIndex;
      round->acknowledgedPeers.insert(m_me);
      round->waiters.swap(m_pendingReadWaiters);
      m_activeReadRound = std::move(round);
      ++m_readIndexRounds;
      completeReadRoundIfQuorumLocked();
    }

    // Enqueue one replication task per follower; dedicated workers serialize
    // each peer's AppendEntries and snapshot traffic.
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了 index:{%d}\n", m_me, i);
      myAssert(m_nextIndex[i] >= 1, format("nextIndex[%d] = {%d}", i, m_nextIndex[i]));
      if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
        ReplicationTask task;
        task.type = ReplicationTaskType::Snapshot;
        m_replicationQueues[i]->Push(task);
        continue;
      }
      int preLogIndex = -1;
      int PrevLogTerm = -1;
      getPrevLogInfo(i, &preLogIndex, &PrevLogTerm);
      std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs =
          std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
      appendEntriesArgs->set_term(m_currentTerm);
      appendEntriesArgs->set_leaderid(m_me);
      appendEntriesArgs->set_prevlogindex(preLogIndex);
      appendEntriesArgs->set_prevlogterm(PrevLogTerm);
      appendEntriesArgs->clear_entries();
      appendEntriesArgs->set_leadercommit(m_commitIndex);
      if (m_activeReadRound && m_activeReadRound->term == m_currentTerm) {
        appendEntriesArgs->set_readcontext(m_activeReadRound->context);
      }
      if (preLogIndex != m_lastSnapshotIncludeIndex) {
        for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j) {
          raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = m_logs[j];
        }
      } else {
        for (const auto& item : m_logs) {
          raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = item;
        }
      }
      int lastLogIndex = getLastLogIndex();
      myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
               format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                      appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));
      const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply =
          std::make_shared<raftRpcProctoc::AppendEntriesReply>();
      appendEntriesReply->set_appstate(Disconnected);

      ReplicationTask task;
      task.type = ReplicationTaskType::AppendEntries;
      task.appendEntriesArgs = appendEntriesArgs;
      task.appendEntriesReply = appendEntriesReply;
      m_replicationQueues[i]->Push(task);
    }
    m_lastResetHearBeatTime = now();  // leader发送心跳，就不是随机时间了
  }
}

void Raft::electionTimeOutTicker() {
  // Check if a Leader election should be started.
  while (true) {
    /**
     * 如果不睡眠，那么对于leader，这个函数会一直空转，浪费cpu。且加入协程之后，空转会导致其他协程无法运行，对于时间敏感的AE，会导致心跳无法正常发送导致异常
     */
    bool isLeader = false;
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      isLeader = m_status == Leader;
    }
    if (isLeader) {
      std::this_thread::sleep_for(std::chrono::milliseconds(HeartBeatTimeout));
      continue;
    }
    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    std::chrono::steady_clock::time_point wakeTime{};
    {
      m_mtx.lock();
      wakeTime = now();
      suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
      m_mtx.unlock();
    }

    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
      auto start = std::chrono::steady_clock::now();

      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());

      if (Debug) {
        auto end = std::chrono::steady_clock::now();
        std::chrono::duration<double, std::milli> duration = end - start;
        std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                  << std::endl;
        std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                  << std::endl;
      }
    }

    {
      std::lock_guard<std::mutex> lock(m_mtx);
      if (m_status == Leader || m_lastResetElectionTime > wakeTime) {
        continue;
      }
    }
    doElection();
  }
}

std::vector<ApplyMsg> Raft::getApplyLogs() {
  std::vector<ApplyMsg> applyMsgs;
  myAssert(m_commitIndex <= getLastLogIndex(), format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}",
                                                      m_me, m_commitIndex, getLastLogIndex()));

  while (m_lastApplied < m_commitIndex) {
    m_lastApplied++;
    myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
             format("applied log index{%d} != lastApplied{%d}",
                    m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
    ApplyMsg applyMsg;
    applyMsg.CommandValid = true;
    applyMsg.SnapshotValid = false;
    applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
    applyMsg.CommandIndex = m_lastApplied;
    applyMsgs.emplace_back(applyMsg);
  }
  return applyMsgs;
}

int Raft::getNewCommandIndex() {
  auto lastLogIndex = getLastLogIndex();
  return lastLogIndex + 1;
}

// Resolve the prefix advertised to one follower in its next AppendEntries.
void Raft::getPrevLogInfo(int server, int* preIndex, int* preTerm) {
  if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1) {
    *preIndex = m_lastSnapshotIncludeIndex;
    *preTerm = m_lastSnapshotIncludeTerm;
    return;
  }
  auto nextIndex = m_nextIndex[server];
  *preIndex = nextIndex - 1;
  *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}

// Return a consistent term and leadership snapshot.
void Raft::GetState(int* term, bool* isLeader) {
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };

  *term = m_currentTerm;
  *isLeader = (m_status == Leader);
}

bool Raft::hasCommittedEntryInCurrentTermLocked() {
  if (m_commitIndex < m_lastSnapshotIncludeIndex || m_commitIndex > getLastLogIndex()) {
    return false;
  }
  return getLogTermFromLogIndex(m_commitIndex) == m_currentTerm;
}

void Raft::completeReadRoundIfQuorumLocked() {
  if (!m_activeReadRound || m_activeReadRound->term != m_currentTerm || m_status != Leader ||
      m_activeReadRound->acknowledgedPeers.size() < m_peers.size() / 2 + 1) {
    return;
  }

  const auto currentTime = std::chrono::steady_clock::now();
  for (const auto& waiter : m_activeReadRound->waiters) {
    waiter->result.status = waiter->deadline <= currentTime ? ReadIndexStatus::Timeout : ReadIndexStatus::Ok;
    waiter->result.term = m_activeReadRound->term;
    waiter->result.readIndex = m_activeReadRound->readIndex;
    waiter->result.context = m_activeReadRound->context;
    waiter->completed = true;
    if (waiter->result.status == ReadIndexStatus::Ok) ++m_readIndexCompleted;
  }
  m_activeReadRound.reset();
  m_readCond.notify_all();
}

void Raft::abortReadIndexLocked(ReadIndexStatus status) {
  for (const auto& waiter : m_pendingReadWaiters) {
    waiter->result = ReadIndexResult{status, m_currentTerm, -1, 0};
    waiter->completed = true;
  }
  m_pendingReadWaiters.clear();
  if (m_activeReadRound) {
    for (const auto& waiter : m_activeReadRound->waiters) {
      waiter->result = ReadIndexResult{status, m_currentTerm, -1, m_activeReadRound->context};
      waiter->completed = true;
    }
    m_activeReadRound.reset();
  }
  m_readCond.notify_all();
}

void Raft::removeReadWaiterLocked(const std::shared_ptr<ReadWaiter>& waiter) {
  const auto remove = [&waiter](auto* waiters) {
    waiters->erase(std::remove(waiters->begin(), waiters->end(), waiter), waiters->end());
  };
  remove(&m_pendingReadWaiters);
  if (m_activeReadRound) {
    remove(&m_activeReadRound->waiters);
    if (m_activeReadRound->waiters.empty()) {
      m_activeReadRound.reset();
    }
  }
}

Raft::ReadIndexResult Raft::ReadIndex(std::chrono::steady_clock::time_point deadline) {
  auto waiter = std::make_shared<ReadWaiter>();
  waiter->deadline = deadline;
  {
    std::lock_guard<std::mutex> lock(m_mtx);
    ++m_readIndexRequests;
    if (m_status != Leader) {
      return ReadIndexResult{ReadIndexStatus::NotLeader, m_currentTerm, -1, 0};
    }
    m_pendingReadWaiters.push_back(waiter);
  }

  // Start a round immediately when possible; otherwise the periodic heartbeat
  // starts it after the new leader's current-term no-op is committed.
  doHeartBeat();

  std::unique_lock<std::mutex> lock(m_mtx);
  while (!waiter->completed) {
    if (m_status != Leader) {
      removeReadWaiterLocked(waiter);
      return ReadIndexResult{ReadIndexStatus::NotLeader, m_currentTerm, -1, 0};
    }
    if (m_readCond.wait_until(lock, deadline) == std::cv_status::timeout && !waiter->completed) {
      removeReadWaiterLocked(waiter);
      return ReadIndexResult{ReadIndexStatus::Timeout, m_currentTerm, -1, 0};
    }
  }
  return waiter->result;
}

bool Raft::IsLeaderInTerm(int term) {
  std::lock_guard<std::mutex> lock(m_mtx);
  return m_status == Leader && m_currentTerm == term;
}

Raft::NodeStatus Raft::GetStatus() {
  std::lock_guard<std::mutex> lock(m_mtx);
  return NodeStatus{m_currentTerm, m_status == Leader, m_commitIndex, m_lastApplied,
                    getLastLogIndex(), m_readIndexRequests, m_readIndexRounds,
                    m_readIndexCompleted,
                    m_appendEntriesSent.load(std::memory_order_relaxed),
                    m_persistCount.load(std::memory_order_relaxed)};
}

std::vector<raftRpcProctoc::LogEntry> Raft::GetLogEntries(size_t maxEntries) {
  std::lock_guard<std::mutex> lock(m_mtx);
  const size_t first = m_logs.size() > maxEntries ? m_logs.size() - maxEntries : 0;
  return std::vector<raftRpcProctoc::LogEntry>(m_logs.begin() + first, m_logs.end());
}

void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest* args,
                           raftRpcProctoc::InstallSnapshotResponse* reply) {
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);
    return;
  }
  if (args->term() > m_currentTerm) {
    abortReadIndexLocked(ReadIndexStatus::NotLeader);
    m_currentTerm = args->term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
  }
  if (m_status == Leader) abortReadIndexLocked(ReadIndexStatus::NotLeader);
  m_status = Follower;
  m_lastResetElectionTime = now();
  m_lastLeaderContactTime = m_lastResetElectionTime;
  // Ignore snapshots already covered by local state.
  if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex) {
    return;
  }
  // Compact entries covered by the snapshot and advance delivery progress.
  auto lastLogIndex = getLastLogIndex();

  if (lastLogIndex > args->lastsnapshotincludeindex()) {
    m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);
  } else {
    m_logs.clear();
  }
  m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
  m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());
  m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
  m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();

  reply->set_term(m_currentTerm);
  ApplyMsg msg;
  msg.SnapshotValid = true;
  msg.Snapshot = args->data();
  msg.SnapshotTerm = args->lastsnapshotincludeterm();
  msg.SnapshotIndex = args->lastsnapshotincludeindex();

  // Preserve state-machine delivery order. A detached push could enqueue a
  // later log entry before this snapshot on a busy process.
  applyChan->Push(msg);
  m_persister->Save(persistData(), args->data());
}

void Raft::pushMsgToRegionPeer(ApplyMsg msg) { applyChan->Push(msg); }

void Raft::leaderHearBeatTicker() {
  while (true) {
    bool isLeader = false;
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      isLeader = m_status == Leader;
    }
    if (!isLeader) {
      std::this_thread::sleep_for(std::chrono::milliseconds(HeartBeatTimeout));
      continue;
    }
    static std::atomic<int32_t> atomicCount = 0;//心跳次数

    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};//合适的睡眠时间
    std::chrono::steady_clock::time_point wakeTime{};//唤醒时间点
    //计算睡眠时间
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      wakeTime = now();
      suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHearBeatTime - wakeTime;//
    }

    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {//
      auto start = std::chrono::steady_clock::now();

      std::this_thread::sleep_for(suitableSleepTime);

      if (Debug) {
        auto end = std::chrono::steady_clock::now();
        std::chrono::duration<double, std::milli> duration = end - start;
        std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数设置睡眠时间为: "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                  << std::endl;
        std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数实际睡眠时间为: " << duration.count()
                  << " 毫秒\033[0m" << std::endl;
        ++atomicCount;
      }
    }

    {
      std::lock_guard<std::mutex> lock(m_mtx);
      if (m_status != Leader || m_lastResetHearBeatTime > wakeTime) {
        continue;
      }
    }
    // DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了\n", m_me);
    doHeartBeat();
  }
}

void Raft::leaderSendSnapShot(int server) {
  m_mtx.lock();
  raftRpcProctoc::InstallSnapshotRequest args;
  args.set_leaderid(m_me);
  args.set_term(m_currentTerm);
  args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
  args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
  args.set_data(m_persister->ReadSnapshot());

  raftRpcProctoc::InstallSnapshotResponse reply;
  m_mtx.unlock();
  bool ok = m_peers[server]->InstallSnapshot(&args, &reply);
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };
  if (!ok) {
    return;
  }
  if (m_status != Leader || m_currentTerm != args.term()) {
    return;  //中间释放过锁，可能状态已经改变了
  }
  //	无论什么时候都要判断term
  if (reply.term() > m_currentTerm) {
    //三变
    abortReadIndexLocked(ReadIndexStatus::NotLeader);
    m_currentTerm = reply.term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
    m_lastResetElectionTime = now();
    return;
  }
  m_matchIndex[server] = args.lastsnapshotincludeindex();
  m_nextIndex[server] = m_matchIndex[server] + 1;
}

void Raft::replicationWorker(int server) {
  while (true) {
    ReplicationTask task = m_replicationQueues[server]->Pop();
    if (task.type == ReplicationTaskType::Snapshot) {
      leaderSendSnapShot(server);
      continue;
    }
    sendAppendEntries(server, task.appendEntriesArgs, task.appendEntriesReply);
  }
}

void Raft::leaderUpdateCommitIndex() {
  for (int index = getLastLogIndex(); index > m_commitIndex; index--) {
    int sum = 0;
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        sum += 1;
        continue;
      }
      if (m_matchIndex[i] >= index) {
        sum += 1;
      }
    }

    // Raft only advances commitIndex by counting replicas for a current-term entry.
    if (sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm) {
      m_commitIndex = index;
      break;
    }
  }
}

// The caller must provide an index present in the snapshot boundary or log.
bool Raft::matchLog(int logIndex, int logTerm) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
           format("logIndex{%d} outside snapshotIndex{%d}..lastLogIndex{%d}",
                  logIndex, m_lastSnapshotIncludeIndex, getLastLogIndex()));
  return logTerm == getLogTermFromLogIndex(logIndex);
}

void Raft::persist() {
  if (!m_persister) {
    return;
  }
  auto data = persistData();
  m_persister->SaveRaftState(data);
  m_persistCount.fetch_add(1, std::memory_order_relaxed);
}

void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs* args, raftRpcProctoc::RequestVoteReply* reply) {
  std::lock_guard<std::mutex> lg(m_mtx);

  // Persist term and vote changes before releasing the state mutex.
  DEFER { persist(); };
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Expire);
    reply->set_votegranted(false);
    return;
  }
  if (args->term() > m_currentTerm &&
      m_lastLeaderContactTime != std::chrono::steady_clock::time_point::min() &&
      now() - m_lastLeaderContactTime <
          std::chrono::milliseconds(minRandomizedElectionTime)) {
    // A follower that just acknowledged the current leader must not
    // immediately help a disconnected peer form a competing higher-term
    // majority. Every two Raft majorities intersect, so retaining the current
    // term until the minimum election timeout makes a shorter bounded leader
    // lease safe. TSO deliberately uses only half of this interval (150 ms).
    reply->set_term(m_currentTerm);
    reply->set_votestate(Expire);
    reply->set_votegranted(false);
    return;
  }
  if (args->term() > m_currentTerm) {
    abortReadIndexLocked(ReadIndexStatus::NotLeader);
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;
  }
  myAssert(args->term() == m_currentTerm,
           format("[RequestVote raft{%d}] request term differs from current term", m_me));
  // A vote requires a candidate log at least as up to date as the local log.
  if (!UpToDate(args->lastlogindex(), args->lastlogterm())) {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);

    return;
  }
  // Repeated RequestVote RPCs from the same candidate are idempotent.
  if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);

    return;
  } else {
    m_votedFor = args->candidateid();
    m_lastResetElectionTime = now();
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(true);

    return;
  }
}

bool Raft::UpToDate(int index, int term) {
  int lastIndex = -1;
  int lastTerm = -1;
  getLastLogIndexAndTerm(&lastIndex, &lastTerm);
  return term > lastTerm || (term == lastTerm && index >= lastIndex);
}

void Raft::getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm) {
  if (m_logs.empty()) {
    *lastLogIndex = m_lastSnapshotIncludeIndex;
    *lastLogTerm = m_lastSnapshotIncludeTerm;
    return;
  } else {
    *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
    *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
    return;
  }
}
/**
 *
 * @return 最新的log的logindex，即log的逻辑index。区别于log在m_logs中的物理index
 * 可见：getLastLogIndexAndTerm()
 */
int Raft::getLastLogIndex() {
  int lastLogIndex = -1;
  int _ = -1;
  getLastLogIndexAndTerm(&lastLogIndex, &_);
  return lastLogIndex;
}

int Raft::getLastLogTerm() {
  int _ = -1;
  int lastLogTerm = -1;
  getLastLogIndexAndTerm(&_, &lastLogTerm);
  return lastLogTerm;
}

/**
 *
 * @param logIndex log的逻辑index。注意区别于m_logs的物理index
 * @return
 */
int Raft::getLogTermFromLogIndex(int logIndex) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex,
           format("[getLogTerm raft{%d}] index{%d} < snapshotIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));

  int lastLogIndex = getLastLogIndex();

  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));

  if (logIndex == m_lastSnapshotIncludeIndex) {
    return m_lastSnapshotIncludeTerm;
  } else {
    return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
  }
}

int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }

// Convert a logical log index after the snapshot boundary to a vector offset.
int Raft::getSlicesIndexFromLogIndex(int logIndex) {
  myAssert(logIndex > m_lastSnapshotIncludeIndex,
           format("[getLogOffset raft{%d}] index{%d} <= snapshotIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));
  int lastLogIndex = getLastLogIndex();
  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));
  int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
  return SliceIndex;
}

bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                           std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum) {
  auto start = now();
    DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 開始", m_me, server);
  bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
    DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 完畢，耗時:{%d} ms", m_me, server,
      now() - start);

  if (!ok) {
    // A transport failure contributes no vote; a later election retries it.
    return false;
  }
  std::lock_guard<std::mutex> lg(m_mtx);
  if (reply->term() > m_currentTerm) {
    abortReadIndexLocked(ReadIndexStatus::NotLeader);
    m_status = Follower;  //三变：身份，term，和投票
    m_currentTerm = reply->term();
    m_votedFor = -1;
    persist();
    return true;
  } else if (reply->term() < m_currentTerm) {
    return true;
  }
  myAssert(reply->term() == m_currentTerm, "RequestVote reply term differs from current term");

  if (!reply->votegranted()) {
    return true;
  }

  *votedNum = *votedNum + 1;
  if (*votedNum >= m_peers.size() / 2 + 1) {
    *votedNum = 0;
    if (m_status == Leader) {
      myAssert(false,
               format("[func-sendRequestVote-rf{%d}]  term:{%d} 同一个term当两次领导，error", m_me, m_currentTerm));
    }
    m_status = Leader;

    DPrintf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", m_me, m_currentTerm,
            getLastLogIndex());

    const int lastLogIndex = getLastLogIndex();
    for (int i = 0; i < m_nextIndex.size(); i++) {
      m_nextIndex[i] = lastLogIndex + 1;
      m_matchIndex[i] = 0;
    }

    // A newly elected leader must commit an entry from its own term before
    // Raft can safely advance commitIndex over entries inherited from older
    // terms. Without this no-op, recovery waited for the first business write;
    // the gateway could read an incomplete MVCC timestamp watermark meanwhile.
    Op noOp;
    noOp.Operation = "RaftNoop";
    noOp.ClientId = "raft-internal";
    noOp.RequestId = m_currentTerm;
    raftRpcProctoc::LogEntry noOpEntry;
    noOpEntry.set_command(noOp.asString());
    noOpEntry.set_logterm(m_currentTerm);
    noOpEntry.set_logindex(lastLogIndex + 1);
    m_logs.emplace_back(std::move(noOpEntry));

    std::thread t(&Raft::doHeartBeat, this);
    t.detach();

    persist();
  }
  return true;
}

bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply) {
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc開始 ， args->entries_size():{%d}", m_me,
          server, args->entries_size());
  m_appendEntriesSent.fetch_add(1, std::memory_order_relaxed);
  bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());

  if (!ok) {
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc失敗", m_me, server);
    return ok;
  }
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功", m_me, server);
  if (reply->appstate() == Disconnected) {
    return ok;
  }
  std::lock_guard<std::mutex> lg1(m_mtx);

  if (reply->term() > m_currentTerm) {
    abortReadIndexLocked(ReadIndexStatus::NotLeader);
    m_status = Follower;
    m_currentTerm = reply->term();
    m_votedFor = -1;
    return ok;
  } else if (reply->term() < m_currentTerm) {
    DPrintf("[func -sendAppendEntries  rf{%d}]  节点：{%d}的term{%d}<rf{%d}的term{%d}\n", m_me, server, reply->term(),
            m_me, m_currentTerm);
    return ok;
  }

  if (m_status != Leader) {
    return ok;
  }

  myAssert(reply->term() == m_currentTerm,
           format("replyTerm{%d} != currentTerm{%d}", reply->term(), m_currentTerm));
  if (m_activeReadRound && reply->readcontextaccepted() &&
      reply->readcontextack() == m_activeReadRound->context &&
      args->term() == m_activeReadRound->term) {
    m_activeReadRound->acknowledgedPeers.insert(server);
    completeReadRoundIfQuorumLocked();
  }
  if (!reply->success()) {
    if (reply->updatenextindex() != -100) {
      DPrintf("[func -sendAppendEntries  rf{%d}]  返回的日志term相等，但是不匹配，回缩nextIndex[%d]：{%d}\n", m_me,
              server, reply->updatenextindex());
      m_nextIndex[server] = reply->updatenextindex();  //失败是不更新mathIndex的
    }
  } else {
    // A reply only advances this follower's replicated position. Commit is
    // derived from all matchIndex values, so delayed or duplicate replies
    // cannot be counted more than once and cannot commit a different batch.
    m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
    m_nextIndex[server] = m_matchIndex[server] + 1;
    const int lastLogIndex = getLastLogIndex();

    myAssert(m_nextIndex[server] <= lastLogIndex + 1,
             format("nextIndex[%d] exceeds lastLogIndex+1; logCount=%zu lastLogIndex=%d", server,
                    m_logs.size(), lastLogIndex));
    const int oldCommitIndex = m_commitIndex;
    leaderUpdateCommitIndex();
    if (m_commitIndex > oldCommitIndex) {
      m_applyCond.notify_one();
      // A current-term no-op may have made pending ReadIndex requests
      // eligible for the next heartbeat round.
      m_readCond.notify_all();
    }
    myAssert(m_commitIndex <= lastLogIndex,
             format("[sendAppendEntries raft{%d}] lastLogIndex:%d commitIndex:%d\n", m_me, lastLogIndex,
                    m_commitIndex));
  }
  return ok;
}

void Raft::AppendEntries(google::protobuf::RpcController* controller,
                         const ::raftRpcProctoc::AppendEntriesArgs* request,
                         ::raftRpcProctoc::AppendEntriesReply* response, ::google::protobuf::Closure* done) {
  if (!m_initialized.load(std::memory_order_acquire)) {
    response->set_term(0);
    response->set_success(false);
    response->set_updatenextindex(1);
    response->set_appstate(Disconnected);
    done->Run();
    return;
  }
  AppendEntries1(request, response);
  done->Run();
}

void Raft::InstallSnapshot(google::protobuf::RpcController* controller,
                           const ::raftRpcProctoc::InstallSnapshotRequest* request,
                           ::raftRpcProctoc::InstallSnapshotResponse* response, ::google::protobuf::Closure* done) {
  if (!m_initialized.load(std::memory_order_acquire)) {
    response->set_term(0);
    done->Run();
    return;
  }
  InstallSnapshot(request, response);

  done->Run();
}

void Raft::RequestVote(google::protobuf::RpcController* controller, const ::raftRpcProctoc::RequestVoteArgs* request,
                       ::raftRpcProctoc::RequestVoteReply* response, ::google::protobuf::Closure* done) {
  if (!m_initialized.load(std::memory_order_acquire)) {
    response->set_term(0);
    response->set_votegranted(false);
    response->set_votestate(Expire);
    done->Run();
    return;
  }
  RequestVote(request, response);
  done->Run();
}

void Raft::Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader) {
  std::lock_guard<std::mutex> lock(m_mtx);
  if (m_status != Leader) {
    DPrintf("[func-Start-rf{%d}]  is not leader", m_me);
    *newLogIndex = -1;
    *newLogTerm = -1;
    *isLeader = false;
    return;
  }

  // Queue the proposal while the leadership decision is still protected.
  // The queue has its own lock; holding m_mtx here closes the data race with
  // election/step-down state changes.
  m_proposeQueue.Push(command);

  *newLogIndex = -1; // Index is unknown asynchronously
  *newLogTerm = m_currentTerm;
  *isLeader = true;
}

void Raft::raftPollerTicker() {
  while (true) {
    std::vector<Op> batch = m_proposeQueue.PopBatch(100);
    if (batch.empty()) {
      sleepNMilliseconds(2);
      continue;
    }

    {
      std::lock_guard<std::mutex> lg1(m_mtx);
      if (m_status != Leader) {
        // Start() accepted these commands while leadership was protected, but
        // the asynchronous batch had not entered the log before step-down.
        // Explicit rejection lets the transaction scheduler release latches.
        for (const auto& command : batch) {
          ApplyMsg rejected;
          rejected.ProposalRejected = true;
          rejected.Command = command.asString();
          applyChan->Push(std::move(rejected));
        }
        continue;
      }

      int lastLogIndex = getLastLogIndex();
      for (const auto& command : batch) {
        raftRpcProctoc::LogEntry newLogEntry;
        newLogEntry.set_command(command.asString());
        newLogEntry.set_logterm(m_currentTerm);
        newLogEntry.set_logindex(++lastLogIndex);
        m_logs.emplace_back(newLogEntry);
      }

      persist(); // Perform a single disk write for the entire batch
    }

    doHeartBeat(); // Enqueue replication tasks immediately after the batch is persisted.
  }
}

// Initialize persistent state and start the long-running Raft workers.
void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
  m_peers = peers;
  m_persister = persister;
  m_me = me;
  m_mtx.lock();

  this->applyChan = applyCh;
  m_currentTerm = 0;
  m_status = Follower;
  m_commitIndex = 0;
  m_lastApplied = 0;
  m_logs.clear();
  for (int i = 0; i < m_peers.size(); i++) {
    m_matchIndex.push_back(0);
    m_nextIndex.push_back(0);
    m_replicationQueues.push_back(std::make_shared<LockQueue<ReplicationTask>>());
  }
  m_votedFor = -1;

  m_lastSnapshotIncludeIndex = 0;
  m_lastSnapshotIncludeTerm = 0;
  m_lastResetElectionTime = now();
  // A restarted voter has forgotten any volatile leader-lease promise it made
  // before the crash. Keep it from voting for one full minimum election
  // interval after startup; this is conservative and closes the restart hole
  // for the TSO's shorter 150 ms fence.
  m_lastLeaderContactTime = m_lastResetElectionTime;
  m_lastResetHearBeatTime = m_lastResetElectionTime;

  // initialize from state persisted before a crash
  readPersist(m_persister->ReadRaftState());
  if (m_lastSnapshotIncludeIndex > 0) {
    m_lastApplied = m_lastSnapshotIncludeIndex;
    // commitIndex is volatile and will be reconstructed from the current leader.
  }

  DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
          m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

  // Publish the fully initialized state before inbound RPC handlers are
  // allowed to touch it. The release/acquire pair also makes applyChan and
  // restored persistent state visible to the RPC worker threads.
  m_initialized.store(true, std::memory_order_release);
  m_mtx.unlock();

  // Consensus liveness must not depend on all perpetual tickers sharing one
  // cooperative scheduler thread. Under sustained load a lost timer wake-up
  // left the scheduler idle and stopped consuming m_proposeQueue indefinitely.
  // Dedicated threads keep election, heartbeat and proposal progress
  // independent; the coroutine runtime remains available to application code.
  std::thread heartbeatThread(&Raft::leaderHearBeatTicker, this);
  heartbeatThread.detach();
  std::thread electionThread(&Raft::electionTimeOutTicker, this);
  electionThread.detach();
  std::thread proposalThread(&Raft::raftPollerTicker, this);
  proposalThread.detach();

  for (int i = 0; i < m_peers.size(); ++i) {
    if (i == m_me) {
      continue;
    }
    std::thread t(&Raft::replicationWorker, this, i);
    t.detach();
  }

  std::thread t3(&Raft::applierTicker, this);
  t3.detach();
}

std::string Raft::persistData() {
  size_t estimatedSize = kRaftPersistMagicSize + sizeof(int32_t) * 4 + sizeof(uint64_t);
  for (const auto& item : m_logs) {
    estimatedSize += sizeof(int32_t) * 2 + sizeof(uint64_t) + item.command().size();
  }

  std::string data;
  data.reserve(estimatedSize);
  data.append(kRaftPersistMagic, kRaftPersistMagicSize);
  AppendInt32(&data, m_currentTerm);
  AppendInt32(&data, m_votedFor);
  AppendInt32(&data, m_lastSnapshotIncludeIndex);
  AppendInt32(&data, m_lastSnapshotIncludeTerm);
  AppendUint64(&data, static_cast<uint64_t>(m_logs.size()));
  for (const auto& item : m_logs) {
    const std::string& command = item.command();
    AppendInt32(&data, item.logterm());
    AppendInt32(&data, item.logindex());
    AppendUint64(&data, static_cast<uint64_t>(command.size()));
    data.append(command);
  }
  return data;
}

void Raft::readPersist(std::string data) {
  if (data.empty()) {
    return;
  }
  if (HasBinaryPersistMagic(data)) {
    size_t offset = kRaftPersistMagicSize;
    uint64_t logCount = 0;
    if (!ReadInt32(data, &offset, &m_currentTerm) || !ReadInt32(data, &offset, &m_votedFor) ||
        !ReadInt32(data, &offset, &m_lastSnapshotIncludeIndex) ||
        !ReadInt32(data, &offset, &m_lastSnapshotIncludeTerm) || !ReadUint64(data, &offset, &logCount)) {
      DPrintf("[Raft::readPersist] invalid binary persist header, ignore persisted state");
      return;
    }

    std::vector<raftRpcProctoc::LogEntry> logs;
    logs.reserve(static_cast<size_t>(logCount));
    for (uint64_t i = 0; i < logCount; ++i) {
      int logTerm = 0;
      int logIndex = 0;
      uint64_t commandSize = 0;
      if (!ReadInt32(data, &offset, &logTerm) || !ReadInt32(data, &offset, &logIndex) ||
          !ReadUint64(data, &offset, &commandSize) || offset + commandSize > data.size()) {
        DPrintf("[Raft::readPersist] invalid binary persisted log, ignore persisted state");
        return;
      }
      raftRpcProctoc::LogEntry logEntry;
      logEntry.set_logterm(logTerm);
      logEntry.set_logindex(logIndex);
      logEntry.set_command(data.data() + offset, static_cast<size_t>(commandSize));
      offset += static_cast<size_t>(commandSize);
      logs.emplace_back(std::move(logEntry));
    }
    m_logs = std::move(logs);
    return;
  }

  std::stringstream iss(data);
  try {
    boost::archive::text_iarchive ia(iss);
    // Decode the legacy Boost archive format for backward compatibility.
    BoostPersistRaftNode boostPersistRaftNode;
    ia >> boostPersistRaftNode;

    m_currentTerm = boostPersistRaftNode.m_currentTerm;
    m_votedFor = boostPersistRaftNode.m_votedFor;
    m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
    m_logs.clear();
    m_logs.reserve(boostPersistRaftNode.m_logs.size());
    for (auto& item : boostPersistRaftNode.m_logs) {
      raftRpcProctoc::LogEntry logEntry;
      logEntry.ParseFromString(item);
      m_logs.emplace_back(std::move(logEntry));
    }
  } catch (const std::exception& e) {
    DPrintf("[Raft::readPersist] failed to read legacy boost persisted state: %s", e.what());
  }
}

void Raft::Snapshot(int index, std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) {
    DPrintf(
        "[func-Snapshot-rf{%d}] rejects replacing log with snapshotIndex %d as current snapshotIndex %d is larger or "
        "smaller ",
        m_me, index, m_lastSnapshotIncludeIndex);
    return;
  }
  auto lastLogIndex = getLastLogIndex();  //为了检查snapshot前后日志是否一样，防止多截取或者少截取日志

  // Retain only entries after the new snapshot boundary.
  int newLastSnapshotIncludeIndex = index;
  int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
  std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
  for (int i = index + 1; i <= getLastLogIndex(); i++) {
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
  }
  m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
  m_logs = trunckedLogs;
  m_commitIndex = std::max(m_commitIndex, index);
  m_lastApplied = std::max(m_lastApplied, index);

  m_persister->Save(persistData(), snapshot);

  DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
          m_lastSnapshotIncludeTerm, m_logs.size());
  myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
           format("logCount{%zu} + snapshotIndex{%d} != lastLogIndex{%d}", m_logs.size(),
                  m_lastSnapshotIncludeIndex, lastLogIndex));
}
