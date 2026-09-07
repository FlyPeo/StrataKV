#ifndef STRATAKV_RAFT_PERSISTER_H
#define STRATAKV_RAFT_PERSISTER_H
#include <fstream>
#include <mutex>
#include <string>
class Persister {
 private:
  std::mutex m_mtx;
  std::string m_raftState;
  std::string m_snapshot;
  /**
   * m_raftStateFileName: raftState文件名
   */
  const std::string m_raftStateFileName;
  /**
   * m_snapshotFileName: snapshot文件名
   */
  const std::string m_snapshotFileName;
  /**
   * 保存raftState的输出流
   */
  std::ofstream m_raftStateOutStream;
  /**
   * 保存snapshot的输出流
   */
  std::ofstream m_snapshotOutStream;
  /**
   * 保存raftStateSize的大小
   * 避免每次都读取文件来获取具体的大小
   */
  long long m_raftStateSize;

 public:
  void Save(std::string raftstate, std::string snapshot);
  // Write the large snapshot payload without holding Raft's state mutex. The
  // staged file is published together with the compacted Raft state later.
  bool StageSnapshot(const std::string& snapshot, std::string* stagedPath);
  bool CommitStagedSnapshot(const std::string& raftstate, const std::string& stagedPath);
  void DiscardStagedSnapshot(const std::string& stagedPath);
  std::string ReadSnapshot();
  void SaveRaftState(std::string data);
  long long RaftStateSize();
  std::string ReadRaftState();
  explicit Persister(int me);
  // Region peers on one physical node must not share Raft state or snapshots.
  explicit Persister(const std::string& identity);
  ~Persister();

 private:
  void clearRaftState();
  void clearSnapshot();
  void clearRaftStateAndSnapshot();
};

#endif  // STRATAKV_RAFT_PERSISTER_H
