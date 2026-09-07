#include "persister.h"
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <sstream>
#include "util.h"

namespace {

constexpr char kRaftPersistMagic[] = "TTRF2";
constexpr size_t kRaftPersistHeaderSize =
    sizeof(kRaftPersistMagic) - 1 + sizeof(int32_t) * 4 + sizeof(uint64_t);

bool HasBinaryHeader(const std::string& data) {
  return data.size() >= kRaftPersistHeaderSize &&
         std::memcmp(data.data(), kRaftPersistMagic,
                     sizeof(kRaftPersistMagic) - 1) == 0;
}

bool WriteFile(const std::string& path, const std::string& data) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output.is_open()) return false;
  output.write(data.data(), static_cast<std::streamsize>(data.size()));
  output.flush();
  return output.good();
}

}  // namespace

// Serialize state and snapshot updates so readers never observe interleaved writes.
void Persister::Save(const std::string raftstate, const std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);
  clearRaftStateAndSnapshot();
  // 将raftstate和snapshot写入本地文件
  m_raftStateOutStream << raftstate;
  m_snapshotOutStream << snapshot;
  m_raftStateOutStream.flush();
  m_snapshotOutStream.flush();
  m_raftStateSize = raftstate.size();
  m_raftState = raftstate;
}

bool Persister::StageSnapshot(const std::string& snapshot, std::string* stagedPath) {
  if (stagedPath == nullptr) return false;
  *stagedPath = m_snapshotFileName + ".pending";
  std::ofstream output(*stagedPath, std::ios::binary | std::ios::trunc);
  if (!output.is_open()) return false;
  output.write(snapshot.data(), static_cast<std::streamsize>(snapshot.size()));
  output.flush();
  if (output.good()) return true;
  output.close();
  std::error_code ec;
  std::filesystem::remove(*stagedPath, ec);
  stagedPath->clear();
  return false;
}

bool Persister::CommitStagedSnapshot(const std::string& raftstate,
                                     const std::string& stagedPath) {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (stagedPath.empty()) return false;

  const std::string stagedRaftState = m_raftStateFileName + ".pending";
  {
    std::ofstream output(stagedRaftState, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) return false;
    output.write(raftstate.data(), static_cast<std::streamsize>(raftstate.size()));
    output.flush();
    if (!output.good()) {
      output.close();
      std::error_code cleanupError;
      std::filesystem::remove(stagedRaftState, cleanupError);
      return false;
    }
  }

  if (m_snapshotOutStream.is_open()) m_snapshotOutStream.close();
  if (m_raftStateOutStream.is_open()) m_raftStateOutStream.close();
  std::error_code ec;
  std::filesystem::rename(stagedPath, m_snapshotFileName, ec);
  if (ec) {
    m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::app);
    m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::app);
    std::error_code cleanupError;
    std::filesystem::remove(stagedRaftState, cleanupError);
    return false;
  }
  std::filesystem::rename(stagedRaftState, m_raftStateFileName, ec);
  m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::app);
  m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::app);
  if (ec) return false;
  m_raftStateSize = raftstate.size();
  m_raftState = raftstate;
  return true;
}

void Persister::DiscardStagedSnapshot(const std::string& stagedPath) {
  if (stagedPath.empty()) return;
  std::error_code ec;
  std::filesystem::remove(stagedPath, ec);
}

std::string Persister::ReadSnapshot() {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }

  DEFER {
    m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::app);
  };
  std::fstream ifs(m_snapshotFileName, std::ios_base::in);
  if (!ifs.good()) {
    return "";
  }
  std::ostringstream snapshotStream;
  snapshotStream << ifs.rdbuf();
  ifs.close();
  return snapshotStream.str();
}

void Persister::SaveRaftState(std::string data) {
  std::lock_guard<std::mutex> lg(m_mtx);

  // The binary format has a fixed-size header followed by immutable encoded
  // log entries. In the common append-only case, write only the new tail and
  // then publish the updated header/log count. A crash before the header write
  // leaves an ignored tail; rewriting the complete growing log on every batch
  // caused quadratic I/O and held the Raft mutex long enough to miss heartbeats.
  const bool appendOnly = HasBinaryHeader(m_raftState) && HasBinaryHeader(data) &&
      data.size() >= m_raftState.size() &&
      std::memcmp(data.data() + kRaftPersistHeaderSize,
                  m_raftState.data() + kRaftPersistHeaderSize,
                  m_raftState.size() - kRaftPersistHeaderSize) == 0;
  if (appendOnly) {
    if (data.size() > m_raftState.size()) {
      m_raftStateOutStream.write(data.data() + m_raftState.size(),
          static_cast<std::streamsize>(data.size() - m_raftState.size()));
      m_raftStateOutStream.flush();
      if (!m_raftStateOutStream.good()) return;
    }
    std::fstream header(m_raftStateFileName,
                        std::ios::binary | std::ios::in | std::ios::out);
    if (!header.is_open()) return;
    header.write(data.data(), static_cast<std::streamsize>(kRaftPersistHeaderSize));
    header.flush();
    if (!header.good()) return;
  } else {
    const std::string stagedPath = m_raftStateFileName + ".pending";
    if (!WriteFile(stagedPath, data)) return;
    if (m_raftStateOutStream.is_open()) m_raftStateOutStream.close();
    std::error_code ec;
    std::filesystem::rename(stagedPath, m_raftStateFileName, ec);
    m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::app);
    if (ec) return;
  }
  m_raftStateSize = data.size();
  m_raftState = std::move(data);
}

long long Persister::RaftStateSize() {
  std::lock_guard<std::mutex> lg(m_mtx);

  return m_raftStateSize;
}

std::string Persister::ReadRaftState() {
  std::lock_guard<std::mutex> lg(m_mtx);
  return m_raftState;
}

Persister::Persister(const int me)
    : m_raftStateFileName("run_data/raftstatePersist" + std::to_string(me) + ".txt"),
      m_snapshotFileName("run_data/snapshotPersist" + std::to_string(me) + ".txt"),
      m_raftStateSize(0) {
  std::filesystem::create_directories("run_data");
  bool fileOpenFlag = true;
  std::fstream file(m_raftStateFileName, std::ios::in);
  if (file.is_open()) {
    file.close();
  } else {
    std::ofstream createFile(m_raftStateFileName, std::ios::out);
    if (createFile.is_open()) createFile.close(); else fileOpenFlag = false;
  }
  file = std::fstream(m_snapshotFileName, std::ios::in);
  if (file.is_open()) {
    file.close();
  } else {
    std::ofstream createFile(m_snapshotFileName, std::ios::out);
    if (createFile.is_open()) createFile.close(); else fileOpenFlag = false;
  }
  if (!fileOpenFlag) DPrintf("[func-Persister::Persister] file open error");
  std::ifstream raftStateIn(m_raftStateFileName, std::ios::binary);
  if (raftStateIn.good()) {
    std::ostringstream contents;
    contents << raftStateIn.rdbuf();
    m_raftState = contents.str();
    m_raftStateSize = m_raftState.size();
  }
  m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::app);
  m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::app);
}

Persister::Persister(const std::string& identity)
    : m_raftStateFileName("run_data/raftstatePersist_" + identity + ".txt"),
      m_snapshotFileName("run_data/snapshotPersist_" + identity + ".txt"),
      m_raftStateSize(0) {
  std::filesystem::create_directories("run_data");
  /**
   * 检查文件状态并在缺失时创建文件。
   * 注意：不能在构造时清空文件，否则节点重启时无法恢复已有持久化状态。
   */
  bool fileOpenFlag = true;
  std::fstream file(m_raftStateFileName, std::ios::in);
  if (file.is_open()) {
    file.close();
  } else {
    std::ofstream createFile(m_raftStateFileName, std::ios::out);
    if (createFile.is_open()) {
      createFile.close();
    } else {
      fileOpenFlag = false;
    }
  }
  file = std::fstream(m_snapshotFileName, std::ios::in);
  if (file.is_open()) {
    file.close();
  } else {
    std::ofstream createFile(m_snapshotFileName, std::ios::out);
    if (createFile.is_open()) {
      createFile.close();
    } else {
      fileOpenFlag = false;
    }
  }
  if (!fileOpenFlag) {
    DPrintf("[func-Persister::Persister] file open error");
  }
  std::ifstream raftStateIn(m_raftStateFileName, std::ios::binary);
  if (raftStateIn.good()) {
    std::ostringstream contents;
    contents << raftStateIn.rdbuf();
    m_raftState = contents.str();
    m_raftStateSize = m_raftState.size();
  }
  raftStateIn.close();
  /**
   * 绑定流
   */
  m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::app);
  m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::app);
}

Persister::~Persister() {
  if (m_raftStateOutStream.is_open()) {
    m_raftStateOutStream.close();
  }
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }
}

void Persister::clearRaftState() {
  m_raftStateSize = 0;
  m_raftState.clear();
  // 关闭文件流
  if (m_raftStateOutStream.is_open()) {
    m_raftStateOutStream.close();
  }
  // 重新打开文件流并清空文件内容
  m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::trunc);
}

void Persister::clearSnapshot() {
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }
  m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::trunc);
}

void Persister::clearRaftStateAndSnapshot() {
  clearRaftState();
  clearSnapshot();
}
