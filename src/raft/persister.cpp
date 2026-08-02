//
// Created by swx on 23-5-30.
//
#include "persister.h"
#include <filesystem>
#include <sstream>
#include "util.h"

// todo:会涉及反复打开文件的操作，没有考虑如果文件出现问题会怎么办？？
void Persister::Save(const std::string raftstate, const std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);
  clearRaftStateAndSnapshot();
  // 将raftstate和snapshot写入本地文件
  m_raftStateOutStream << raftstate;
  m_snapshotOutStream << snapshot;
  m_raftStateOutStream.flush();
  m_snapshotOutStream.flush();
  m_raftStateSize = raftstate.size();
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

void Persister::SaveRaftState(const std::string &data) {
  std::lock_guard<std::mutex> lg(m_mtx);
  // 将raftstate和snapshot写入本地文件
  clearRaftState();
  m_raftStateOutStream << data;
  m_raftStateOutStream.flush();
  m_raftStateSize = data.size();
}

long long Persister::RaftStateSize() {
  std::lock_guard<std::mutex> lg(m_mtx);

  return m_raftStateSize;
}

std::string Persister::ReadRaftState() {
  std::lock_guard<std::mutex> lg(m_mtx);

  std::fstream ifs(m_raftStateFileName, std::ios_base::in);
  if (!ifs.good()) {
    return "";
  }
  std::ostringstream raftStateStream;
  raftStateStream << ifs.rdbuf();
  ifs.close();
  return raftStateStream.str();
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
  std::ifstream raftStateIn(m_raftStateFileName, std::ios::binary | std::ios::ate);
  if (raftStateIn.good()) m_raftStateSize = raftStateIn.tellg();
  raftStateIn.close();
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
  std::ifstream raftStateIn(m_raftStateFileName, std::ios::binary | std::ios::ate);
  if (raftStateIn.good()) {
    m_raftStateSize = raftStateIn.tellg();
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
