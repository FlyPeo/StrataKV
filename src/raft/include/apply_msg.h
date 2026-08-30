#ifndef STRATAKV_RAFT_APPLY_MSG_H
#define STRATAKV_RAFT_APPLY_MSG_H
#include <string>
class ApplyMsg {
 public:
  bool CommandValid;
  std::string Command;
  int CommandIndex;
  bool ProposalRejected;
  bool SnapshotValid;
  std::string Snapshot;
  int SnapshotTerm;
  int SnapshotIndex;

 public:
  //两个valid最开始要赋予false！！
  ApplyMsg()
      : CommandValid(false),
        Command(),
        CommandIndex(-1),
        ProposalRejected(false),
        SnapshotValid(false),
        SnapshotTerm(-1),
        SnapshotIndex(-1){

        };
};

#endif  // STRATAKV_RAFT_APPLY_MSG_H
