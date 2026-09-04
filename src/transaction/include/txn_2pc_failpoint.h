#ifndef STRATAKV_TRANSACTION_TXN_2PC_FAILPOINT_H
#define STRATAKV_TRANSACTION_TXN_2PC_FAILPOINT_H

#include <chrono>
#include <condition_variable>
#include <fstream>
#include <functional>
#include <mutex>
#include <string>

namespace stratakv::transaction::failpoint {

enum class FailpointLocation {
  AfterAllPrewriteBeforePrimaryCommit,
  AfterPrimaryCommitBeforeSecondaries,
};

enum class BarrierAction {
  BlockUntilReleased,
  SimulateCrash,
};

class CoordinatorCrashedException : public std::runtime_error {
 public:
  CoordinatorCrashedException() : std::runtime_error("Simulated coordinator crash at barrier") {}
};

inline const char* LocationName(FailpointLocation loc) {
  switch (loc) {
    case FailpointLocation::AfterAllPrewriteBeforePrimaryCommit:
      return "after_all_prewrite_before_primary_commit";
    case FailpointLocation::AfterPrimaryCommitBeforeSecondaries:
      return "after_primary_commit_before_secondaries";
  }
  return "unknown";
}

#ifdef STRATAKV_ENABLE_TEST_FAILPOINTS

// Arm a one-shot failpoint barrier for a specific project token and location.
// When hit, writes markerPath atomically and either blocks until released or simulates crash.
void Arm(const std::string& projectToken, FailpointLocation location, const std::string& markerPath,
         BarrierAction action = BarrierAction::BlockUntilReleased);

// Clear any armed failpoint
void Disarm();

// Called from 2PC Coordinator during Commit()
// Does NOT hold transaction or Raft locks!
void MaybeTrigger(FailpointLocation location, const std::string& projectToken);

// Test harness helper: wait until the barrier was hit
bool WaitForHit(int timeoutMs = 5000);

// Test harness helper: resume blocked thread without killing process
void Release();

// Status query
bool IsArmed();
bool IsHit();
bool IsEnabled();

#else

// In default production build, failpoints cannot be triggered and overhead is zero.
inline void Arm(const std::string&, FailpointLocation, const std::string&) {}
inline void Disarm() {}
inline void MaybeTrigger(FailpointLocation, const std::string&) {}
inline bool WaitForHit(int = 0) { return false; }
inline void Release() {}
inline bool IsArmed() { return false; }
inline bool IsHit() { return false; }
inline bool IsEnabled() { return false; }

#endif  // STRATAKV_ENABLE_TEST_FAILPOINTS

}  // namespace stratakv::transaction::failpoint

#endif  // STRATAKV_TRANSACTION_TXN_2PC_FAILPOINT_H
