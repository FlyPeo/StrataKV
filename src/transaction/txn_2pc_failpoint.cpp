#include "txn_2pc_failpoint.h"

#ifdef STRATAKV_ENABLE_TEST_FAILPOINTS

#include <filesystem>
#include <iostream>

namespace stratakv::transaction::failpoint {

namespace {
std::mutex g_mutex;
std::condition_variable g_cv;
std::condition_variable g_hit_cv;

bool g_armed = false;
bool g_hit = false;
bool g_released = false;
FailpointLocation g_location = FailpointLocation::AfterAllPrewriteBeforePrimaryCommit;
BarrierAction g_action = BarrierAction::BlockUntilReleased;
std::string g_projectToken;
std::string g_markerPath;

void WriteAtomicMarker(const std::string& markerPath, FailpointLocation loc, const std::string& token) {
  try {
    const std::filesystem::path target(markerPath);
    if (target.has_parent_path()) {
      std::filesystem::create_directories(target.parent_path());
    }
    const std::filesystem::path tempPath = target.string() + ".tmp." +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    {
      std::ofstream out(tempPath);
      out << "event=failpoint_hit\n"
          << "location=" << LocationName(loc) << "\n"
          << "project_token=" << token << "\n"
          << "time_ns=" << std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::system_clock::now().time_since_epoch()).count() << "\n";
    }
    std::filesystem::rename(tempPath, target);
  } catch (const std::exception& e) {
    std::cerr << "Failpoint failed to write marker: " << e.what() << std::endl;
  }
}
}  // namespace

void Arm(const std::string& projectToken, FailpointLocation location, const std::string& markerPath,
         BarrierAction action) {
  std::unique_lock<std::mutex> lock(g_mutex);
  g_projectToken = projectToken;
  g_location = location;
  g_markerPath = markerPath;
  g_action = action;
  g_armed = true;
  g_hit = false;
  g_released = false;
}

void Disarm() {
  std::unique_lock<std::mutex> lock(g_mutex);
  g_armed = false;
  g_hit = false;
  g_released = true;
  g_action = BarrierAction::BlockUntilReleased;
  g_cv.notify_all();
  g_hit_cv.notify_all();
}

void MaybeTrigger(FailpointLocation location, const std::string& projectToken) {
  std::unique_lock<std::mutex> lock(g_mutex);
  if (!g_armed || g_hit || g_location != location) {
    return;
  }
  if (g_projectToken.empty() || g_projectToken != projectToken) {
    return;
  }

  g_hit = true;
  const std::string marker = g_markerPath;
  WriteAtomicMarker(marker, location, projectToken);
  g_hit_cv.notify_all();

  if (g_action == BarrierAction::SimulateCrash) {
    throw CoordinatorCrashedException();
  }

  g_cv.wait(lock, [] { return g_released; });
}

bool WaitForHit(int timeoutMs) {
  std::unique_lock<std::mutex> lock(g_mutex);
  if (g_hit) return true;
  return g_hit_cv.wait_for(lock, std::chrono::milliseconds(timeoutMs), [] { return g_hit; });
}

void Release() {
  std::unique_lock<std::mutex> lock(g_mutex);
  g_released = true;
  g_cv.notify_all();
}

bool IsArmed() {
  std::unique_lock<std::mutex> lock(g_mutex);
  return g_armed;
}

bool IsHit() {
  std::unique_lock<std::mutex> lock(g_mutex);
  return g_hit;
}

bool IsEnabled() {
  return true;
}

}  // namespace stratakv::transaction::failpoint

#endif  // STRATAKV_ENABLE_TEST_FAILPOINTS
