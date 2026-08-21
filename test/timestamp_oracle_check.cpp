#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <unistd.h>

#include "timestamp_oracle.h"

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

std::filesystem::path MakeTempDirectory() {
  char path[] = "/tmp/stratakv-hlc-test-XXXXXX";
  char* created = mkdtemp(path);
  if (created == nullptr) throw std::runtime_error("mkdtemp failed");
  return created;
}

void TestComposeAndExtract() {
  const uint64_t timestamp = HlcTimestamp::Compose(123456789, 17);
  Require(HlcTimestamp::PhysicalMs(timestamp) == 123456789, "physical component mismatch");
  Require(HlcTimestamp::Logical(timestamp) == 17, "logical component mismatch");
}

void TestSameMillisecondAndClockRollback() {
  const auto directory = MakeTempDirectory();
  const auto path = (directory / "tso.state").string();
  uint64_t now = 1000;
  {
    PersistentTimestampOracle oracle(path, 8, [&now] { return now; });
    const uint64_t first = oracle.Next();
    const uint64_t second = oracle.Next();
    Require(HlcTimestamp::PhysicalMs(first) == 1000, "first physical time mismatch");
    Require(HlcTimestamp::Logical(second) == HlcTimestamp::Logical(first) + 1,
            "logical counter did not advance");

    now = 900;
    const uint64_t afterRollback = oracle.Next();
    Require(afterRollback > second, "clock rollback regressed timestamp");
    Require(HlcTimestamp::PhysicalMs(afterRollback) == 1000,
            "clock rollback changed physical component");
  }
  std::filesystem::remove_all(directory);
}

void TestLogicalOverflowAdvancesPhysical() {
  const auto directory = MakeTempDirectory();
  const auto path = (directory / "tso.state").string();
  const uint64_t physical = 5000;
  const uint64_t last = HlcTimestamp::Compose(physical, HlcTimestamp::kLogicalMask);
  {
    std::ofstream output(path);
    output << "v2 " << last << "\n";
  }
  PersistentTimestampOracle oracle(path, 1, [physical] { return physical; });
  const uint64_t next = oracle.Next();
  Require(HlcTimestamp::PhysicalMs(next) == physical + 1,
          "logical overflow did not advance physical time");
  Require(HlcTimestamp::Logical(next) == 0, "logical overflow did not reset logical time");
  std::filesystem::remove_all(directory);
}

void TestLegacyMigrationAndRestart() {
  const auto directory = MakeTempDirectory();
  const auto path = (directory / "tso.state").string();
  {
    std::ofstream output(path);
    output << "42\n";
  }
  uint64_t now = 2000;
  uint64_t issued = 0;
  {
    PersistentTimestampOracle oracle(path, 4, [&now] { return now; });
    issued = oracle.Next();
    Require(issued > 42, "legacy migration regressed below old high water");
  }
  {
    std::ifstream input(path);
    std::string version;
    uint64_t limit = 0;
    Require(static_cast<bool>(input >> version >> limit), "migrated state is unreadable");
    Require(version == "v2", "legacy state was not versioned");
    Require(limit >= issued, "migrated durable limit is below issued timestamp");
  }
  {
    now = 1900;
    PersistentTimestampOracle oracle(path, 4, [&now] { return now; });
    Require(oracle.Next() > issued, "restart repeated a migrated timestamp");
  }
  std::filesystem::remove_all(directory);
}

void TestUnknownVersionRejected() {
  const auto directory = MakeTempDirectory();
  const auto path = (directory / "tso.state").string();
  {
    std::ofstream output(path);
    output << "v99 123\n";
  }
  bool rejected = false;
  try {
    PersistentTimestampOracle oracle(path, 1, [] { return uint64_t{1000}; });
  } catch (const std::runtime_error&) {
    rejected = true;
  }
  Require(rejected, "unknown TSO state version was accepted");
  std::filesystem::remove_all(directory);
}

}  // namespace

int main() {
  try {
    TestComposeAndExtract();
    TestSameMillisecondAndClockRollback();
    TestLogicalOverflowAdvancesPhysical();
    TestLegacyMigrationAndRestart();
    TestUnknownVersionRejected();
    std::cout << "timestamp oracle checks passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "timestamp oracle check failed: " << error.what() << '\n';
    return 1;
  }
}
