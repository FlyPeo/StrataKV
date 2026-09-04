/*
 * 测试目标：声明性能测试共享的数据契约、生成器、直方图和结果目录接口。
 * 测试策略：所有 workload 以不可变配置和全局序号驱动，统计在线程局部固定桶中完成。
 * 测试规模：覆盖 smoke 3,000 records/10,000 ops 与 full 100,000 records 的公共上限。
 * 验证内容：由 performance_support_check.cpp 验证 schema、分布、路由、分位数和原子发布。
 */
#ifndef STRATAKV_TEST_SUPPORT_PERFORMANCE_SUPPORT_H
#define STRATAKV_TEST_SUPPORT_PERFORMANCE_SUPPORT_H

#include <cstdint>
#include <cstddef>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

namespace stratakv::test::performance {

inline constexpr int kSchemaVersion = 1;
inline constexpr uint64_t kP999MinimumSamples = 100000;

enum class Workload { kA, kB, kC, kF };
enum class Distribution { kUniform, kZipfian };
enum class OperationKind { kRead, kUpdate, kReadModifyWrite };

const char* WorkloadName(Workload workload);
const char* DistributionName(Distribution distribution);
const char* OperationName(OperationKind operation);
Workload ParseWorkload(const std::string& value);
Distribution ParseDistribution(const std::string& value);

struct WorkloadSpec {
  std::string profile = "interview-smoke";
  std::string path = "gateway";
  Workload workload = Workload::kA;
  Distribution distribution = Distribution::kUniform;
  uint64_t seed = 20260904;
  uint64_t recordCount = 3000;
  uint64_t operationCount = 10000;
  size_t valueSize = 256;
  int workers = 1;
  int maxAttempts = 1;
  int retryDelayMs = 20;
  int timeoutMs = 180000;

  void Validate() const;
};

struct DatasetManifest {
  int schemaVersion = kSchemaVersion;
  std::string state = "incomplete";
  std::string runId;
  std::string profile = "interview-smoke";
  uint64_t seed = 20260904;
  uint64_t recordCount = 3000;
  size_t valueSize = 256;
  std::string keyCodec = "region-range-v1";
  std::vector<int> regionIds;

  void Validate() const;
  std::string ToJson() const;
};

struct Operation {
  uint64_t sequence = 0;
  uint64_t recordId = 0;
  OperationKind kind = OperationKind::kRead;
};

class OperationGenerator {
 public:
  explicit OperationGenerator(WorkloadSpec spec);
  Operation At(uint64_t sequence) const;

 private:
  WorkloadSpec spec_;
};

struct RegionRange {
  int regionId = -1;
  std::string startKey;
  std::string endKey;
};

class RegionKeyCodec {
 public:
  RegionKeyCodec(std::vector<RegionRange> ranges, std::string runId);
  std::string Key(uint64_t recordId) const;
  int RegionId(uint64_t recordId) const;
  std::string KeyForRegion(size_t regionIndex, uint64_t ordinal) const;
  int RegionIdForIndex(size_t regionIndex) const;
  int LocateKeyRegion(const std::string& key) const;
  size_t RegionCount() const { return ranges_.size(); }
  const std::vector<RegionRange>& Ranges() const { return ranges_; }
  const std::string& RunId() const { return runId_; }

 private:
  std::string PrefixFor(const RegionRange& range) const;

  std::vector<RegionRange> ranges_;
  std::string runId_;
};

class Histogram {
 public:
  Histogram();
  void Record(uint64_t microseconds);
  void Merge(const Histogram& other);
  uint64_t Count() const { return count_; }
  uint64_t Max() const { return max_; }
  uint64_t Percentile(double percentile) const;
  bool HasReliableP999() const { return count_ >= kP999MinimumSamples; }

 private:
  static size_t BucketFor(uint64_t microseconds);
  static uint64_t BucketUpperBound(size_t bucket);

  std::vector<uint64_t> buckets_;
  uint64_t count_ = 0;
  uint64_t max_ = 0;
};

struct RunSummary {
  std::string caseId;
  std::string path;
  std::string workload;
  std::string distribution;
  int workers = 0;
  uint64_t attempted = 0;
  uint64_t successful = 0;
  uint64_t reads = 0;
  uint64_t updates = 0;
  uint64_t readModifyWrites = 0;
  uint64_t conflicts = 0;
  uint64_t timeouts = 0;
  uint64_t unavailable = 0;
  uint64_t cleanupPending = 0;
  uint64_t resultUnknown = 0;
  uint64_t retries = 0;
  double elapsedSeconds = 0.0;
  Histogram latency;

  double AttemptedPerSecond() const;
  double SuccessfulPerSecond() const;
  std::string ToJson(const std::string& subject = "record") const;
  std::string ToKeyValues(const std::string& subject = "record") const;
};

std::string JsonEscape(const std::string& value);
std::string StableValue(uint64_t sequence, size_t size);
void AtomicWrite(const std::filesystem::path& path, const std::string& contents);

class ResultRun {
 public:
  ResultRun(std::filesystem::path root, DatasetManifest manifest);
  const std::filesystem::path& Root() const { return root_; }
  void Begin();
  void Complete();

 private:
  std::filesystem::path root_;
  DatasetManifest manifest_;
};

struct A1Point {
  std::string caseId;
  std::string path = "gateway";
  Workload workload = Workload::kA;
  Distribution distribution = Distribution::kUniform;
  int workers = 1;

  WorkloadSpec ToSpec(const std::string& profile = "interview-full",
                      uint64_t recordCount = 100000,
                      uint64_t operationCount = 20000,
                      size_t valueSize = 1024,
                      uint64_t seed = 20260904,
                      int maxAttempts = 1,
                      int retryDelayMs = 20,
                      int timeoutMs = 180000) const;
};

std::vector<A1Point> StandardA1Matrix();
std::vector<A1Point> SmokeA1Matrix();

struct A2Transaction {
  uint64_t sequence = 0;
  bool isDistributed = false;
  int targetRegionIndex = -1;
  int targetRegionId = -1;
  struct Mutation {
    std::string key;
    std::string value;
    int regionIndex = -1;
    int regionId = -1;
  };
  std::vector<Mutation> mutations;
};

class A2TransactionGenerator {
 public:
  A2TransactionGenerator(int crossPercent, uint64_t seed,
                         const RegionKeyCodec& keyCodec,
                         size_t valueSize = 256);

  A2Transaction At(uint64_t sequence) const;

  int CrossPercent() const { return crossPercent_; }
  uint64_t Seed() const { return seed_; }
  size_t ValueSize() const { return valueSize_; }
  const RegionKeyCodec& KeyCodec() const { return *keyCodec_; }

 private:
  int crossPercent_ = 0;
  uint64_t seed_ = 20260904;
  const RegionKeyCodec* keyCodec_ = nullptr;
  size_t valueSize_ = 256;
};

struct A2Summary {
  std::string caseId;
  int targetCrossPercent = 0;
  int workers = 8;
  uint64_t totalAttempted = 0;
  uint64_t totalCommitted = 0;
  uint64_t totalFailed = 0;
  double elapsedSeconds = 0.0;

  uint64_t localAttempted = 0;
  uint64_t localCommitted = 0;
  uint64_t localFailed = 0;

  uint64_t distributedAttempted = 0;
  uint64_t distributedCommitted = 0;
  uint64_t distributedFailed = 0;

  Histogram totalLatency;
  Histogram localLatency;
  Histogram distributedLatency;

  std::map<int, uint64_t> localRegionTxnCount;
  std::map<int, uint64_t> distributedRegionMutationCount;

  double TotalAttemptedTps() const;
  double TotalCommittedTps() const;
  double LocalCommittedTps() const;
  double DistributedCommittedTps() const;
  double ActualCrossRatio() const;

  std::string ToKeyValues() const;
  std::string ToJson() const;
};

struct A3Transaction {
  uint64_t sequence = 0;
  int fanout = 1;
  struct Mutation {
    std::string key;
    std::string value;
    int regionIndex = -1;
    int regionId = -1;
  };
  std::vector<Mutation> mutations;
};

class A3TransactionGenerator {
 public:
  A3TransactionGenerator(int fanout, uint64_t seed,
                         const RegionKeyCodec& keyCodec,
                         size_t valueSize = 256);

  A3Transaction At(uint64_t sequence) const;

  int Fanout() const { return fanout_; }
  uint64_t Seed() const { return seed_; }
  size_t ValueSize() const { return valueSize_; }
  const RegionKeyCodec& KeyCodec() const { return *keyCodec_; }

 private:
  int fanout_ = 1;
  uint64_t seed_ = 20260904;
  const RegionKeyCodec* keyCodec_ = nullptr;
  size_t valueSize_ = 256;
};

struct A3Summary {
  std::string caseId;
  int fanout = 1;
  int workers = 8;
  uint64_t totalAttempted = 0;
  uint64_t totalCommitted = 0;
  uint64_t totalFailed = 0;
  double elapsedSeconds = 0.0;
  Histogram latency;
  std::map<int, uint64_t> regionMutationCounts;

  double AttemptedTps() const;
  double CommittedTps() const;

  std::string ToKeyValues() const;
  std::string ToJson() const;
};

struct A4PointResult {
  std::string mode;
  double targetShare = 0.0;
  double actualConflictRate = 0.0;
  double successRate = 0.0;
  double attemptsPerCommit = 0.0;
  double committedTps = 0.0;
  double attemptedTps = 0.0;
  uint64_t p50LatencyUs = 0;
  uint64_t p95LatencyUs = 0;
  uint64_t p99LatencyUs = 0;
  uint64_t maxLatencyUs = 0;
};

struct A4CrossoverAnalysis {
  bool hasCrossover = false;
  double crossoverShareLow = 0.0;
  double crossoverShareHigh = 0.0;
  std::string dominantMode;
  std::string summaryMessage;
};

A4CrossoverAnalysis EvaluateA4Crossover(const std::vector<A4PointResult>& optimistic,
                                        const std::vector<A4PointResult>& pessimistic);

}  // namespace stratakv::test::performance

#endif
