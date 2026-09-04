/*
 * 测试目标：为性能与可靠性测试提供确定性 workload、固定内存延迟统计和安全结果写入。
 * 测试策略：以 sequence+seed 生成操作和键，用对数桶聚合延迟，并以同目录 rename 发布文件。
 * 测试规模：支持 interview-smoke 的 3,000 条记录/10,000 次操作及 interview-full 的
 *           100,000 条记录；直方图固定覆盖 1 us～180 s。
 * 验证内容：操作分布与线程调度无关、键始终落在声明 Region、分位数有界、结果不会半写。
 */
#include "support/performance/performance_support.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace stratakv::test::performance {
namespace {

constexpr double kHistogramBase = 1.01;
constexpr uint64_t kHistogramMaximumMicros = 180000000;

uint64_t SplitMix64(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31U);
}

bool IsSafeRunId(const std::string& value) {
  if (value.empty()) return false;
  return std::all_of(value.begin(), value.end(), [](unsigned char c) {
    return std::isalnum(c) || c == '-' || c == '_';
  });
}

}  // namespace

const char* WorkloadName(Workload workload) {
  switch (workload) {
    case Workload::kA: return "A";
    case Workload::kB: return "B";
    case Workload::kC: return "C";
    case Workload::kF: return "F";
  }
  return "unknown";
}

const char* DistributionName(Distribution distribution) {
  return distribution == Distribution::kUniform ? "uniform" : "zipfian";
}

const char* OperationName(OperationKind operation) {
  switch (operation) {
    case OperationKind::kRead: return "read";
    case OperationKind::kUpdate: return "update";
    case OperationKind::kReadModifyWrite: return "read-modify-write";
  }
  return "unknown";
}

Workload ParseWorkload(const std::string& value) {
  if (value == "A" || value == "a") return Workload::kA;
  if (value == "B" || value == "b") return Workload::kB;
  if (value == "C" || value == "c") return Workload::kC;
  if (value == "F" || value == "f") return Workload::kF;
  if (value == "E" || value == "e" || value == "scan" || value == "Scan") {
    throw std::invalid_argument("workload E/Scan is unsupported: StrataKV has no range-scan API");
  }
  throw std::invalid_argument("workload must be A, B, C, or F");
}

Distribution ParseDistribution(const std::string& value) {
  if (value == "uniform") return Distribution::kUniform;
  if (value == "zipfian") return Distribution::kZipfian;
  throw std::invalid_argument("distribution must be uniform or zipfian");
}

void WorkloadSpec::Validate() const {
  if (path != "gateway" && path != "direct") throw std::invalid_argument("path must be gateway or direct");
  if (recordCount == 0 || operationCount == 0 || valueSize == 0 || workers <= 0 || maxAttempts <= 0 ||
      retryDelayMs < 0 || timeoutMs <= 0) {
    throw std::invalid_argument("workload counts, value size, workers, attempts and timeout must be positive");
  }
}

void DatasetManifest::Validate() const {
  if (schemaVersion != kSchemaVersion || !IsSafeRunId(runId) || recordCount == 0 || valueSize == 0 ||
      regionIds.empty() || (state != "incomplete" && state != "complete")) {
    throw std::invalid_argument("invalid dataset manifest");
  }
}

std::string DatasetManifest::ToJson() const {
  Validate();
  std::ostringstream output;
  output << "{\n"
         << "  \"schema_version\":" << schemaVersion << ",\n"
         << "  \"state\":\"" << JsonEscape(state) << "\",\n"
         << "  \"run_id\":\"" << JsonEscape(runId) << "\",\n"
         << "  \"profile\":\"" << JsonEscape(profile) << "\",\n"
         << "  \"environment_class\":\"development-baseline\",\n"
         << "  \"capacity_claim\":\"invalid-for-production-capacity\",\n"
         << "  \"seed\":" << seed << ",\n"
         << "  \"record_count\":" << recordCount << ",\n"
         << "  \"value_size_bytes\":" << valueSize << ",\n"
         << "  \"key_codec\":\"" << JsonEscape(keyCodec) << "\",\n"
         << "  \"region_ids\":[";
  for (size_t index = 0; index < regionIds.size(); ++index) {
    if (index != 0) output << ',';
    output << regionIds[index];
  }
  output << "]\n}\n";
  return output.str();
}

OperationGenerator::OperationGenerator(WorkloadSpec spec) : spec_(std::move(spec)) { spec_.Validate(); }

Operation OperationGenerator::At(uint64_t sequence) const {
  // 21 is coprime with 100, so every contiguous block of 100 operations has
  // exactly the requested mix while the seed rotates its order.
  const uint64_t slot = ((sequence % 100U) * 21U + spec_.seed % 100U) % 100U;
  OperationKind kind = OperationKind::kRead;
  if (spec_.workload == Workload::kA) kind = slot < 50 ? OperationKind::kRead : OperationKind::kUpdate;
  else if (spec_.workload == Workload::kB) kind = slot < 95 ? OperationKind::kRead : OperationKind::kUpdate;
  else if (spec_.workload == Workload::kF) {
    kind = slot < 50 ? OperationKind::kRead : OperationKind::kReadModifyWrite;
  }

  const uint64_t random = SplitMix64(sequence ^ spec_.seed);
  uint64_t recordId = 0;
  if (spec_.distribution == Distribution::kUniform) {
    recordId = random % spec_.recordCount;
  } else {
    constexpr double theta = 0.99;
    const double unit = static_cast<double>(random >> 11U) * (1.0 / 9007199254740992.0);
    const double exponent = 1.0 - theta;
    const double upper = std::pow(static_cast<double>(spec_.recordCount), exponent);
    const double rank = std::pow(1.0 + unit * (upper - 1.0), 1.0 / exponent) - 1.0;
    recordId = std::min<uint64_t>(spec_.recordCount - 1, static_cast<uint64_t>(rank));
  }
  return {sequence, recordId, kind};
}

RegionKeyCodec::RegionKeyCodec(std::vector<RegionRange> ranges, std::string runId)
    : ranges_(std::move(ranges)), runId_(std::move(runId)) {
  if (ranges_.empty() || !IsSafeRunId(runId_)) throw std::invalid_argument("key codec needs ranges and a safe run id");
  for (size_t index = 0; index < ranges_.size(); ++index) {
    if (ranges_[index].regionId < 0 ||
        (!ranges_[index].endKey.empty() && ranges_[index].startKey >= ranges_[index].endKey) ||
        (index != 0 && ranges_[index - 1].endKey != ranges_[index].startKey)) {
      throw std::invalid_argument("Region ranges must be ordered, contiguous half-open intervals");
    }
  }
}

std::string RegionKeyCodec::PrefixFor(const RegionRange& range) const {
  std::string candidate = range.startKey.empty() ? "0" : range.startKey + ":";
  if ((!range.startKey.empty() && candidate < range.startKey) ||
      (!range.endKey.empty() && candidate >= range.endKey)) {
    throw std::invalid_argument("cannot derive a stable printable key inside Region range");
  }
  return candidate;
}

std::string RegionKeyCodec::Key(uint64_t recordId) const {
  const size_t regionIndex = static_cast<size_t>(recordId % ranges_.size());
  const uint64_t ordinal = recordId / ranges_.size();
  return PrefixFor(ranges_[regionIndex]) + "stratakv-perf:" + runId_ + ':' + std::to_string(ordinal);
}

int RegionKeyCodec::RegionId(uint64_t recordId) const {
  return ranges_[static_cast<size_t>(recordId % ranges_.size())].regionId;
}

std::string RegionKeyCodec::KeyForRegion(size_t regionIndex, uint64_t ordinal) const {
  if (regionIndex >= ranges_.size()) throw std::out_of_range("regionIndex out of range");
  const uint64_t recordId = ordinal * ranges_.size() + regionIndex;
  return Key(recordId);
}

int RegionKeyCodec::RegionIdForIndex(size_t regionIndex) const {
  if (regionIndex >= ranges_.size()) throw std::out_of_range("regionIndex out of range");
  return ranges_[regionIndex].regionId;
}

int RegionKeyCodec::LocateKeyRegion(const std::string& key) const {
  for (const auto& range : ranges_) {
    const bool geStart = range.startKey.empty() || key >= range.startKey;
    const bool ltEnd = range.endKey.empty() || key < range.endKey;
    if (geStart && ltEnd) return range.regionId;
  }
  return -1;
}

Histogram::Histogram()
    : buckets_(static_cast<size_t>(std::ceil(std::log(static_cast<double>(kHistogramMaximumMicros)) /
                                             std::log(kHistogramBase))) + 2U,
               0) {}

size_t Histogram::BucketFor(uint64_t microseconds) {
  if (microseconds <= 1) return 0;
  return static_cast<size_t>(std::ceil(std::log(static_cast<double>(microseconds)) /
                                       std::log(kHistogramBase)));
}

uint64_t Histogram::BucketUpperBound(size_t bucket) {
  if (bucket == 0) return 1;
  const double value = std::pow(kHistogramBase, static_cast<double>(bucket));
  return static_cast<uint64_t>(std::ceil(value));
}

void Histogram::Record(uint64_t microseconds) {
  const uint64_t bounded = std::min(microseconds, kHistogramMaximumMicros);
  const size_t bucket = std::min(BucketFor(bounded), buckets_.size() - 1);
  ++buckets_[bucket];
  ++count_;
  max_ = std::max(max_, microseconds);
}

void Histogram::Merge(const Histogram& other) {
  if (buckets_.size() != other.buckets_.size()) throw std::invalid_argument("incompatible histograms");
  for (size_t index = 0; index < buckets_.size(); ++index) buckets_[index] += other.buckets_[index];
  count_ += other.count_;
  max_ = std::max(max_, other.max_);
}

uint64_t Histogram::Percentile(double percentile) const {
  if (count_ == 0) return 0;
  if (!(percentile >= 0.0 && percentile <= 1.0)) throw std::invalid_argument("percentile must be in [0,1]");
  const uint64_t target = std::max<uint64_t>(1, static_cast<uint64_t>(std::ceil(percentile * count_)));
  uint64_t cumulative = 0;
  for (size_t index = 0; index < buckets_.size(); ++index) {
    cumulative += buckets_[index];
    if (cumulative >= target) return percentile == 1.0 ? max_ : std::min(max_, BucketUpperBound(index));
  }
  return max_;
}

double RunSummary::AttemptedPerSecond() const { return elapsedSeconds <= 0.0 ? 0.0 : attempted / elapsedSeconds; }
double RunSummary::SuccessfulPerSecond() const { return elapsedSeconds <= 0.0 ? 0.0 : successful / elapsedSeconds; }

std::string RunSummary::ToJson(const std::string& subject) const {
  std::ostringstream output;
  output << std::fixed << std::setprecision(3)
         << "{\n"
         << "  \"case_id\":\"" << JsonEscape(caseId) << "\",\n"
         << "  \"subject\":\"" << JsonEscape(subject) << "\",\n"
         << "  \"path\":\"" << JsonEscape(path) << "\",\n"
         << "  \"workload\":\"" << JsonEscape(workload) << "\",\n"
         << "  \"distribution\":\"" << JsonEscape(distribution) << "\",\n"
         << "  \"workers\":" << workers << ",\n"
         << "  \"attempted\":" << attempted << ",\n"
         << "  \"successful\":" << successful << ",\n"
         << "  \"attempted_per_second\":" << AttemptedPerSecond() << ",\n"
         << "  \"successful_per_second\":" << SuccessfulPerSecond() << ",\n"
         << "  \"reads\":" << reads << ",\n"
         << "  \"updates\":" << updates << ",\n"
         << "  \"read_modify_writes\":" << readModifyWrites << ",\n"
         << "  \"conflicts\":" << conflicts << ",\n"
         << "  \"timeouts\":" << timeouts << ",\n"
         << "  \"unavailable\":" << unavailable << ",\n"
         << "  \"cleanup_pending\":" << cleanupPending << ",\n"
         << "  \"result_unknown\":" << resultUnknown << ",\n"
         << "  \"retries\":" << retries << ",\n"
         << "  \"elapsed_seconds\":" << elapsedSeconds << ",\n"
         << "  \"latency_us\":{\"samples\":" << latency.Count()
         << ",\"p50\":" << latency.Percentile(0.50)
         << ",\"p95\":" << latency.Percentile(0.95)
         << ",\"p99\":" << latency.Percentile(0.99)
         << ",\"max\":" << latency.Max()
         << ",\"p999_valid\":" << (latency.HasReliableP999() ? "true" : "false") << "}\n}\n";
  return output.str();
}

std::string RunSummary::ToKeyValues(const std::string& subject) const {
  std::ostringstream output;
  output << std::fixed << std::setprecision(3)
         << "case_id=" << caseId << '\n'
         << "subject=" << subject << '\n'
         << "path=" << path << '\n'
         << "workload=" << workload << '\n'
         << "distribution=" << distribution << '\n'
         << "workers=" << workers << '\n'
         << "attempted=" << attempted << '\n'
         << "successful=" << successful << '\n'
         << "attempted_per_second=" << AttemptedPerSecond() << '\n'
         << "successful_per_second=" << SuccessfulPerSecond() << '\n'
         << "reads=" << reads << '\n'
         << "updates=" << updates << '\n'
         << "read_modify_writes=" << readModifyWrites << '\n'
         << "conflicts=" << conflicts << '\n'
         << "timeouts=" << timeouts << '\n'
         << "unavailable=" << unavailable << '\n'
         << "cleanup_pending=" << cleanupPending << '\n'
         << "result_unknown=" << resultUnknown << '\n'
         << "retries=" << retries << '\n'
         << "elapsed_seconds=" << elapsedSeconds << '\n'
         << "latency_p50_us=" << latency.Percentile(0.50) << '\n'
         << "latency_p95_us=" << latency.Percentile(0.95) << '\n'
         << "latency_p99_us=" << latency.Percentile(0.99) << '\n'
         << "latency_max_us=" << latency.Max() << '\n'
         << "latency_p999_valid=" << (latency.HasReliableP999() ? "true" : "false") << '\n';
  return output.str();
}

std::string JsonEscape(const std::string& value) {
  std::string output;
  output.reserve(value.size());
  for (const unsigned char character : value) {
    switch (character) {
      case '"': output += "\\\""; break;
      case '\\': output += "\\\\"; break;
      case '\n': output += "\\n"; break;
      case '\r': output += "\\r"; break;
      case '\t': output += "\\t"; break;
      default: output += character < 0x20 ? '?' : static_cast<char>(character); break;
    }
  }
  return output;
}

std::string StableValue(uint64_t sequence, size_t size) {
  const std::string prefix = "value-" + std::to_string(sequence) + '-';
  std::string value = prefix;
  value.reserve(size);
  while (value.size() < size) value.push_back(static_cast<char>('a' + (value.size() + sequence) % 26U));
  if (value.size() > size) value.resize(size);
  return value;
}

void AtomicWrite(const std::filesystem::path& path, const std::string& contents) {
  std::filesystem::create_directories(path.parent_path());
  const std::filesystem::path temporary = path.string() + ".tmp";
  {
    std::ofstream stream(temporary, std::ios::binary | std::ios::trunc);
    if (!stream) throw std::runtime_error("cannot open temporary result file " + temporary.string());
    stream.write(contents.data(), static_cast<std::streamsize>(contents.size()));
    stream.flush();
    if (!stream) throw std::runtime_error("cannot write temporary result file " + temporary.string());
  }
  std::error_code error;
  std::filesystem::rename(temporary, path, error);
  if (error) {
    std::filesystem::remove(temporary);
    throw std::runtime_error("cannot publish result file " + path.string() + ": " + error.message());
  }
}

ResultRun::ResultRun(std::filesystem::path root, DatasetManifest manifest)
    : root_(std::move(root)), manifest_(std::move(manifest)) {}

void ResultRun::Begin() {
  if (std::filesystem::exists(root_ / "manifest.json")) {
    throw std::runtime_error("result run already exists: " + root_.string());
  }
  for (const char* directory : {"raw", "metrics", "faults", "histories"}) {
    std::filesystem::create_directories(root_ / directory);
  }
  manifest_.state = "incomplete";
  AtomicWrite(root_ / "manifest.json", manifest_.ToJson());
}

void ResultRun::Complete() {
  if (!std::filesystem::exists(root_ / "manifest.json")) throw std::runtime_error("run was not begun");
  manifest_.state = "complete";
  AtomicWrite(root_ / "manifest.json", manifest_.ToJson());
}

WorkloadSpec A1Point::ToSpec(const std::string& profile, uint64_t recordCount, uint64_t operationCount,
                             size_t valueSize, uint64_t seed, int maxAttempts, int retryDelayMs,
                             int timeoutMs) const {
  WorkloadSpec spec;
  spec.profile = profile;
  spec.path = path;
  spec.workload = workload;
  spec.distribution = distribution;
  spec.seed = seed;
  spec.recordCount = recordCount;
  spec.operationCount = operationCount;
  spec.valueSize = valueSize;
  spec.workers = workers;
  spec.maxAttempts = maxAttempts;
  spec.retryDelayMs = retryDelayMs;
  spec.timeoutMs = timeoutMs;
  spec.Validate();
  return spec;
}

std::vector<A1Point> StandardA1Matrix() {
  std::vector<A1Point> matrix;
  matrix.reserve(24);

  // 1. Gateway Uniform: A, B, C, F x workers {1, 4, 8, 16, 32} = 20 points
  const std::vector<Workload> uniformWorkloads = {Workload::kA, Workload::kB, Workload::kC, Workload::kF};
  const std::vector<int> workerCounts = {1, 4, 8, 16, 32};
  for (const auto wl : uniformWorkloads) {
    for (const int w : workerCounts) {
      A1Point point;
      point.caseId = "a1-" + std::string(WorkloadName(wl)) + "-w" + std::to_string(w);
      point.path = "gateway";
      point.workload = wl;
      point.distribution = Distribution::kUniform;
      point.workers = w;
      matrix.push_back(std::move(point));
    }
  }

  // 2. Gateway Zipfian: A, B x workers {8} = 2 points
  const std::vector<Workload> zipfianWorkloads = {Workload::kA, Workload::kB};
  for (const auto wl : zipfianWorkloads) {
    A1Point point;
    point.caseId = "a1-zipfian-" + std::string(WorkloadName(wl)) + "-w8";
    point.path = "gateway";
    point.workload = wl;
    point.distribution = Distribution::kZipfian;
    point.workers = 8;
    matrix.push_back(std::move(point));
  }

  // 3. Direct Uniform: A, C x workers {8} = 2 points
  const std::vector<Workload> directWorkloads = {Workload::kA, Workload::kC};
  for (const auto wl : directWorkloads) {
    A1Point point;
    point.caseId = "a1-direct-" + std::string(WorkloadName(wl)) + "-w8";
    point.path = "direct";
    point.workload = wl;
    point.distribution = Distribution::kUniform;
    point.workers = 8;
    matrix.push_back(std::move(point));
  }

  return matrix;
}

std::vector<A1Point> SmokeA1Matrix() {
  std::vector<A1Point> matrix;
  matrix.reserve(4);
  for (const auto wl : {Workload::kA, Workload::kC}) {
    for (const int w : {1, 8}) {
      A1Point point;
      point.caseId = "a1-" + std::string(WorkloadName(wl)) + "-w" + std::to_string(w);
      point.path = "gateway";
      point.workload = wl;
      point.distribution = Distribution::kUniform;
      point.workers = w;
      matrix.push_back(std::move(point));
    }
  }
  return matrix;
}

A2TransactionGenerator::A2TransactionGenerator(int crossPercent, uint64_t seed,
                                               const RegionKeyCodec& keyCodec,
                                               size_t valueSize)
    : crossPercent_(crossPercent), seed_(seed), keyCodec_(&keyCodec), valueSize_(valueSize) {
  if (crossPercent_ != 0 && crossPercent_ != 15 && crossPercent_ != 100) {
    throw std::invalid_argument("A2 cross-region ratio must be 0, 15, or 100");
  }
  if (keyCodec_->Ranges().size() < 3) {
    throw std::invalid_argument("A2 requires at least 3 regions");
  }
}

A2Transaction A2TransactionGenerator::At(uint64_t sequence) const {
  const uint64_t slot = ((sequence % 100U) * 21U + seed_ % 100U) % 100U;
  const bool isDistributed = slot < static_cast<uint64_t>(crossPercent_);

  A2Transaction txn;
  txn.sequence = sequence;
  txn.isDistributed = isDistributed;
  txn.mutations.resize(3);
  const std::string value = StableValue(sequence, valueSize_);

  if (isDistributed) {
    txn.targetRegionIndex = -1;
    txn.targetRegionId = -1;
    for (size_t i = 0; i < 3; ++i) {
      txn.mutations[i].key = keyCodec_->KeyForRegion(i, sequence * 3U + i);
      txn.mutations[i].value = value;
      txn.mutations[i].regionIndex = static_cast<int>(i);
      txn.mutations[i].regionId = keyCodec_->RegionIdForIndex(i);
    }
  } else {
    const size_t localRegionIndex = static_cast<size_t>(sequence % 3U);
    txn.targetRegionIndex = static_cast<int>(localRegionIndex);
    txn.targetRegionId = keyCodec_->RegionIdForIndex(localRegionIndex);
    for (size_t i = 0; i < 3; ++i) {
      txn.mutations[i].key = keyCodec_->KeyForRegion(localRegionIndex, sequence * 3U + i);
      txn.mutations[i].value = value;
      txn.mutations[i].regionIndex = static_cast<int>(localRegionIndex);
      txn.mutations[i].regionId = txn.targetRegionId;
    }
  }
  return txn;
}

double A2Summary::TotalAttemptedTps() const {
  return elapsedSeconds > 0.0 ? static_cast<double>(totalAttempted) / elapsedSeconds : 0.0;
}

double A2Summary::TotalCommittedTps() const {
  return elapsedSeconds > 0.0 ? static_cast<double>(totalCommitted) / elapsedSeconds : 0.0;
}

double A2Summary::LocalCommittedTps() const {
  return elapsedSeconds > 0.0 ? static_cast<double>(localCommitted) / elapsedSeconds : 0.0;
}

double A2Summary::DistributedCommittedTps() const {
  return elapsedSeconds > 0.0 ? static_cast<double>(distributedCommitted) / elapsedSeconds : 0.0;
}

double A2Summary::ActualCrossRatio() const {
  return totalAttempted > 0 ? static_cast<double>(distributedAttempted) / static_cast<double>(totalAttempted) : 0.0;
}

std::string A2Summary::ToKeyValues() const {
  std::ostringstream output;
  output << std::fixed << std::setprecision(3)
         << "case_id=" << caseId << '\n'
         << "subject=transaction\n"
         << "path=direct\n"
         << "target_cross_percent=" << targetCrossPercent << '\n'
         << "actual_cross_ratio=" << ActualCrossRatio() << '\n'
         << "workers=" << workers << '\n'
         << "transactions_total=" << totalAttempted << '\n'
         << "transactions_attempted=" << totalAttempted << '\n'
         << "transactions_committed=" << totalCommitted << '\n'
         << "transactions_failed=" << totalFailed << '\n'
         << "throughput_attempted_txn_per_sec=" << TotalAttemptedTps() << '\n'
         << "throughput_committed_txn_per_sec=" << TotalCommittedTps() << '\n'
         << "latency_boundary=begin_to_final_commit_result\n"
         << "latency_p50_us=" << totalLatency.Percentile(0.50) << '\n'
         << "latency_p95_us=" << totalLatency.Percentile(0.95) << '\n'
         << "latency_p99_us=" << totalLatency.Percentile(0.99) << '\n'
         << "latency_max_us=" << totalLatency.Max() << '\n'
         << "local_attempted=" << localAttempted << '\n'
         << "local_committed=" << localCommitted << '\n'
         << "local_throughput_committed_txn_per_sec=" << LocalCommittedTps() << '\n'
         << "local_latency_p50_us=" << localLatency.Percentile(0.50) << '\n'
         << "local_latency_p99_us=" << localLatency.Percentile(0.99) << '\n'
         << "distributed_attempted=" << distributedAttempted << '\n'
         << "distributed_committed=" << distributedCommitted << '\n'
         << "distributed_throughput_committed_txn_per_sec=" << DistributedCommittedTps() << '\n'
         << "distributed_latency_p50_us=" << distributedLatency.Percentile(0.50) << '\n'
         << "distributed_latency_p99_us=" << distributedLatency.Percentile(0.99) << '\n';
  return output.str();
}

std::string A2Summary::ToJson() const {
  std::ostringstream output;
  output << std::fixed << std::setprecision(3)
         << "{\n"
         << "  \"case_id\":\"" << JsonEscape(caseId) << "\",\n"
         << "  \"subject\":\"transaction\",\n"
         << "  \"path\":\"direct\",\n"
         << "  \"target_cross_percent\":" << targetCrossPercent << ",\n"
         << "  \"actual_cross_ratio\":" << ActualCrossRatio() << ",\n"
         << "  \"workers\":" << workers << ",\n"
         << "  \"total_attempted\":" << totalAttempted << ",\n"
         << "  \"total_committed\":" << totalCommitted << ",\n"
         << "  \"total_failed\":" << totalFailed << ",\n"
         << "  \"throughput_attempted_txn_per_sec\":" << TotalAttemptedTps() << ",\n"
         << "  \"throughput_committed_txn_per_sec\":" << TotalCommittedTps() << ",\n"
         << "  \"elapsed_seconds\":" << elapsedSeconds << ",\n"
         << "  \"latency_us\":{\"p50\":" << totalLatency.Percentile(0.50)
         << ",\"p95\":" << totalLatency.Percentile(0.95)
         << ",\"p99\":" << totalLatency.Percentile(0.99)
         << ",\"max\":" << totalLatency.Max() << "},\n"
         << "  \"local\":{\"attempted\":" << localAttempted
         << ",\"committed\":" << localCommitted
         << ",\"throughput_committed_txn_per_sec\":" << LocalCommittedTps()
         << ",\"latency_us\":{\"p50\":" << localLatency.Percentile(0.50)
         << ",\"p99\":" << localLatency.Percentile(0.99) << "}},\n"
         << "  \"distributed\":{\"attempted\":" << distributedAttempted
         << ",\"committed\":" << distributedCommitted
         << ",\"throughput_committed_txn_per_sec\":" << DistributedCommittedTps()
         << ",\"latency_us\":{\"p50\":" << distributedLatency.Percentile(0.50)
         << ",\"p99\":" << distributedLatency.Percentile(0.99) << "}}\n"
         << "}\n";
  return output.str();
}

A3TransactionGenerator::A3TransactionGenerator(int fanout, uint64_t seed,
                                               const RegionKeyCodec& keyCodec,
                                               size_t valueSize)
    : fanout_(fanout), seed_(seed), keyCodec_(&keyCodec), valueSize_(valueSize) {
  if (fanout_ < 1 || fanout_ > 3) {
    throw std::invalid_argument("A3 fanout must be 1, 2, or 3");
  }
  if (keyCodec_->Ranges().size() < 3) {
    throw std::invalid_argument("A3 requires at least 3 regions");
  }
}

A3Transaction A3TransactionGenerator::At(uint64_t sequence) const {
  A3Transaction txn;
  txn.sequence = sequence;
  txn.fanout = fanout_;
  txn.mutations.resize(3);
  const std::string value = StableValue(sequence, valueSize_);

  if (fanout_ == 1) {
    const size_t regionIdx = static_cast<size_t>((sequence + seed_) % 3U);
    for (size_t i = 0; i < 3; ++i) {
      txn.mutations[i].key = keyCodec_->KeyForRegion(regionIdx, sequence * 3U + i);
      txn.mutations[i].value = value;
      txn.mutations[i].regionIndex = static_cast<int>(regionIdx);
      txn.mutations[i].regionId = keyCodec_->RegionIdForIndex(regionIdx);
    }
  } else if (fanout_ == 2) {
    const size_t r1 = static_cast<size_t>((sequence + seed_) % 3U);
    const size_t r2 = (r1 + 1U) % 3U;
    txn.mutations[0].key = keyCodec_->KeyForRegion(r1, sequence * 3U + 0);
    txn.mutations[0].value = value;
    txn.mutations[0].regionIndex = static_cast<int>(r1);
    txn.mutations[0].regionId = keyCodec_->RegionIdForIndex(r1);

    txn.mutations[1].key = keyCodec_->KeyForRegion(r1, sequence * 3U + 1);
    txn.mutations[1].value = value;
    txn.mutations[1].regionIndex = static_cast<int>(r1);
    txn.mutations[1].regionId = keyCodec_->RegionIdForIndex(r1);

    txn.mutations[2].key = keyCodec_->KeyForRegion(r2, sequence * 3U + 2);
    txn.mutations[2].value = value;
    txn.mutations[2].regionIndex = static_cast<int>(r2);
    txn.mutations[2].regionId = keyCodec_->RegionIdForIndex(r2);
  } else {
    for (size_t i = 0; i < 3; ++i) {
      txn.mutations[i].key = keyCodec_->KeyForRegion(i, sequence * 3U + i);
      txn.mutations[i].value = value;
      txn.mutations[i].regionIndex = static_cast<int>(i);
      txn.mutations[i].regionId = keyCodec_->RegionIdForIndex(i);
    }
  }

  return txn;
}

double A3Summary::AttemptedTps() const {
  return elapsedSeconds > 0.0 ? static_cast<double>(totalAttempted) / elapsedSeconds : 0.0;
}

double A3Summary::CommittedTps() const {
  return elapsedSeconds > 0.0 ? static_cast<double>(totalCommitted) / elapsedSeconds : 0.0;
}

std::string A3Summary::ToKeyValues() const {
  std::ostringstream output;
  output << std::fixed << std::setprecision(3)
         << "case_id=" << caseId << '\n'
         << "subject=transaction\n"
         << "path=direct\n"
         << "fanout_regions=" << fanout << '\n'
         << "region_count=" << fanout << '\n'
         << "workers=" << workers << '\n'
         << "keys_per_transaction=3\n"
         << "transactions_total=" << totalAttempted << '\n'
         << "transactions_attempted=" << totalAttempted << '\n'
         << "transactions_committed=" << totalCommitted << '\n'
         << "transactions_failed=" << totalFailed << '\n'
         << "throughput_attempted_txn_per_sec=" << AttemptedTps() << '\n'
         << "throughput_committed_txn_per_sec=" << CommittedTps() << '\n'
         << "latency_boundary=begin_to_final_commit_result\n"
         << "latency_p50_us=" << latency.Percentile(0.50) << '\n'
         << "latency_p95_us=" << latency.Percentile(0.95) << '\n'
         << "latency_p99_us=" << latency.Percentile(0.99) << '\n'
         << "latency_max_us=" << latency.Max() << '\n';
  return output.str();
}

std::string A3Summary::ToJson() const {
  std::ostringstream output;
  output << std::fixed << std::setprecision(3)
         << "{\n"
         << "  \"case_id\":\"" << JsonEscape(caseId) << "\",\n"
         << "  \"subject\":\"transaction\",\n"
         << "  \"path\":\"direct\",\n"
         << "  \"fanout_regions\":" << fanout << ",\n"
         << "  \"region_count\":" << fanout << ",\n"
         << "  \"workers\":" << workers << ",\n"
         << "  \"keys_per_transaction\":3,\n"
         << "  \"total_attempted\":" << totalAttempted << ",\n"
         << "  \"total_committed\":" << totalCommitted << ",\n"
         << "  \"total_failed\":" << totalFailed << ",\n"
         << "  \"throughput_attempted_txn_per_sec\":" << AttemptedTps() << ",\n"
         << "  \"throughput_committed_txn_per_sec\":" << CommittedTps() << ",\n"
         << "  \"elapsed_seconds\":" << elapsedSeconds << ",\n"
         << "  \"latency_us\":{\"p50\":" << latency.Percentile(0.50)
         << ",\"p95\":" << latency.Percentile(0.95)
         << ",\"p99\":" << latency.Percentile(0.99)
         << ",\"max\":" << latency.Max() << "}\n"
         << "}\n";
  return output.str();
}

A4CrossoverAnalysis EvaluateA4Crossover(const std::vector<A4PointResult>& optimistic,
                                        const std::vector<A4PointResult>& pessimistic) {
  A4CrossoverAnalysis analysis;
  if (optimistic.empty() || pessimistic.empty() || optimistic.size() != pessimistic.size()) {
    analysis.summaryMessage = "Incompatible or empty A4 datasets for crossover evaluation.";
    return analysis;
  }

  int optWins = 0;
  int pessWins = 0;
  std::vector<int> comparisons;
  for (size_t i = 0; i < optimistic.size(); ++i) {
    if (optimistic[i].committedTps > pessimistic[i].committedTps) {
      ++optWins;
      comparisons.push_back(1);
    } else if (pessimistic[i].committedTps > optimistic[i].committedTps) {
      ++pessWins;
      comparisons.push_back(-1);
    } else {
      comparisons.push_back(0);
    }
  }

  for (size_t i = 1; i < comparisons.size(); ++i) {
    if ((comparisons[i - 1] > 0 && comparisons[i] < 0) ||
        (comparisons[i - 1] < 0 && comparisons[i] > 0)) {
      analysis.hasCrossover = true;
      analysis.crossoverShareLow = optimistic[i - 1].targetShare;
      analysis.crossoverShareHigh = optimistic[i].targetShare;
      break;
    }
  }

  if (analysis.hasCrossover) {
    analysis.summaryMessage = "Crossover observed between target contention share " +
                              std::to_string(static_cast<int>(analysis.crossoverShareLow * 100)) + "% and " +
                              std::to_string(static_cast<int>(analysis.crossoverShareHigh * 100)) + "%.";
  } else if (optWins == static_cast<int>(optimistic.size())) {
    analysis.dominantMode = "optimistic";
    analysis.summaryMessage = "Optimistic mode dominates committed TPS across all evaluated shares (0%, 5%, 20%). No crossover observed; no fictitious switching point extrapolated.";
  } else if (pessWins == static_cast<int>(pessimistic.size())) {
    analysis.dominantMode = "pessimistic";
    analysis.summaryMessage = "Pessimistic mode dominates committed TPS across all evaluated shares. No crossover observed.";
  } else {
    analysis.summaryMessage = "No clean crossover observed across measured points.";
  }

  return analysis;
}

}  // namespace stratakv::test::performance
