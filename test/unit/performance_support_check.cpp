/*
 * 测试目标：验证 interview 测试体系的确定性生成、Region 路由、延迟统计和结果发布契约。
 * 测试策略：使用固定 seed 的 golden 计数、自定义 Region 边界、合成延迟及临时结果目录。
 * 测试规模：每个 YCSB workload 10,000 operations，Zipfian 20,000 samples，直方图 1,000 samples。
 * 验证内容：A/B/C/F 比例精确、线程无关、Key 不越界、P50/P99/Max 正确且结果状态原子切换。
 */
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

#include "support/performance/performance_support.h"

namespace perf = stratakv::test::performance;

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

void CheckMix(perf::Workload workload, uint64_t reads, uint64_t updates, uint64_t rmw) {
  perf::WorkloadSpec spec;
  spec.workload = workload;
  spec.operationCount = 10000;
  perf::OperationGenerator generator(spec);
  uint64_t actualReads = 0;
  uint64_t actualUpdates = 0;
  uint64_t actualRmw = 0;
  for (uint64_t sequence = 0; sequence < spec.operationCount; ++sequence) {
    const perf::Operation first = generator.At(sequence);
    const perf::Operation second = generator.At(sequence);
    Require(first.recordId == second.recordId && first.kind == second.kind, "generator is not deterministic");
    actualReads += first.kind == perf::OperationKind::kRead;
    actualUpdates += first.kind == perf::OperationKind::kUpdate;
    actualRmw += first.kind == perf::OperationKind::kReadModifyWrite;
  }
  Require(actualReads == reads && actualUpdates == updates && actualRmw == rmw,
          std::string("wrong operation mix for workload ") + perf::WorkloadName(workload));
}

void CheckGenerator() {
  CheckMix(perf::Workload::kA, 5000, 5000, 0);
  CheckMix(perf::Workload::kB, 9500, 500, 0);
  CheckMix(perf::Workload::kC, 10000, 0, 0);
  CheckMix(perf::Workload::kF, 5000, 0, 5000);

  bool rejectedScan = false;
  try {
    (void)perf::ParseWorkload("E");
  } catch (const std::invalid_argument&) {
    rejectedScan = true;
  }
  Require(rejectedScan, "workload E must be rejected");

  perf::WorkloadSpec zipf;
  zipf.distribution = perf::Distribution::kZipfian;
  zipf.recordCount = 10000;
  zipf.operationCount = 20000;
  perf::OperationGenerator generator(zipf);
  uint64_t hottestDecile = 0;
  for (uint64_t sequence = 0; sequence < zipf.operationCount; ++sequence) {
    const auto operation = generator.At(sequence);
    Require(operation.recordId < zipf.recordCount, "Zipfian record id is out of bounds");
    hottestDecile += operation.recordId < zipf.recordCount / 10;
  }
  Require(hottestDecile > zipf.operationCount / 2, "Zipfian selector did not concentrate the hot set");
}

void CheckRegionKeys() {
  perf::RegionKeyCodec codec({{7, "", "b"}, {9, "b", "m"}, {12, "m", ""}}, "unit-run");
  for (uint64_t record = 0; record < 3000; ++record) {
    const size_t rangeIndex = static_cast<size_t>(record % 3);
    const auto& range = codec.Ranges()[rangeIndex];
    const std::string key = codec.Key(record);
    Require((range.startKey.empty() || key >= range.startKey) &&
                (range.endKey.empty() || key < range.endKey),
            "generated key is outside its Region");
    Require(codec.RegionId(record) == range.regionId, "reported Region id is wrong");
  }
}

void CheckHistogram() {
  perf::Histogram first;
  perf::Histogram second;
  for (uint64_t value = 1; value <= 500; ++value) first.Record(value);
  for (uint64_t value = 501; value <= 1000; ++value) second.Record(value);
  first.Merge(second);
  Require(first.Count() == 1000, "histogram count is wrong");
  Require(first.Percentile(0.50) >= 495 && first.Percentile(0.50) <= 505, "histogram P50 is outside 1%");
  Require(first.Percentile(0.99) >= 980 && first.Percentile(0.99) <= 1000, "histogram P99 is outside 1%");
  Require(first.Max() == 1000, "histogram Max is wrong");
  Require(!first.HasReliableP999(), "P99.9 must not be valid below 100,000 samples");
}

void CheckResultFiles() {
  const std::filesystem::path root =
      std::filesystem::temp_directory_path() / ("stratakv-performance-support-" + std::to_string(getpid()));
  std::error_code ignored;
  std::filesystem::remove_all(root, ignored);

  perf::DatasetManifest manifest;
  manifest.runId = "unit-run";
  manifest.regionIds = {7, 9, 12};
  perf::ResultRun run(root, manifest);
  run.Begin();
  std::ifstream incomplete(root / "manifest.json");
  const std::string before((std::istreambuf_iterator<char>(incomplete)), std::istreambuf_iterator<char>());
  Require(before.find("\"state\":\"incomplete\"") != std::string::npos, "run must begin incomplete");
  run.Complete();
  std::ifstream complete(root / "manifest.json");
  const std::string after((std::istreambuf_iterator<char>(complete)), std::istreambuf_iterator<char>());
  Require(after.find("\"state\":\"complete\"") != std::string::npos, "run must complete atomically");

  bool refusedOverwrite = false;
  try {
    perf::ResultRun duplicate(root, manifest);
    duplicate.Begin();
  } catch (const std::runtime_error&) {
    refusedOverwrite = true;
  }
  Require(refusedOverwrite, "existing run must not be overwritten");
  std::filesystem::remove_all(root, ignored);
}

}  // namespace

int main() {
  try {
    CheckGenerator();
    CheckRegionKeys();
    CheckHistogram();
    CheckResultFiles();
    Require(perf::StableValue(17, 256).size() == 256, "stable value size is wrong");
    std::cout << "performance support checks passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "performance support check failed: " << error.what() << '\n';
    return 1;
  }
}
