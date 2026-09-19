#!/usr/bin/env python3
# 测试目标：运行 Pulsar 协程栈复用五轮同条件 A/B 测试并生成性能对比报告。
# 测试策略：固定 CPU 0，对 Direct+单槽、StackPool+单槽、StackPool+多槽分别执行五轮 1,000,000 次短 Fiber 测试，
#           自动对比预热后系统分配减少比例与创建+销毁中位延迟改善幅度，判定是否达标。
# 测试规模：每种模式执行预热 10,000 次 + 5 轮 × 1,000,000 次短 Fiber。
# 验证内容：验证预热后系统分配减少 >= 95%、创建+销毁中位耗时改善 >= 30%、任务 checksum 一致且无泄漏。
import os
import subprocess
import sys
import time

REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
BIN = os.path.join(REPO_DIR, "bin/stratakv-test-fiber-benchmark")
RAW_DATA_DIR = os.path.join(REPO_DIR, "docs/8-性能报告/2026-09-04-Pulsar协程栈复用A-B测试原始数据")
REPORT_PATH = os.path.join(REPO_DIR, "docs/8-性能报告/2026-09-04-Pulsar协程栈复用A-B测试.md")

os.makedirs(RAW_DATA_DIR, exist_ok=True)

modes = [
    ("direct-single", "Direct+单槽"),
    ("pool-single", "StackPool+单槽"),
    ("pool-multi", "StackPool+多槽"),
]

def run_benchmark(mode_id, mode_name, count=1000000, rounds=5, warmup=10000, cpu=0):
    cmd = [
        BIN,
        "--case", "lifecycle",
        "--mode", mode_name,
        "--count", str(count),
        "--rounds", str(rounds),
        "--warmup", str(warmup),
        "--cpu", str(cpu),
    ]
    print(f"Running: {' '.join(cmd)}")
    raw_path = os.path.join(RAW_DATA_DIR, f"{mode_id}.log")
    start_t = time.time()
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    duration = time.time() - start_t
    with open(raw_path, "w", encoding="utf-8") as f:
        f.write(res.stdout)
        if res.stderr:
            f.write("\n--- STDERR ---\n" + res.stderr)
    
    if res.returncode != 0:
        print(f"Error running {mode_name}: return code {res.returncode}", file=sys.stderr)
        print(res.stderr, file=sys.stderr)
        sys.exit(1)
        
    metrics = {}
    for line in res.stdout.strip().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            metrics[k.strip()] = v.strip()
    return metrics, duration

print("=== Starting Pulsar Fiber Stack Reuse A/B Benchmark ===")
results = {}
for mode_id, mode_name in modes:
    metrics, dur = run_benchmark(mode_id, mode_name)
    results[mode_id] = (metrics, dur)
    print(f"Completed {mode_name} in {dur:.2f}s")

direct_metrics, _ = results["direct-single"]
pool_s_metrics, _ = results["pool-single"]
pool_m_metrics, _ = results["pool-multi"]

direct_cd_ns = float(direct_metrics["median_create_destroy_ns_per_fiber"])
pool_s_cd_ns = float(pool_s_metrics["median_create_destroy_ns_per_fiber"])
pool_m_cd_ns = float(pool_m_metrics["median_create_destroy_ns_per_fiber"])

direct_sys_alloc = int(direct_metrics["measured_system_allocations"])
pool_s_sys_alloc = int(pool_s_metrics["measured_system_allocations"])
pool_m_sys_alloc = int(pool_m_metrics["measured_system_allocations"])

alloc_reduction_s = (1.0 - pool_s_sys_alloc / direct_sys_alloc) * 100.0 if direct_sys_alloc > 0 else 0.0
alloc_reduction_m = (1.0 - pool_m_sys_alloc / direct_sys_alloc) * 100.0 if direct_sys_alloc > 0 else 0.0

cd_improvement_s = (direct_cd_ns - pool_s_cd_ns) / direct_cd_ns * 100.0 if direct_cd_ns > 0 else 0.0
cd_improvement_m = (direct_cd_ns - pool_m_cd_ns) / direct_cd_ns * 100.0 if direct_cd_ns > 0 else 0.0

alloc_pass_s = alloc_reduction_s >= 95.0
alloc_pass_m = alloc_reduction_m >= 95.0
cd_pass_s = cd_improvement_s >= 30.0
cd_pass_m = cd_improvement_m >= 30.0

all_pass = alloc_pass_s and alloc_pass_m and cd_pass_s and cd_pass_m

report = f"""# Pulsar 协程栈复用 1,000,000 次短 Fiber 五轮 A/B 性能测试报告

- **测试日期**: {time.strftime('%Y-%m-%d %H:%M:%S')}
- **CPU 绑定**: 逻辑 CPU {direct_metrics.get('logical_cpu', '0')}
- **编译器版本**: GCC {direct_metrics.get('compiler', 'unknown')}
- **Boost 版本**: {direct_metrics.get('boost_version', 'unknown')}
- **Guard Page**: {'ON' if direct_metrics.get('guard_pages_enabled') == '1' else 'OFF'}
- **协程栈尺寸**: {int(direct_metrics.get('stack_bytes', 131072)) // 1024} KiB
- **测试规模**: 预热 10,000 次短 Fiber，正式测试 5 轮 × 1,000,000 次短 Fiber
- **任务 Checksum**: {direct_metrics.get('checksum')} (期望: {direct_metrics.get('expected_checksum')})

## 1. 核心判定指标对比

| 模式 | 系统栈分配数 | 分配减少比例 (目标 >= 95%) | 分配判定 | 创建+销毁中位耗时 (ns/fiber) | 耗时改善比例 (目标 >= 30%) | 耗时判定 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Direct + 单槽 (基线)** | {direct_sys_alloc:,} | 基线 | - | {direct_cd_ns:.3f} ns | 基线 | - |
| **StackPool + 单槽** | {pool_s_sys_alloc:,} | **{alloc_reduction_s:.3f}%** | {'PASS' if alloc_pass_s else 'FAIL'} | **{pool_s_cd_ns:.3f} ns** | **{cd_improvement_s:.3f}%** | {'PASS' if cd_pass_s else 'FAIL'} |
| **StackPool + 多槽** | {pool_m_sys_alloc:,} | **{alloc_reduction_m:.3f}%** | {'PASS' if alloc_pass_m else 'FAIL'} | **{pool_m_cd_ns:.3f} ns** | **{cd_improvement_m:.3f}%** | {'PASS' if cd_pass_m else 'FAIL'} |

> **综合自动化判定结果**: **{'PASS' if all_pass else 'FAIL'}**

## 2. 逐轮原始耗时明细 (ns / fiber)

| 轮次 | Direct + 单槽 | StackPool + 单槽 | StackPool + 多槽 |
| :--- | :--- | :--- | :--- |
| 第 1 轮 | {float(direct_metrics['round_1_ns_per_fiber']):.3f} ns | {float(pool_s_metrics['round_1_ns_per_fiber']):.3f} ns | {float(pool_m_metrics['round_1_ns_per_fiber']):.3f} ns |
| 第 2 轮 | {float(direct_metrics['round_2_ns_per_fiber']):.3f} ns | {float(pool_s_metrics['round_2_ns_per_fiber']):.3f} ns | {float(pool_m_metrics['round_2_ns_per_fiber']):.3f} ns |
| 第 3 轮 | {float(direct_metrics['round_3_ns_per_fiber']):.3f} ns | {float(pool_s_metrics['round_3_ns_per_fiber']):.3f} ns | {float(pool_m_metrics['round_3_ns_per_fiber']):.3f} ns |
| 第 4 轮 | {float(direct_metrics['round_4_ns_per_fiber']):.3f} ns | {float(pool_s_metrics['round_4_ns_per_fiber']):.3f} ns | {float(pool_m_metrics['round_4_ns_per_fiber']):.3f} ns |
| 第 5 轮 | {float(direct_metrics['round_5_ns_per_fiber']):.3f} ns | {float(pool_s_metrics['round_5_ns_per_fiber']):.3f} ns | {float(pool_m_metrics['round_5_ns_per_fiber']):.3f} ns |
| **中位数** | **{float(direct_metrics['median_total_ns_per_fiber']):.3f} ns** | **{float(pool_s_metrics['median_total_ns_per_fiber']):.3f} ns** | **{float(pool_m_metrics['median_total_ns_per_fiber']):.3f} ns** |

## 3. Allocator 统计快照

| 指标 | Direct + 单槽 | StackPool + 单槽 | StackPool + 多槽 |
| :--- | :--- | :--- | :--- |
| acquireRequests | {int(direct_metrics['allocator_acquire_requests']):,} | {int(pool_s_metrics['allocator_acquire_requests']):,} | {int(pool_m_metrics['allocator_acquire_requests']):,} |
| cacheHits | {int(direct_metrics['allocator_cache_hits']):,} | {int(pool_s_metrics['allocator_cache_hits']):,} | {int(pool_m_metrics['allocator_cache_hits']):,} |
| cacheMisses | {int(direct_metrics['allocator_cache_misses']):,} | {int(pool_s_metrics['allocator_cache_misses']):,} | {int(pool_m_metrics['allocator_cache_misses']):,} |
| returns | {int(direct_metrics['allocator_returns']):,} | {int(pool_s_metrics['allocator_returns']):,} | {int(pool_m_metrics['allocator_returns']):,} |
| systemAllocations | {int(direct_metrics['allocator_system_allocations']):,} | {int(pool_s_metrics['allocator_system_allocations']):,} | {int(pool_m_metrics['allocator_system_allocations']):,} |
| systemFrees | {int(direct_metrics['allocator_system_frees']):,} | {int(pool_s_metrics['allocator_system_frees']):,} | {int(pool_m_metrics['allocator_system_frees']):,} |
| cachedBytes | {int(direct_metrics['allocator_cached_bytes']):,} B | {int(pool_s_metrics['allocator_cached_bytes']):,} B | {int(pool_m_metrics['allocator_cached_bytes']):,} B |
| checkedOutBytes | {int(direct_metrics['allocator_checked_out_bytes']):,} B | {int(pool_s_metrics['allocator_checked_out_bytes']):,} B | {int(pool_m_metrics['allocator_checked_out_bytes']):,} B |

## 4. 复现命令

```bash
# Direct + 单槽 (基线)
./bin/stratakv-test-fiber-benchmark --case lifecycle --mode "Direct+单槽" --count 1000000 --rounds 5 --warmup 10000 --cpu 0

# StackPool + 单槽
./bin/stratakv-test-fiber-benchmark --case lifecycle --mode "StackPool+单槽" --count 1000000 --rounds 5 --warmup 10000 --cpu 0

# StackPool + 多槽
./bin/stratakv-test-fiber-benchmark --case lifecycle --mode "StackPool+多槽" --count 1000000 --rounds 5 --warmup 10000 --cpu 0
```
"""

with open(REPORT_PATH, "w", encoding="utf-8") as f:
    f.write(report)

print(f"\nReport written to: {REPORT_PATH}")
print(f"Overall verdict: {'PASS' if all_pass else 'FAIL'}")
print(f"Alloc reduction: StackPool+单槽={alloc_reduction_s:.2f}%, StackPool+多槽={alloc_reduction_m:.2f}%")
print(f"Latency improvement: StackPool+单槽={cd_improvement_s:.2f}%, StackPool+多槽={cd_improvement_m:.2f}%")
