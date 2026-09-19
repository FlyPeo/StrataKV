#!/usr/bin/env python3
"""
测试目标：公平对比 Pulsar（栈池关闭/开启）与 Photon 的纯协程上下文切换成本与回退率。
测试策略：绑定同一逻辑 CPU 0，相同 64 KiB 栈尺寸、Release 编译优化参数与消除错误字符串构造的热路径，
          分别对 Pulsar 栈池关闭 (Direct)、Pulsar 栈池开启 (Pooled) 与 Photon 执行各 5 轮严格 A/B 测量。
测试规模：每轮 1,000,000 次往返预热，50,000,000 次正式往返（1 亿次 context transfer），重复 5 轮取中位数与极值。
验证内容：记录逐轮原始输出、ns/transfer、吞吐、正确性 PASS，并自动判定 Pulsar 开启栈池后纯 transfer
          中位数回退是否严格满足 <= 3%。
"""

import json
import os
import re
import statistics
import subprocess
import sys
from pathlib import Path

ROUNDS = 5
CPU = 0
STACK_BYTES = 65536
WARMUP = 1000000
MEASURED = 50000000

PULSAR_BIN = Path("./bin/stratakv-test-fiber-context-ab")
PHOTON_BIN = Path("/tmp/photon-context-ab")

OUT_DIR = Path("docs/8-性能报告/2026-09-04-Pulsar-Photon上下文切换对照原始数据")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def run_single(cmd, log_path):
    print(f"Running: {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
    with open(log_path, "w") as f:
        f.write(result.stdout)
        if result.stderr:
            f.write("\n--- stderr ---\n")
            f.write(result.stderr)
    return parse_output(result.stdout)

def parse_output(output_text):
    data = {}
    for line in output_text.splitlines():
        line = line.strip()
        if "=" in line:
            k, v = line.split("=", 1)
            data[k] = v
    return {
        "ns_per_transfer": float(data["ns_per_transfer"]),
        "transfers_per_sec": float(data["transfers_per_sec"]),
        "adjusted_elapsed_ns": int(data["adjusted_elapsed_ns"]),
        "correctness": data.get("correctness", "FAIL")
    }

def main():
    if not PULSAR_BIN.exists():
        sys.exit(f"Pulsar binary not found: {PULSAR_BIN}")

    runs = {
        "pulsar_direct": {
            "name": "Pulsar (栈池关闭 / Direct)",
            "cmd": [str(PULSAR_BIN), "--cpu", str(CPU), "--stack-bytes", str(STACK_BYTES),
                    "--stack-pool", "0", "--warmup-round-trips", str(WARMUP),
                    "--round-trips", str(MEASURED)],
            "results": []
        },
        "pulsar_pooled": {
            "name": "Pulsar (栈池开启 / Pooled)",
            "cmd": [str(PULSAR_BIN), "--cpu", str(CPU), "--stack-bytes", str(STACK_BYTES),
                    "--stack-pool", "1", "--warmup-round-trips", str(WARMUP),
                    "--round-trips", str(MEASURED)],
            "results": []
        }
    }

    if PHOTON_BIN.exists():
        runs["photon"] = {
            "name": "Photon (v0.9.4 / thread_yield_to)",
            "cmd": [str(PHOTON_BIN), "--cpu", str(CPU), "--stack-bytes", str(STACK_BYTES),
                    "--warmup-round-trips", str(WARMUP),
                    "--round-trips", str(MEASURED)],
            "results": []
        }

    for key, info in runs.items():
        print(f"\n=== Executing 5 rounds for {info['name']} ===")
        for r in range(1, ROUNDS + 1):
            log_file = OUT_DIR / f"{key}_round_{r}.log"
            res = run_single(info["cmd"], log_file)
            info["results"].append(res)
            print(f"  Round {r}: {res['ns_per_transfer']:.3f} ns/transfer, {res['transfers_per_sec']/1e6:.3f} M/s ({res['correctness']})")

    direct_ns = [r["ns_per_transfer"] for r in runs["pulsar_direct"]["results"]]
    pooled_ns = [r["ns_per_transfer"] for r in runs["pulsar_pooled"]["results"]]

    direct_median = statistics.median(direct_ns)
    pooled_median = statistics.median(pooled_ns)

    regression_pct = ((pooled_median - direct_median) / direct_median) * 100.0
    pass_threshold = regression_pct <= 3.0

    summary = {
        "direct_median_ns": direct_median,
        "direct_min_ns": min(direct_ns),
        "direct_max_ns": max(direct_ns),
        "pooled_median_ns": pooled_median,
        "pooled_min_ns": min(pooled_ns),
        "pooled_max_ns": max(pooled_ns),
        "regression_pct": regression_pct,
        "pass_threshold": pass_threshold
    }

    if "photon" in runs:
        photon_ns = [r["ns_per_transfer"] for r in runs["photon"]["results"]]
        summary["photon_median_ns"] = statistics.median(photon_ns)
        summary["photon_min_ns"] = min(photon_ns)
        summary["photon_max_ns"] = max(photon_ns)

    with open(OUT_DIR / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("\n================== SUMMARY ==================")
    print(f"Pulsar Direct Median: {direct_median:.3f} ns/transfer (range {min(direct_ns):.3f} - {max(direct_ns):.3f})")
    print(f"Pulsar Pooled Median: {pooled_median:.3f} ns/transfer (range {min(pooled_ns):.3f} - {max(pooled_ns):.3f})")
    print(f"Regression: {regression_pct:+.2f}% (Threshold <= 3.00%): {'PASS' if pass_threshold else 'FAIL'}")
    if "photon" in runs:
        print(f"Photon v0.9.4 Median: {summary['photon_median_ns']:.3f} ns/transfer (range {summary['photon_min_ns']:.3f} - {summary['photon_max_ns']:.3f})")

if __name__ == "__main__":
    main()
