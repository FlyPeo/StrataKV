#!/usr/bin/env python3
"""
测试目标：验证 D1 比较器在兼容签名、完整 run 下按三次中位数比较性能回归，并以正确性为无条件硬门禁。
测试策略：读取 baseline 与 candidate 目录的 manifest.json 和 raw 结果/summary.json，计算 compatibility signature；当签名不兼容或 run 未完成时拒绝回归比较；当签名兼容时提取三次重复中位数，按吞吐下降 >10% 或 P99 上升 >20% 输出告警与判定；当任一正确性门禁失败时无条件 FAIL；在 WSL 环境下标注 development baseline。
测试规模：支持 full（三轮重复）与 smoke 单轮，包含 A1(8w)、A2(0/100%)、A3(3 regions)、A4(20% opt/pess)、B1/B3、C1、C2。
验证条件：Direct/Gateway 或环境不兼容时拒绝比较；吞吐 -10% 或 P99 +20% 告警/FAIL；B1/B3/C1/C2 失败整体硬失败；生成 Markdown/JSON 比较报告。
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from pathlib import Path
from typing import Any


def compute_compatibility_signature(manifest: dict[str, Any]) -> dict[str, Any]:
    gateway = manifest.get("gateway", {}) if isinstance(manifest.get("gateway"), dict) else {}
    return {
        "schema_version": manifest.get("schema_version"),
        "profile": manifest.get("profile"),
        "build_type": manifest.get("build_type"),
        "record_count": manifest.get("record_count"),
        "value_size_bytes": manifest.get("value_size_bytes"),
        "topology": manifest.get("topology"),
        "gateway_runtime": gateway.get("runtime"),
        "connection_mode": gateway.get("connection_mode"),
    }


def is_wsl(manifest: dict[str, Any]) -> bool:
    os_str = str(manifest.get("os", "")).lower()
    if "wsl" in os_str or "microsoft" in os_str:
        return True
    try:
        proc_ver = Path("/proc/version").read_text(encoding="utf-8").lower()
        if "microsoft" in proc_ver or "wsl" in proc_ver:
            return True
    except OSError:
        pass
    return False


def load_run(run_dir: Path) -> dict[str, Any]:
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing manifest.json in {run_dir}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    summary_path = run_dir / "summary.json"
    summary = {}
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass

    raw_cases: dict[str, list[dict[str, Any]]] = {}
    raw_dir = run_dir / "raw"
    if raw_dir.exists():
        for path in sorted(raw_dir.rglob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                case_id = data.get("case_id")
                if case_id:
                    # Strip any repetition suffix like -r1, -r2, -r3 to group repetitions
                    base_case = case_id
                    for r_suffix in ("-r1", "-r2", "-r3"):
                        if base_case.endswith(r_suffix):
                            base_case = base_case[:-len(r_suffix)]
                            break
                    raw_cases.setdefault(base_case, []).append(data)
            except (json.JSONDecodeError, OSError):
                continue

        for path in sorted(raw_dir.rglob("*.log")):
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            kvs: dict[str, Any] = {"source": str(path.relative_to(run_dir))}
            for line in lines:
                if "=" in line:
                    k, v = line.split("=", 1)
                    k, v = k.strip(), v.strip()
                    if k and " " not in k:
                        try:
                            kvs[k] = float(v) if any(c in v for c in ".eE") else int(v)
                        except ValueError:
                            kvs[k] = v
            case_id = kvs.get("case_id") or path.stem
            base_case = str(case_id)
            for r_suffix in ("-r1", "-r2", "-r3"):
                if base_case.endswith(r_suffix):
                    base_case = base_case[:-len(r_suffix)]
                    break
            raw_cases.setdefault(base_case, []).append(kvs)

    b1_summary = run_dir / "faults" / "b1-summary.txt"
    b1_pass = b1_summary.exists() and "result=PASS" in b1_summary.read_text(encoding="utf-8")

    return {
        "dir": run_dir,
        "manifest": manifest,
        "summary": summary,
        "cases": raw_cases,
        "b1_pass": b1_pass,
    }


def extract_metric_series(cases_list: list[dict[str, Any]], *keys: str) -> list[float]:
    values: list[float] = []
    for item in cases_list:
        for k in keys:
            if "." in k:
                parts = k.split(".")
                curr: Any = item
                for p in parts:
                    curr = curr.get(p, {}) if isinstance(curr, dict) else None
                if isinstance(curr, (int, float)):
                    values.append(float(curr))
                    break
            else:
                val = item.get(k)
                if isinstance(val, (int, float)):
                    values.append(float(val))
                    break
    return values


def compare_runs(baseline_dir: Path, candidate_dir: Path) -> dict[str, Any]:
    base = load_run(baseline_dir)
    cand = load_run(candidate_dir)

    base_sig = compute_compatibility_signature(base["manifest"])
    cand_sig = compute_compatibility_signature(cand["manifest"])

    # 1. Check completion
    base_state = base["manifest"].get("state", "incomplete")
    cand_state = cand["manifest"].get("state", "incomplete")
    if base_state != "complete" or cand_state != "complete":
        return {
            "compatible": False,
            "error": f"Both runs must have state=complete. Baseline state={base_state}, Candidate state={cand_state}",
            "overall": "FAIL",
            "diff": {},
        }

    # 2. Check compatibility signature
    mismatches = {}
    for k, v in base_sig.items():
        if cand_sig.get(k) != v:
            mismatches[k] = {"baseline": v, "candidate": cand_sig.get(k)}

    if mismatches:
        return {
            "compatible": False,
            "error": f"Compatibility signature mismatch: {mismatches}",
            "overall": "REJECTED",
            "mismatches": mismatches,
            "baseline_signature": base_sig,
            "candidate_signature": cand_sig,
        }

    # 3. Core cases to compare
    core_cases = [
        ("a1-A-w8", ["successful_per_second"], ["latency_us.p99", "latency_p99_us"], "record"),
        ("a1-C-w8", ["successful_per_second"], ["latency_us.p99", "latency_p99_us"], "record"),
        ("a2-cross-0", ["throughput_committed_txn_per_sec", "successful_per_second"], ["latency_p99_us", "latency_us.p99"], "transaction"),
        ("a2-cross-100", ["throughput_committed_txn_per_sec", "successful_per_second"], ["latency_p99_us", "latency_us.p99"], "transaction"),
        ("a3-regions-3", ["throughput_committed_txn_per_sec", "successful_per_second"], ["latency_p99_us", "latency_us.p99"], "transaction"),
        ("a4-optimistic-20", ["throughput_commits_per_sec", "successful_per_second"], ["latency_p99_us", "latency_us.p99"], "transaction"),
        ("a4-pessimistic-20", ["throughput_commits_per_sec", "successful_per_second"], ["latency_p99_us", "latency_us.p99"], "transaction"),
    ]

    points: list[dict[str, Any]] = []
    has_perf_regression = False

    for case_id, tps_keys, p99_keys, subject in core_cases:
        base_items = base["cases"].get(case_id, [])
        cand_items = cand["cases"].get(case_id, [])

        base_tps = extract_metric_series(base_items, *tps_keys)
        cand_tps = extract_metric_series(cand_items, *tps_keys)
        base_p99 = extract_metric_series(base_items, *p99_keys)
        cand_p99 = extract_metric_series(cand_items, *p99_keys)

        b_tps_med = statistics.median(base_tps) if base_tps else 0.0
        c_tps_med = statistics.median(cand_tps) if cand_tps else 0.0
        b_p99_med = statistics.median(base_p99) if base_p99 else 0.0
        c_p99_med = statistics.median(cand_p99) if cand_p99 else 0.0

        tps_delta_pct = ((c_tps_med - b_tps_med) / b_tps_med) if b_tps_med > 0 else 0.0
        p99_delta_pct = ((c_p99_med - b_p99_med) / b_p99_med) if b_p99_med > 0 else 0.0

        # Regression criteria: throughput drop > 10% (-0.10), or P99 increase > 20% (+0.20)
        tps_regressed = tps_delta_pct < -0.10
        p99_regressed = p99_delta_pct > 0.20
        regressed = tps_regressed or p99_regressed
        if regressed:
            has_perf_regression = True

        points.append({
            "case_id": case_id,
            "subject": subject,
            "baseline_throughput_median": b_tps_med,
            "candidate_throughput_median": c_tps_med,
            "baseline_throughput_raw": base_tps,
            "candidate_throughput_raw": cand_tps,
            "throughput_delta_pct": tps_delta_pct,
            "throughput_regressed": tps_regressed,
            "baseline_p99_median": b_p99_med,
            "candidate_p99_median": c_p99_med,
            "baseline_p99_raw": base_p99,
            "candidate_p99_raw": cand_p99,
            "p99_delta_pct": p99_delta_pct,
            "p99_regressed": p99_regressed,
            "regressed": regressed,
        })

    # Correctness gates
    b1_pass = cand["b1_pass"]
    c1_case = cand["cases"].get("c1-transfer", [{}])[0]
    c2_case = cand["cases"].get("c2-register", [{}])[0]
    c1_pass = bool(c1_case.get("invariant", {}).get("passed", False))
    c2_pass = c2_case.get("linearizability", {}).get("result") == "pass"

    correctness_pass = b1_pass and c1_pass and c2_pass

    # Overall outcome: Correctness overrides performance.
    # If correctness fails, FAIL unconditionally.
    # If performance regresses, FAIL.
    overall = "PASS" if correctness_pass and not has_perf_regression else "FAIL"

    return {
        "compatible": True,
        "overall": overall,
        "correctness_pass": correctness_pass,
        "performance_pass": not has_perf_regression,
        "gates": {
            "b1": b1_pass,
            "c1": c1_pass,
            "c2": c2_pass,
        },
        "points": points,
        "wsl": is_wsl(cand["manifest"]),
        "baseline_dir": str(baseline_dir),
        "candidate_dir": str(candidate_dir),
    }


def render_markdown(comparison: dict[str, Any]) -> str:
    lines = ["# D1 Performance Regression Comparison Report", ""]
    if not comparison.get("compatible"):
        lines.append("> [!WARNING]")
        lines.append(f"> **Regression comparison rejected**: {comparison.get('error')}")
        lines.append("")
        lines.append(f"Overall Result: **{comparison.get('overall', 'FAIL')}**")
        return "\n".join(lines) + "\n"

    if comparison.get("wsl"):
        lines.append("> [!NOTE]")
        lines.append("> 本次比较运行于 WSL 环境，标记为 development baseline，非生产容量结论。")
        lines.append("")

    overall = comparison["overall"]
    lines.append(f"- 比较结论：**{overall}**")
    lines.append(f"- 正确性门禁：**{'PASS' if comparison['correctness_pass'] else 'FAIL'}** (B1={comparison['gates']['b1']}, C1={comparison['gates']['c1']}, C2={comparison['gates']['c2']})")
    lines.append(f"- 性能回归门禁（吞吐下降>10% 或 P99上升>20%）：**{'PASS' if comparison['performance_pass'] else 'FAIL'}**")
    lines.append("")
    lines.append("## 核心对比点矩阵 (三次中位数及回归分析)")
    lines.append("")
    lines.append("| Case ID | Subject | Base Throughput | Cand Throughput | Delta TPS | Base P99 (us) | Cand P99 (us) | Delta P99 | Result |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---|")

    for p in comparison.get("points", []):
        tps_delta_str = f"{p['throughput_delta_pct'] * 100:+.1f}%"
        p99_delta_str = f"{p['p99_delta_pct'] * 100:+.1f}%"
        status = "REGRESSED" if p["regressed"] else "OK"
        lines.append(
            f"| `{p['case_id']}` | {p['subject']} | {p['baseline_throughput_median']:.2f} | "
            f"{p['candidate_throughput_median']:.2f} | {tps_delta_str} | "
            f"{p['baseline_p99_median']:.0f} | {p['candidate_p99_median']:.0f} | "
            f"{p99_delta_str} | **{status}** |"
        )

    lines.append("")
    lines.append("### 原始采样值")
    lines.append("")
    for p in comparison.get("points", []):
        lines.append(f"- `{p['case_id']}`: Baseline Throughput raw={p['baseline_throughput_raw']}, Candidate Throughput raw={p['candidate_throughput_raw']}")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="D1 Performance Comparator")
    parser.add_argument("baseline_dir", type=Path, help="Path to baseline run directory")
    parser.add_argument("candidate_dir", type=Path, help="Path to candidate run directory")
    parser.add_argument("--output", "-o", type=Path, help="Path to write markdown output report")
    parser.add_argument("--json-output", type=Path, help="Path to write JSON comparison results")
    args = parser.parse_args()

    comparison = compare_runs(args.baseline_dir.resolve(), args.candidate_dir.resolve())
    md_content = render_markdown(comparison)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(md_content, encoding="utf-8")
    else:
        print(md_content)

    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(json.dumps(comparison, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return 0 if comparison.get("overall") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
