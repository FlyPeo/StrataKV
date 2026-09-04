#!/usr/bin/env python3
"""
测试目标：把 interview-smoke 的分散原始输出汇总为可追溯的 CSV/JSON/Markdown 报告。
测试策略：只读取 run 目录内的 JSON 与 key=value 日志，检查必选 case 后原子发布汇总。
测试规模：smoke 固定汇总 A1 4 点、A2 2 点、A3 2 点、A4 4 点、B1、C1、C2。
验证内容：缺文件或正确性失败时保持 incomplete；完整时每个报告数字可回溯到 raw/metrics/history。
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any


def atomic_text(path: Path, contents: str) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(contents, encoding="utf-8")
    os.replace(temporary, path)


def atomic_json(path: Path, value: Any) -> None:
    atomic_text(path, json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def parse_key_values(path: Path) -> dict[str, Any]:
    parsed: dict[str, Any] = {"source": str(path)}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or " " in key:
            continue
        try:
            parsed[key] = float(value) if any(c in value for c in ".eE") else int(value)
        except ValueError:
            parsed[key] = value
    return parsed


def load_cases(run_dir: Path) -> dict[str, dict[str, Any]]:
    cases: dict[str, dict[str, Any]] = {}
    for path in sorted((run_dir / "raw").rglob("*.json")):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        case_id = value.get("case_id")
        if case_id:
            value["source"] = str(path.relative_to(run_dir))
            cases[str(case_id)] = value
            base_case = str(case_id)
            for r_suffix in ("-r1", "-r2", "-r3"):
                if base_case.endswith(r_suffix):
                    base_case = base_case[:-len(r_suffix)]
                    break
            if base_case not in cases:
                cases[base_case] = value
    for path in sorted((run_dir / "raw").rglob("*.log")):
        value = parse_key_values(path)
        case_id = value.get("case_id") or path.stem
        value["case_id"] = case_id
        value["source"] = str(path.relative_to(run_dir))
        if str(case_id) in cases:
            cases[str(case_id)]["log_source"] = str(path.relative_to(run_dir))
        else:
            cases[str(case_id)] = value
        base_case = str(case_id)
        for r_suffix in ("-r1", "-r2", "-r3"):
            if base_case.endswith(r_suffix):
                base_case = base_case[:-len(r_suffix)]
                break
        if base_case not in cases:
            cases[base_case] = value
    return cases


def number(case: dict[str, Any], *names: str) -> float:
    for name in names:
        value = case.get(name)
        if isinstance(value, (int, float)):
            return float(value)
    return 0.0


def pct_change(current: float, baseline: float) -> str:
    if baseline == 0:
        return "不可计算"
    return f"{(current / baseline - 1.0) * 100:+.1f}%"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--profile", default="interview-smoke")
    args = parser.parse_args()
    run_dir: Path = args.run_dir.resolve()
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    cases = load_cases(run_dir)

    required = {
        "a1-A-w1", "a1-A-w8", "a1-C-w1", "a1-C-w8",
        "a2-cross-0", "a2-cross-100", "a3-regions-1", "a3-regions-3",
        "a4-optimistic-0", "a4-optimistic-20", "a4-pessimistic-0", "a4-pessimistic-20",
        "c1-transfer", "c2-register",
    }
    missing = sorted(required - cases.keys())
    b1_summary = run_dir / "faults" / "b1-summary.txt"
    b1_timeline = run_dir / "faults" / "b1-timeline.jsonl"
    b1_pass = (b1_summary.exists() and b1_timeline.exists()
               and "result=PASS" in b1_summary.read_text(encoding="utf-8"))
    b3_summary = run_dir / "faults" / "b3-summary.txt"
    b3_pass = (b3_summary.exists()
               and "result=PASS" in b3_summary.read_text(encoding="utf-8"))
    c1 = cases.get("c1-transfer", {})
    c2 = cases.get("c2-register", {})
    c1_pass = bool(c1.get("invariant", {}).get("passed"))
    c2_pass = c2.get("linearizability", {}).get("result") == "pass"
    performance_success = all(number(cases[name], "successful", "transactions_committed") > 0
                              for name in required if name in cases and not name.startswith("c"))
    b3_ok = b3_pass if b3_summary.exists() else True
    complete = not missing and b1_pass and b3_ok and c1_pass and c2_pass and performance_success

    rows: list[dict[str, Any]] = []
    for case_id, case in sorted(cases.items()):
        latency = case.get("latency_us", {}) if isinstance(case.get("latency_us"), dict) else {}
        rows.append({
            "case_id": case_id,
            "subject": case.get("subject", "transaction" if case_id.startswith(("a2", "a3", "a4", "c1")) else "record"),
            "path": case.get("path", "direct" if case_id.startswith(("a2", "a3", "a4")) else "gateway"),
            "workers": case.get("workers", ""),
            "attempted": case.get("attempted", case.get("transactions_attempted", case.get("transactions_total", ""))),
            "successful": case.get("successful", case.get("transactions_committed", "")),
            "attempted_per_second": case.get("attempted_per_second", case.get("throughput_attempted_txn_per_sec", case.get("throughput_attempts_per_sec", ""))),
            "successful_per_second": case.get("successful_per_second", case.get("throughput_committed_txn_per_sec", case.get("throughput_commits_per_sec", ""))),
            "p50_us": latency.get("p50", case.get("latency_p50_us", "")),
            "p95_us": latency.get("p95", case.get("latency_p95_us", "")),
            "p99_us": latency.get("p99", case.get("latency_p99_us", "")),
            "max_us": latency.get("max", case.get("latency_max_us", "")),
            "source": case.get("source", ""),
        })

    csv_path = run_dir / "summary.csv"
    csv_tmp = csv_path.with_name(csv_path.name + ".tmp")
    with csv_tmp.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0].keys()) if rows else ["case_id"])
        writer.writeheader()
        writer.writerows(rows)
    os.replace(csv_tmp, csv_path)

    summary = {
        "schema_version": 1,
        "profile": args.profile,
        "state": "complete" if complete else "incomplete",
        "overall": "PASS" if complete else "FAIL",
        "missing_cases": missing,
        "gates": {"performance_cases": performance_success, "b1": b1_pass, "c1": c1_pass, "c2": c2_pass},
        "cases": cases,
    }
    atomic_json(run_dir / "summary.json", summary)

    lines = [
        "# StrataKV interview-smoke 报告",
        "",
        "> 本结果来自客户端与服务共置的 WSL，仅是 development baseline，不能作为生产容量。",
        "",
        f"- 总体结论：**{'PASS' if complete else 'FAIL'}**",
        f"- 数据规模：{manifest.get('record_count')} records × {manifest.get('value_size_bytes')} B",
        f"- Git commit：`{manifest.get('git_commit', 'unknown')}`（dirty={manifest.get('git_dirty', True)}）",
        "- P99.9：本 smoke 单点样本少于 100,000，不作为有效结论。",
        "",
        "## A1 Gateway 全链路单记录 OPS",
        "",
        "| Workload | workers | successful OPS | P99 (us) | 原始文件 |",
        "|---|---:|---:|---:|---|",
    ]
    for workload in ("A", "C"):
        for workers in (1, 8):
            case = cases.get(f"a1-{workload}-w{workers}", {})
            latency = case.get("latency_us", {})
            lines.append(f"| {workload} | {workers} | {number(case, 'successful_per_second'):.2f} | "
                         f"{number(latency, 'p99'):.0f} | `{case.get('source', 'missing')}` |")
    for workload in ("A", "C"):
        one = number(cases.get(f"a1-{workload}-w1", {}), "successful_per_second")
        eight = number(cases.get(f"a1-{workload}-w8", {}), "successful_per_second")
        lines.append(f"- Workload {workload} 从 1→8 workers 的吞吐变化：{pct_change(eight, one)}。")
        one_p99 = number(cases.get(f"a1-{workload}-w1", {}).get("latency_us", {}), "p99")
        eight_p99 = number(cases.get(f"a1-{workload}-w8", {}).get("latency_us", {}), "p99")
        lines.append(f"- Workload {workload} 从 1→8 workers 的 P99 变化：{pct_change(eight_p99, one_p99)}。")
    a_eight = number(cases.get("a1-A-w8", {}), "successful_per_second")
    c_eight = number(cases.get("a1-C-w8", {}), "successful_per_second")
    if a_eight > 0:
        lines.append(f"- 8 workers 下纯读 C 的 OPS 是读写混合 A 的 {c_eight / a_eight:.2f} 倍。")

    lines += ["", "## A2/A3 跨 Region 事务", "", "| Case | committed TPS | P99 (us) | 原始文件 |",
              "|---|---:|---:|---|"]
    for case_id in ("a2-cross-0", "a2-cross-100", "a3-regions-1", "a3-regions-3"):
        case = cases.get(case_id, {})
        lines.append(f"| {case_id} | {number(case, 'throughput_committed_txn_per_sec'):.2f} | "
                     f"{number(case, 'latency_p99_us'):.0f} | `{case.get('source', 'missing')}` |")
    a2_local = number(cases.get("a2-cross-0", {}), "throughput_committed_txn_per_sec")
    a2_cross = number(cases.get("a2-cross-100", {}), "throughput_committed_txn_per_sec")
    a3_one = number(cases.get("a3-regions-1", {}), "throughput_committed_txn_per_sec")
    a3_three = number(cases.get("a3-regions-3", {}), "throughput_committed_txn_per_sec")
    lines.append(f"- A2 全跨 Region 相对全本地 committed TPS：{pct_change(a2_cross, a2_local)}。")
    lines.append(f"- A3 3 Region 相对 1 Region committed TPS：{pct_change(a3_three, a3_one)}。")

    lines += ["", "## A4 争用策略", "", "| Mode | target share | committed TPS | conflict rate | success rate |",
              "|---|---:|---:|---:|---:|"]
    for mode in ("optimistic", "pessimistic"):
        for share in (0, 20):
            case = cases.get(f"a4-{mode}-{share}", {})
            lines.append(f"| {mode} | {share}% | {number(case, 'throughput_commits_per_sec'):.2f} | "
                         f"{number(case, 'actual_conflict_rate'):.4f} | {number(case, 'success_rate'):.4f} |")
    optimistic_wins = [
        number(cases.get(f"a4-optimistic-{share}", {}), "throughput_commits_per_sec") >
        number(cases.get(f"a4-pessimistic-{share}", {}), "throughput_commits_per_sec")
        for share in (0, 20)
    ]
    if all(optimistic_wins):
        lines.append("- 本次 0% 与 20% 两个实测点均为 optimistic committed TPS 更高，未观察到策略切换点。")
    elif not any(optimistic_wins):
        lines.append("- 本次 0% 与 20% 两个实测点均为 pessimistic committed TPS 更高，未观察到曲线交叉。")
    else:
        lines.append("- optimistic/pessimistic 曲线在本次采样区间发生交叉；需用 full 的 5% 点和三次重复定位切换范围。")

    lines += [
        "", "## B1/B3/C1/C2 正确性门禁", "",
        f"- B1 Region Leader SIGKILL、追赶与完整重启校验：**{'PASS' if b1_pass else 'FAIL'}**；证据 `faults/`。",
    ]
    if b3_summary.exists():
        lines.append(f"- B3 2PC Coordinator 崩溃恢复（Rollback / Roll-forward 幂等）：**{'PASS' if b3_pass else 'FAIL'}**；证据 `faults/b3-summary.txt`。")
    lines += [
        f"- C1 并发转账总额守恒：**{'PASS' if c1_pass else 'FAIL'}**；证据 `{c1.get('source', 'missing')}`。",
        f"- C2 bounded register linearizability：**{'PASS' if c2_pass else 'FAIL'}**；证据 `{c2.get('source', 'missing')}` 与 `histories/`。",
        "", "## 边界", "",
        "- A1 的数据集横跨三个 Region，但每次 YCSB operation 只访问一个 record/Region。",
        "- A2/A3 的主体是完整三 mutation transaction，因此报告 TPS，不与 A1 OPS 混用。",
        "- smoke 每点只运行一次，只用于证明测试链路可执行；性能回归结论需 full 的三次重复。",
        "- A2 与 A3 的相同端点仍有单次运行波动，因此小幅差值只作为现象，不解释为确定收益。",
        "- 报告只陈述原始指标支持的变化，不据此宣称生产容量或与 TiKV 官方硬件横向比较。",
    ]
    if not b1_pass and (run_dir / "faults" / "B1-DIAGNOSIS.md").exists():
        lines += ["", "B1 的指标级根因分析见 `faults/B1-DIAGNOSIS.md`。"]
    if missing:
        lines += ["", "缺失 case：`" + "`, `".join(missing) + "`。"]
    atomic_text(run_dir / "REPORT.md", "\n".join(lines) + "\n")

    manifest["state"] = "complete" if complete else "incomplete"
    manifest["overall"] = "PASS" if complete else "FAIL"
    atomic_json(manifest_path, manifest)
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
