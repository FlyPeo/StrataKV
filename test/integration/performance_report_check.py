#!/usr/bin/env python3
"""
测试目标：验证 smoke 报告器只在必选性能与正确性证据齐全时把 run 标为 complete。
测试策略：在临时目录构造最小 A1–A4、B1、C1、C2 fixture，分别执行完整与缺失 case。
测试规模：14 个必选 case、1 条 B1 timeline、2 个 correctness gate。
验证内容：完整 fixture 生成 CSV/JSON/REPORT 并 PASS，缺失 fixture 返回非零且保持 incomplete。
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")


def create_fixture(root: Path) -> None:
    for directory in ("raw/a1", "raw/a2", "raw/a3", "raw/a4", "faults", "histories", "metrics"):
        (root / directory).mkdir(parents=True, exist_ok=True)
    write_json(root / "manifest.json", {
        "schema_version": 1, "state": "incomplete", "record_count": 3000,
        "value_size_bytes": 256, "git_commit": "fixture", "git_dirty": False,
    })
    for workload in ("A", "C"):
        for workers in (1, 8):
            case = f"a1-{workload}-w{workers}"
            write_json(root / "raw/a1" / f"{case}.json", {
                "case_id": case, "subject": "record", "path": "gateway", "workers": workers,
                "attempted": 10000, "successful": 10000, "successful_per_second": 100.0 * workers,
                "latency_us": {"p50": 10, "p95": 20, "p99": 30, "max": 40},
            })
    for case in ("a2-cross-0", "a2-cross-100", "a3-regions-1", "a3-regions-3"):
        (root / "raw" / ("a2" if case.startswith("a2") else "a3") / f"{case}.log").write_text(
            f"case_id={case}\ntransactions_committed=1000\nthroughput_committed_txn_per_sec=50\nlatency_p99_us=90\n",
            encoding="utf-8")
    for mode in ("optimistic", "pessimistic"):
        for share in (0, 20):
            case = f"a4-{mode}-{share}"
            (root / "raw/a4" / f"{case}.log").write_text(
                f"case_id={case}\ntransactions_committed=900\nthroughput_commits_per_sec=45\n"
                "actual_conflict_rate=0.1\nsuccess_rate=0.9\n",
                encoding="utf-8")
    write_json(root / "raw/c1-transfer.json", {
        "case_id": "c1-transfer", "attempted": 1000, "successful": 1000,
        "invariant": {"passed": True}, "latency_us": {"p50": 1, "p95": 2, "p99": 3, "max": 4},
    })
    write_json(root / "raw/c2-register.json", {
        "case_id": "c2-register", "attempted": 300, "successful": 300,
        "linearizability": {"result": "pass"}, "latency_us": {"p50": 1, "p95": 2, "p99": 3, "max": 4},
    })
    (root / "faults/b1-summary.txt").write_text("result=PASS\n", encoding="utf-8")
    (root / "faults/b1-timeline.jsonl").write_text("{}\n", encoding="utf-8")


def main() -> int:
    repo = Path(__file__).resolve().parents[2]
    report = repo / "test/support/performance/report.py"
    temporary = Path(tempfile.mkdtemp(prefix="stratakv-performance-report-"))
    try:
        complete = temporary / "complete"
        create_fixture(complete)
        subprocess.run(["python3", str(report), str(complete)], check=True)
        manifest = json.loads((complete / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["state"] == "complete"
        assert (complete / "summary.csv").exists()
        assert "总体结论：**PASS**" in (complete / "REPORT.md").read_text(encoding="utf-8")

        incomplete = temporary / "incomplete"
        shutil.copytree(complete, incomplete)
        (incomplete / "raw/a1/a1-A-w1.json").unlink()
        (incomplete / "manifest.json").write_text(json.dumps({"state": "incomplete"}), encoding="utf-8")
        result = subprocess.run(["python3", str(report), str(incomplete)], check=False)
        assert result.returncode != 0
        manifest = json.loads((incomplete / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["state"] == "incomplete"
        return 0
    finally:
        shutil.rmtree(temporary)


if __name__ == "__main__":
    raise SystemExit(main())
