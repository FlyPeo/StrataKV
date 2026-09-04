#!/usr/bin/env python3
"""
测试目标：验证 D1 比较器的兼容性签名校验、状态检查、三次中位数提取、性能回归判定和正确性绝对优先门禁。
测试策略：在临时目录下构造 golden fixtures，分别模拟环境兼容/不兼容、run 完整/不完整、正常基线、吞吐下降 12%（回归失败）、P99 上升 25%（回归失败）、正确性失败（B1/C1/C2 失败）场景。
测试规模：测试包含 7 组端到端场景，每组覆盖 A1(8w)、A2(0/100%)、A3(3 regions)、A4(20%) 以及 B1/C1/C2 正确性门禁。
验证条件：兼容签名不一致时拒绝回归并返回非零；incomplete run 拒绝比较；性能退化（吞吐-12% 或 P99+25%）返回非零；正确性失败时吞吐即使提升 200% 也必须硬失败；兼容且性能未退化时成功返回 0。
"""

from __future__ import annotations

import copy
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def create_mock_run(root: Path, *, tps_scale: float = 1.0, p99_scale: float = 1.0,
                    correctness: bool = True, record_count: int = 3000,
                    state: str = "complete", repetitions: int = 3) -> None:
    manifest = {
        "schema_version": 1,
        "state": state,
        "profile": "interview-smoke",
        "build_type": "Release",
        "record_count": record_count,
        "value_size_bytes": 256,
        "os": "Linux-WSL2-x86_64",
        "topology": {"gateway": 1, "tso": 3, "nodes": 3, "regions": 3, "replicas_per_region": 3},
        "gateway": {"runtime": "fiber", "connection_mode": "close"},
    }
    write_json(root / "manifest.json", manifest)

    # Core cases
    cases_config = [
        ("a1-A-w8", 800.0, 1500.0, "gateway"),
        ("a1-C-w8", 1200.0, 1000.0, "gateway"),
        ("a2-cross-0", 500.0, 2000.0, "direct"),
        ("a2-cross-100", 350.0, 3000.0, "direct"),
        ("a3-regions-3", 300.0, 3500.0, "direct"),
        ("a4-optimistic-20", 400.0, 2500.0, "direct"),
        ("a4-pessimistic-20", 380.0, 2600.0, "direct"),
    ]

    for case_id, base_tps, base_p99, path_mode in cases_config:
        for r in range(1, repetitions + 1):
            suffix = f"-r{r}" if repetitions > 1 else ""
            tps_val = base_tps * tps_scale * (1.0 + 0.01 * (r - 2))
            p99_val = base_p99 * p99_scale * (1.0 + 0.01 * (r - 2))
            if path_mode == "gateway":
                write_json(root / "raw/a1" / f"{case_id}{suffix}.json", {
                    "case_id": f"{case_id}{suffix}",
                    "subject": "record",
                    "path": "gateway",
                    "workers": 8,
                    "attempted": 10000,
                    "successful": 10000,
                    "successful_per_second": tps_val,
                    "latency_us": {"p50": p99_val * 0.5, "p95": p99_val * 0.8, "p99": p99_val, "max": p99_val * 1.5},
                })
            else:
                prefix = case_id.split("-")[0]
                lines = [
                    f"case_id={case_id}{suffix}",
                    f"transactions_committed=1000",
                    f"throughput_committed_txn_per_sec={tps_val}",
                    f"throughput_commits_per_sec={tps_val}",
                    f"latency_p99_us={p99_val}",
                ]
                p = root / "raw" / prefix / f"{case_id}{suffix}.log"
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # Correctness outputs
    write_json(root / "raw/c1-transfer.json", {
        "case_id": "c1-transfer",
        "attempted": 1000,
        "successful": 1000,
        "invariant": {"passed": correctness},
        "latency_us": {"p50": 10, "p95": 20, "p99": 30, "max": 40},
    })
    write_json(root / "raw/c2-register.json", {
        "case_id": "c2-register",
        "attempted": 300,
        "successful": 300,
        "linearizability": {"result": "pass" if correctness else "fail"},
        "latency_us": {"p50": 10, "p95": 20, "p99": 30, "max": 40},
    })
    b1_dir = root / "faults"
    b1_dir.mkdir(parents=True, exist_ok=True)
    (b1_dir / "b1-summary.txt").write_text(f"result={'PASS' if correctness else 'FAIL'}\n", encoding="utf-8")


def main() -> int:
    repo_dir = Path(__file__).resolve().parents[2]
    comparator = repo_dir / "test/support/performance/d1_compare.py"

    temp_dir = Path(tempfile.mkdtemp(prefix="d1-check-"))
    try:
        # Scenario 1: Identical baseline and candidate -> PASS
        base_dir = temp_dir / "base"
        cand_pass_dir = temp_dir / "cand_pass"
        create_mock_run(base_dir, tps_scale=1.0, p99_scale=1.0)
        create_mock_run(cand_pass_dir, tps_scale=1.02, p99_scale=0.98)

        res = subprocess.run(["python3", str(comparator), str(base_dir), str(cand_pass_dir)],
                             capture_output=True, text=True)
        assert res.returncode == 0, f"Expected PASS for healthy comparison, got: {res.stdout}\n{res.stderr}"
        assert "比较结论：**PASS**" in res.stdout
        assert "标记为 development baseline" in res.stdout

        # Scenario 2: Throughput regresses by 12% (> 10% threshold) -> FAIL
        cand_tps_reg_dir = temp_dir / "cand_tps_reg"
        create_mock_run(cand_tps_reg_dir, tps_scale=0.88, p99_scale=1.0)
        res = subprocess.run(["python3", str(comparator), str(base_dir), str(cand_tps_reg_dir)],
                             capture_output=True, text=True)
        assert res.returncode != 0, "Expected FAIL for >10% throughput drop"
        assert "比较结论：**FAIL**" in res.stdout
        assert "**REGRESSED**" in res.stdout

        # Scenario 3: P99 regresses by 25% (> 20% threshold) -> FAIL
        cand_p99_reg_dir = temp_dir / "cand_p99_reg"
        create_mock_run(cand_p99_reg_dir, tps_scale=1.0, p99_scale=1.25)
        res = subprocess.run(["python3", str(comparator), str(base_dir), str(cand_p99_reg_dir)],
                             capture_output=True, text=True)
        assert res.returncode != 0, "Expected FAIL for >20% P99 increase"
        assert "比较结论：**FAIL**" in res.stdout
        assert "**REGRESSED**" in res.stdout

        # Scenario 4: Correctness failure overrides 200% throughput gain -> hard FAIL
        cand_correctness_fail_dir = temp_dir / "cand_correctness_fail"
        create_mock_run(cand_correctness_fail_dir, tps_scale=2.0, p99_scale=0.5, correctness=False)
        res = subprocess.run(["python3", str(comparator), str(base_dir), str(cand_correctness_fail_dir)],
                             capture_output=True, text=True)
        assert res.returncode != 0, "Expected hard FAIL when correctness fails despite 2x throughput"
        assert "正确性门禁：**FAIL**" in res.stdout
        assert "比较结论：**FAIL**" in res.stdout

        # Scenario 5: Incompatible signature (different record_count) -> Rejected / FAIL
        cand_incompatible_dir = temp_dir / "cand_incompatible"
        create_mock_run(cand_incompatible_dir, record_count=100000)
        res = subprocess.run(["python3", str(comparator), str(base_dir), str(cand_incompatible_dir)],
                             capture_output=True, text=True)
        assert res.returncode != 0, "Expected rejection for incompatible signatures"
        assert "Compatibility signature mismatch" in res.stdout or "REJECTED" in res.stdout

        # Scenario 6: Incomplete run -> Rejected / FAIL
        cand_incomplete_dir = temp_dir / "cand_incomplete"
        create_mock_run(cand_incomplete_dir, state="incomplete")
        res = subprocess.run(["python3", str(comparator), str(base_dir), str(cand_incomplete_dir)],
                             capture_output=True, text=True)
        assert res.returncode != 0, "Expected rejection for incomplete run"
        assert "state=complete" in res.stdout

        print("--- All D1 Comparator Verification Scenarios Passed Successfully ---")
        return 0
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
