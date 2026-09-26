#!/usr/bin/env python3
from pathlib import Path
from statistics import median
import re

root = Path(__file__).parent / "ab"
for case, metric in (
    ("context", "ns_per_transfer"),
    ("scheduler", "tasks_per_sec"),
    ("hook-echo", "echo_requests_per_sec"),
):
    values = {}
    for phase in ("before", "after"):
        content = (root / f"{phase}-{case}.log").read_text()
        samples = [float(item) for item in re.findall(rf"^{metric}=([0-9.]+)$", content, re.M)]
        assert len(samples) == 5, (phase, case, samples)
        assert content.count("correctness=PASS") == 5
        values[phase] = median(samples)
        print(f"{phase} {case}: {metric} median={values[phase]:.3f}, "
              f"range={min(samples):.3f}..{max(samples):.3f}")
    print(f"  change={(values['after'] / values['before'] - 1) * 100:+.2f}%")

for case, metric in (
    ("context", "ns_per_transfer"),
    ("scheduler", "tasks_per_sec"),
    ("hook-echo", "echo_requests_per_sec"),
):
    pairs = []
    for phase in ("before", "after"):
        content = (root / f"interleaved-{phase}-{case}.log").read_text()
        samples = [float(item) for item in re.findall(rf"^{metric}=([0-9.]+)$", content, re.M)]
        assert len(samples) == 5 and content.count("correctness=PASS") == 5
        pairs.append(samples)
    changes = [(after / before - 1) * 100 for before, after in zip(*pairs)]
    print(f"interleaved {case}: median pair change={median(changes):+.2f}%, "
          f"range={min(changes):+.2f}..{max(changes):+.2f}%")

for mode in ("per-timestamp baseline", "range reservation"):
    values = {}
    for phase in ("before", "after"):
        content = (root / f"{phase}-tso-range.log").read_text()
        rows = [line.split("|")[1:-1] for line in content.splitlines()
                if line.startswith(f"| {mode} |")]
        assert len(rows) == 5, (phase, mode, rows)
        parsed = [[float(cell.strip()) for cell in row[1:6]] for row in rows]
        values[phase] = [median(items) for items in zip(*parsed)]
        print(f"{phase} {mode}: throughput={values[phase][1]:.1f} ops/s, "
              f"P95={values[phase][3]:.1f} us, P99={values[phase][4]:.1f} us")
    print(f"  throughput change={(values['after'][1] / values['before'][1] - 1) * 100:+.2f}%")
