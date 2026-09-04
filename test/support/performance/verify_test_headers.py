#!/usr/bin/env python3
"""
验证所有测试源文件头部包含规范的 4 项说明：
1. 测试目标
2. 测试策略
3. 测试规模 / 数据规模
4. 验证条件 / 验证内容
"""
import glob
import os
import sys

patterns = [
    "test/unit/*.cpp",
    "test/integration/*.cpp",
    "test/integration/*.py",
    "test/integration/*.sh",
]

required = ["目标", "策略", "规模", "验"]

files = []
for p in patterns:
    files.extend(glob.glob(p))

failed = False
for path in sorted(files):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        header = f.read(1200)
    missing = [req for req in required if req not in header]
    if missing:
        print(f"FAIL: {path} is missing header items: {missing}")
        failed = True
    else:
        print(f"OK:   {path}")

if failed:
    sys.exit(1)

print(f"\nAll {len(files)} test source files meet the 4-item header documentation standard.")
