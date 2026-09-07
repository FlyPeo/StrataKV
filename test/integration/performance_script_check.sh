#!/usr/bin/env bash
# 测试目标：验证 interview-smoke/interview-full 编排参数与 project 隔离的静态契约。
# 测试策略：只运行 plan/非法参数，不启动集群，并扫描脚本禁止宽泛 pkill。
# 测试规模：检查 smoke 的 14 个性能/一致性 case 组、full 矩阵及一键入口。
# 验证内容：规模不漂移、非法 project 被拒绝、脚本不通过进程名批量杀进程。
set -euo pipefail

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
script="${repo_dir}/deploy/stratakv-performance"
full_script="${repo_dir}/deploy/stratakv-performance-full"
test -x "$script"
test -x "$full_script"
grep -q 'interview-full' "$full_script"

smoke=$(bash "$script" plan --profile interview-smoke --run-id script-check --project script-check)
grep -q 'load=3000x256B' <<<"$smoke"
grep -q 'gateway-uniform-A/C@1/8;10000opsx1' <<<"$smoke"
grep -q 'cross-region-ratio-0/100;1000txx1' <<<"$smoke"
grep -q 'optimistic/pessimisticx0/20;1000txx1' <<<"$smoke"
grep -q 'faults=B1 consistency=C1-1000,C2-300' <<<"$smoke"
grep -q 'wait_for_region_convergence 120' "$script"
grep -q '"event":"node_caught_up"' "$script"

full=$(bash "$script" plan --profile interview-full --run-id script-check --project script-check)
grep -q 'gateway-uniform-A/B/C/F@1/4/8/16/32' <<<"$full"
lite=$(bash "$script" plan --profile interview-full-10pct --run-id script-check --project script-check)
grep -q 'load=10000x1024B' <<<"$lite"
grep -q '2000opsx3' <<<"$lite"
bash "$full_script" --help | grep -q -- '--lite'

if bash "$script" plan --run-id script-check --project '../unsafe' >/dev/null 2>&1; then
  echo "unsafe project name was accepted" >&2
  exit 1
fi
if grep -Eq '(^|[^[:alnum:]_])pkill([^[:alnum:]_]|$)' "$script"; then
  echo "performance script contains broad pkill" >&2
  exit 1
fi
