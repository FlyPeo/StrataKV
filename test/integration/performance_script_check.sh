#!/usr/bin/env bash
# 测试目标：验证 interview-smoke 编排参数与 project 隔离的静态契约。
# 测试策略：只运行 plan/非法参数，不启动集群，并扫描脚本禁止宽泛 pkill。
# 测试规模：检查 smoke 的 14 个性能/一致性 case 组及 full 的保留矩阵描述。
# 验证内容：规模不漂移、非法 project 被拒绝、脚本不通过进程名批量杀进程。
set -euo pipefail

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
script="${repo_dir}/deploy/stratakv-performance"

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

if bash "$script" plan --run-id script-check --project '../unsafe' >/dev/null 2>&1; then
  echo "unsafe project name was accepted" >&2
  exit 1
fi
if grep -Eq '(^|[^[:alnum:]_])pkill([^[:alnum:]_]|$)' "$script"; then
  echo "performance script contains broad pkill" >&2
  exit 1
fi
