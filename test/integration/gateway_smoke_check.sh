#!/usr/bin/env bash
# 测试目标：验证 Gateway 参数校验与 fiber socket 运行时在 Direct/StackPool 模式下的 HTTP 响应。
# 测试策略：测试参数合法性；启动 Direct+单槽、StackPool+单槽、StackPool+多槽并通过 /healthz 冒烟；
#           验证 bounded request executor 在过载时提供背压并返回 503 GATEWAY_BUSY。
# 测试规模：每种配置各启动一次服务，发送 HTTP 探针；运行 bounded thread pool 边界测试。
# 验证内容：非法参数退出码为 2、三种模式均成功响应 /healthz、过载返回 503 且无后台进程残留。
set -euo pipefail

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
gateway_bin="${repo_dir}/bin/stratakv-gateway"
regions_conf="${repo_dir}/deploy/config/regions.conf"

if [ ! -x "$gateway_bin" ]; then
  echo "Gateway binary not found: $gateway_bin" >&2
  exit 1
fi

# 1. 验证参数校验与退出码
set +e
"$gateway_bin" --regions-config "$regions_conf" --runtime thread --fiber-stack-cache-mib 10 >/dev/null 2>&1
rc_thread_mismatch=$?
"$gateway_bin" --regions-config "$regions_conf" --runtime fiber --fiber-stack-cache-mib -1 >/dev/null 2>&1
rc_neg_cache=$?
"$gateway_bin" --regions-config "$regions_conf" --runtime fiber --fiber-cache-per-worker -1 >/dev/null 2>&1
rc_neg_worker_cache=$?
"$gateway_bin" --regions-config "$regions_conf" --runtime invalid_runtime >/dev/null 2>&1
rc_invalid_runtime=$?
set -e

if [ "$rc_thread_mismatch" -ne 2 ] || [ "$rc_neg_cache" -ne 2 ] || \
   [ "$rc_neg_worker_cache" -ne 2 ] || [ "$rc_invalid_runtime" -ne 2 ]; then
  echo "Parameter validation failed: thread_mismatch=$rc_thread_mismatch neg_cache=$rc_neg_cache neg_worker=$rc_neg_worker_cache invalid_runtime=$rc_invalid_runtime" >&2
  exit 2
fi

# 2. 依次以三种模式启动并冒烟测试
test_gateway_mode() {
  local mode_name="$1"
  local cache_mib="$2"
  local per_worker="$3"
  local port="${4:-19080}"

  # 启动 gateway
  "$gateway_bin" \
    --regions-config "$regions_conf" \
    --host 127.0.0.1 \
    --port "$port" \
    --tso-host 127.0.0.1 \
    --tso-port 26380 \
    --runtime fiber \
    --workers 2 \
    --request-workers 2 \
    --fiber-stack-cache-mib "$cache_mib" \
    --fiber-cache-per-worker "$per_worker" >/dev/null 2>&1 &
  local pid=$!

  # 等待端口就绪
  local ready=0
  for i in $(seq 1 30); do
    if curl -s -o /dev/null "http://127.0.0.1:${port}/healthz" 2>/dev/null; then
      ready=1
      break
    fi
    sleep 0.1
  done

  if [ "$ready" -ne 1 ]; then
    kill -9 "$pid" 2>/dev/null || true
    echo "Gateway failed to start in mode: $mode_name" >&2
    return 1
  fi

  local response
  response=$(curl -s "http://127.0.0.1:${port}/healthz")
  kill -9 "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  sleep 0.1

  if [ "$response" != '{"status":"ok"}' ]; then
    echo "Gateway returned unexpected healthz in mode $mode_name: $response" >&2
    return 1
  fi
}

test_gateway_mode "Direct+单槽" 0 1 19081
test_gateway_mode "StackPool+单槽" 64 1 19082
test_gateway_mode "StackPool+多槽" 64 4 19083

# 3. 验证 bounded thread pool 的背压与拒绝指标
"${repo_dir}/bin/stratakv-test-bounded-thread-pool" >/dev/null

echo "correctness=PASS"
