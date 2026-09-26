#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../../.."
out="openspec/changes/evolve-pulsar-as-modular-io-library/evidence/ab"
mkdir -p "$out"
for file in "$out"/*.log; do
  [[ -e "$file" ]] && : > "$file"
done

# Run each command alone. Both binaries use the same source checkout, compiler,
# Release flags and host; the baseline binary was built before edits.
for phase in before after; do
  if [[ "$phase" == before ]]; then
    binary="./build-pulsar-baseline/pulsar-benchmark"
  else
    binary="./build-pulsar-modular/pulsar-benchmark"
  fi
  for _ in 1 2 3 4 5; do
    "$binary" --case context --iterations 1000000 --cpu 0 >> "$out/$phase-context.log"
  done
  for _ in 1 2 3 4 5; do
    "$binary" --case scheduler --mode pool-multi --count 50000 --threads 4 --rounds 5 >> "$out/$phase-scheduler.log"
  done
  for _ in 1 2 3 4 5; do
    "$binary" --case hook-echo --count 100 --round-trips 20 --threads 4 --rounds 5 >> "$out/$phase-hook-echo.log"
  done
done

for phase in before after; do
  for _ in 1 2 3 4 5; do
    ./bin/stratakv-test-tso-range-benchmark --requests 2048 --clients 8 --warmup 128 --baseline-range 1 --optimized-range 4096 >> "$out/$phase-tso-range.log"
  done
done
