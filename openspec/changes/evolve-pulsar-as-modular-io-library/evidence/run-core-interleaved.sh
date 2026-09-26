#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../../.."
out="openspec/changes/evolve-pulsar-as-modular-io-library/evidence/ab"
for case in context scheduler hook-echo; do
  for _ in 1 2 3 4 5; do
    for phase in before after; do
      if [[ "$phase" == before ]]; then binary="./build-pulsar-baseline/pulsar-benchmark"
      else binary="./build-pulsar-modular/pulsar-benchmark"; fi
      if [[ "$case" == context ]]; then
        "$binary" --case context --iterations 1000000 --cpu 0 >> "$out/interleaved-$phase-context.log"
      elif [[ "$case" == scheduler ]]; then
        "$binary" --case scheduler --mode pool-multi --count 50000 --threads 4 --rounds 5 >> "$out/interleaved-$phase-scheduler.log"
      else
        "$binary" --case hook-echo --count 100 --round-trips 20 --threads 4 --rounds 5 >> "$out/interleaved-$phase-hook-echo.log"
      fi
    done
  done
done
