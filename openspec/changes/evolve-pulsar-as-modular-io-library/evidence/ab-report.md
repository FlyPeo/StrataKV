# Same-host A/B (2026-09-26)

The commands and all five raw observations per phase are in `run-ab.sh` and `ab/*.log`. The core binaries were built with the same GCC 11.4/Boost 1.74 Release settings. Their SHA-256 values are identical:

- `libpulsar.a`: `8f5b874dae5be047612233d600dc5a1324221b44fcdb9db28a681b62a87dfe19`
- `pulsar-benchmark`: `81455fed596c87e97e140ae66f0f8e2c256d34052a58e96f7b2189183c7b6cb5`

| Workload (median of five) | Before | After | Observed change |
| --- | ---: | ---: | ---: |
| Fiber context (ns/transfer; lower is better) | 14.791 | 16.026 | +8.35% |
| Four-worker scheduler (M task/s) | 13.872 | 14.550 | +4.89% |
| Hook echo (K request/s) | 90.537 | 96.643 | +6.74% |
| StrataKV TSO per-timestamp throughput (ops/s) | 119.2 | 119.2 | 0.00% |
| StrataKV TSO per-timestamp P95 / P99 (µs) | 165016.4 / 180765.3 | 162511.4 / 180844.4 | mixed |
| StrataKV TSO range throughput (ops/s) | 39598.9 | 36337.5 | −8.24% |
| StrataKV TSO range P95 / P99 (µs) | 131.5 / 461.6 | 140.8 / 384.1 | mixed |

The StrataKV service benchmark ran the **same `stratakv-test-tso-range-benchmark` executable** in both phases. It launches the same three-process RPC/Raft workload each time. Its range throughput varied from 34.2K to 41.6K before and 34.6K to 51.9K after. The two medians differ, but this cannot be attributed to a code change in that executable.

To investigate the core timing drift, `run-core-interleaved.sh` alternated the before/after binaries five times per case. Pairwise differences crossed zero in each case: context −1.81% to +24.77%, scheduler −2.55% to +9.98%, Hook echo −8.29% to +10.07%. Both binaries are byte-for-byte identical, so these differences reflect measurement conditions. No code-caused regression is established; these observations do **not** prove an absolute zero-overhead result.

The default StrataKV node link still includes `libstratakv_rpc.a` and `libpulsar.a`, with no `libpulsar_net.a` or `libpulsar_rpc.a`. The optional targets have no default runtime path.

Pulsar's complete optional build passed 10/10 CTest checks. StrataKV passed 58/59 both before and after: the same Auto-Balancer integration check fails with “balancer must be disabled by default”. The root all-target build fails in the same pre-existing `src/client/client_main.cpp` syntax errors before and after. There are no new correctness failures in the exercised paths.
