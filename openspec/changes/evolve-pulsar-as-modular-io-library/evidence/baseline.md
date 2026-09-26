# Isolated baseline (2026-09-26, WSL Ubuntu)

- Root commit: `fba615794873b9994d785b840ee52394e56d087b`.
- Pulsar submodule commit: `e32aced0d30eac4a3e72f0e665a830a36fb33129`.
- Isolated checkout: `/home/fly/_projects/2__StrataKV_pulsar_modular`, branch `codex/pulsar-modular-io`.
- Original checkout was already dirty: `deploy/stratakv-performance` (SHA-256 `d652599faf18104963c1cfda305438780212af4e61f6efbe61b4b7bcf23b5655`), Pulsar `README.md` (SHA-256 `5d47b38f10707361a2684ca94b22579daa6deb86fa80a5e9b9a9a78dba73f83b`), and this OpenSpec change. The performance script was not copied into the isolated checkout. The README edit and OpenSpec change were copied. None of these original files was overwritten.
- AMD Ryzen 7 9700X, 12 visible logical CPUs (0–11); Ubuntu 22.04 WSL; GCC 11.4.0, CMake 3.22.1, Boost.Context 1.74; Release build, guard pages off.

## Commands and raw output

Pulsar: `cmake -S src/pulsar -B build-pulsar-baseline -DCMAKE_BUILD_TYPE=Release -DPULSAR_BUILD_TESTS=ON -DPULSAR_BUILD_BENCHMARKS=ON`, then build and `ctest --test-dir build-pulsar-baseline --output-on-failure`. Six of six passed.

Five independent runs each, with raw output in `evidence/baseline/`:

- `pulsar-benchmark --case context --iterations 1000000 --cpu 0`
- `pulsar-benchmark --case scheduler --mode pool-multi --count 50000 --threads 4 --rounds 5`
- `pulsar-benchmark --case hook-echo --count 100 --round-trips 20 --threads 4 --rounds 5`
- `stratakv-test-tso-range-benchmark --requests 2048 --clients 8 --warmup 128 --baseline-range 1 --optimized-range 4096`

The TSO benchmark launches three real RPC/Raft processes per case. Its five reports contain throughput and P50/P95/P99. The core benchmark reports all end in `correctness=PASS`.

The root Release build reached its existing `src/client/client_main.cpp` errors (duplicate `cmd` declaration and unmatched braces). Building with make's keep-going option produced all CTest binaries; the error is saved in `root-build.log`. Root CTest passed 58/59: `stratakv-test-auto-balancer-integration` already fails with “balancer must be disabled by default”. This is a pre-change baseline failure and outside this change's files. Raw results are in `stratakv-ctest.log`.
