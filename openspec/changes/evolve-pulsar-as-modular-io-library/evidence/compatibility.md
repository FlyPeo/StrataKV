# Compatibility contract

The existing `Pulsar::pulsar` target remains a static C++17 library. Its public umbrella header `<pulsar/pulsar.h>` and Fiber, Scheduler, IOManager, Hook, synchronization, thread, fd and utility headers remain available with their existing signatures. Its public link dependencies remain Threads, dl and Boost.Context. `PULSAR_BUILD_NET` and `PULSAR_BUILD_RPC` default to OFF, so the default build and exported target graph remain core-only.

StrataKV's `stratakv-node` and `stratakv-tso` still link `stratakv_core` and the pre-existing `stratakv_rpc`. The observed node link command contains `libstratakv_rpc.a` and `libpulsar.a`; it contains no new Pulsar network/RPC archive. `RpcProvider` still uses Muduo and 32 worker threads; its configured node port and TSO peer ports have not been changed. `stratakv-node --help` exits successfully. The baseline TSO benchmark launched three peer processes and served requests through the existing RPC path.

The standalone downstream example in `src/pulsar/examples/downstream` was built and run using both the baseline core install and the optional network install via `find_package(Pulsar CONFIG REQUIRED)`; it was also built and run with `add_subdirectory`. The example contains no StrataKV headers or libraries.
