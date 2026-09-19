/*
 * 测试目标：公平比较 Pulsar、原生 Boost.Context 与 Photon 的协程上下文切换成本。
 * 测试策略：固定 CPU、栈大小和往返次数，先预热，再用相同 ping-pong 循环测量各实现，
 *           并扣除空循环基线以减少计时框架本身的影响。
 * 测试规模：每种实现默认预热 1,000,000 次、正式测量 50,000,000 次往返，使用
 *           64 KiB 子协程栈并固定到逻辑 CPU 0；均可通过命令行参数调整。
 * 验证内容：确认往返计数准确、子协程正常结束、yield 无失败，并输出延迟、吞吐与
 *           correctness=PASS，防止用错误结果得出性能结论。
 */
#include <errno.h>
#include <sched.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>

#if (defined(STRATAKV_AB_PULSAR) + defined(STRATAKV_AB_PHOTON) + \
     defined(STRATAKV_AB_BOOST_RAW)) != 1
#error "Define exactly one StrataKV context A/B backend"
#endif

#ifdef STRATAKV_AB_PULSAR
#include <pulsar/fiber.hpp>
#elif defined(STRATAKV_AB_PHOTON)
#include <photon/photon.h>
#include <photon/thread/thread.h>
#else
#include <boost/context/fiber.hpp>
#include <boost/context/fixedsize_stack.hpp>
#endif

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
  uint64_t warmupRoundTrips = 1000000;
  uint64_t measuredRoundTrips = 50000000;
  uint64_t stackBytes = 64 * 1024;
  int cpu = 0;
  bool stackPool = false;
};

uint64_t ParseUint64(const std::string& text, const char* name, bool allowZero = false) {
  try {
    size_t consumed = 0;
    const unsigned long long value = std::stoull(text, &consumed);
    if (consumed != text.size() || (!allowZero && value == 0)) {
      throw std::invalid_argument("range");
    }
    return static_cast<uint64_t>(value);
  } catch (const std::exception&) {
    throw std::invalid_argument(std::string(name) + " must be a positive integer");
  }
}

Options ParseOptions(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      std::cout
          << "Usage: " << argv[0] << " [options]\n"
          << "  --warmup-round-trips N   Warm-up ping-pong rounds (default 1000000)\n"
          << "  --round-trips N          Measured ping-pong rounds (default 50000000)\n"
          << "  --stack-bytes N          Child coroutine stack (default 65536)\n"
          << "  --cpu N                  Pin process to logical CPU (default 0)\n"
          << "  --stack-pool N           Stack pool enabled (0=direct, 1=pooled, default 0)\n";
      std::exit(0);
    }
    if (i + 1 >= argc) throw std::invalid_argument("missing value for " + arg);
    const std::string value = argv[++i];
    if (arg == "--warmup-round-trips") {
      options.warmupRoundTrips = ParseUint64(value, "--warmup-round-trips");
    } else if (arg == "--round-trips") {
      options.measuredRoundTrips = ParseUint64(value, "--round-trips");
    } else if (arg == "--stack-bytes") {
      options.stackBytes = ParseUint64(value, "--stack-bytes");
    } else if (arg == "--cpu") {
      options.cpu = static_cast<int>(ParseUint64(value, "--cpu", true));
    } else if (arg == "--stack-pool") {
      options.stackPool = (ParseUint64(value, "--stack-pool", true) != 0);
    } else {
      throw std::invalid_argument("unknown option " + arg);
    }
  }
  if (options.stackBytes < 16 * 1024) {
    throw std::invalid_argument("--stack-bytes must be at least 16384");
  }
  return options;
}

void PinToCpu(int cpu) {
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  if (sched_setaffinity(0, sizeof(set), &set) != 0) {
    throw std::runtime_error("sched_setaffinity failed: " + std::string(std::strerror(errno)));
  }
}

long long Nanoseconds(Clock::duration duration) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
}

long long MeasureLoopBaseline(uint64_t iterations) {
  volatile uint64_t counter = 0;
  const auto start = Clock::now();
  for (uint64_t i = 0; i < iterations; ++i) ++counter;
  const auto end = Clock::now();
  if (counter != iterations) throw std::runtime_error("baseline loop was not executed");
  return Nanoseconds(end - start);
}

struct Measurement {
  uint64_t observedRoundTrips = 0;
  long long rawNs = 0;
};

#ifdef STRATAKV_AB_PULSAR

Measurement MeasureSwitches(const Options& options) {
  pulsar::Fiber::GetThis();
  const uint64_t total = options.warmupRoundTrips + options.measuredRoundTrips;
  uint64_t observed = 0;
  std::shared_ptr<pulsar::FiberStackAllocator> allocator;
  if (options.stackPool) {
    pulsar::StackPoolOptions poolOpts;
    allocator = pulsar::MakePooledStackAllocator(poolOpts);
  } else {
    allocator = pulsar::MakeDirectStackAllocator();
  }
  pulsar::Fiber::ptr child(new pulsar::Fiber(
      [&]() {
        pulsar::Fiber* self = pulsar::Fiber::GetThis().get();
        for (uint64_t i = 0; i < total; ++i) {
          ++observed;
          self->yield();
        }
      },
      options.stackBytes, false, allocator));

  for (uint64_t i = 0; i < options.warmupRoundTrips; ++i) child->resume();
  const auto start = Clock::now();
  for (uint64_t i = 0; i < options.measuredRoundTrips; ++i) child->resume();
  const auto end = Clock::now();
  child->resume();

  if (child->getState() != pulsar::Fiber::TERM) {
    throw std::runtime_error("Pulsar child did not terminate");
  }
  return {observed, Nanoseconds(end - start)};
}

constexpr const char* kBackend = "pulsar-public-resume-yield";

#elif defined(STRATAKV_AB_BOOST_RAW)

Measurement MeasureSwitches(const Options& options) {
  const uint64_t total = options.warmupRoundTrips + options.measuredRoundTrips;
  uint64_t observed = 0;
  boost::context::fiber child(
      std::allocator_arg, boost::context::fixedsize_stack(options.stackBytes),
      [&](boost::context::fiber&& caller) mutable {
        for (uint64_t i = 0; i < total; ++i) {
          ++observed;
          caller = std::move(caller).resume();
        }
        return std::move(caller);
      });

  for (uint64_t i = 0; i < options.warmupRoundTrips; ++i) {
    child = std::move(child).resume();
  }
  const auto start = Clock::now();
  for (uint64_t i = 0; i < options.measuredRoundTrips; ++i) {
    child = std::move(child).resume();
  }
  const auto end = Clock::now();
  child = std::move(child).resume();
  if (child) throw std::runtime_error("raw Boost.Context child did not terminate");
  return {observed, Nanoseconds(end - start)};
}

constexpr const char* kBackend = "boost-context-public-fiber";

#else

struct PhotonState {
  photon::thread* mainThread = nullptr;
  uint64_t totalRoundTrips = 0;
  uint64_t observedRoundTrips = 0;
  int yieldFailures = 0;
};

void* PhotonChild(void* argument) {
  PhotonState* state = static_cast<PhotonState*>(argument);
  for (uint64_t i = 0; i < state->totalRoundTrips; ++i) {
    ++state->observedRoundTrips;
    if (photon::thread_yield_to(state->mainThread) != 0) ++state->yieldFailures;
  }
  return nullptr;
}

Measurement MeasureSwitches(const Options& options) {
  if (photon::init(photon::INIT_EVENT_NONE, photon::INIT_IO_NONE) != 0) {
    throw std::runtime_error("photon::init failed");
  }
  struct PhotonFini {
    ~PhotonFini() { photon::fini(); }
  } fini;

  PhotonState state;
  state.mainThread = photon::CURRENT;
  state.totalRoundTrips = options.warmupRoundTrips + options.measuredRoundTrips;
  photon::thread* child =
      photon::thread_create(&PhotonChild, &state, options.stackBytes);
  if (child == nullptr) throw std::runtime_error("photon::thread_create failed");
  photon::join_handle* join = photon::thread_enable_join(child);
  if (join == nullptr) throw std::runtime_error("photon::thread_enable_join failed");

  for (uint64_t i = 0; i < options.warmupRoundTrips; ++i) {
    if (photon::thread_yield_to(child) != 0) ++state.yieldFailures;
  }
  const auto start = Clock::now();
  for (uint64_t i = 0; i < options.measuredRoundTrips; ++i) {
    if (photon::thread_yield_to(child) != 0) ++state.yieldFailures;
  }
  const auto end = Clock::now();

  // The final measured round leaves the child suspended immediately after its
  // last yield. Resume it once outside the timing window so it can return.
  if (photon::thread_yield_to(child) != 0) ++state.yieldFailures;
  photon::thread_join(join);
  if (state.yieldFailures != 0) throw std::runtime_error("Photon yield_to failed");
  return {state.observedRoundTrips, Nanoseconds(end - start)};
}

constexpr const char* kBackend = "photon-public-yield-to";

#endif

}  // namespace

int main(int argc, char** argv) {
  try {
    const Options options = ParseOptions(argc, argv);
    PinToCpu(options.cpu);
    const long long baselineNs = MeasureLoopBaseline(options.measuredRoundTrips);
    const Measurement measurement = MeasureSwitches(options);
    const uint64_t expected = options.warmupRoundTrips + options.measuredRoundTrips;
    if (measurement.observedRoundTrips != expected) {
      throw std::runtime_error("round-trip count mismatch");
    }

    const long long adjustedNs = std::max<long long>(0, measurement.rawNs - baselineNs);
    const uint64_t transfers = 2 * options.measuredRoundTrips;
    std::cout << std::fixed << std::setprecision(3)
              << "backend=" << kBackend << '\n'
              << "build_type="
#ifdef NDEBUG
              << "Release\n"
#else
              << "Debug\n"
#endif
              << "logical_cpu=" << sched_getcpu() << '\n'
              << "stack_bytes=" << options.stackBytes << '\n'
              << "stack_pool=" << (options.stackPool ? 1 : 0) << '\n'
              << "warmup_round_trips=" << options.warmupRoundTrips << '\n'
              << "measured_round_trips=" << options.measuredRoundTrips << '\n'
              << "context_transfers=" << transfers << '\n'
              << "loop_baseline_ns=" << baselineNs << '\n'
              << "raw_elapsed_ns=" << measurement.rawNs << '\n'
              << "adjusted_elapsed_ns=" << adjustedNs << '\n'
              << "ns_per_transfer=" << static_cast<double>(adjustedNs) / transfers << '\n'
              << "transfers_per_sec="
              << static_cast<double>(transfers) * 1e9 / adjustedNs << '\n'
              << "correctness=PASS\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "fiber-context-ab: " << error.what() << '\n';
    return 1;
  }
}
