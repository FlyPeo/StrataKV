// stratakv-client — 面向业务使用的常驻命令行客户端。
// 与 stratakv-admin (底层 Region 运维工具) 不同,本工具只做业务读写,
// 全部命令走事务 SDK:连接一次常驻复用。
// 用法:
//   stratakv-client <regions.conf> [tso-endpoints]          (静态拓扑)
//   STRATAKV_TOPOLOGY_MODE=dynamic STRATAKV_METADATA_ENDPOINTS=host:port,...
//     stratakv-client                                        (动态拓扑)
#include <algorithm>
#include <cctype>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

#include <stratakv/client.h>

namespace {

volatile std::sig_atomic_t g_stop = 0;

void SignalHandler(int) {
  g_stop = 1;
}

const char* kHelp =
    "commands:\n"
    "  get <k>                          read a key\n"
    "  put <k> <v>                      write one key (auto-commit)\n"
    "  del <k>                          delete one key (auto-commit)\n"
    "  list [prefix]                    list entries (prefix sugar over scan)\n"
    "  scan <start> <end> [limit]       raw snapshot range, empty = unbounded\n"
    "  begin [lockTtlMs]                start a transaction session\n"
    "  commit                           commit the session transaction\n"
    "  rollback                         discard the session transaction\n"
    "  get-for-update <k>               locking read (session required)\n"
    "  batch-get-for-update <k>...      locking batch read (session required)\n"
    "  lock-keys <k>...                 lock keys without values (session required)\n"
    "  multi put <k> <v> [<k> <v>...]   one-shot atomic multi write\n"
    "  multi del <k> [<k>...]           one-shot atomic multi delete\n"
    "  txn-status                       query the session transaction's status\n"
    "  metrics                          client routing/retry counters\n"
    "  quit / exit                      exit (auto-rolls back an open session)";

std::string PrefixSuccessor(const std::string& prefix) {
  if (prefix.empty()) return "";
  std::string next = prefix;
  while (!next.empty()) {
    const char last = next.back();
    if (last != '\xff') {
      next.back() = static_cast<char>(last + 1);
      return next;
    }
    next.pop_back();
  }
  return "";
}

std::vector<std::string> SplitArgs(const std::string& line) {
  std::vector<std::string> args;
  std::istringstream stream(line);
  std::string token;
  while (stream >> token) args.push_back(token);
  return args;
}

std::string ToLower(std::string str) {
  std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return str;
}

stratakv::ConnectionOptions LoadOptions(int argc, char** argv) {
  stratakv::ConnectionOptions options;
  const char* mode = std::getenv("STRATAKV_TOPOLOGY_MODE");
  if (mode != nullptr && std::string(mode) == "dynamic") {
    options.topologyMode = stratakv::TopologyMode::kDynamic;
    if (const char* endpoints = std::getenv("STRATAKV_METADATA_ENDPOINTS")) {
      options.metadataEndpoints = endpoints;
    }
    if (const char* timeoutMs = std::getenv("STRATAKV_METADATA_TIMEOUT_MS")) {
      options.metadataTimeoutMs = std::stoull(timeoutMs);
    }
    return options;
  }
  options.topologyMode = stratakv::TopologyMode::kStatic;
  if (argc < 2) {
    std::cerr << "Usage: stratakv-client <regions.conf> [tso-endpoints]\n";
    std::exit(2);
  }
  options.regionConfigPath = argv[1];
  if (argc >= 3) options.tsoEndpoints = argv[2];
  return options;
}

// 冲突重试必须以新事务重新执行全部暂存操作。
template <typename Stage>
stratakv::Result RunWithRetry(stratakv::Client& client, Stage stage) {
  for (int attempt = 1;; ++attempt) {
    try {
      auto tx = client.Begin();
      const auto op = stage(tx);
      if (!op.ok()) {
        try { client.Rollback(tx); } catch (...) {}
        return op;
      }
      const auto commit = client.Commit(tx);
      if (commit.ok() || !commit.retryable || attempt >= 5) return commit;
    } catch (const std::exception& e) {
      stratakv::Result error;
      error.status = stratakv::Status::kUnavailable;
      error.message = e.what();
      return error;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5 * attempt));
  }
}

void PrintResult(const stratakv::Result& result) {
  if (result.ok()) {
    std::cout << "OK\n";
    return;
  }
  std::cout << "ERR " << stratakv::StatusName(result.status);
  if (!result.message.empty()) std::cout << " (" << result.message << ")";
  std::cout << '\n';
}

void PrintEntries(const stratakv::ScanResult& result) {
  for (const auto& [key, value] : result.entries) {
    std::cout << key << '\t' << value << '\n';
  }
  std::cout << "-- " << result.entries.size() << " entries\n";
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  std::shared_ptr<stratakv::Client> client;
  try {
    client = stratakv::Client::Connect(LoadOptions(argc, argv));
  } catch (const std::exception& e) {
    std::cerr << "connect failed: " << e.what() << '\n';
    return 1;
  }
  std::cout << "connected. type 'help' for commands.\n";

  std::shared_ptr<stratakv::Transaction> session;
  uint64_t sessionTtlMs = 120000;
  const bool interactive = isatty(STDIN_FILENO);
  std::string line;
  while (!g_stop) {
    if (interactive) std::cout << "stratakv> " << std::flush;
    if (!std::getline(std::cin, line)) {
      if (interactive) std::cout << '\n';
      break;
    }
    if (g_stop) break;

    const auto args = SplitArgs(line);
    if (args.empty()) continue;
    const std::string cmd = ToLower(args[0]);
    try {
      if (cmd == "quit" || cmd == "exit" || cmd == "q") {
        if (session) {
          try { client->Rollback(session); } catch (...) {}
          session.reset();
          std::cout << "notice: open session rolled back on exit\n";
        }
        break;
      }
      if (cmd == "help" || cmd == "?") {
        std::cout << kHelp << '\n';
        continue;
      }

      if (cmd == "begin") {
        if (session) {
          std::cout << "ERR session already active (commit or rollback first)\n";
          continue;
        }
        sessionTtlMs = args.size() >= 2 ? std::stoull(args[1]) : 120000;
        session = client->Begin(sessionTtlMs);
        std::cout << "OK session started (lockTtlMs=" << sessionTtlMs << ")\n";
        continue;
      }
      if (cmd == "commit") {
        if (!session) {
          std::cout << "ERR no active session; run 'begin' first\n";
          continue;
        }
        const auto result = client->Commit(session);
        PrintResult(result);
        if (result.ok() || !result.retryable) session.reset();
        continue;
      }
      if (cmd == "rollback") {
        if (!session) {
          std::cout << "ERR no active session; run 'begin' first\n";
          continue;
        }
        const auto result = client->Rollback(session);
        if (result.ok()) {
          std::cout << "OK rolled back\n";
        } else {
          PrintResult(result);
        }
        if (result.ok() || !result.retryable) session.reset();
        continue;
      }

      if (cmd == "get-for-update" || cmd == "batch-get-for-update" ||
          cmd == "lock-keys") {
        if (!session) {
          std::cout << "ERR no active session; run 'begin' first\n";
          continue;
        }
        if (args.size() < 2) {
          std::cout << "usage: " << cmd << " <key>...\n";
          continue;
        }
        const std::vector<std::string> keys(args.begin() + 1, args.end());
        if (cmd == "get-for-update") {
          const auto result = client->GetForUpdate(session, keys[0]);
          if (result.ok() && result.found) {
            std::cout << result.value << '\n';
          } else if (result.status == stratakv::Status::kNotFound ||
                     (result.ok() && !result.found)) {
            std::cout << "(not found)\n";
          } else {
            PrintResult(result);
          }
        } else if (cmd == "batch-get-for-update") {
          const auto result = client->BatchGetForUpdate(session, keys);
          for (const auto& [key, value] : result.values) {
            std::cout << key << '\t';
            if (value.ok() && value.found) {
              std::cout << value.value;
            } else if (value.status == stratakv::Status::kNotFound ||
                       (value.ok() && !value.found)) {
              std::cout << "(not found)";
            } else {
              std::cout << stratakv::StatusName(value.status);
            }
            std::cout << '\n';
          }
          stratakv::Result summary;
          summary.status = result.status;
          summary.message = result.message;
          PrintResult(summary);
        } else {
          PrintResult(client->LockKeys(session, keys));
        }
        continue;
      }

      if (cmd == "multi") {
        if (session) {
          std::cout << "ERR session active; multi runs its own transaction (rollback first)\n";
          continue;
        }
        if (args.size() < 3) {
          std::cout << "usage: multi put <k> <v> [<k> <v>...] | multi del <k> [<k>...]\n";
          continue;
        }
        const std::string subCmd = ToLower(args[1]);
        const bool isPut = subCmd == "put";
        const bool isDel = subCmd == "del" || subCmd == "delete";
        if (!isPut && !isDel) {
          std::cout << "usage: multi put <k> <v> ... | multi del <k> ...\n";
          continue;
        }
        if (isPut && (args.size() - 2) % 2 != 0) {
          std::cout << "usage: multi put requires <key> <value> pairs\n";
          continue;
        }
        const auto result = RunWithRetry(*client, [&](const auto& tx) {
          for (size_t i = 2; i < args.size();) {
            const auto staged = isPut
                ? client->Put(tx, args[i], args[i + 1])
                : client->Delete(tx, args[i]);
            if (!staged.ok()) return staged;
            i += isPut ? 2 : 1;
          }
          return stratakv::Result{};
        });
        PrintResult(result);
        continue;
      }

      if (cmd == "put" || cmd == "del" || cmd == "delete") {
        const bool isPut = cmd == "put";
        if (args.size() < (isPut ? 3U : 2U)) {
          std::cout << (isPut ? "usage: put <key> <value>\n"
                              : "usage: del <key>\n");
          continue;
        }
        if (session) {
          const auto result = isPut
              ? client->Put(session, args[1], args[2])
              : client->Delete(session, args[1]);
          if (result.ok()) {
            std::cout << "OK (staged in session)\n";
          } else {
            PrintResult(result);
          }
        } else {
          const auto result = RunWithRetry(*client, [&](const auto& tx) {
            return isPut ? client->Put(tx, args[1], args[2])
                         : client->Delete(tx, args[1]);
          });
          PrintResult(result);
        }
        continue;
      }

      if (cmd == "get") {
        if (args.size() < 2) {
          std::cout << "usage: get <key>\n";
          continue;
        }
        auto tx = session ? session : client->Begin();
        const auto result = client->Get(tx, args[1]);
        if (result.ok() && result.found) {
          std::cout << result.value << '\n';
          } else if (result.status == stratakv::Status::kNotFound ||
                     (result.ok() && !result.found)) {
          std::cout << "(not found)\n";
        } else {
          PrintResult(result);
        }
        if (!session && tx) {
          try { client->Rollback(tx); } catch (...) {}
        }
        continue;
      }

      if (cmd == "list" || cmd == "scan") {
        std::string start, end;
        size_t limit = 0;
        if (cmd == "list") {
          start = args.size() >= 2 ? args[1] : std::string();
          end = PrefixSuccessor(start);
        } else {
          if (args.size() < 3) {
            std::cout << "usage: scan <startKey> <endKey> [limit]\n";
            continue;
          }
          start = args[1];
          end = args[2];
          limit = args.size() >= 4 ? std::stoull(args[3]) : 0;
        }
        auto tx = session ? session : client->Begin();
        const auto result = client->Scan(tx, start, end, limit);
        if (result.ok()) {
          PrintEntries(result);
        } else {
          std::cout << "ERR " << stratakv::StatusName(result.status);
          if (!result.message.empty()) std::cout << " (" << result.message << ")";
          if (result.retryable) std::cout << " (retryable)";
          std::cout << '\n';
        }
        if (!session && tx) {
          try { client->Rollback(tx); } catch (...) {}
        }
        continue;
      }

      if (cmd == "txn-status") {
        if (!session) {
          std::cout << "ERR no active session; run 'begin' first\n";
          continue;
        }
        const auto result = client->QueryTransactionStatus(session);
        std::cout << "state=" << static_cast<int>(result.state)
                  << " commitTimestamp=" << result.commitTimestamp
                  << " status=" << stratakv::StatusName(result.status) << '\n';
        continue;
      }
      if (cmd == "metrics") {
        const auto metrics = client->Metrics();
        std::cout << "routingSends=" << metrics.routingSends
                  << " routingAttempts=" << metrics.routingAttempts
                  << " leaderRetries=" << metrics.leaderRetries
                  << " epochRefreshes=" << metrics.epochRefreshes
                  << " rollbackRegionCount=" << metrics.rollbackRegionCount << '\n';
        continue;
      }
      std::cout << "unknown command. type 'help' for commands.\n";
    } catch (const std::exception& e) {
      const std::string message = e.what();
      if (message.find("connect fail") != std::string::npos ||
          message.find("TSO") != std::string::npos ||
          message.find("RPC failed") != std::string::npos ||
          message.find("errno:111") != std::string::npos) {
        std::cout << "ERR cluster unavailable: " << message << '\n';
      } else {
        std::cout << "ERR " << message << '\n';
      }
    }
  }

  if (session) {
    try { client->Rollback(session); } catch (...) {}
  }
  return 0;
}
