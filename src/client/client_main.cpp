// stratakv-client — 面向业务使用的常驻命令行客户端。
// 与 stratakv-admin(底层 Region 运维工具)不同,本工具只做业务读写,
// 全部命令走事务 SDK:连接一次常驻复用;SDK 能力全量暴露——
// 事务会话(begin/commit/rollback)、跨分片原子写(multi)、悲观锁读
// (get-for-update/batch-get-for-update/lock-keys)、范围扫描(scan/list)、
// 事务状态查询(txn-status)与客户端指标(metrics)。
//
// 用法:
//   stratakv-client <regions.conf> [tso-endpoints]          (静态拓扑)
//   STRATAKV_TOPOLOGY_MODE=dynamic STRATAKV_METADATA_ENDPOINTS=host:port,... \
//     stratakv-client                                        (动态拓扑)
#include <algorithm>
#include <cctype>
#include <chrono>
#include <csignal>
#include <cstdlib>
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
    "  quit                             exit (auto-rolls back an open session)";
    "  quit / exit                      exit (auto-rolls back an open session)";

// 返回前缀之后最小的字符串;空结果表示前缀已覆盖到键空间尽头。
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

// 单键自动提交写与 multi 写共用的冲突重试:冲突意味着事务作废,
// 必须换全新事务重做全部暂存。
template <typename Stage>
stratakv::Result RunWithRetry(stratakv::Client& client, Stage stage) {
  for (int attempt = 1;; ++attempt) {
    auto tx = client.Begin();
    const auto op = stage(tx);
    if (!op.ok()) {
      client.Rollback(tx);
      return op;
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
      stratakv::Result errResult;
      errResult.status = stratakv::Status::kUnavailable;
      errResult.message = e.what();
      return errResult;
    }
    const auto commit = client.Commit(tx);
    if (commit.ok() || !commit.retryable || attempt >= 5) return commit;
    std::this_thread::sleep_for(std::chrono::milliseconds(5 * attempt));
  }
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

  const stratakv::ConnectionOptions options = LoadOptions(argc, argv);

  std::shared_ptr<stratakv::Client> client;
  try {
    client = stratakv::Client::Connect(options);
  } catch (const std::exception& e) {
    std::cerr << "connect failed: " << e.what() << '\n';
    return 1;
  }
  std::cout << "connected. type 'help' for commands.\n";

  std::shared_ptr<stratakv::Transaction> session;
  uint64_t sessionTtlMs = 120000;

  const auto requireSession = [&]() -> const std::shared_ptr<stratakv::Transaction>& {
    static const std::shared_ptr<stratakv::Transaction> none;
    if (!session) std::cout << "ERR no active session; run 'begin' first\n";
    return session ? session : none;
  };

  std::string line;
  while (std::getline(std::cin, line)) {
  const bool interactive = isatty(STDIN_FILENO);

  while (!g_stop) {
    if (interactive) {
      std::cout << "stratakv> " << std::flush;
    }
    if (!std::getline(std::cin, line)) {
      if (interactive) std::cout << '\n';
      break;
    }
    if (g_stop) break;

    const auto args = SplitArgs(line);
    if (args.empty()) continue;
    const std::string& cmd = args[0];
    const std::string cmd = ToLower(args[0]);

    if (cmd == "quit" || cmd == "exit") {
    if (cmd == "quit" || cmd == "exit" || cmd == "q") {
      if (session) {
        client->Rollback(session);
        try { client->Rollback(session); } catch (...) {}
        session.reset();
        std::cout << "notice: open session rolled back on exit\n";
      }
      break;
    }
    if (cmd == "help") {
    if (cmd == "help" || cmd == "?") {
      std::cout << kHelp << '\n';
      continue;
    }

    // ---- 会话管理 -------------------------------------------------------
    if (cmd == "begin") {
      if (session) {
        std::cout << "ERR session already active (commit or rollback first)\n";
    try {
      // ---- 会话管理 -------------------------------------------------------
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
      sessionTtlMs = args.size() >= 2 ? std::stoull(args[1]) : 120000;
      session = client->Begin(sessionTtlMs);
      std::cout << "OK session started (lockTtlMs=" << sessionTtlMs << ")\n";
      continue;
    }
    if (cmd == "commit") {
      if (!session) {
        std::cout << "ERR no active session; run 'begin' first\n";
      if (cmd == "commit") {
        if (!session) {
          std::cout << "ERR no active session; run 'begin' first\n";
          continue;
        }
        const auto r = client->Commit(session);
        if (r.ok()) {
          std::cout << "OK\n";
        } else {
          std::cout << "ERR " << stratakv::StatusName(r.status);
          if (!r.message.empty()) std::cout << " (" << r.message << ")";
          std::cout << '\n';
        }
        if (r.ok() || !r.retryable) session.reset();
        continue;
      }
      const auto r = client->Commit(session);
      std::cout << (r.ok() ? "OK" : std::string("ERR ") + stratakv::StatusName(r.status)) << '\n';
      if (r.ok() || !r.retryable) session.reset();
      continue;
    }
    if (cmd == "rollback") {
      if (!session) {
        std::cout << "ERR no active session; run 'begin' first\n";
      if (cmd == "rollback") {
        if (!session) {
          std::cout << "ERR no active session; run 'begin' first\n";
          continue;
        }
        client->Rollback(session);
        session.reset();
        std::cout << "OK rolled back\n";
        continue;
      }
      client->Rollback(session);
      session.reset();
      std::cout << "OK rolled back\n";
      continue;
    }

    // ---- 锁操作(要求会话)----------------------------------------------
    if (cmd == "get-for-update" || cmd == "batch-get-for-update" || cmd == "lock-keys") {
      const auto& tx = requireSession();
      if (!tx) continue;
      if (args.size() < 2) {
        std::cout << "usage: " << cmd << " <key>...\n";
      // ---- 锁操作(要求会话)----------------------------------------------
      if (cmd == "get-for-update" || cmd == "batch-get-for-update" || cmd == "lock-keys") {
        const auto& tx = requireSession();
        if (!tx) continue;
        if (args.size() < 2) {
          std::cout << "usage: " << cmd << " <key>...\n";
          continue;
        }
        const std::vector<std::string> keys(args.begin() + 1, args.end());
        if (cmd == "get-for-update") {
          const auto r = client->GetForUpdate(tx, keys[0]);
          if (r.ok() && r.found) {
            std::cout << r.value << '\n';
          } else if (r.status == stratakv::Status::kNotFound) {
            std::cout << "(not found)\n";
          } else {
            std::cout << "ERR " << stratakv::StatusName(r.status);
            if (!r.message.empty()) std::cout << " (" << r.message << ")";
            std::cout << '\n';
          }
        } else if (cmd == "batch-get-for-update") {
          const auto r = client->BatchGetForUpdate(tx, keys);
          for (const auto& [key, result] : r.values) {
            std::cout << key << '\t'
                      << (result.ok() && result.found ? result.value : stratakv::StatusName(result.status))
                      << '\n';
          }
          if (r.status == stratakv::Status::kOk) {
            std::cout << "OK\n";
          } else {
            std::cout << "ERR " << stratakv::StatusName(r.status);
            if (!r.message.empty()) std::cout << " (" << r.message << ")";
            std::cout << '\n';
          }
        } else {
          const auto r = client->LockKeys(tx, keys);
          if (r.ok()) {
            std::cout << "OK\n";
          } else {
            std::cout << "ERR " << stratakv::StatusName(r.status);
            if (!r.message.empty()) std::cout << " (" << r.message << ")";
            std::cout << '\n';
          }
        }
        continue;
      }
      const std::vector<std::string> keys(args.begin() + 1, args.end());
      if (cmd == "get-for-update") {
        const auto r = client->GetForUpdate(tx, keys[0]);
        if (r.ok() && r.found) std::cout << r.value << '\n';
        else if (r.status == stratakv::Status::kNotFound) std::cout << "(not found)\n";
        else std::cout << "ERR " << stratakv::StatusName(r.status) << '\n';
      } else if (cmd == "batch-get-for-update") {
        const auto r = client->BatchGetForUpdate(tx, keys);
        for (const auto& [key, result] : r.values) {
          std::cout << key << '\t'
                    << (result.ok() && result.found ? result.value : stratakv::StatusName(result.status))
                    << '\n';

      // ---- multi 一次性原子写 ---------------------------------------------
      if (cmd == "multi") {
        if (session) {
          std::cout << "ERR session active; multi runs its own transaction (rollback first)\n";
          continue;
        }
        std::cout << (r.status == stratakv::Status::kOk
                          ? "OK"
                          : std::string("ERR ") + stratakv::StatusName(r.status))
                  << '\n';
      } else {
        const auto r = client->LockKeys(tx, keys);
        std::cout << (r.ok() ? "OK" : std::string("ERR ") + stratakv::StatusName(r.status)) << '\n';
        if (args.size() < 3) {
          std::cout << "usage: multi put <k> <v> [<k> <v>...] | multi del <k> [<k>...]\n";
          continue;
        }
        const std::string subCmd = ToLower(args[1]);
        const bool isPut = (subCmd == "put");
        const bool isDel = (subCmd == "del" || subCmd == "delete");
        if (!isPut && !isDel) {
          std::cout << "usage: multi put <k> <v> ... | multi del <k> ...\n";
          continue;
        }
        if (isPut && (args.size() - 2) % 2 != 0) {
          std::cout << "usage: multi put requires <key> <value> pairs\n";
          continue;
        }
        const auto r = RunWithRetry(*client, [&](const std::shared_ptr<stratakv::Transaction>& tx) {
          for (size_t i = 2; i < args.size();) {
            if (isPut) {
              client->Put(tx, args[i], args[i + 1]);
              i += 2;
            } else {
              client->Delete(tx, args[i]);
              i += 1;
            }
          }
          return stratakv::Result{};
        });
        if (r.ok()) {
          std::cout << "OK\n";
        } else {
          std::cout << "ERR " << stratakv::StatusName(r.status);
          if (!r.message.empty()) std::cout << " (" << r.message << ")";
          std::cout << '\n';
        }
        continue;
      }
      continue;
    }

    // ---- multi 一次性原子写 ---------------------------------------------
    if (cmd == "multi") {
      if (session) {
        std::cout << "ERR session active; multi runs its own transaction (rollback first)\n";
      // ---- 会话内暂存,会话外自动提交 --------------------------------------
      if (cmd == "put") {
        if (args.size() < 3) {
          std::cout << "usage: put <key> <value>\n";
          continue;
        }
        if (session) {
          const auto r = client->Put(session, args[1], args[2]);
          if (r.ok()) {
            std::cout << "OK (staged in session)\n";
          } else {
            std::cout << "ERR " << stratakv::StatusName(r.status);
            if (!r.message.empty()) std::cout << " (" << r.message << ")";
            std::cout << '\n';
          }
        } else {
          const auto r = RunWithRetry(*client,
              [&](const std::shared_ptr<stratakv::Transaction>& tx) {
                return client->Put(tx, args[1], args[2]);
              });
          if (r.ok()) {
            std::cout << "OK\n";
          } else {
            std::cout << "ERR " << stratakv::StatusName(r.status);
            if (!r.message.empty()) std::cout << " (" << r.message << ")";
            std::cout << '\n';
          }
        }
        continue;
      }
      if (args.size() < 3) {
        std::cout << "usage: multi put <k> <v> [<k> <v>...] | multi del <k> [<k>...]\n";
      if (cmd == "del" || cmd == "delete") {
        if (args.size() < 2) {
          std::cout << "usage: del <key>\n";
          continue;
        }
        if (session) {
          const auto r = client->Delete(session, args[1]);
          if (r.ok()) {
            std::cout << "OK (staged in session)\n";
          } else {
            std::cout << "ERR " << stratakv::StatusName(r.status);
            if (!r.message.empty()) std::cout << " (" << r.message << ")";
            std::cout << '\n';
          }
        } else {
          const auto r = RunWithRetry(*client,
              [&](const std::shared_ptr<stratakv::Transaction>& tx) {
                return client->Delete(tx, args[1]);
              });
          if (r.ok()) {
            std::cout << "OK\n";
          } else {
            std::cout << "ERR " << stratakv::StatusName(r.status);
            if (!r.message.empty()) std::cout << " (" << r.message << ")";
            std::cout << '\n';
          }
        }
        continue;
      }
      const bool isPut = args[1] == "put";
      const bool isDel = args[1] == "del";
      if (!isPut && !isDel) {
        std::cout << "usage: multi put <k> <v> ... | multi del <k> ...\n";
      if (cmd == "get") {
        if (args.size() < 2) {
          std::cout << "usage: get <key>\n";
          continue;
        }
        auto ownTx = session ? session : client->Begin();  // 会话内 = read-your-writes
        const auto r = client->Get(ownTx, args[1]);
        if (r.ok() && r.found) {
          std::cout << r.value << '\n';
        } else if (r.status == stratakv::Status::kNotFound) {
          std::cout << "(not found)\n";
        } else {
          std::cout << "ERR " << stratakv::StatusName(r.status);
          if (!r.message.empty()) std::cout << " (" << r.message << ")";
          std::cout << '\n';
        }
        if (!session && ownTx) {
          try { client->Rollback(ownTx); } catch (...) {}
        }
        continue;
      }
      if (isPut && (args.size() - 2) % 2 != 0) {
        std::cout << "usage: multi put requires <key> <value> pairs\n";
        continue;
      }
      const auto r = RunWithRetry(*client, [&](const std::shared_ptr<stratakv::Transaction>& tx) {
        for (size_t i = 2; i < args.size();) {
          if (isPut) {
            client->Put(tx, args[i], args[i + 1]);
            i += 2;
          } else {
            client->Delete(tx, args[i]);
            i += 1;

      // ---- 范围扫描 --------------------------------------------------------
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
        return stratakv::Result{};
      });
      std::cout << (r.ok() ? "OK" : std::string("ERR ") + stratakv::StatusName(r.status)) << '\n';
      continue;
    }

    // ---- 会话内暂存,会话外自动提交 --------------------------------------
    if (cmd == "put") {
      if (args.size() < 3) {
        std::cout << "usage: put <key> <value>\n";
        auto ownTx = session ? session : client->Begin();  // 会话内 = 会话快照
        const auto r = client->Scan(ownTx, start, end, limit);
        if (!r.ok()) {
          std::cout << "ERR " << stratakv::StatusName(r.status)
                    << (r.message.empty() ? "" : (" (" + r.message + ")"))
                    << (r.retryable ? " (retryable)" : "") << '\n';
        } else {
          PrintEntries(r);
        }
        if (!session && ownTx) {
          try { client->Rollback(ownTx); } catch (...) {}
        }
        continue;
      }
      if (session) {
        client->Put(session, args[1], args[2]);
        std::cout << "OK (staged in session)\n";
      } else {
        const auto r = RunWithRetry(*client,
            [&](const std::shared_ptr<stratakv::Transaction>& tx) {
              return client->Put(tx, args[1], args[2]);
            });
        std::cout << (r.ok() ? "OK" : std::string("ERR ") + stratakv::StatusName(r.status)) << '\n';
      }
      continue;
    }
    if (cmd == "del") {
      if (args.size() < 2) {
        std::cout << "usage: del <key>\n";

      // ---- 观测 ------------------------------------------------------------
      if (cmd == "txn-status") {
        const auto& tx = requireSession();
        if (!tx) continue;
        const auto r = client->QueryTransactionStatus(tx);
        std::cout << "state=" << static_cast<int>(r.state)
                  << " commitTimestamp=" << r.commitTimestamp
                  << " status=" << stratakv::StatusName(r.status) << '\n';
        continue;
      }
      if (session) {
        client->Delete(session, args[1]);
        std::cout << "OK (staged in session)\n";
      } else {
        const auto r = RunWithRetry(*client,
            [&](const std::shared_ptr<stratakv::Transaction>& tx) {
              return client->Delete(tx, args[1]);
            });
        std::cout << (r.ok() ? "OK" : std::string("ERR ") + stratakv::StatusName(r.status)) << '\n';
      }
      continue;
    }
    if (cmd == "get") {
      if (args.size() < 2) {
        std::cout << "usage: get <key>\n";
      if (cmd == "metrics") {
        const auto m = client->Metrics();
        std::cout << "routingSends=" << m.routingSends
                  << " routingAttempts=" << m.routingAttempts
                  << " leaderRetries=" << m.leaderRetries
                  << " epochRefreshes=" << m.epochRefreshes
                  << " rollbackRegionCount=" << m.rollbackRegionCount << '\n';
        continue;
      }
      auto ownTx = session ? session : client->Begin();  // 会话内 = read-your-writes
      const auto r = client->Get(ownTx, args[1]);
      if (r.ok() && r.found) std::cout << r.value << '\n';
      else if (r.status == stratakv::Status::kNotFound) std::cout << "(not found)\n";
      else std::cout << "ERR " << stratakv::StatusName(r.status) << '\n';
      if (!session) client->Rollback(ownTx);
      continue;
    }

    // ---- 范围扫描 --------------------------------------------------------
    if (cmd == "list" || cmd == "scan") {
      std::string start, end;
      size_t limit = 0;
      if (cmd == "list") {
        start = args.size() >= 2 ? args[1] : std::string();
        end = PrefixSuccessor(start);
      std::cout << "unknown command. type 'help' for commands.\n";
    } catch (const std::exception& e) {
      const std::string msg = e.what();
      if (msg.find("connect fail") != std::string::npos ||
          msg.find("TSO") != std::string::npos ||
          msg.find("RPC failed") != std::string::npos ||
          msg.find("errno:111") != std::string::npos) {
        std::cout << "ERR cluster unavailable: " << msg << '\n';
      } else {
        if (args.size() < 3) {
          std::cout << "usage: scan <startKey> <endKey> [limit]\n";
          continue;
        }
        start = args[1];
        end = args[2];
        limit = args.size() >= 4 ? std::stoull(args[3]) : 0;
        std::cout << "ERR " << msg << '\n';
      }
      auto ownTx = session ? session : client->Begin();  // 会话内 = 会话快照
      const auto r = client->Scan(ownTx, start, end, limit);
      if (!r.ok()) {
        std::cout << "ERR " << stratakv::StatusName(r.status)
                  << (r.retryable ? " (retryable)" : "") << '\n';
      } else {
        PrintEntries(r);
      }
      if (!session) client->Rollback(ownTx);
      continue;
    }
  }

    // ---- 观测 ------------------------------------------------------------
    if (cmd == "txn-status") {
      const auto& tx = requireSession();
      if (!tx) continue;
      const auto r = client->QueryTransactionStatus(tx);
      std::cout << "state=" << static_cast<int>(r.state)
                << " commitTimestamp=" << r.commitTimestamp
                << " status=" << stratakv::StatusName(r.status) << '\n';
      continue;
    }
    if (cmd == "metrics") {
      const auto m = client->Metrics();
      std::cout << "routingSends=" << m.routingSends
                << " routingAttempts=" << m.routingAttempts
                << " leaderRetries=" << m.leaderRetries
                << " epochRefreshes=" << m.epochRefreshes
                << " rollbackRegionCount=" << m.rollbackRegionCount << '\n';
      continue;
    }

    std::cout << "unknown command. type 'help' for commands.\n";
  if (session) {
    try { client->Rollback(session); } catch (...) {}
    session.reset();
  }
  return 0;
}
