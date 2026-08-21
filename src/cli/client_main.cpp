// End-user CLI entrypoint for the public HTTP gateway.
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cctype>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

struct Endpoint {
  std::string host;
  std::string port;
};

struct Response {
  int status = 0;
  std::string body;
};

void Usage(const char* program) {
  std::cerr << "Usage: " << program << " <put|get|delete|demo|shell> [key] [value]\n"
            << "Set STRATAKV_GATEWAY (default: http://gateway:8080).\n";
}

Endpoint LoadEndpoint() {
  const char* configured = std::getenv("STRATAKV_GATEWAY");
  std::string value = configured == nullptr ? "http://gateway:8080" : configured;
  constexpr const char* prefix = "http://";
  if (value.rfind(prefix, 0) == 0) value.erase(0, std::char_traits<char>::length(prefix));
  const size_t separator = value.rfind(':');
  if (separator == std::string::npos || separator == 0 || separator + 1 == value.size() || value.find('/') != std::string::npos) {
    throw std::invalid_argument("STRATAKV_GATEWAY must be an http://host:port endpoint");
  }
  return {value.substr(0, separator), value.substr(separator + 1)};
}

std::string JsonEscape(const std::string& value) {
  std::string result;
  for (char c : value) {
    switch (c) {
      case '"': result += "\\\""; break;
      case '\\': result += "\\\\"; break;
      case '\n': result += "\\n"; break;
      case '\r': result += "\\r"; break;
      case '\t': result += "\\t"; break;
      default: result += c;
    }
  }
  return result;
}

std::string UrlEncode(const std::string& value) {
  static constexpr char hex[] = "0123456789ABCDEF";
  std::string result;
  for (unsigned char c : value) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      result += static_cast<char>(c);
    } else {
      result += '%';
      result += hex[(c >> 4) & 0x0f];
      result += hex[c & 0x0f];
    }
  }
  return result;
}

Response Request(const Endpoint& endpoint, const std::string& method, const std::string& path, const std::string& body = {}) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* resolved = nullptr;
  if (getaddrinfo(endpoint.host.c_str(), endpoint.port.c_str(), &hints, &resolved) != 0) {
    throw std::runtime_error("cannot resolve Gateway host " + endpoint.host);
  }
  int fd = -1;
  for (addrinfo* current = resolved; current != nullptr; current = current->ai_next) {
    fd = socket(current->ai_family, current->ai_socktype, current->ai_protocol);
    if (fd >= 0) {
      const timeval timeout{3, 0};
      setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
      setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    }
    if (fd >= 0 && connect(fd, current->ai_addr, current->ai_addrlen) == 0) break;
    if (fd >= 0) close(fd);
    fd = -1;
  }
  freeaddrinfo(resolved);
  if (fd < 0) throw std::runtime_error("cannot connect to Gateway " + endpoint.host + ':' + endpoint.port);

  std::ostringstream encoded;
  encoded << method << ' ' << path << " HTTP/1.1\r\nHost: " << endpoint.host << "\r\nConnection: close\r\n";
  if (!body.empty()) encoded << "Content-Type: application/json\r\nContent-Length: " << body.size() << "\r\n";
  encoded << "\r\n" << body;
  const std::string request = encoded.str();
  size_t sent = 0;
  while (sent < request.size()) {
    const ssize_t written = send(fd, request.data() + sent, request.size() - sent, 0);
    if (written <= 0) {
      close(fd);
      throw std::runtime_error("Gateway request write failed");
    }
    sent += static_cast<size_t>(written);
  }

  std::string received;
  char buffer[4096];
  while (true) {
    const ssize_t count = recv(fd, buffer, sizeof(buffer), 0);
    if (count == 0) break;
    if (count < 0) {
      close(fd);
      throw std::runtime_error("Gateway response read failed or timed out");
    }
    received.append(buffer, static_cast<size_t>(count));
  }
  close(fd);
  const size_t headerEnd = received.find("\r\n\r\n");
  if (headerEnd == std::string::npos) throw std::runtime_error("Gateway returned an invalid HTTP response");
  std::istringstream lineStream(received.substr(0, received.find("\r\n")));
  std::string version;
  Response response;
  lineStream >> version >> response.status;
  response.body = received.substr(headerEnd + 4);
  return response;
}

std::optional<std::string> JsonField(const std::string& body, const std::string& name) {
  const std::string token = "\"" + name + "\"";
  const size_t tokenPos = body.find(token);
  if (tokenPos == std::string::npos) return std::nullopt;
  size_t cursor = body.find(':', tokenPos + token.size());
  if (cursor == std::string::npos) return std::nullopt;
  ++cursor;
  while (cursor < body.size() && std::isspace(static_cast<unsigned char>(body[cursor]))) ++cursor;
  if (cursor >= body.size() || body[cursor] != '"') return std::nullopt;
  const size_t end = body.find('"', cursor + 1);
  if (end == std::string::npos) return std::nullopt;
  return body.substr(cursor + 1, end - cursor - 1);
}

std::string Begin(const Endpoint& endpoint) {
  const std::string body = "{\"lockTtlMs\":3000}";
  const Response response = Request(endpoint, "POST", "/v1/transactions", body);
  const auto id = JsonField(response.body, "id");
  if (response.status != 201 || !id) throw std::runtime_error("Begin failed: " + response.body);
  return *id;
}

void RequireOk(const Response& response, const std::string& operation) {
  const auto status = JsonField(response.body, "status");
  if ((response.status != 200 && response.status != 201) || !status || (*status != "OK" && *status != "ALREADY_COMMITTED")) {
    throw std::runtime_error(operation + " failed: " + response.body);
  }
}

void Put(const Endpoint& endpoint, const std::string& transactionId, const std::string& key, const std::string& value) {
  const std::string body = "{\"key\":\"" + JsonEscape(key) + "\",\"value\":\"" + JsonEscape(value) + "\"}";
  RequireOk(Request(endpoint, "POST", "/v1/transactions/" + transactionId + "/mutations", body), "Put");
}

void Delete(const Endpoint& endpoint, const std::string& transactionId, const std::string& key) {
  const std::string body = "{\"key\":\"" + JsonEscape(key) + "\",\"delete\":true}";
  RequireOk(Request(endpoint, "POST", "/v1/transactions/" + transactionId + "/mutations", body), "Delete");
}

void Commit(const Endpoint& endpoint, const std::string& transactionId) {
  RequireOk(Request(endpoint, "POST", "/v1/transactions/" + transactionId + "/commit"), "Commit");
}

void Rollback(const Endpoint& endpoint, const std::string& transactionId) {
  const Response response = Request(endpoint, "POST", "/v1/transactions/" + transactionId + "/rollback");
  if (response.status != 200) throw std::runtime_error("Rollback failed: " + response.body);
}

std::string Get(const Endpoint& endpoint, const std::string& transactionId, const std::string& key) {
  const Response response = Request(endpoint, "GET", "/v1/transactions/" + transactionId + "/keys/" + UrlEncode(key));
  const auto status = JsonField(response.body, "status");
  if (response.status != 200 || !status || *status != "OK") throw std::runtime_error("Get failed: " + response.body);
  const auto value = JsonField(response.body, "value");
  return value.value_or("");
}

std::string GetForUpdate(const Endpoint& endpoint, const std::string& transactionId, const std::string& key) {
  const Response response = Request(endpoint, "POST", "/v1/transactions/" + transactionId + "/keys/" + UrlEncode(key) + "/lock");
  const auto status = JsonField(response.body, "status");
  if (response.status != 200 || !status || (*status != "OK" && *status != "NOT_FOUND")) throw std::runtime_error("GetForUpdate failed: " + response.body);
  const auto value = JsonField(response.body, "value");
  return value.value_or("");
}

void LockKeys(const Endpoint& endpoint, const std::string& transactionId, const std::vector<std::string>& keys) {
  std::string body = "{\"keys\":[";
  for (size_t i = 0; i < keys.size(); ++i) {
    if (i > 0) body += ",";
    body += "\"" + JsonEscape(keys[i]) + "\"";
  }
  body += "]}";
  RequireOk(Request(endpoint, "POST", "/v1/transactions/" + transactionId + "/locks", body), "LockKeys");
}

void QueryStatus(const Endpoint& endpoint, const std::string& transactionId) {
  const Response response = Request(endpoint, "GET", "/v1/transactions/" + transactionId);
  std::cout << response.body << "\n";
}

std::string Trim(const std::string& value) {
  const size_t begin = value.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos) return "";
  const size_t end = value.find_last_not_of(" \t\r\n");
  return value.substr(begin, end - begin + 1);
}

bool TakeWord(std::string* input, std::string* word) {
  *input = Trim(*input);
  if (input->empty()) return false;
  const size_t separator = input->find_first_of(" \t");
  *word = input->substr(0, separator);
  *input = separator == std::string::npos ? "" : Trim(input->substr(separator + 1));
  return true;
}

// A long-lived user process avoids recreating a container for each command.
// Explicit begin/commit lets a batch share one distributed transaction; without
// begin, put/get/delete remain convenient single-operation transactions.
int RunShell(const Endpoint& endpoint) {
  const bool interactive = isatty(STDIN_FILENO) != 0;
  std::string transactionId;
  bool hadError = false;
  std::string line;
  if (interactive) {
    std::cout << "StrataKV user shell. Type help for commands.\n";
  }

  while (true) {
    if (interactive) {
      std::cout << "stratakv" << (transactionId.empty() ? "> " : " [txn]> ") << std::flush;
    }
    if (!std::getline(std::cin, line)) break;
    line = Trim(line);
    if (line.empty() || line[0] == '#') continue;

    std::string command;
    TakeWord(&line, &command);
    try {
      if (command == "help") {
        std::cout << "begin | put <key> <value> | get <key> | get-for-update <key> | lock <key>... | delete <key> | commit | rollback | status | quit\n";
      } else if (command == "quit" || command == "exit") {
        break;
      } else if (command == "status") {
        if (transactionId.empty()) {
          std::cout << "no active transaction\n";
        } else {
          QueryStatus(endpoint, transactionId);
        }
      } else if (command == "begin") {
        if (!transactionId.empty()) throw std::runtime_error("a transaction is already active; commit or rollback it first");
        transactionId = Begin(endpoint);
        std::cout << "OK transaction=" << transactionId << '\n';
      } else if (command == "commit") {
        if (transactionId.empty()) throw std::runtime_error("no active transaction");
        Commit(endpoint, transactionId);
        transactionId.clear();
        std::cout << "OK committed\n";
      } else if (command == "rollback") {
        if (transactionId.empty()) throw std::runtime_error("no active transaction");
        Rollback(endpoint, transactionId);
        transactionId.clear();
        std::cout << "OK rolled back\n";
      } else if (command == "get") {
        std::string key;
        if (!TakeWord(&line, &key)) throw std::runtime_error("usage: get <key>");
        const bool autoCommit = transactionId.empty();
        const std::string id = autoCommit ? Begin(endpoint) : transactionId;
        try {
          std::cout << Get(endpoint, id, key) << '\n';
          if (autoCommit) Commit(endpoint, id);
        } catch (...) {
          if (autoCommit) try { Rollback(endpoint, id); } catch (...) {}
          throw;
        }
      } else if (command == "get-for-update") {
        std::string key;
        if (!TakeWord(&line, &key)) throw std::runtime_error("usage: get-for-update <key>");
        const bool autoCommit = transactionId.empty();
        const std::string id = autoCommit ? Begin(endpoint) : transactionId;
        try {
          std::cout << GetForUpdate(endpoint, id, key) << '\n';
          if (autoCommit) Commit(endpoint, id);
        } catch (...) {
          if (autoCommit) try { Rollback(endpoint, id); } catch (...) {}
          throw;
        }
      } else if (command == "lock") {
        std::vector<std::string> keys;
        std::string key;
        while (TakeWord(&line, &key)) keys.push_back(key);
        if (keys.empty()) throw std::runtime_error("usage: lock <key1> [key2...]");
        const bool autoCommit = transactionId.empty();
        const std::string id = autoCommit ? Begin(endpoint) : transactionId;
        try {
          LockKeys(endpoint, id, keys);
          std::cout << "OK locked\n";
          if (autoCommit) Commit(endpoint, id);
        } catch (...) {
          if (autoCommit) try { Rollback(endpoint, id); } catch (...) {}
          throw;
        }
      } else if (command == "put") {
        std::string key;
        if (!TakeWord(&line, &key) || line.empty()) throw std::runtime_error("usage: put <key> <value>");
        const bool autoCommit = transactionId.empty();
        const std::string id = autoCommit ? Begin(endpoint) : transactionId;
        try {
          Put(endpoint, id, key, line);
          if (autoCommit) Commit(endpoint, id);
        } catch (...) {
          if (autoCommit) try { Rollback(endpoint, id); } catch (...) {}
          throw;
        }
        std::cout << "OK key=" << key << (autoCommit ? " committed" : " staged") << '\n';
      } else if (command == "delete") {
        const std::string key = Trim(line);
        if (key.empty()) throw std::runtime_error("usage: delete <key>");
        const bool autoCommit = transactionId.empty();
        if (autoCommit) transactionId = Begin(endpoint);
        try {
          Delete(endpoint, transactionId, key);
          if (autoCommit) {
            Commit(endpoint, transactionId);
            transactionId.clear();
          }
        } catch (...) {
          if (autoCommit && !transactionId.empty()) {
            try { Rollback(endpoint, transactionId); } catch (...) {}
            transactionId.clear();
          }
          throw;
        }
        std::cout << "OK deleted=" << key << (autoCommit ? " committed" : " staged") << '\n';
      } else if (command == "get") {
        const std::string key = Trim(line);
        if (key.empty()) throw std::runtime_error("usage: get <key>");
        const bool readOnly = transactionId.empty();
        if (readOnly) transactionId = Begin(endpoint);
        try {
          const std::string value = Get(endpoint, transactionId, key);
          if (readOnly) {
            Rollback(endpoint, transactionId);
            transactionId.clear();
          }
          std::cout << value << '\n';
        } catch (...) {
          if (readOnly && !transactionId.empty()) {
            try { Rollback(endpoint, transactionId); } catch (...) {}
            transactionId.clear();
          }
          throw;
        }
      } else {
        throw std::runtime_error("unknown command; type help");
      }
    } catch (const std::exception& error) {
      hadError = true;
      std::cerr << "ERROR " << error.what() << '\n';
    }
  }

  if (!transactionId.empty()) {
    try {
      Rollback(endpoint, transactionId);
      std::cerr << "Rolled back unfinished transaction.\n";
    } catch (const std::exception& error) {
      std::cerr << "Unable to roll back unfinished transaction: " << error.what() << '\n';
      return 1;
    }
  }
  return hadError ? 1 : 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    Usage(argv[0]);
    return 2;
  }
  try {
    const Endpoint endpoint = LoadEndpoint();
    const std::string command = argv[1];
    if (command == "shell" && argc == 2) {
      return RunShell(endpoint);
    }
    if (command == "put" && argc == 4) {
      const std::string transactionId = Begin(endpoint);
      try {
        Put(endpoint, transactionId, argv[2], argv[3]);
        Commit(endpoint, transactionId);
      } catch (...) {
        try { Rollback(endpoint, transactionId); } catch (...) {}
        throw;
      }
      std::cout << "OK key=" << argv[2] << '\n';
      return 0;
    }
    if (command == "delete" && argc == 3) {
      const std::string transactionId = Begin(endpoint);
      try {
        Delete(endpoint, transactionId, argv[2]);
        Commit(endpoint, transactionId);
      } catch (...) {
        try { Rollback(endpoint, transactionId); } catch (...) {}
        throw;
      }
      std::cout << "OK deleted=" << argv[2] << '\n';
      return 0;
    }
    if (command == "get" && argc == 3) {
      const std::string transactionId = Begin(endpoint);
      try {
        const std::string value = Get(endpoint, transactionId, argv[2]);
        Rollback(endpoint, transactionId);
        std::cout << value << '\n';
      } catch (...) {
        try { Rollback(endpoint, transactionId); } catch (...) {}
        throw;
      }
      return 0;
    }
    if (command == "demo" && argc == 2) {
      const std::string transactionId = Begin(endpoint);
      try {
        Put(endpoint, transactionId, "apple:user-demo", "reserved");
        Put(endpoint, transactionId, "hello:user-demo", "order-created");
        Put(endpoint, transactionId, "zoo:user-demo", "audit-written");
        Commit(endpoint, transactionId);
      } catch (...) {
        try { Rollback(endpoint, transactionId); } catch (...) {}
        throw;
      }
      std::cout << "OK committed one user transaction across Region 100, 101, and 102\n";
      return 0;
    }
  } catch (const std::exception& error) {
    std::cerr << "stratakv-client: " << error.what() << '\n';
    return 1;
  }
  Usage(argv[0]);
  return 2;
}
