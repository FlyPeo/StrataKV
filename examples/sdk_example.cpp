// Minimal StrataKV SDK consumer: one transactional put followed by a get.
// Build against a release package with:
//   cmake -S . -B build -DCMAKE_PREFIX_PATH=<package root>
//   cmake --build build
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>

#include <stratakv/client.h>

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: stratakv_sdk_example <regions.conf> <tso-endpoints>\n"
              << "  tso-endpoints example: 127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302\n";
    return 2;
  }

  stratakv::ConnectionOptions options;
  options.topologyMode = stratakv::TopologyMode::kStatic;
  options.regionConfigPath = argv[1];
  options.tsoEndpoints = argv[2];

  std::shared_ptr<stratakv::Client> client;
  try {
    client = stratakv::Client::Connect(options);
  } catch (const std::exception& error) {
    std::cerr << "connect failed: " << error.what() << '\n';
    return 1;
  }

  const std::string key = "sdk-example-key";
  const std::string value = "hello-stratakv";

  auto transaction = client->Begin();
  auto put = client->Put(transaction, key, value);
  if (!put.ok()) {
    std::cerr << "put failed: " << stratakv::StatusName(put.status) << '\n';
    return 1;
  }
  auto commit = client->Commit(transaction);
  if (!commit.ok()) {
    std::cerr << "commit failed: " << stratakv::StatusName(commit.status) << '\n';
    return 1;
  }

  auto read = client->Begin();
  auto get = client->Get(read, key);
  if (!get.ok() || !get.found || get.value != value) {
    std::cerr << "get mismatch: status=" << stratakv::StatusName(get.status)
              << " found=" << get.found << " value=" << get.value << '\n';
    client->Rollback(read);
    return 1;
  }
  client->Rollback(read);

  std::cout << "put/get round-trip OK: " << get.value << '\n';
  return 0;
}
