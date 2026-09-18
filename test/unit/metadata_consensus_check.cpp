#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include "metadata_consensus.h"
#include "metadata_rpc.pb.h"

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

// One live member without quorum: elections fail, so every routed call takes
// the follower/non-leader path. This exercises waiter registration, cleanup,
// deadline handling, and bounded shutdown without real peer traffic.
std::unique_ptr<MetadataConsensusNode> MakeLoneNode(const std::string& directory) {
  return std::make_unique<MetadataConsensusNode>(
      0, std::vector<MetadataConsensusNode::Endpoint>{
             {"127.0.0.1", 27880}, {"127.0.0.1", 27881}, {"127.0.0.1", 27882}},
      directory, 64);
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-metadata-consensus-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) return 1;
  try {
    auto node = MakeLoneNode(std::string(temporaryDirectory) + "/node-0");
    node->Start();

    // A read on a non-leader must fail fast with a typed leader error and
    // never substitute an uncommitted local view.
    bool followerReadRejected = false;
    try {
      node->LinearizableView(std::chrono::steady_clock::now() + std::chrono::seconds(2));
    } catch (const MetadataNotLeaderError&) {
      followerReadRejected = true;
    }
    Require(followerReadRejected, "non-leader linearizable read must fail with NotLeader");

    // A mutation on a non-leader must clean up its proposal waiter.
    metadataRpcProtocol::MetadataCommand command;
    command.set_mutationid("consensus-check-1");
    command.set_expectedrevision(0);
    bool followerMutateRejected = false;
    try {
      node->Mutate(command, std::chrono::steady_clock::now() + std::chrono::seconds(2));
    } catch (const MetadataNotLeaderError&) {
      followerMutateRejected = true;
    }
    Require(followerMutateRejected, "non-leader mutation must fail with NotLeader");
    Require(node->PendingWaitersForTest() == 0,
            "rejected mutation must release its proposal waiter");

    // Bounded shutdown: Stop() joins the apply loop and resolves the node even
    // while the process holds no quorum.
    const auto stopBegin = std::chrono::steady_clock::now();
    node->Stop();
    const auto stopElapsed = std::chrono::steady_clock::now() - stopBegin;
    Require(node->IsStopped(), "Stop must mark the node stopped");
    Require(stopElapsed < std::chrono::seconds(5),
            "Stop must return in bounded time without quorum");

    // Calls after Stop fail fast instead of reaching a stopped apply loop.
    bool mutateAfterStopRejected = false;
    try {
      node->Mutate(command, std::chrono::steady_clock::now() + std::chrono::seconds(2));
    } catch (const MetadataNotLeaderError&) {
      mutateAfterStopRejected = true;
    }
    Require(mutateAfterStopRejected, "mutation after Stop must fail fast");

    // Restart from the same data directory restores an empty-but-valid view
    // and remains stoppable.
    auto restarted = MakeLoneNode(std::string(temporaryDirectory) + "/node-0");
    restarted->Start();
    Require(restarted->LocalView() != nullptr &&
                restarted->LocalView()->revision >= 0,
            "restart must restore a valid local view");
    restarted->Stop();

    std::filesystem::remove_all(temporaryDirectory);
    std::cout << "Metadata consensus checks passed" << std::endl;
    // All assertions passed. The nodes' Raft tickers are detached by design
    // (process-lifetime model) and keep running against the dead peer ports,
    // so a normal exit() would interact with live detached threads and hang.
    // Flush the result and terminate without static destruction.
    std::cout.flush();
    std::cerr.flush();
    std::_Exit(0);
  } catch (const std::exception& error) {
    std::filesystem::remove_all(temporaryDirectory);
    std::cerr << "Metadata consensus checks failed: " << error.what() << std::endl;
    return 1;
  }
}
