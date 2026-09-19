/*
 * 测试目标：验证 MetadataClient 的 Leader 重定向、端点轮转、mutation 重试去重与路由表校验。
 * 测试策略：每个场景挂一个进程内 muduo RPC 服务返回脚本化应答（NotLeader 提示、先超时后成功、
 *           带间隙的 Scan、永久 NotLeader），在没有真实共识集群的情况下驱动真实客户端网络路径。
 * 测试规模：5 个场景：Leader 重定向、传输失败端点轮转、响应丢失后 mutation 重放、间隙 Scan
 *           校验拒绝、超时耗尽。
 * 验证内容：重定向跟随 hint 指向的端点且 leaderRedirects 计数，传输失败轮转到下一端点并计为
 *           重试，同一 mutation id 重发拿到原始结果且服务端恰好收到 2 次，跨键空间间隙的 Scan 被
 *           客户端校验拒绝并计 validationFailures，持续失败时返回类型化超时错误。
 */
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "metadata_client.h"
#include "metadata_rpc.pb.h"
#include "region.pb.h"
#include "rpc_provider.h"

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

stratakv::region::RegionDescriptor RegionProto(int id, const std::string& start,
                                               const std::string& end, uint64_t revision) {
  stratakv::region::RegionDescriptor descriptor;
  descriptor.set_regionid(static_cast<uint64_t>(id));
  descriptor.set_startkey(start);
  descriptor.set_endkey(end);
  descriptor.mutable_epoch()->set_version(1);
  descriptor.mutable_epoch()->set_confversion(1);
  descriptor.set_metadatarevision(revision);
  auto* peer = descriptor.add_peers();
  peer->set_peerid(static_cast<uint64_t>(id * 10 + 1));
  peer->set_storeid(1);
  peer->set_host("127.0.0.1");
  peer->set_port(26000);
  descriptor.set_leaderpeerid(peer->peerid());
  return descriptor;
}

// Scripted metadata RPC service. Every scenario serves deterministic replies
// from one process-local listener so MetadataClient retry behavior is tested
// without a real consensus cluster.
class ScriptedMetadataService final : public metadataRpcProtocol::metadataRpc {
 public:
  enum class Mode {
    kLookupNotLeader,
    kLookupOk,
    kMutateTimeoutThenOk,
    kMutateAlwaysNotLeader,
    kScanOk,
    kScanGap,
  };

  explicit ScriptedMetadataService(Mode mode) : mode_(mode) {}

  void Mutate(google::protobuf::RpcController*, const metadataRpcProtocol::MutateRequest* request,
              metadataRpcProtocol::MutateReply* reply, google::protobuf::Closure* done) override {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string mutationId = request->command().mutationid();
    ++seenMutations_[mutationId];
    auto* result = reply->mutable_result();
    switch (mode_) {
      case Mode::kMutateTimeoutThenOk:
        if (seenMutations_[mutationId] == 1) {
          result->set_error(metadataRpcProtocol::METADATA_TIMEOUT);
          result->set_message("response lost after commit");
        } else {
          result->set_error(metadataRpcProtocol::METADATA_OK);
          result->set_revision(7);
        }
        break;
      case Mode::kMutateAlwaysNotLeader:
        reply->set_leaderid(0);
        result->set_error(metadataRpcProtocol::METADATA_NOT_LEADER);
        result->set_message("scripted not leader");
        break;
      default:
        break;
    }
    done->Run();
  }

  void LookupKey(google::protobuf::RpcController*,
                 const metadataRpcProtocol::LookupKeyRequest*,
                 metadataRpcProtocol::RegionReply* reply,
                 google::protobuf::Closure* done) override {
    switch (mode_) {
      case Mode::kLookupNotLeader:
        reply->set_error(metadataRpcProtocol::METADATA_NOT_LEADER);
        reply->set_message("scripted redirect");
        reply->set_leaderid(1);
        break;
      case Mode::kLookupOk: {
        reply->set_error(metadataRpcProtocol::METADATA_OK);
        reply->set_revision(3);
        reply->set_leaderid(1);
        *reply->mutable_region() = RegionProto(100, "a", "m", 3);
        break;
      }
      default:
        break;
    }
    done->Run();
  }

  void LookupRegion(google::protobuf::RpcController*,
                    const metadataRpcProtocol::LookupRegionRequest*,
                    metadataRpcProtocol::RegionReply* reply,
                    google::protobuf::Closure* done) override {
    reply->set_error(metadataRpcProtocol::METADATA_NOT_FOUND);
    reply->set_message("unused in this test");
    done->Run();
  }

  void ScanRegions(google::protobuf::RpcController*,
                   const metadataRpcProtocol::ScanRegionsRequest*,
                   metadataRpcProtocol::ScanRegionsReply* reply,
                   google::protobuf::Closure* done) override {
    reply->set_error(metadataRpcProtocol::METADATA_OK);
    reply->set_revision(4);
    reply->set_leaderid(1);
    *reply->add_regions() = RegionProto(100, "", "b", 4);
    if (mode_ == Mode::kScanOk) {
      *reply->add_regions() = RegionProto(101, "b", "", 4);
    } else {
      *reply->add_regions() = RegionProto(101, "c", "", 4);
    }
    done->Run();
  }

  void Status(google::protobuf::RpcController*, const metadataRpcProtocol::MetadataStatusRequest*,
              metadataRpcProtocol::MetadataStatusReply* reply,
              google::protobuf::Closure* done) override {
    reply->set_nodeid(0);
    done->Run();
  }

  size_t MutationCount(const std::string& mutationId) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = seenMutations_.find(mutationId);
    return found == seenMutations_.end() ? 0 : found->second;
  }

 private:
  Mode mode_;
  mutable std::mutex mutex_;
  std::map<std::string, size_t> seenMutations_;
};

// Owns the muduo EventLoop on one dedicated thread, mirroring meta_main.
class FakeServer {
 public:
  FakeServer(short port, ScriptedMetadataService::Mode mode) : service_(mode) {
    std::thread thread([this, port]() {
      RpcProvider provider;
      provider.NotifyService(&service_);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        provider_ = &provider;
        published_ = true;
      }
      ready_.notify_all();
      provider.Run(0, port);
    });
    thread_ = std::move(thread);
    std::unique_lock<std::mutex> lock(mutex_);
    ready_.wait(lock, [this] { return published_; });
  }

  ~FakeServer() {
    if (provider_ != nullptr) provider_->Stop();
    thread_.join();
  }

  ScriptedMetadataService& service() { return service_; }

 private:
  ScriptedMetadataService service_;
  RpcProvider* provider_ = nullptr;
  bool published_ = false;
  std::mutex mutex_;
  std::condition_variable ready_;
  std::thread thread_;
};

constexpr std::chrono::seconds kDeadline{5};

}  // namespace

int main() {
  try {
    // Leader redirect: the first endpoint declines with a hint, the hinted
    // endpoint answers, and the client keeps one lookup deadline.
    {
      FakeServer first(27790, ScriptedMetadataService::Mode::kLookupNotLeader);
      FakeServer second(27791, ScriptedMetadataService::Mode::kLookupOk);
      MetadataClient client("127.0.0.1:27790,127.0.0.1:27791");
      uint64_t revision = 0;
      const RegionMetadata region =
          client.LookupKey("apple", std::chrono::steady_clock::now() + kDeadline, &revision);
      Require(region.regionId == 100 && revision == 3,
              "redirected lookup must return the hinted descriptor");
      Require(client.Metrics().leaderRedirects >= 1,
              "leader hint change must be observed");
      Require(second.service().MutationCount("unused") == 0, "sanity");
    }

    // Transport failure: the dead endpoint rotates to the live one.
    {
      FakeServer live(27792, ScriptedMetadataService::Mode::kLookupOk);
      MetadataClient client("127.0.0.1:27795,127.0.0.1:27792");
      const RegionMetadata region =
          client.LookupKey("apple", std::chrono::steady_clock::now() + kDeadline, nullptr);
      Require(region.regionId == 100, "transport failure must rotate to a live endpoint");
      Require(client.Metrics().retries >= 1, "transport failure must count as a retry");
    }

    // Duplicate retry: a mutation whose response is lost is re-sent with the
    // same mutation identifier and returns the committed result.
    {
      FakeServer server(27793, ScriptedMetadataService::Mode::kMutateTimeoutThenOk);
      MetadataClient client("127.0.0.1:27793");
      metadataRpcProtocol::MetadataCommand command;
      command.set_mutationid("mut-dup-1");
      command.set_expectedrevision(0);
      const auto result =
          client.Mutate(command, std::chrono::steady_clock::now() + kDeadline);
      Require(result.error() == metadataRpcProtocol::METADATA_OK && result.revision() == 7,
              "retry with a stable mutation ID must return the committed result");
      Require(server.service().MutationCount("mut-dup-1") == 2,
              "the retried mutation must reuse the same mutation identifier");
    }

    // Malformed topology: a scan with a gap fails validation and never
    // reaches the caller as routing data.
    {
      FakeServer server(27794, ScriptedMetadataService::Mode::kScanGap);
      MetadataClient client("127.0.0.1:27794");
      bool rejected = false;
      try {
        client.Scan("a", 10, std::chrono::steady_clock::now() + kDeadline, nullptr);
      } catch (const std::invalid_argument&) {
        rejected = true;
      }
      Require(rejected, "gap scan must be rejected by client validation");
      Require(client.Metrics().validationFailures >= 1,
              "malformed topology must be counted as a validation failure");
    }

    // Retry exhaustion: a permanent NotLeader budget exhausts into a timeout
    // result instead of retrying forever.
    {
      FakeServer server(27796, ScriptedMetadataService::Mode::kMutateAlwaysNotLeader);
      MetadataClient client("127.0.0.1:27796");
      metadataRpcProtocol::MetadataCommand command;
      command.set_mutationid("mut-exhaust-1");
      command.set_expectedrevision(0);
      const auto result = client.Mutate(
          command, std::chrono::steady_clock::now() + std::chrono::milliseconds(700));
      Require(result.error() == metadataRpcProtocol::METADATA_TIMEOUT,
              "permanent NotLeader must exhaust into a timeout result");
      Require(client.Metrics().retries > 0 && client.Metrics().timeouts >= 1,
              "exhaustion must be observable in client metrics");
    }

    std::cout << "Metadata client checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Metadata client checks failed: " << error.what() << std::endl;
    return 1;
  }
}
