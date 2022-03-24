#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <vector>

#include "util.h"

struct SendOptions {
  int threads = 4;
  int per_thread = 64;
  size_t small_size = 64;
  size_t medium_size = 1200;
  size_t large_size = 64000;
  bool zero_send_buf = true;
  float run_seconds = 5;
  int maxOutstanding = 16000;

  std::string host;
  bool ipv6 = true;
};

struct LatencyResult {
  std::chrono::microseconds p100 = {};
  std::chrono::microseconds p95 = {};
  std::chrono::microseconds p50 = {};
  std::chrono::microseconds avg = {};
  double count = 0.0 /* avg count done per burst */;

  static LatencyResult from(std::vector<std::chrono::microseconds>&& durations);
  void mergeIn(LatencyResult&& l);
  static LatencyResult avgMerge(std::vector<LatencyResult> const& bs);
  std::string toString() const;
};

struct SendResults {
  double packetsPerSecond = 0;
  double bytesPerSecond = 0;
  size_t connects = 0;
  size_t connectErrors = 0;
  size_t sendErrors = 0;
  size_t recvErrors = 0;
  LatencyResult latencies;
  std::vector<LatencyResult> burstResults;

  void mergeIn(SendResults&& b) {
    packetsPerSecond += b.packetsPerSecond;
    bytesPerSecond += b.bytesPerSecond;
    sendErrors += b.sendErrors;
    recvErrors += b.recvErrors;
    connectErrors += b.connectErrors;
    connects += b.connects;
    latencies.mergeIn(std::move(b.latencies));
    burstResults.insert(
        burstResults.end(), b.burstResults.begin(), b.burstResults.end());
  }

  std::string burstString() const {
    if (burstResults.empty()) {
      return {};
    }
    auto r = LatencyResult::avgMerge(burstResults);
    return strcat(" burst={", r.toString(), "}");
  }

  std::string latencyString() const {
    if (!latencies.count) {
      return {};
    }
    return strcat(" latency={", latencies.toString(), "}");
  }

  std::string toString() const {
    return strcat(
        "packetsPerSecond=",
        leftpad(strcat((int)(packetsPerSecond / 1000)), 7),
        "k bytesPerSecond=",
        leftpad(strcat((int)(bytesPerSecond / 1000000)), 5),
        "M connectErrors=",
        connectErrors,
        " sendErrors=",
        sendErrors,
        " recvErrors=",
        recvErrors,
        " connects=",
        connects,
        latencyString(),
        burstString());
  }
};

SendResults
runSender(std::string const& test, SendOptions const& options, uint16_t port);

std::vector<std::string> allScenarios();
