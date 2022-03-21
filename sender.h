#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <vector>

#include "util.h"

struct SendOptions {
  int threads = 4;
  int per_thread = 128;
  size_t small_size = 64;
  size_t medium_size = 1200;
  size_t large_size = 64000;
  bool zero_send_buf = true;
  float run_seconds = 5;
  int maxOutstanding = 16000;

  std::string host;
  bool ipv6 = true;
};

struct BurstResult {
  std::chrono::microseconds p100 = {};
  std::chrono::microseconds p95 = {};
  std::chrono::microseconds p50 = {};
  std::chrono::microseconds avg = {};
  double count = 0.0 /* avg count done per burst */;

  static BurstResult avgMerge(std::vector<BurstResult> const& bs) {
    BurstResult ret;
    if (bs.empty()) {
      return ret;
    }
    for (auto& b : bs) {
      ret.p100 += b.p100;
      ret.p95 += b.p95;
      ret.p50 += b.p50;
      ret.avg += b.avg;
      ret.count += b.count;
    }
    ret.p100 /= bs.size();
    ret.p95 /= bs.size();
    ret.p50 /= bs.size();
    ret.avg /= bs.size();
    ret.count /= (double)bs.size();
    return ret;
  }
};

struct SendResults {
  double packetsPerSecond = 0;
  double bytesPerSecond = 0;
  size_t connects = 0;
  size_t connectErrors = 0;
  size_t sendErrors = 0;
  size_t recvErrors = 0;
  std::vector<BurstResult> burstResults;

  static SendResults merge(SendResults const& a, SendResults const& b) {
    SendResults ret;
    ret.packetsPerSecond = a.packetsPerSecond + b.packetsPerSecond;
    ret.bytesPerSecond = a.bytesPerSecond + b.bytesPerSecond;
    ret.sendErrors = a.sendErrors + b.sendErrors;
    ret.recvErrors = a.recvErrors + b.recvErrors;
    ret.connectErrors = a.connectErrors + b.connectErrors;
    ret.connects = a.connects + b.connects;
    ret.burstResults = a.burstResults;
    ret.burstResults.insert(
        ret.burstResults.end(), b.burstResults.begin(), b.burstResults.end());
    return ret;
  }

  std::string burstString() const {
    if (burstResults.empty()) {
      return {};
    }
    auto r = BurstResult::avgMerge(burstResults);
    return strcat(
        " burst_p95=",
        r.p95.count(),
        "us ",
        " burst_p50=",
        r.p50.count(),
        "us ",
        " burst_avg=",
        r.avg.count(),
        "us ",
        " burst_p100=",
        r.p100.count(),
        " burst_done_in_time=",
        r.count);
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
        burstString());
  }
};

SendResults
runSender(std::string const& test, SendOptions const& options, uint16_t port);

std::vector<std::string> allScenarios();
