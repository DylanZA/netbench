#pragma once

#include <chrono>
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

struct SendResults {
  double packetsPerSecond = 0;
  double bytesPerSecond = 0;
  size_t connects = 0;
  size_t connectErrors = 0;
  size_t sendErrors = 0;
  size_t recvErrors = 0;

  static SendResults merge(SendResults const& a, SendResults const& b) {
    SendResults ret;
    ret.packetsPerSecond = a.packetsPerSecond + b.packetsPerSecond;
    ret.bytesPerSecond = a.bytesPerSecond + b.bytesPerSecond;
    ret.sendErrors = a.sendErrors + b.sendErrors;
    ret.recvErrors = a.recvErrors + b.recvErrors;
    ret.connectErrors = a.connectErrors + b.connectErrors;
    ret.connects = a.connects + b.connects;
    return ret;
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
        connects);
  }
};

SendResults
runSender(std::string const& test, SendOptions const& options, uint16_t port);

std::vector<std::string> allScenarios();
