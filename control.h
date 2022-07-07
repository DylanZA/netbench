
#pragma once

#include <memory>
#include <string>
#include <unordered_map>

std::unordered_map<uint16_t, std::string>
getPortNameMap(std::string host, uint16_t port, bool ipv6);

class IControlServer {
 public:
  virtual ~IControlServer() = default;
};

std::unique_ptr<IControlServer> makeControlServer(
    std::unordered_map<uint16_t, std::string> data,
    uint16_t port,
    bool ipv6);
