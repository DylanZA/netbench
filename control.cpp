

#include "control.h"
#include <poll.h>
#include <atomic>
#include <cstring>
#include <thread>
#include <vector>

#include "socket.h"
#include "util.h"

namespace {

template <class T>
void addT(T i, std::vector<uint8_t>& data) {
  size_t was = data.size();
  data.resize(data.size() + sizeof(i));
  memcpy(data.data() + was, &i, sizeof(i));
}

void addS(std::string const& s, std::vector<uint8_t>& data) {
  addT<uint32_t>(s.size(), data);
  size_t was = data.size();
  data.resize(data.size() + s.size());
  memcpy(data.data() + was, s.data(), s.size());
}

template <class T>
T readT(uint8_t*& at, size_t& len) {
  if (len < sizeof(T)) {
    die("not enough data len=", len);
  }
  T ret;
  memcpy(&ret, at, sizeof(T));
  len -= sizeof(T);
  at += sizeof(T);
  return ret;
}

std::string readS(uint8_t*& at, size_t& len) {
  uint32_t str_len = readT<uint32_t>(at, len);

  if (len < str_len) {
    die("not enough data len=", len, " str_len=", str_len);
  }
  std::string ret;
  ret.resize(str_len);
  memcpy(ret.data(), at, str_len);
  len -= str_len;
  at += str_len;
  return ret;
}

std::vector<uint8_t> encode(std::unordered_map<uint16_t, std::string> data) {
  std::vector<uint8_t> ret;
  addT<uint32_t>(data.size(), ret);
  for (auto const& kv : data) {
    addT<uint16_t>(kv.first, ret);
    addS(kv.second, ret);
  }
  return ret;
}

std::vector<uint8_t> payload(std::vector<uint8_t> data) {
  uint32_t size = data.size();
  std::vector<uint8_t> ret;
  ret.resize(data.size() + sizeof(size));
  memcpy(ret.data(), &size, sizeof(size));
  memcpy(ret.data() + sizeof(size), data.data(), data.size());
  return ret;
}

std::unordered_map<uint16_t, std::string> decode(std::vector<uint8_t> data) {
  std::unordered_map<uint16_t, std::string> ret;
  uint8_t* at = data.data();
  size_t len = data.size();
  uint32_t count = readT<uint32_t>(at, len);
  for (uint32_t i = 0; i < count; i++) {
    uint16_t port = readT<uint16_t>(at, len);
    std::string str = readS(at, len);
    ret[port] = std::move(str);
  }
  return ret;
}

} // namespace

class ControlServer : public IControlServer {
 public:
  ControlServer(std::vector<uint8_t> data, uint16_t port, bool ipv6)
      : data_(std::move(data)) {
    fd_ = checkedErrno(mkBoundSock(port, ipv6), "mkBoundSock control server");
    thread_ = std::thread(wrapThread("ControlServer", [this]() { go(); }));
  }

  void go() {
    checkedErrno(listen(fd_, 5), "listen control server");

    while (!done_) {
      pollfd p{};
      p.fd = fd_;
      p.events = POLLIN;
      // polls[1].fd = eventfd_;
      // polls[1].events = POLLIN;
      checkedErrno(poll(&p, 1, 250), "poll control fd");
      if (done_) {
        break;
      }
      if (!(p.revents & POLLIN)) {
        continue;
      }
      int nextfd = accept(fd_, NULL, NULL);
      if (nextfd < 0) {
        break;
      }
      uint8_t* at = data_.data();
      int togo = data_.size();
      int ret = 1;
      while (togo > 0 && ret > 0) {
        ret = send(nextfd, at, togo, 0);
        if (ret > 0) {
          at += ret;
          togo -= ret;
        } else if (ret < 0 && errno == -EINTR) {
          ret = 1;
        }
      }
      close(nextfd);
    }
    vlog("ControlServer done");
  }

  ~ControlServer() override {
    vlog("end control server");
    done_ = true;
    close(fd_);
    thread_.join();
  }

 private:
  std::vector<uint8_t> data_;
  std::atomic<bool> done_{false};
  int fd_;
  std::thread thread_;
};

std::unique_ptr<IControlServer> makeControlServer(
    std::unordered_map<uint16_t, std::string> data,
    uint16_t port,
    bool ipv6) {
  return std::make_unique<ControlServer>(payload(encode(data)), port, ipv6);
}

std::unordered_map<uint16_t, std::string>
getPortNameMap(std::string host, uint16_t port, bool ipv6) {
  int fd = checkedErrno(mkBasicSock(ipv6), "mksock");
  struct sockaddr_storage addr;
  socklen_t addrLen;
  getAddress(host, ipv6, port, &addr, &addrLen);
  checkedErrno(
      ::connect(fd, (const struct sockaddr*)&addr, addrLen), "control connect");

  uint32_t total_length;
  int ret = recv(fd, &total_length, 4, 0);
  if (ret != 4) {
    die("bad recv, wanted 4 got ", ret);
  }
  std::vector<uint8_t> data;
  data.resize(total_length);
  uint8_t* at = data.data();
  int size = data.size();
  while (size > 0) {
    ret = checkedErrno(recv(fd, at, size, 0), "receive control data");
    size -= ret;
    at += ret;
  }
  return decode(std::move(data));
}
