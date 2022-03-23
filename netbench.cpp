#include <boost/algorithm/string/join.hpp>
#include <boost/core/noncopyable.hpp>
#include <string_view>
#include <thread>
#include <unordered_set>

#include <errno.h>
#include <fcntl.h>
#include <liburing.h>
#include <unistd.h>

#include <netinet/in.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/times.h>

#include "sender.h"
#include "util.h"

namespace po = boost::program_options;

/*
 * Network benchmark tool.
 *
 * This tool will benchmark network coordinator stacks. specifically looking at
 * io_uring vs epoll.
 * The approach is to setup a single threaded receiver, and then spawn up N
 * threads with M connections. They wijll then send some requests, where a
 * request is a single (host endian) 32 bit unsigned int indicating length, and
 * then that number of bytes. The receiver when it collects a single "request"
 * will respond with a single byte (contents unimportant). The sender can then
 * treat this as a completed transaction and add it to it's stats.
 *
 */

std::atomic<bool> globalShouldShutdown{false};
void intHandler(int dummy) {
  if (globalShouldShutdown.load()) {
    die("already should have shutdown at signal");
  }
  globalShouldShutdown = true;
}

enum class RxEngine { IoUring, Epoll };
struct RxConfig {
  int backlog = 100000;
  int max_events = 32;
  int recv_size = 4096;
};

struct IoUringRxConfig : RxConfig {
  bool supports_nonblock_accept = false;
  bool register_ring = true;
  bool provide_buffers = true;
  bool fixed_files = true;
  bool loop_recv = false;
  int sqe_count = 64;
  int max_cqe_loop = 128;
  int provided_buffer_count = 8000;
  int fixed_file_count = 16000;
  int provided_buffer_low_watermark = 2000;
  int provided_buffer_compact = 1;

  std::string const toString() const {
    // only give the important options:
    return strcat(
        "fixed_files=",
        fixed_files ? strcat("1 (count=", fixed_file_count, ")") : strcat("0"),
        " provide_buffers=",
        provide_buffers ? strcat(
                              "1 (count=",
                              provided_buffer_count,
                              " refill=",
                              provided_buffer_low_watermark,
                              " compact=",
                              provided_buffer_compact,
                              ")")
                        : strcat("0"));
  }
};

struct EpollRxConfig : RxConfig {};

struct Config {
  std::vector<uint16_t> use_port;
  bool client_only = false;
  bool server_only = false;
  SendOptions send_options;

  bool print_rx_stats = true;
  std::vector<std::string> tx;
  std::vector<std::string> rx;
};

int mkBasicSock(uint16_t port, bool const isv6, int extra_flags = 0) {
  struct sockaddr_in serv_addr;
  struct sockaddr_in6 serv_addr6;
  int fd = checkedErrno(
      socket(isv6 ? AF_INET6 : AF_INET, SOCK_STREAM | extra_flags, 0),
      "make socket v6=",
      isv6);
  doSetSockOpt<int>(fd, SOL_SOCKET, SO_REUSEADDR, 1);
  if (isv6) {
    doSetSockOpt<int>(fd, IPPROTO_IPV6, IPV6_V6ONLY, 1);
  }
  struct sockaddr* paddr;
  size_t paddrlen;
  if (isv6) {
    memset(&serv_addr6, 0, sizeof(serv_addr6));
    serv_addr6.sin6_family = AF_INET6;
    serv_addr6.sin6_port = htons(port);
    serv_addr6.sin6_addr = in6addr_any;
    paddr = (struct sockaddr*)&serv_addr6;
    paddrlen = sizeof(serv_addr6);
  } else {
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    paddr = (struct sockaddr*)&serv_addr;
    paddrlen = sizeof(serv_addr);
  }
  if (bind(fd, paddr, paddrlen)) {
    int err = errno;
    close(fd);
    errno = err;
    return -1;
  }
  return fd;
}

int mkServerSock(
    RxConfig const& rx_cfg,
    uint16_t port,
    bool const isv6,
    int extra_flags) {
  int fd = checkedErrno(mkBasicSock(port, isv6, extra_flags));
  checkedErrno(listen(fd, rx_cfg.backlog), "listen");
  vlog("made sock ", fd, " v6=", isv6, " port=", port);
  return fd;
}

struct io_uring mkIoUring(IoUringRxConfig const& rx_cfg) {
  struct io_uring_params params;
  struct io_uring ring;
  memset(&params, 0, sizeof(params));

  checkedErrno(
      io_uring_queue_init_params(rx_cfg.sqe_count, &ring, &params),
      "io_uring_queue_init_params");
  if (rx_cfg.register_ring) {
    io_uring_register_ring_fd(&ring);
  }
  return ring;
}

// benchmark protocol is <uint32_t length>:<payload of size length>
// response is a single byte when it is received
struct ProtocolParser {
  // consume data and return number of new sends
  uint32_t consume(char const* data, size_t n) {
    uint32_t ret = 0;
    while (n > 0) {
      so_far += n;
      if (!is_reading) {
        uint32_t size_buff_add = std::min<uint32_t>(n, 4 - size_buff_have);
        memcpy(size_buff + size_buff_have, data, size_buff_add);
        size_buff_have += size_buff_add;
        if (size_buff_have >= 4) {
          memcpy(&is_reading, size_buff, 4);
        }
      }
      // vlog("consume ", n, " is_reading=", is_reading);
      if (is_reading && so_far >= is_reading + 4) {
        data += n;
        n = so_far - (is_reading + 4);
        so_far = size_buff_have = is_reading = 0;
        ret++;
      } else {
        break;
      }
    }
    return ret;
  }

  uint32_t size_buff_have = 0;
  char size_buff[4];
  uint32_t is_reading = 0;
  uint32_t so_far = 0;
};

class RxStats {
 public:
  RxStats(std::string const& name) : name_(name) {
    auto const now = std::chrono::steady_clock::now();
    started_ = lastStats_ = now;
    lastClock_ = checkedErrno(times(&lastTimes_), "initial times");
  }

  void startWait() {
    waitStarted_ = std::chrono::steady_clock::now();
  }

  void doneWait() {
    auto now = std::chrono::steady_clock::now();
    // anything under 100us seems to be very noisy
    static constexpr std::chrono::microseconds kEpsilon{100};
    if (now > waitStarted_ + kEpsilon) {
      idle_ += (now - waitStarted_);
    }
  }

  void doneLoop(size_t bytes, size_t requests) {
    auto const now = std::chrono::steady_clock::now();
    auto const duration = now - lastStats_;
    ++loops_;

    if (duration >= std::chrono::seconds(1)) {
      doLog(bytes, requests, now, duration);
    }
  }

 private:
  std::chrono::milliseconds getMs(clock_t from, clock_t to) {
    return std::chrono::milliseconds(
        to <= from ? 0llu : (((to - from) * 1000llu) / ticksPerSecond_));
  }

  void doLog(
      size_t bytes,
      size_t requests,
      std::chrono::steady_clock::time_point now,
      std::chrono::steady_clock::duration duration) {
    using namespace std::chrono;
    uint64_t const millis = duration_cast<milliseconds>(duration).count();
    double bps = ((bytes - lastBytes_) * 1000.0) / millis;
    double rps = ((requests - lastRequests_) * 1000.0) / millis;
    struct tms times_now {};
    clock_t clock_now = checkedErrno(::times(&times_now), "loop times");

    if (requests > lastRequests_ && lastRps_) {
      char buff[2048];
      // use snprintf as I like the floating point formatting
      int written = snprintf(
          buff,
          sizeof(buff),
          "%s: rps:%6.2fk Bps:%6.2fM idle=%lums "
          "user=%lums system=%lums wall=%lums loops=%lu",
          name_.c_str(),
          rps / 1000.0,
          bps / 1000000.0,
          duration_cast<milliseconds>(idle_).count(),
          getMs(lastTimes_.tms_utime, times_now.tms_utime).count(),
          getMs(lastTimes_.tms_stime, times_now.tms_stime).count(),
          getMs(lastClock_, clock_now).count(),
          loops_);
      if (written >= 0) {
        log(std::string_view(buff, written));
      }
    }
    loops_ = 0;
    idle_ = steady_clock::duration{0};
    lastClock_ = clock_now;
    lastTimes_ = times_now;
    lastBytes_ = bytes;
    lastRequests_ = requests;
    lastStats_ = now;
    lastRps_ = rps;
  }

 private:
  std::string const& name_;
  std::chrono::steady_clock::time_point started_ =
      std::chrono::steady_clock::now();
  std::chrono::steady_clock::time_point lastStats_ =
      std::chrono::steady_clock::now();

  std::chrono::steady_clock::time_point waitStarted;
  std::chrono::steady_clock::duration totalWaited{0};
  size_t lastBytes = 0;
  size_t lastRequests = 0;
  uint64_t ticksPerSecond_ = sysconf(_SC_CLK_TCK);
  struct tms lastTimes_;
  clock_t lastClock_;
  uint64_t loops_ = 0;

  std::chrono::steady_clock::time_point waitStarted_;
  std::chrono::steady_clock::duration idle_{0};
  size_t lastBytes_ = 0;
  size_t lastRequests_ = 0;
  size_t lastRps_ = 0;
};

class RunnerBase {
 public:
  explicit RunnerBase(std::string const& name) : name_(name) {}
  std::string const& name() const {
    return name_;
  }
  virtual void loop(std::atomic<bool>* should_shutdown) = 0;
  virtual void stop() = 0;
  virtual void addListenSock(int fd, bool v6) = 0;
  virtual ~RunnerBase() = default;

 protected:
  void didRead(int x) {
    bytesRx_ += x;
  }

  void finishedRequests(int n) {
    requestsRx_ += n;
  }

  void newSock() {
    socks_++;
    if (socks_ % 100 == 0) {
      vlog("add sock: now ", socks_);
    }
  }

  void delSock() {
    socks_--;
    if (socks_ % 100 == 0) {
      vlog("del sock: now ", socks_);
    }
  }

  int socks() const {
    return socks_;
  }

  size_t requestsRx_ = 0;
  size_t bytesRx_ = 0;

 private:
  std::string const name_;
  int socks_ = 0;
};

class NullRunner : public RunnerBase {
 public:
  explicit NullRunner(std::string const& name) : RunnerBase(name) {}
  void loop(std::atomic<bool>*) override {}
  void stop() override {}
  void addListenSock(int fd, bool) {
    close(fd);
  }
};

class BufferProvider : private boost::noncopyable {
 public:
  static constexpr int kBgid = 1;

  explicit BufferProvider(size_t count, size_t size, int lowWatermark)
      : sizePerBuffer_(size), lowWatermark_(lowWatermark) {
    buffer_.resize(count * size);
    for (size_t i = 0; i < count; i++) {
      buffers_.push_back(buffer_.data() + i * size);
    }
    toProvide_.reserve(128);
    toProvide2_.reserve(128);
    toProvide_.emplace_back(0, count);
    toProvideCount_ = count;
  }

  size_t count() const {
    return buffers_.size();
  }

  size_t sizePerBuffer() const {
    return sizePerBuffer_;
  }

  size_t toProvideCount() const {
    return toProvideCount_;
  }

  bool canProvide() const {
    return toProvide_.size();
  }

  bool needsToProvide() const {
    return toProvideCount_ > lowWatermark_;
  }

  void compact() {
    if (toProvide_.size() <= 1) {
      return;
    }
    std::sort(
        toProvide_.begin(), toProvide_.end(), [](auto const& a, auto const& b) {
          return a.start < b.start;
        });
    int merged = 0;
    toProvide2_.clear();
    toProvide2_.push_back(toProvide_[0]);
    for (size_t i = 1; i < toProvide_.size(); i++) {
      auto const& p = toProvide_[i];
      if (!toProvide2_.back().merge(p)) {
        toProvide2_.push_back(p);
      } else {
        ++merged;
      }
    }
    toProvide_.swap(toProvide2_);
    // log("merged ",
    //     merged,
    //     " was ",
    //     toProvide2_.size(),
    //     " now ",
    //     toProvide_.size());
  }

  void returnIndex(int i) {
    if (toProvide_.empty()) {
      // log("return ", i, " was empty");
      toProvide_.emplace_back(i);
    } else if (toProvide_.back().merge(i)) {
      // yay, nothing to do
    } else if (
        toProvide_.size() >= 2 && toProvide_[toProvide_.size() - 2].merge(i)) {
      // yay too, try merge these two. this accounts for out of order by 1 index
      // where we receive 1,3,2. so we merge 2 into 3, and then (2,3) into 1
      if (toProvide_[toProvide_.size() - 2].merge(toProvide_.back())) {
        toProvide_.pop_back();
      }
    } else {
      toProvide_.emplace_back(i);
    }
    ++toProvideCount_;
  }

  void provide(struct io_uring_sqe* sqe) {
    Range const& r = toProvide_.back();
    io_uring_prep_provide_buffers(
        sqe, buffers_[r.start], sizePerBuffer_, r.count, kBgid, r.start);
    sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
    // log("recycle ",
    //     r.start,
    //     " count=",
    //     r.count,
    //     " to=",
    //     toProvideCount_,
    //     " size=",
    //     toProvide_.size());
    toProvideCount_ -= r.count;
    toProvide_.pop_back();
    if (toProvide_.size() == 0) {
      // URGH
      if (toProvideCount_ != 0) {
        log("huh was ", toProvideCount_, " but wanted 0");
      }
      toProvideCount_ = 0;
    }
  }

  char const* getData(int i) const {
    return buffers_.at(i);
  }

 private:
  size_t sizePerBuffer_;
  std::vector<char> buffer_;
  std::vector<char*> buffers_;
  struct Range {
    explicit Range(int idx, int count = 1) : start(idx), count(count) {}
    int start;
    int count = 1;

    bool merge(int idx) {
      if (idx == start - 1) {
        start = idx;
        count++;
        return true;
      } else if (idx == start + count) {
        count++;
        return true;
      } else {
        return false;
      }
    }

    bool merge(Range const& r) {
      if (start + count == r.start) {
        count += r.count;
        return true;
      } else if (r.start + r.count == start) {
        count += r.count;
        start = r.start;
        return true;
      } else {
        return false;
      }
    }
  };
  ssize_t toProvideCount_ = 0;
  int lowWatermark_;
  std::vector<Range> toProvide_;
  std::vector<Range> toProvide2_;
};

static constexpr int kUseBufferProviderFlag = 1;
static constexpr int kUseFixedFilesFlag = 2;
static constexpr int kLoopRecvFlag = 4;

template <size_t ReadSize = 4096, size_t Flags = 0>
struct BasicSock {
  static constexpr bool kUseBufferProvider = Flags & kUseBufferProviderFlag;
  static constexpr bool kUseFixedFiles = Flags & kUseFixedFilesFlag;
  static constexpr bool kShouldLoop = Flags & kLoopRecvFlag;
  explicit BasicSock(int fd) : fd(fd) {}

  ~BasicSock() {
    if (!closed_) {
      log("socket not closed at destruct");
    }
  }

  uint32_t peekSend() {
    return do_send;
  }

  void didSend(uint32_t count) {}

  void addSend(struct io_uring_sqe* sqe, uint32_t len) {
    if (len > ReadSize) {
      die("too big send");
    }
    io_uring_prep_send(sqe, fd, &buff[0], len, 0);
    if (kUseFixedFiles) {
      sqe->flags |= IOSQE_FIXED_FILE;
    }
    sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
    do_send -= std::min(len, do_send);
  }

  void addRead(struct io_uring_sqe* sqe, BufferProvider& provider) {
    if (kUseBufferProvider) {
      io_uring_prep_recv(sqe, fd, NULL, provider.sizePerBuffer(), 0);
      sqe->flags |= IOSQE_BUFFER_SELECT;
      sqe->buf_group = BufferProvider::kBgid;
    } else {
      io_uring_prep_recv(sqe, fd, &buff[0], sizeof(buff), 0);
    }

    if (kUseFixedFiles) {
      sqe->flags |= IOSQE_FIXED_FILE;
    }
  }

  void doClose() {
    closed_ = true;
    ::close(fd);
  }

  void addClose(struct io_uring_sqe* sqe) {
    closed_ = true;
    io_uring_prep_close(sqe, fd);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, 0);
  }

  int didRead(size_t size, BufferProvider& provider, struct io_uring_cqe* cqe) {
    // pull remaining data
    int res = size;
    int recycleBufferIdx = -1;
    if (kUseBufferProvider) {
      recycleBufferIdx = cqe->flags >> 16;
      didRead(res, provider, recycleBufferIdx);
    } else {
      didRead(res);
    }
    while (kShouldLoop && res == (int)size) {
      res = recv(this->fd, buff, sizeof(buff), MSG_NOSIGNAL);
      if (res > 0) {
        didRead(res);
      }
    }
    return recycleBufferIdx;
  }

 private:
  void didRead(size_t n) {
    // normal read from buffer
    didRead(buff, n);
  }

  void didRead(size_t n, BufferProvider& provider, int idx) {
    // read from a provided buffer
    didRead(provider.getData(idx), n);
  }
  void didRead(char const* b, size_t n) {
    do_send += parser.consume(b, n);
  }

  int fd;
  ProtocolParser parser;
  uint32_t do_send = 0;
  bool closed_ = false;

  char buff[ReadSize];
};

struct ListenSock : private boost::noncopyable {
  ListenSock(int fd, bool v6) : fd(fd), isv6(v6) {}
  virtual ~ListenSock() {
    if (!closed) {
      ::close(fd);
    }
    vlog("close ListenSock");
  }

  void close() {
    ::close(fd);
    closed = true;
  }

  int fd;
  bool isv6;
  struct sockaddr_in addr;
  struct sockaddr_in6 addr6;
  socklen_t client_len;
  bool closed = false;
  int nextAcceptIdx = -1;
};

template <class TSock>
struct IOUringRunner : public RunnerBase {
  explicit IOUringRunner(
      Config const& cfg,
      IoUringRxConfig const& rx_cfg,
      std::string const& name)
      : RunnerBase(name),
        cfg_(cfg),
        rxCfg_(rx_cfg),
        ring(mkIoUring(rx_cfg)),
        buffers_(
            rx_cfg.provided_buffer_count,
            rx_cfg.recv_size,
            rx_cfg.provided_buffer_low_watermark) {
    if (TSock::kUseFixedFiles && TSock::kShouldLoop) {
      die("can't have fixed files and looping, "
          "we don't have the fd to call recv() !");
    }

    cqes_.resize(rx_cfg.max_events);

    if (TSock::kUseBufferProvider) {
      provideBuffers(true);
      submit();
    }

    if (TSock::kUseFixedFiles) {
      std::vector<int> files(rx_cfg.fixed_file_count, -1);
      checkedErrno(
          io_uring_register_files(&ring, files.data(), files.size()),
          "io_uring_register_files");
    }
  }

  ~IOUringRunner() {
    if (socks()) {
      vlog(
          "IOUringRunner shutting down with ",
          socks(),
          " sockets still: stopping=",
          stopping);
    }

    io_uring_queue_exit(&ring);
  }

  void provideBuffers(bool force) {
    if (!TSock::kUseBufferProvider) {
      return;
    }
    if (!(force || buffers_.needsToProvide())) {
      return;
    }

    if (rxCfg_.provided_buffer_compact) {
      buffers_.compact();
    }
    while (buffers_.canProvide()) {
      auto* sqe = get_sqe();
      buffers_.provide(sqe);
      io_uring_sqe_set_data(sqe, NULL);
    }
  }

  static constexpr int kAccept = 1;
  static constexpr int kRead = 2;
  static constexpr int kWrite = 3;
  static constexpr int kIgnore = 0;

  void addListenSock(int fd, bool v6) override {
    listeners_++;
    listenSocks_.push_back(std::make_unique<ListenSock>(fd, v6));
    addAccept(listenSocks_.back().get());
  }

  void addAccept(ListenSock* ls) {
    struct io_uring_sqe* sqe = get_sqe();
    struct sockaddr* addr;
    if (ls->isv6) {
      ls->client_len = sizeof(ls->addr6);
      addr = (struct sockaddr*)&ls->addr6;
    } else {
      ls->client_len = sizeof(ls->addr);
      addr = (struct sockaddr*)&ls->addr;
    }
    if (TSock::kUseFixedFiles) {
      if (ls->nextAcceptIdx >= 0) {
        die("only allowed one accept at a time");
      }
      ls->nextAcceptIdx = nextFdIdx();
      io_uring_prep_accept_direct(
          sqe, ls->fd, addr, &ls->client_len, SOCK_NONBLOCK, ls->nextAcceptIdx);
    } else {
      io_uring_prep_accept(sqe, ls->fd, addr, &ls->client_len, SOCK_NONBLOCK);
    }
    io_uring_sqe_set_data(sqe, tag(ls, kAccept));
  }

  struct io_uring_sqe* get_sqe() {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
      submit();
      sqe = io_uring_get_sqe(&ring);
      if (!sqe) {
        throw std::runtime_error("no sqe available");
      }
    }
    ++expected;
    return sqe;
  }

  void addRead(TSock* sock) {
    struct io_uring_sqe* sqe = get_sqe();
    sock->addRead(sqe, buffers_);
    io_uring_sqe_set_data(sqe, tag(sock, kRead));
  }

  void addSend(TSock* sock, uint32_t len) {
    struct io_uring_sqe* sqe = get_sqe();
    sock->addSend(sqe, len);
    io_uring_sqe_set_data(sqe, tag(sock, kWrite));
  }

  void processAccept(struct io_uring_cqe* cqe) {
    int fd = cqe->res;
    ListenSock* ls = untag<ListenSock>(cqe->user_data);
    if (fd >= 0) {
      int used_fd = fd;
      if (TSock::kUseFixedFiles) {
        if (fd > 0) {
          die("trying to use fixed files, but got given an actual fd. "
              "implies that this kernel does not support this feature");
        }
        if (ls->nextAcceptIdx < 0) {
          die("no nextAcceptIdx");
        }
        used_fd = ls->nextAcceptIdx;
        ls->nextAcceptIdx = -1;
      }
      TSock* sock = new TSock(used_fd);
      addRead(sock);
      newSock();
    } else if (!stopping) {
      die("unexpected accept result ",
          strerror(-fd),
          "(",
          fd,
          ") ud=",
          cqe->user_data);
    }

    if (stopping) {
      return;
    } else {
      if (rxCfg_.supports_nonblock_accept && !TSock::kUseFixedFiles) {
        // get any outstanding sockets
        struct sockaddr_in addr;
        struct sockaddr_in6 addr6;
        socklen_t addrlen = ls->isv6 ? sizeof(addr6) : sizeof(addr);
        struct sockaddr* paddr =
            ls->isv6 ? (struct sockaddr*)&addr6 : (struct sockaddr*)&addr;
        while (1) {
          int sock_fd = accept4(ls->fd, paddr, &addrlen, SOCK_NONBLOCK);
          if (sock_fd == -1 && errno == EAGAIN) {
            break;
          } else if (sock_fd == -1) {
            checkedErrno(sock_fd, "accept4");
          }
          TSock* sock = new TSock(sock_fd);
          addRead(sock);
          newSock();
        }
      }
      addAccept(untag<ListenSock>(cqe->user_data));
    }
  }

  void processRead(struct io_uring_cqe* cqe) {
    int amount = cqe->res;
    TSock* sock = untag<TSock>(cqe->user_data);
    if (amount > 0) {
      int recycleBufferIdx = sock->didRead(amount, buffers_, cqe);
      if (recycleBufferIdx > 0) {
        buffers_.returnIndex(recycleBufferIdx);
        provideBuffers(false);
      }
      if (uint32_t sends = sock->peekSend(); sends > 0) {
        finishedRequests(sends);
        addSend(sock, sends);
      }
      didRead(amount);
      addRead(sock);
    } else if (amount <= 0) {
      if (cqe->res == -ENOBUFS) {
        log("not enough buffers, but will just requeue. so far have ",
            ++enobuffCount_);
        addRead(sock);
        return;
      }
      if (cqe->res < 0 && !stopping) {
        if (cqe->res != -ECONNRESET) {
          log("unexpected read: ", amount, " delete ", sock);
        }
      }

      if (TSock::kUseFixedFiles) {
        auto* sqe = get_sqe();
        sock->addClose(sqe);
        io_uring_sqe_set_data(sqe, NULL);
      } else {
        sock->doClose();
      }
      delete sock;
      delSock();
    }
  }

  void processCqe(struct io_uring_cqe* cqe) {
    switch (get_tag(cqe->user_data)) {
      case kAccept:
        processAccept(cqe);
        break;
      case kRead:
        processRead(cqe);
        break;
      case kWrite:
        // be careful if you do something here as kRead might delete sockets.
        // this is ok as we only ever have one read outstanding
        // at once
        if (cqe->res < 0) {
          // we should track these down and make sure they only happen when the
          // sender socket is closed
          log("bad socket write ", cqe->res);
        }
        break;
      case kIgnore:
        break;
      default:
        if (cqe->user_data == LIBURING_UDATA_TIMEOUT) {
          break;
        }
        die("unexpected completion:", cqe->user_data);
        break;
    }
  }

  void submit() {
    while (expected) {
      int got = io_uring_submit(&ring);
      if (got != expected) {
        // log("sender: expected to submit ", expected, " but did ", got);
        if (got == 0) {
          if (stopping) {
            // assume some kind of cancel issue?
            expected--;
          } else {
            die("literally sent nothing, wanted ", expected);
          }
        }
      }
      // log("submitted ", got);
      expected -= got;
    }
  }

  void loop(std::atomic<bool>* should_shutdown) override {
    RxStats rx_stats{name()};
    struct __kernel_timespec timeout;
    timeout.tv_sec = 1;
    timeout.tv_nsec = 0;

    if (rxCfg_.register_ring) {
      io_uring_register_ring_fd(&ring);
    }

    while (socks() || !stopping) {
      provideBuffers(false /* maybe we should force? */);
      submit();

      rx_stats.startWait();
      int wait_res = checkedErrno(
          io_uring_wait_cqe_timeout(&ring, &cqes_[0], &timeout),
          "wait_cqe_timeout");
      rx_stats.doneWait();
      if (!wait_res) {
        rx_stats.doneWait();
        processCqe(cqes_[0]);
        io_uring_cqe_seen(&ring, cqes_[0]);
      }

      if (should_shutdown->load() || globalShouldShutdown.load()) {
        if (stopping) {
          // eh we gave it a good try
          break;
        }
        vlog("stopping");
        stop();
        vlog("stopped");
        timeout.tv_sec = 0;
        timeout.tv_nsec = 100000000;
      }

      int cqe_count;
      int loop_count = 0;
      do {
        cqe_count = io_uring_peek_batch_cqe(&ring, cqes_.data(), cqes_.size());
        for (int i = 0; i < cqe_count; i++) {
          processCqe(cqes_[i]);
        }
        io_uring_cq_advance(&ring, cqe_count);
      } while (cqe_count > 0 && ++loop_count < rxCfg_.max_cqe_loop);

      if (!cqe_count && stopping) {
        vlog("processed ", cqe_count, " socks()=", socks());
      }

      if (cfg_.print_rx_stats) {
        rx_stats.doneLoop(bytesRx_, requestsRx_);
      }
    }
  }

  void stop() override {
    stopping = true;
    for (auto& l : listenSocks_) {
      l->close();
    }
  }

  int nextFdIdx() {
    int ret = nextFdIdx_++;
    if (ret >= rxCfg_.fixed_file_count) {
      die("too many files, limit is ", ret);
    }
    return ret;
  }

  static inline void* tag(void* ptr, int x) {
    size_t uptr;
    memcpy(&uptr, &ptr, sizeof(size_t));
#ifndef NDEBUG
    if (uptr & (size_t)0x0f) {
      die("bad ptr");
    }
    if (x > 4) {
      die("bad tag");
    }
#endif
    return (void*)(uptr | x);
  }

  template <class T>
  static T* untag(size_t ptr) {
    return (T*)(ptr & ~((size_t)0x0f));
  }

  static int get_tag(uint64_t ptr) {
    return (int)(ptr & 0x0f);
  }

  Config cfg_;
  IoUringRxConfig rxCfg_;
  int expected = 0;
  bool stopping = false;
  struct io_uring ring;
  BufferProvider buffers_;
  std::vector<std::unique_ptr<ListenSock>> listenSocks_;
  std::vector<struct io_uring_cqe*> cqes_;
  int listeners_ = 0;
  uint32_t enobuffCount_ = 0;
  int nextFdIdx_ = 0;
};

static constexpr uint32_t kSocket = 0;
static constexpr uint32_t kAccept4 = 1;
static constexpr uint32_t kAccept6 = 2;

struct EPollData {
  uint32_t type;
  int fd;
  uint32_t to_write = 0;
  bool write_in_epoll = false;
  ProtocolParser parser;
};

struct EPollRunner : public RunnerBase {
  explicit EPollRunner(
      Config const& cfg,
      EpollRxConfig const& rx_cfg,
      std::string const& name)
      : RunnerBase(name), cfg_(cfg), rxCfg_(rx_cfg) {
    epoll_fd = checkedErrno(epoll_create(rx_cfg.max_events), "epoll_create");
    rcvbuff.resize(rx_cfg.recv_size);
    events.resize(rx_cfg.max_events);
  }

  ~EPollRunner() {
    for (auto& l : listeners_) {
      close(l->fd);
    }
    for (auto* ed : sockets_) {
      close(ed->fd);
      delete ed;
    }
    close(epoll_fd);
    vlog("EPollRunner cleaned up");
  }

  void addListenSock(int fd, bool v6) override {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));

    auto ed = std::make_unique<EPollData>();
    ed->type = v6 ? kAccept6 : kAccept4;
    ed->fd = fd;
    ev.events = EPOLLIN;
    ev.data.ptr = ed.get();
    checkedErrno(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev), "epoll_add");
    listeners_.push_back(std::move(ed));
    vlog("listening on ", fd, " v=", v6);
  }

  void doSocket(EPollData* ed, uint32_t events) {
    if (events & EPOLLIN) {
      if (doRead(ed)) {
        return;
      }
    }
    if (events & EPOLLOUT || ed->to_write) {
      doWrite(ed);
    }
  }

  void doWrite(EPollData* ed) {
    int res;

    while (ed->to_write) {
      res = send(
          ed->fd,
          rcvbuff.data(),
          std::min<size_t>(ed->to_write, rcvbuff.size()),
          MSG_NOSIGNAL);
      if (res < 0 && errno == EAGAIN) {
        break;
      }
      if (res < 0) {
        // something went wrong - probably socket is dead
        ed->to_write = 0;
      } else {
        ed->to_write -= std::min<uint32_t>(ed->to_write, res);
      }
    }

    if (ed->write_in_epoll && !ed->to_write) {
      struct epoll_event ev;
      memset(&ev, 0, sizeof(ev));
      ev.events = EPOLLIN;
      ev.data.ptr = ed;
      checkedErrno(
          epoll_ctl(epoll_fd, EPOLL_CTL_MOD, ed->fd, &ev),
          "epoll_remove_write");
      ed->write_in_epoll = false;
    } else if (!ed->write_in_epoll && ed->to_write) {
      struct epoll_event ev;
      memset(&ev, 0, sizeof(ev));
      ev.events = EPOLLIN | EPOLLOUT;
      ev.data.ptr = ed;
      checkedErrno(
          epoll_ctl(epoll_fd, EPOLL_CTL_MOD, ed->fd, &ev), "epoll_add_write");
      ed->write_in_epoll = true;
    }
  }

  int doRead(EPollData* ed) {
    int res;
    int fd = ed->fd;
    do {
      res = recv(fd, rcvbuff.data(), rcvbuff.size(), MSG_NOSIGNAL);
      if (res <= 0) {
        int errnum = errno;
        if (res < 0 && errnum == EAGAIN) {
          return 0;
        }

        checkedErrno(
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL),
            "epoll_del fd=",
            fd,
            " res=",
            res,
            " errno=",
            errnum);
        delSock();
        close(fd);
        delete ed;
        sockets_.erase(ed);

        return -1;
      } else {
        didRead(res);
        int consumed = ed->parser.consume(rcvbuff.data(), res);
        finishedRequests(consumed);
        ed->to_write += consumed;
      }
    } while (res == (int)rcvbuff.size());
    return 0;
  }

  void doAccept(int fd, bool isv6) {
    struct sockaddr_in addr;
    struct sockaddr_in6 addr6;
    socklen_t addrlen = isv6 ? sizeof(addr6) : sizeof(addr);
    struct sockaddr* paddr =
        isv6 ? (struct sockaddr*)&addr6 : (struct sockaddr*)&addr;
    while (true) {
      int sock_fd = accept4(fd, paddr, &addrlen, SOCK_NONBLOCK);
      if (sock_fd == -1 && errno == EAGAIN) {
        break;
      } else if (sock_fd == -1) {
        checkedErrno(sock_fd, "accept4");
      }
      struct epoll_event ev;
      memset(&ev, 0, sizeof(ev));
      ev.events = EPOLLIN | EPOLLET;
      EPollData* ed = new EPollData();
      ed->type = kSocket;
      ed->fd = sock_fd;
      ev.data.ptr = ed;
      checkedErrno(
          epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock_fd, &ev), "epoll add sock");
      sockets_.insert(ed);
      newSock();
    }
  }

  void stop() override {}

  void loop(std::atomic<bool>* should_shutdown) override {
    RxStats rx_stats{name()};
    while (!should_shutdown->load() && !globalShouldShutdown.load()) {
      rx_stats.startWait();
      int nevents = checkedErrno(
          epoll_wait(epoll_fd, events.data(), events.size(), 1000),
          "epoll_wait");
      rx_stats.doneWait();
      if (!nevents) {
        vlog("epoll: no events socks()=", socks());
      }
      for (int i = 0; i < nevents; ++i) {
        EPollData* ed = (EPollData*)events[i].data.ptr;
        switch (ed->type) {
          case kAccept4:
            doAccept(ed->fd, false);
            break;
          case kAccept6:
            doAccept(ed->fd, true);
            break;
          default:
            doSocket(ed, events[i].events);
            break;
        }
      }
      if (cfg_.print_rx_stats) {
        rx_stats.doneLoop(bytesRx_, requestsRx_);
      }
    }

    vlog("epollrunner: done socks=", socks());
  }

  Config const cfg_;
  EpollRxConfig const rxCfg_;
  int epoll_fd;
  std::vector<struct epoll_event> events;
  std::vector<char> rcvbuff;
  std::vector<std::unique_ptr<EPollData>> listeners_;
  std::unordered_set<EPollData*> sockets_;
};

uint16_t pickPort(Config const& config) {
  static uint16_t startPort =
      config.use_port.size() ? config.use_port[0] : 10000 + rand() % 2000;
  bool v6 = config.send_options.ipv6;
  if (config.use_port.size()) {
    return startPort++;
  }
  for (int i = 0; i < 1000; i++) {
    auto port = startPort++;
    if (v6) {
      int v6 = mkBasicSock(port, true);
      if (v6 < 0) {
        continue;
      }
      close(v6);
    } else {
      int v4 = mkBasicSock(port, false);
      if (v4 < 0) {
        continue;
      }
      close(v4);
    }
    return port;
  }
  die("no port found");
  return 0;
}

void run(std::unique_ptr<RunnerBase> runner, std::atomic<bool>* shutdown) {
  try {
    runner->loop(shutdown);
  } catch (InterruptedException const&) {
    vlog("interrupted, cleaning up nicely");
    runner->stop();
    vlog("waiting until done:");
    runner->loop(shutdown);
    vlog("done");
  } catch (std::exception const& ex) {
    log("caught exception, terminating: ", ex.what());
    throw;
  }
}

struct Receiver {
  std::unique_ptr<RunnerBase> r;
  uint16_t port;
  std::string name;
  std::string rxCfg;
};

Receiver makeEpollRx(Config const& cfg, EpollRxConfig const& rx_cfg) {
  uint16_t port = pickPort(cfg);
  auto runner =
      std::make_unique<EPollRunner>(cfg, rx_cfg, strcat("epoll port=", port));
  runner->addListenSock(
      mkServerSock(rx_cfg, port, cfg.send_options.ipv6, SOCK_NONBLOCK),
      cfg.send_options.ipv6);
  return Receiver{std::move(runner), port, "epoll", ""};
}

template <size_t flags>
struct BasicSockPicker {
  // if using buffer provider, don't need any buffer
  // except for to do sending
  using Sock = std::conditional_t<
      flags & kUseBufferProviderFlag,
      BasicSock<64, flags>,
      BasicSock<4096, flags>>;
};

template <size_t MbFlag>
void mbIoUringRxFactory(
    Config const& cfg,
    IoUringRxConfig const& rx_cfg,
    std::string const& name,
    size_t flags,
    std::unique_ptr<RunnerBase>& runner) {
  if (flags == MbFlag) {
    if (runner) {
      die("already had a runner? flags=", flags, " this=", MbFlag);
    }
    runner =
        std::make_unique<IOUringRunner<typename BasicSockPicker<MbFlag>::Sock>>(
            cfg, rx_cfg, name);
  }
}

template <size_t... PossibleFlag>
Receiver makeIoUringRx(
    Config const& cfg,
    IoUringRxConfig const& rx_cfg,
    std::index_sequence<PossibleFlag...>) {
  uint16_t port = pickPort(cfg);

  std::unique_ptr<RunnerBase> runner;
  size_t flags = (rx_cfg.provide_buffers ? kUseBufferProviderFlag : 0) |
      (rx_cfg.fixed_files ? kUseFixedFilesFlag : 0) |
      (rx_cfg.loop_recv ? kLoopRecvFlag : 0);

  ((mbIoUringRxFactory<PossibleFlag>(
       cfg, rx_cfg, strcat("io_uring port=", port), flags, runner)),
   ...);

  if (!runner) {
    die("no factory for runner flags=",
        flags,
        " maybe you need to increase the index sequence "
        "size in the caller of this");
  }

  // io_uring doesnt seem to like accepting on a nonblocking socket
  int sock_flags = rx_cfg.supports_nonblock_accept ? SOCK_NONBLOCK : 0;

  runner->addListenSock(
      mkServerSock(rx_cfg, port, cfg.send_options.ipv6, sock_flags),
      cfg.send_options.ipv6);

  return Receiver{std::move(runner), port, "io_uring", rx_cfg.toString()};
}

Config parse(int argc, char** argv) {
  Config config;
  po::options_description desc;
  // clang-format off
desc.add_options()
("help", "produce help message")
("verbose", "verbose logging")
("print_rx_stats", po::value(&config.print_rx_stats))
("use_port", po::value<std::vector<uint16_t>>(&config.use_port)->multitoken(),
 "what target port")
("server_only", po::value(&config.server_only),
 "do not tx locally, wait for it")
("client_only", po::value(&config.client_only),
 "do not rx locally, only send requests")
("host", po::value(&config.send_options.host))
("v6", po::value(&config.send_options.ipv6))
("time", po::value(&config.send_options.run_seconds))
("send_threads", po::value(&config.send_options.threads),
  "number of sender threads")
("send_connections_per_thread", po::value(&config.send_options.per_thread),
 "send: number of connections made per sender thread")
("tx", po::value<std::vector<std::string> >()->multitoken(),
 "tx scenarios to run (can be multiple)")
("rx", po::value<std::vector<std::string> >()->multitoken(),
 "rx engines to run (can be multiple)")
("send_small_size", po::value(&config.send_options.small_size))
("send_medium_size", po::value(&config.send_options.medium_size))
("send_large_size", po::value(&config.send_options.large_size))
;
  // clang-format on

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);
  if (vm.count("help")) {
    std::cerr << desc << "\n";
    std::cerr << "tx options are:\n";
    for (auto tx : allScenarios()) {
      std::cerr << "    " << tx << "\n";
    }
    std::cerr << "rx engines are: epoll, io_uring\n";
    exit(1);
  }
  if (vm.count("verbose")) {
    setVerbose();
  }
  if (vm.count("tx")) {
    for (auto const& tx : vm["tx"].as<std::vector<std::string>>()) {
      if (tx == "all") {
        auto all = allScenarios();
        config.tx.insert(config.tx.end(), all.begin(), all.end());
      } else {
        config.tx.push_back(tx);
      }
    }
  } else {
    config.tx.push_back("small");
  }

  if (vm.count("rx")) {
    for (auto const& rx : vm["rx"].as<std::vector<std::string>>()) {
      if (rx.empty()) {
        continue;
      }
      config.rx.push_back(rx);
    }
  } else {
    config.rx.push_back("io_uring");
    config.rx.push_back("epoll");
  }

  if (config.server_only) {
    config.tx.clear();
  }

  if (config.client_only) {
    config.rx.clear();
  }

  // validations:

  if (config.server_only && config.client_only) {
    die("only one of server/client only please");
  }

  return config;
}

std::pair<RxEngine, std::vector<std::string>> getRxEngine(
    std::string const& parse) {
  auto split = po::split_unix(parse);
  if (split.size() < 1) {
    die("no engine in ", parse);
  }
  auto e = split[0];
  // don't erase the first one, boost skips it expecting an executable
  if (e == "epoll") {
    return std::make_pair(RxEngine::Epoll, split);
  } else if (e == "io_uring") {
    return std::make_pair(RxEngine::IoUring, split);
  } else {
    die("bad rx engine ", e);
  }
  throw std::logic_error("should not get here");
}

std::function<Receiver(Config const&)> parseRx(std::string const& parse) {
  IoUringRxConfig io_uring_cfg;
  EpollRxConfig epoll_cfg;
  po::options_description epoll_desc;
  po::options_description io_uring_desc;

  // clang-format off
auto add_base = [&](po::options_description& d, RxConfig& cfg) {
  d.add_options()
("backlog", po::value(&cfg.backlog)->default_value(cfg.backlog))
("max_events", po::value(&cfg.max_events)->default_value(cfg.max_events))
("recv_size", po::value(&cfg.recv_size)->default_value(cfg.recv_size))
  ;
};

add_base(epoll_desc, epoll_cfg);
add_base(io_uring_desc, io_uring_cfg);

io_uring_desc.add_options()
  ("provide_buffers",  po::value(&io_uring_cfg.provide_buffers)
     ->default_value(io_uring_cfg.provide_buffers))
  ("fixed_files",  po::value(&io_uring_cfg.fixed_files)
     ->default_value(io_uring_cfg.fixed_files))
  ("loop_recv", po::value(&io_uring_cfg.loop_recv)
     ->default_value(io_uring_cfg.loop_recv))
  ("max_cqe_loop",  po::value(&io_uring_cfg.max_cqe_loop)
     ->default_value(io_uring_cfg.max_cqe_loop))
  ("supports_nonblock_accept",  po::value(&io_uring_cfg.supports_nonblock_accept)
     ->default_value(io_uring_cfg.supports_nonblock_accept))
  ("register_ring",  po::value(&io_uring_cfg.register_ring)
     ->default_value(io_uring_cfg.register_ring))
  ("sqe_count", po::value(&io_uring_cfg.sqe_count)
     ->default_value(io_uring_cfg.sqe_count))
  ("provided_buffer_count", po::value(&io_uring_cfg.provided_buffer_count)
     ->default_value(io_uring_cfg.provided_buffer_count))
  ("fixed_file_count", po::value(&io_uring_cfg.fixed_file_count)
     ->default_value(io_uring_cfg.fixed_file_count))
  ("provided_buffer_low_watermark", po::value(&io_uring_cfg.provided_buffer_low_watermark)
     ->default_value(io_uring_cfg.provided_buffer_low_watermark))
  ("provided_buffer_compact", po::value(&io_uring_cfg.provided_buffer_compact)
     ->default_value(io_uring_cfg.provided_buffer_compact))
  ;
  // clang-format on

  po::options_description* used_desc = NULL;
  auto const [engine, splits] = getRxEngine(parse);
  switch (engine) {
    case RxEngine::IoUring:
      used_desc = &io_uring_desc;
      break;
    case RxEngine::Epoll:
      used_desc = &epoll_desc;
      break;
  };

  simpleParse(*used_desc, splits);

  switch (engine) {
    case RxEngine::IoUring:
      return [io_uring_cfg](Config const& cfg) -> Receiver {
        return makeIoUringRx(cfg, io_uring_cfg, std::make_index_sequence<16>{});
      };
    case RxEngine::Epoll:
      return [epoll_cfg](Config const& cfg) -> Receiver {
        return makeEpollRx(cfg, epoll_cfg);
      };
      break;
  };
  die("bad engine ", (int)engine);
  return {};
}

int main(int argc, char** argv) {
  Config const cfg = parse(argc, argv);
  signal(SIGINT, intHandler);
  std::vector<std::function<Receiver()>> receiver_factories;
  for (auto const& rx : cfg.rx) {
    receiver_factories.push_back(
        [&cfg, parsed = parseRx(rx)]() -> Receiver { return parsed(cfg); });
  }

  if (cfg.client_only) {
    if (cfg.use_port.empty()) {
      die("please specify port for client_only");
    }
    receiver_factories.clear();
    log("using given ports not setting up local receivers");
    for (auto port : cfg.use_port) {
      receiver_factories.push_back([port]() -> Receiver {
        return Receiver{
            std::make_unique<NullRunner>(strcat("null port=", port)),
            port,
            strcat("given_port port=", port)};
      });
    }
  }

  std::vector<std::pair<std::string, std::string>> results;
  if (cfg.tx.size()) {
    log("sending using ",
        cfg.send_options.threads,
        " threads, ",
        cfg.send_options.per_thread,
        " per thread");

    for (auto const& tx : cfg.tx) {
      for (auto const& r : receiver_factories) {
        Receiver rcv = r();
        std::atomic<bool> should_shutdown{false};
        log("running ", tx, " for ", rcv.name, " cfg=", rcv.rxCfg);

        std::thread rcv_thread(wrapThread(
            strcat("rcv", rcv.name),
            [r = std::move(rcv.r), shutdown = &should_shutdown]() mutable {
              run(std::move(r), shutdown);
            }));

        auto res = runSender(tx, cfg.send_options, rcv.port);
        should_shutdown = true;
        log("...done sender");
        rcv_thread.join();
        log("...done receiver");
        results.emplace_back(
            strcat(tx, "_", rcv.name, "_", rcv.rxCfg), res.toString());
      }
    }
    for (auto const& r : results) {
      log(r.first);
      log(std::string(30, ' '), r.second);
    }
  } else {
    // no built in sender mode
    std::atomic<bool> should_shutdown{false};
    std::vector<Receiver> receivers;
    std::vector<std::thread> receiver_threads;
    for (auto& r : receiver_factories) {
      receivers.push_back(r());
    }
    log("using receivers: ");
    for (auto const& r : receivers) {
      log(r.name, " port=", r.port, " rx_cfg=", r.rxCfg);
    }

    for (auto& r : receivers) {
      receiver_threads.emplace_back(wrapThread(
          strcat("rcv", r.name),
          [r = std::move(r.r), shutdown = &should_shutdown]() mutable {
            run(std::move(r), shutdown);
          }));
    }

    for (size_t i = 0; i < receivers.size(); ++i) {
      vlog("waiting for ", receivers[i].name);
      receiver_threads[i].join();
    }
  }

  vlog("all done");
  return 0;
}
