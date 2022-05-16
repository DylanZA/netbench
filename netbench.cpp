#include <boost/algorithm/string/join.hpp>
#include <boost/align/aligned_allocator.hpp>
#include <boost/core/noncopyable.hpp>
#include <numeric>
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
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/times.h>

#include "sender.h"
#include "util.h"

namespace po = boost::program_options;

namespace {
static constexpr uint32_t __IORING_SETUP_COOP_TASKRUN = (1U << 8);

#ifndef __NR_io_uring_enter
static constexpr int __NR_io_uring_enter = 426;
#endif
#ifndef __NR_io_uring_register
static constexpr int __NR_io_uring_register = 427;
#endif
static inline int ____sys_io_uring_register(
    int fd,
    unsigned opcode,
    const void* arg,
    unsigned nr_args) {
  int ret;

  ret = syscall(__NR_io_uring_register, fd, opcode, arg, nr_args);
  return (ret < 0) ? -errno : ret;
}

static inline int ____sys_io_uring_enter2(
    int fd,
    unsigned to_submit,
    unsigned min_complete,
    unsigned flags,
    sigset_t* sig,
    int sz) {
  int ret;

  ret =
      syscall(__NR_io_uring_enter, fd, to_submit, min_complete, flags, sig, sz);
  return (ret < 0) ? -errno : ret;
}

} // namespace

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
  int provide_buffers = 1;
  bool fixed_files = true;
  bool loop_recv = false;
  int sqe_count = 64;
  int cqe_count = 0;
  int max_cqe_loop = 256 * 32;
  int provided_buffer_count = 8000;
  int fixed_file_count = 16000;
  int provided_buffer_low_watermark = -1;
  int provided_buffer_compact = 1;
  bool huge_pages = false;
  bool no_mmap = false;

  std::string const toString() const {
    // only give the important options:
    auto is_default = [this](auto IoUringRxConfig::*x) {
      IoUringRxConfig base;
      return this->*x == base.*x;
    };
    return strcat(
        "fixed_files=",
        fixed_files ? strcat("1 (count=", fixed_file_count, ")") : strcat("0"),
        " provide_buffers=",
        provide_buffers ? strcat(
                              "v=",
                              provide_buffers,
                              " (count=",
                              provided_buffer_count,
                              " refill=",
                              provided_buffer_low_watermark,
                              " compact=",
                              provided_buffer_compact,
                              ")")
                        : strcat("0"),
        is_default(&IoUringRxConfig::sqe_count)
            ? ""
            : strcat(" sqe_count=", sqe_count),
        is_default(&IoUringRxConfig::cqe_count)
            ? ""
            : strcat(" cqe_count=", cqe_count),
        is_default(&IoUringRxConfig::max_cqe_loop)
            ? ""
            : strcat(" max_cqe_loop=", max_cqe_loop),
        is_default(&IoUringRxConfig::huge_pages)
            ? ""
            : strcat(" huge_pages=", huge_pages));
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

std::pair<unsigned int, struct io_uring_params> mkIoUringParams(
    IoUringRxConfig const& rx_cfg) {
  struct io_uring_params params;
  memset(&params, 0, sizeof(params));

  // default to 8x sqe_count as we are very happy to submit multiple sqe off one
  // cqe (eg send,read) and this can build up quickly
  int cqe_count =
      rx_cfg.cqe_count <= 0 ? 8 * rx_cfg.sqe_count : rx_cfg.cqe_count;

  params.flags |= IORING_SETUP_CQSIZE;
  params.cq_entries = cqe_count;
  return {rx_cfg.sqe_count, params};
}

template <class TProvidedBuffers>
class HugeBuffer : private boost::noncopyable {
 public:
  static constexpr size_t kHugePageMask = (1LLU << 21) - 1; // 2MB

  HugeBuffer(IoUringRxConfig const& rx_cfg) {
    buffersSize_ = TProvidedBuffers::getSize(rx_cfg);
    auto const [entries, params] = mkIoUringParams(rx_cfg);
    checkedErrno(
        io_uring_queue_required_mem(entries, &params, &ringSize_),
        "calculate ring size");

    // buffers wants to be at the start of a page
    if (!rx_cfg.no_mmap) {
      ringSize_ = 0;
    }
    ringSize_ = (ringSize_ + 4095) & (~4095LLU);

    allSize_ = ringSize_ + buffersSize_;
    allSize_ = (allSize_ + 4095) & (~4095LLU);

    int extra_mmap_flags = 0;

    if (rx_cfg.huge_pages) {
      allSize_ = (allSize_ + kHugePageMask) & (~kHugePageMask);
      extra_mmap_flags |= MAP_HUGETLB;
      checkHugePages(allSize_ / (1 + kHugePageMask));
    }

    buffer_ = mmap(
        NULL,
        allSize_,
        PROT_READ | PROT_WRITE,
        MAP_ANONYMOUS | MAP_PRIVATE | extra_mmap_flags,
        -1,
        0);

    if (buffer_ == MAP_FAILED) {
      auto errnoCopy = errno;
      die("unable to allocate pages of size ",
          allSize_,
          ": ",
          strerror(errnoCopy));
    }

    ringPtr_ = buffer_;
    buffersPtr_ = (void*)((uint64_t)buffer_ + ringSize_);

    vlog(
        "made huge buffer size ",
        allSize_,
        " starting at ",
        buffer_,
        " ring: (",
        ringPtr_,
        ", ",
        ringSize_,
        ")",
        " buffers: (",
        buffersPtr_,
        ", ",
        buffersSize_,
        ")");
  }

  std::pair<void*, size_t> ring() const {
    return std::make_pair(ringPtr_, ringSize_);
  }

  std::pair<void*, size_t> buffer() const {
    return std::make_pair(buffersPtr_, buffersSize_);
  }

  ~HugeBuffer() {
    munmap(buffer_, allSize_);
  }

 private:
  void* buffer_;
  size_t allSize_;

  void* buffersPtr_;
  size_t buffersSize_;

  void* ringPtr_;
  size_t ringSize_;
};

template <class THugeBuffer>
struct io_uring mkIoUring(IoUringRxConfig const& rx_cfg, THugeBuffer& buffer) {
  struct io_uring ring;
  auto entries_params = mkIoUringParams(rx_cfg);
  auto& entries = entries_params.first;
  auto& params = entries_params.second;
  unsigned int const newer_flags =
      IORING_SETUP_SUBMIT_ALL | __IORING_SETUP_COOP_TASKRUN;

  auto initRings = [&]() -> int {
    if (rx_cfg.no_mmap) {
      auto buf = buffer.ring();
      return io_uring_queue_init_mem(
          entries, &ring, &params, buf.first, buf.second);
    } else {
      return io_uring_queue_init_params(entries, &ring, &params);
    }
  };

  int ret = initRings();
  if (ret < 0) {
    log("trying init again without COOP_TASKRUN or SUBMIT_ALL as ret=", ret);
    params.flags = params.flags & (~newer_flags);
    checkedErrno(initRings(), "io_uring_queue_init_params");
  }

  if (rx_cfg.register_ring) {
    checkedErrno(io_uring_register_ring_fd(&ring), "register ring fd");
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
        if (likely(n >= sizeof(is_reading) && size_buff_have == 0)) {
          size_buff_have = sizeof(is_reading);
          memcpy(&is_reading, data, sizeof(is_reading));
        } else {
          uint32_t size_buff_add =
              std::min<uint32_t>(n, sizeof(is_reading) - size_buff_have);
          memcpy(size_buff + size_buff_have, data, size_buff_add);
          size_buff_have += size_buff_add;
          if (size_buff_have >= sizeof(is_reading)) {
            memcpy(&is_reading, size_buff, sizeof(is_reading));
          }
        }
      }
      // vlog("consume ", n, " is_reading=", is_reading);
      if (is_reading && so_far >= is_reading + sizeof(is_reading)) {
        data += n;
        n = so_far - (is_reading + sizeof(is_reading));
        so_far = size_buff_have = is_reading = 0;
        ret++;
      } else {
        break;
      }
    }
    return ret;
  }

  uint32_t size_buff_have = 0;
  uint32_t is_reading = 0;
  char size_buff[sizeof(is_reading)];
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

  void doneLoop(size_t bytes, size_t requests, bool is_overflow = false) {
    auto const now = std::chrono::steady_clock::now();
    auto const duration = now - lastStats_;
    ++loops_;

    if (is_overflow) {
      ++overflows_;
    }

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
          "user=%lums system=%lums wall=%lums loops=%lu overflows=%lu",
          name_.c_str(),
          rps / 1000.0,
          bps / 1000000.0,
          duration_cast<milliseconds>(idle_).count(),
          getMs(lastTimes_.tms_utime, times_now.tms_utime).count(),
          getMs(lastTimes_.tms_stime, times_now.tms_stime).count(),
          getMs(lastClock_, clock_now).count(),
          loops_,
          overflows_);
      if (written >= 0) {
        log(std::string_view(buff, written));
      }
    }
    loops_ = overflows_ = 0;
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
  uint64_t ticksPerSecond_ = sysconf(_SC_CLK_TCK);
  struct tms lastTimes_;
  clock_t lastClock_;
  uint64_t loops_ = 0;
  uint64_t overflows_ = 0;

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
  void addListenSock(int fd, bool) override {
    close(fd);
  }
};

class BufferProviderV1 : private boost::noncopyable {
 public:
  static constexpr int kBgid = 1;

  explicit BufferProviderV1(
      IoUringRxConfig const& rx_cfg,
      HugeBuffer<BufferProviderV1>& buffers)
      : sizePerBuffer_(addAlignment(rx_cfg.recv_size)),
        lowWatermark_(rx_cfg.provided_buffer_low_watermark) {
    auto count = rx_cfg.provided_buffer_count;
    auto buff = buffers.buffer();
    if (buff.second < count * sizePerBuffer_) {
      die("buffer not big enough: ", buff.second);
    }
    char* buffer = (char*)buff.first;
    for (ssize_t i = 0; i < count; i++) {
      buffers_.push_back(buffer + i * sizePerBuffer_);
    }
    toProvide_.reserve(128);
    toProvide2_.reserve(128);
    toProvide_.emplace_back(0, count);
    toProvideCount_ = count;
  }

  static size_t getSize(IoUringRxConfig const& rx_cfg) {
    return rx_cfg.provided_buffer_count * addAlignment(rx_cfg.recv_size);
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

  void initialRegister(struct io_uring*) {}

  void compact() {
    if (toProvide_.size() <= 1) {
      return;
    } else if (toProvide_.size() == 2) {
      // actually a common case due to the way the kernel internals work
      if (toProvide_[0].merge(toProvide_[1])) {
        toProvide_.pop_back();
      }
      return;
    }
    auto was = toProvide_.size();
    std::sort(
        toProvide_.begin(), toProvide_.end(), [](auto const& a, auto const& b) {
          return a.sortable < b.sortable;
        });
    toProvide2_.clear();
    toProvide2_.push_back(toProvide_[0]);
    for (size_t i = 1; i < toProvide_.size(); i++) {
      auto const& p = toProvide_[i];
      if (!toProvide2_.back().merge(p)) {
        toProvide2_.push_back(p);
      }
    }
    toProvide_.swap(toProvide2_);
    if (unlikely(isVerbose())) {
      vlog("compact() was ", was, " now ", toProvide_.size());
      for (auto const& t : toProvide_) {
        vlog("...", t.start, " count=", t.count);
      }
    }
  }

  void returnIndex(uint16_t i) {
    if (toProvide_.empty()) {
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
    toProvideCount_ -= r.count;
    toProvide_.pop_back();
    assert(toProvide_.size() != 0 || toProvideCount_ == 0);
  }

  char const* getData(uint16_t i) const {
    return buffers_[i];
  }

 private:
  static constexpr int kAlignment = 32;

  static size_t addAlignment(size_t n) {
    return kAlignment * ((n + kAlignment - 1) / kAlignment);
  }

  struct Range {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    explicit Range(uint16_t idx, uint16_t count = 1)
        : count(count), start(idx) {}
#else
    explicit Range(uint16_t idx, uint16_t count = 1)
        : start(idx), count(count) {}
#endif
    union {
      struct {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        uint16_t count;
        uint16_t start;
#else
        uint16_t start;
        uint16_t count;
#endif
      };
      uint32_t sortable; // big endian might need to swap these around
    };

    bool merge(uint16_t idx) {
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

  size_t sizePerBuffer_;
  std::vector<char*> buffers_;
  ssize_t toProvideCount_ = 0;
  int lowWatermark_;
  std::vector<Range> toProvide_;
  std::vector<Range> toProvide2_;
};

class BufferProviderV2 : private boost::noncopyable {
 private:
  struct __io_uring_buf_reg {
    __u64 ring_addr;
    __u32 ring_entries;
    __u16 bgid;
    __u16 pad;
    __u64 resv[3];
  };

  struct __io_uring_buf {
    __u64 addr;
    __u32 len;
    __u16 bid;
    __u16 resv;
  };

 public:
  static constexpr int kBgid = 1;
  static constexpr size_t kBufferAlignMask = 31LLU;

  struct Sizes {
    size_t ringSize;
    size_t ringMask;
    size_t ringMemSize;
    size_t sizePerBuffer;
    size_t count;
    size_t totalSize;
  };
  static Sizes calcSizes(IoUringRxConfig const& rx_cfg) {
    Sizes ret;
    ret.count = rx_cfg.provided_buffer_count;
    ret.sizePerBuffer = addAlignment(rx_cfg.recv_size);
    ret.ringSize = 1;
    ret.ringMask = 0;
    while (ret.ringSize < ret.count) {
      ret.ringSize *= 2;
      ret.ringMask = (ret.ringMask << 1) | 1;
    }

    ret.ringMemSize = ret.ringSize * sizeof(entry_union);
    ret.ringMemSize =
        (ret.ringMemSize + kBufferAlignMask) & (~kBufferAlignMask);
    ret.totalSize = ret.count * ret.sizePerBuffer + ret.ringMemSize;

    return ret;
  }

  static size_t getSize(IoUringRxConfig const& rx_cfg) {
    auto sizes = calcSizes(rx_cfg);
    return sizes.totalSize;
  }

  explicit BufferProviderV2(
      IoUringRxConfig const& rx_cfg,
      HugeBuffer<BufferProviderV2>& buffers) {
    auto const sizes = calcSizes(rx_cfg);
    count_ = sizes.count;
    sizePerBuffer_ = sizes.sizePerBuffer;
    ringMask_ = sizes.ringMask;
    ringSize_ = sizes.ringSize;

    auto buff = buffers.buffer();
    auto* buffer_base = (char*)buff.first + sizes.ringMemSize;
    ring_ = (entry_union*)buff.first;
    for (size_t i = 0; i < count_; i++) {
      buffers_.push_back(buffer_base + i * sizePerBuffer_);
    }

    if (count_ >= std::numeric_limits<uint16_t>::max()) {
      die("buffer count too large: ", count_);
    }
    for (uint16_t i = 0; i < count_; i++) {
      ring_[i].buf = {};
      populate(ring_[i].buf, i);
    }
    headCached_ = count_;
    ring_[0].head.store(headCached_, std::memory_order_release);

    vlog(
        "ring address=",
        ring_,
        " ring size=",
        ringSize_,
        " buffer count=",
        count_,
        " ring_mask=",
        ringMask_,
        " head now ",
        headCached_);
  }

  ~BufferProviderV2() {}

  size_t count() const {
    return count_;
  }

  size_t sizePerBuffer() const {
    return sizePerBuffer_;
  }

  size_t toProvideCount() const {
    return cachedIndices;
  }

  bool canProvide() const {
    return false;
  }

  bool needsToProvide() const {
    return false;
  }

  void compact() {}

  inline void populate(struct __io_uring_buf& b, uint16_t i) {
    b.bid = i;
    b.addr = (__u64)getData(i);

    // can we assume kernel doesnt touch len or resv?
    b.len = sizePerBuffer_;
    // b.resv = 0;
  }

  void returnIndex(uint16_t i) {
    indices[cachedIndices++] = i;
    if (likely(cachedIndices < indices.size())) {
      return;
    }
    cachedIndices = 0;
    for (uint16_t idx : indices) {
      populate(ring_[(headCached_ & ringMask_)].buf, idx);
      ++headCached_;
    }

    ring_[0].head.store(headCached_, std::memory_order_release);
  }

  void provide(struct io_uring_sqe*) {}

  char const* getData(uint16_t i) const {
    return buffers_[i];
  }

  void initialRegister(struct io_uring* ring) {
    __io_uring_buf_reg reg;
    memset(&reg, 0, sizeof(reg));
    reg.ring_addr = (__u64)ring_;
    reg.ring_entries = ringSize_;
    reg.bgid = kBgid;
    static constexpr int __IORING_REGISTER_PBUF_RING = 22;

    checkedErrno(
        ____sys_io_uring_register(
            ring->ring_fd, __IORING_REGISTER_PBUF_RING, (void*)&reg, 1),
        "register pbuf");

    // silly io_uring, clobbering our data:
    ring_[0].head.store(headCached_);
  }

 private:
  static constexpr int kAlignment = 32;

  union entry_union {
    entry_union() : buf() {}
    entry_union(entry_union const& r) : buf(r.buf) {}
    struct {
      __u64 resv1;
      __u32 resv2;
      __u16 resv3;
      std::atomic<__u16> head;
    };
    struct __io_uring_buf buf;
  };

  static size_t addAlignment(size_t n) {
    return kAlignment * ((n + kAlignment - 1) / kAlignment);
  }

  size_t count_;
  size_t sizePerBuffer_;
  std::vector<char*> buffers_;
  uint32_t headCached_ = 0;
  uint32_t ringSize_;
  uint32_t ringMask_;
  uint32_t cachedIndices = 0;
  entry_union* ring_;
  std::array<uint16_t, 32> indices;
};

static constexpr int kUseBufferProviderFlag = 1;
static constexpr int kUseBufferProviderV2Flag = 2;
static constexpr int kUseFixedFilesFlag = 4;
static constexpr int kLoopRecvFlag = 8;

int providedBufferIdx(struct io_uring_cqe* cqe) {
  return cqe->flags >> 16;
}

template <size_t ReadSize = 4096, size_t Flags = 0>
struct BasicSock {
  static constexpr int kUseBufferProviderVersion =
      (Flags & kUseBufferProviderV2Flag) ? 2
      : (Flags & kUseBufferProviderFlag) ? 1
                                         : 0;
  static constexpr bool kUseFixedFiles = Flags & kUseFixedFilesFlag;
  static constexpr bool kShouldLoop = Flags & kLoopRecvFlag;

  using TBufferProvider = std::conditional_t<
      kUseBufferProviderVersion == 2,
      BufferProviderV2,
      BufferProviderV1>;

  explicit BasicSock(int fd) : fd_(fd) {}

  ~BasicSock() {
    if (!closed_) {
      log("socket not closed at destruct");
    }
  }

  int fd() const {
    return fd_;
  }

  uint32_t peekSend() {
    return do_send;
  }

  void didSend(uint32_t count) {}

  void addSend(struct io_uring_sqe* sqe, uint32_t len) {
    if (len > ReadSize) {
      die("too big send");
    }
    io_uring_prep_send(sqe, fd_, &buff[0], len, 0);
    if (kUseFixedFiles) {
      sqe->flags |= IOSQE_FIXED_FILE;
    }
    sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
    do_send -= std::min(len, do_send);
  }

  void addRead(struct io_uring_sqe* sqe, TBufferProvider& provider) {
    if (kUseBufferProviderVersion) {
      io_uring_prep_recv(sqe, fd_, NULL, provider.sizePerBuffer(), 0);
      sqe->flags |= IOSQE_BUFFER_SELECT;
      sqe->buf_group = TBufferProvider::kBgid;
    } else {
      io_uring_prep_recv(sqe, fd_, &buff[0], sizeof(buff), 0);
    }

    if (kUseFixedFiles) {
      sqe->flags |= IOSQE_FIXED_FILE;
    }
  }

  bool closing() const {
    return closed_;
  }

  void doClose() {
    closed_ = true;
    ::close(fd_);
  }

  void addClose(struct io_uring_sqe* sqe) {
    closed_ = true;
    if (kUseFixedFiles)
      io_uring_prep_close_direct(sqe, fd_);
    else
      io_uring_prep_close(sqe, fd_);
  }

  int didRead(
      size_t size,
      TBufferProvider& provider,
      struct io_uring_cqe* cqe) {
    // pull remaining data
    int res = size;
    int recycleBufferIdx = -1;
    if (kUseBufferProviderVersion) {
      recycleBufferIdx = providedBufferIdx(cqe);
      didRead(res, provider, recycleBufferIdx);
    } else {
      didRead(res);
    }
    while (kShouldLoop && res == (int)size) {
      res = recv(this->fd_, buff, sizeof(buff), MSG_NOSIGNAL);
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

  void didRead(size_t n, TBufferProvider& provider, int idx) {
    // read from a provided buffer
    didRead(provider.getData(idx), n);
  }
  void didRead(char const* b, size_t n) {
    do_send += parser.consume(b, n);
  }

  int fd_;
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
  using THugeBuffer = HugeBuffer<typename TSock::TBufferProvider>;
  explicit IOUringRunner(
      Config const& cfg,
      IoUringRxConfig const& rx_cfg,
      std::string const& name,
      std::unique_ptr<THugeBuffer> huge_buffer)
      : RunnerBase(name),
        cfg_(cfg),
        rxCfg_(rx_cfg),
        ring(mkIoUring(rx_cfg, *huge_buffer)),
        buffers_(rx_cfg, *huge_buffer) {
    hugeBuffer_ = std::move(huge_buffer);
    if (TSock::kUseFixedFiles && TSock::kShouldLoop) {
      die("can't have fixed files and looping, "
          "we don't have the fd to call recv() !");
    }

    cqes_.resize(rx_cfg.max_events);

    if (TSock::kUseBufferProviderVersion) {
      buffers_.initialRegister(&ring);
      provideBuffers(true);
      submit();
    }

    if (TSock::kUseFixedFiles) {
      std::vector<int> files(rx_cfg.fixed_file_count, -1);
      checkedErrno(
          io_uring_register_files(&ring, files.data(), files.size()),
          "io_uring_register_files");
      for (int i = rx_cfg.fixed_file_count - 1; i >= 0; i--)
        acceptFdPool_.push_back(i);
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
    if (TSock::kUseBufferProviderVersion != 1) {
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
  static constexpr int kOther = 0;

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

  void processClose(struct io_uring_cqe* cqe, TSock* sock) {
    int res = cqe->res;
    if (!res || res == -EBADF) {
      if (TSock::kUseFixedFiles) {
        // recycle index
        acceptFdPool_.push_back(sock->fd());
      }
    } else {
      // cannot recycle
      log("unable to close fd, ret=", res);
    }
    delete sock;
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
      if (unlikely(cqe->res == -ENOBUFS)) {
        die("not enough buffers, but will just requeue. so far have ",
            ++enobuffCount_,
            "state: can provide=",
            buffers_.toProvideCount(),
            " need=",
            buffers_.needsToProvide());
        addRead(sock);
        return;
      }
      if (cqe->res < 0 && !stopping) {
        if (unlikely(cqe->res != -ECONNRESET)) {
          log("unexpected read: ",
              cqe->res,
              "(",
              strerror(-cqe->res),
              ") deleting ",
              sock);
        }
      }
      if (cqe->res == 0 && TSock::kUseBufferProviderVersion) {
        buffers_.returnIndex(providedBufferIdx(cqe));
      }
      if (TSock::kUseFixedFiles) {
        auto* sqe = get_sqe();
        sock->addClose(sqe);
        io_uring_sqe_set_data(sqe, tag(sock, kOther));
      } else {
        sock->doClose();
        delete sock;
        delSock();
      }
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
      case kOther:
        if (cqe->user_data) {
          TSock* sock = untag<TSock>(cqe->user_data);
          if (sock->closing()) {
            // assume this was a close
            processClose(cqe, sock);
          }
        }
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
        if (got == 0) {
          if (stopping) {
            // assume some kind of cancel issue?
            expected--;
          } else {
            die("literally submitted nothing, wanted ", expected);
          }
        }
      }
      expected -= got;
    }
  }

  int submitAndWait1(struct io_uring_cqe** cqe, struct __kernel_timespec* ts) {
    int got = checkedErrno(
        io_uring_submit_and_wait_timeout(&ring, cqe, 1, ts, NULL),
        "submit_and_wait_timeout");
    if (got >= 0) {
      // io_uring_submit_and_wait_timeout actually returns 0
      expected = 0;
      return 0;
    } else if (got == -ETIME || got == -EINTR) {
      return 0;
    } else {
      die("submit_and_wait_timeout failed with ", got);
      return got;
    }
  }

  bool isOverflow() const {
    return IO_URING_READ_ONCE(*ring.sq.kflags) & IORING_SQ_CQ_OVERFLOW;
  }

  // todo: replace with io_uring_flush_overflow when it lands
  int flushOverflow() const {
    int flags = IORING_ENTER_GETEVENTS;
    if (rxCfg_.register_ring) {
      flags |= IORING_ENTER_REGISTERED_RING;
    }
    return ____sys_io_uring_enter2(
        ring.enter_ring_fd, 0, 0, flags, NULL, _NSIG / 8);
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
      bool const was_overflow = isOverflow();
      provideBuffers(false);

      rx_stats.startWait();

      if (was_overflow) {
        flushOverflow();
        rx_stats.doneWait();
      } else if (expected) {
        cqes_[0] = nullptr;
        submitAndWait1(&cqes_[0], &timeout);
        rx_stats.doneWait();
        // cqe might not be set here if we submitted
      } else {
        int wait_res = checkedErrno(
            io_uring_wait_cqe_timeout(&ring, &cqes_[0], &timeout),
            "wait_cqe_timeout");
        rx_stats.doneWait();

        // can trust here that cqe will be set
        if (!wait_res && cqes_[0]) {
          rx_stats.doneWait();
          processCqe(cqes_[0]);
          io_uring_cqe_seen(&ring, cqes_[0]);
        }
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
        loop_count += cqe_count;
      } while (cqe_count > 0 && loop_count < rxCfg_.max_cqe_loop);

      if (!cqe_count && stopping) {
        vlog("processed ", cqe_count, " socks()=", socks());
      }

      if (cfg_.print_rx_stats) {
        rx_stats.doneLoop(bytesRx_, requestsRx_, was_overflow);
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
    if (acceptFdPool_.empty()) {
      die("no fd for accept");
    }
    int ret = acceptFdPool_.back();
    acceptFdPool_.pop_back();
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

  typename TSock::TBufferProvider buffers_;
  std::vector<std::unique_ptr<ListenSock>> listenSocks_;
  std::vector<struct io_uring_cqe*> cqes_;
  int listeners_ = 0;
  uint32_t enobuffCount_ = 0;
  std::vector<int> acceptFdPool_;
  std::unique_ptr<THugeBuffer> hugeBuffer_;
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
    using Runner = IOUringRunner<typename BasicSockPicker<MbFlag>::Sock>;
    runner = std::make_unique<Runner>(
        cfg,
        rx_cfg,
        name,
        std::make_unique<typename Runner::THugeBuffer>(rx_cfg));
  }
}

template <size_t... PossibleFlag>
Receiver makeIoUringRx(
    Config const& cfg,
    IoUringRxConfig const& rx_cfg,
    std::index_sequence<PossibleFlag...>) {
  uint16_t port = pickPort(cfg);

  std::unique_ptr<RunnerBase> runner;
  size_t flags = (rx_cfg.provide_buffers == 1 ? kUseBufferProviderFlag : 0) |
      (rx_cfg.provide_buffers == 2 ? kUseBufferProviderV2Flag : 0) |
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
  int runs = 1;
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
("runs", po::value(&runs)->default_value(runs),
  "how many times to run the test")
("host", po::value(&config.send_options.host))
("v6", po::value(&config.send_options.ipv6))
("time", po::value(&config.send_options.run_seconds))
("send_threads", po::value(&config.send_options.threads)
   ->default_value(config.send_options.threads),
  "number of sender threads")
("send_connections_per_thread", po::value(&config.send_options.per_thread)
   ->default_value(config.send_options.per_thread),
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

  if (runs <= 0) {
    die("bad runs");
  } else if (runs > 1) {
    auto const rx = config.rx;
    auto const tx = config.tx;
    for (int i = 1; i < runs; i++) {
      config.rx.insert(config.rx.end(), rx.begin(), rx.end());
      config.tx.insert(config.tx.end(), tx.begin(), tx.end());
    }
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
  ("huge_pages",  po::value(&io_uring_cfg.huge_pages)
     ->default_value(io_uring_cfg.huge_pages))
  ("no_mmap",  po::value(&io_uring_cfg.no_mmap)
     ->default_value(io_uring_cfg.no_mmap))
  ("supports_nonblock_accept",  po::value(&io_uring_cfg.supports_nonblock_accept)
     ->default_value(io_uring_cfg.supports_nonblock_accept))
  ("register_ring",  po::value(&io_uring_cfg.register_ring)
     ->default_value(io_uring_cfg.register_ring))
  ("sqe_count", po::value(&io_uring_cfg.sqe_count)
     ->default_value(io_uring_cfg.sqe_count))
  ("cqe_count", po::value(&io_uring_cfg.cqe_count)
     ->default_value(io_uring_cfg.cqe_count))
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

  if (io_uring_cfg.provided_buffer_low_watermark < 0) {
    // default to quarter unless explicitly told
    io_uring_cfg.provided_buffer_low_watermark =
        io_uring_cfg.provided_buffer_count / 4;
  }

  switch (engine) {
    case RxEngine::IoUring:
      return [io_uring_cfg](Config const& cfg) -> Receiver {
        return makeIoUringRx(cfg, io_uring_cfg, std::make_index_sequence<32>{});
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

template <class T>
struct SimpleAggregate {
  explicit SimpleAggregate(std::vector<T>&& vals) {
    std::sort(vals.begin(), vals.end());
    avg = std::accumulate(vals.begin(), vals.end(), T(0)) / vals.size();
    p50 = vals[vals.size() / 2];
    p100 = vals.back();
  }

  template <class Formatter>
  std::string toString(Formatter f) const {
    return strcat("p50=", f(p50), " avg=", f(avg), " p100=", f(p100));
  }
  T avg;
  T p50;
  T p100;
};

struct AggregateResults {
  AggregateResults(SimpleAggregate<double> pps, SimpleAggregate<double> bps)
      : packetsPerSecond(std::move(pps)), bytesPerSecond(std::move(bps)) {}

  std::string toString() const {
    return strcat(
        "packetsPerSecond={",
        packetsPerSecond.toString(
            [](double x) { return strcat(x / 1000, "k"); }),
        "} bytesPerSecond={",
        bytesPerSecond.toString(
            [](double x) { return strcat(x / 1000000, "M"); }),
        "}");
  }

  SimpleAggregate<double> packetsPerSecond;
  SimpleAggregate<double> bytesPerSecond;
};

AggregateResults aggregateResults(std::vector<SendResults> const& results) {
  std::vector<double> pps;
  std::vector<double> bps;
  for (auto const& r : results) {
    pps.push_back(r.packetsPerSecond);
    bps.push_back(r.bytesPerSecond);
  }
  return AggregateResults{
      SimpleAggregate<double>{std::move(pps)},
      SimpleAggregate<double>{std::move(bps)}};
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

  std::vector<std::pair<std::string, SendResults>> results;
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
            strcat(tx, "_", rcv.name, "_", rcv.rxCfg), std::move(res));
      }
    }

    for (auto& r : results) {
      log(r.first);
      log(std::string(30, ' '), r.second.toString());
    }

    // build up to_agg but do it in insertion order of results
    // hence the nasty but probably not a big deal std::find_if
    std::vector<std::pair<std::string, std::vector<SendResults>>> to_agg;
    for (auto& r : results) {
      auto it = std::find_if(to_agg.begin(), to_agg.end(), [&](auto const& x) {
        return x.first == r.first;
      });
      if (it == to_agg.end()) {
        to_agg
            .emplace_back(
                std::piecewise_construct,
                std::make_tuple(r.first),
                std::make_tuple())
            .second.push_back(std::move(r.second));
      } else {
        it->second.push_back(std::move(r.second));
      }
    }

    for (auto& kv : to_agg) {
      if (kv.second.size() <= 1) {
        continue;
      }
      log("aggregated_", kv.first);
      log(std::string(30, ' '),
          aggregateResults(std::move(kv.second)).toString());
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
