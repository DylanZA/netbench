#include <boost/algorithm/string/join.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/program_options.hpp>
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
 * Different send scenarios can be built pretty easily.
 *
 * eg command lines:
 *
 * # run a simple benchmark for io_uring and epoll in one process
 * $ ./tcprecvbench
 *
 * # prepare an io_uring listener
 * $ ./tcprecvbench --server_only 1 --rx io_uring
 *
 * # prepare an epoll listener on a given port
 * $ ./tcprecvbench --server_only 1 --rx epoll --use_port 1234
 *
 * # run a test to a prepared host
 * $ ./tcprecvbench --client_only 1 --use_port 11383 --host "foo"
 *
 *
 */

std::atomic<bool> globalShouldShutdown{false};
void intHandler(int dummy) {
  if (globalShouldShutdown.load()) {
    die("already should have shutdown at signal");
  }
  globalShouldShutdown = true;
}

struct Config {
  int backlog = 100000;
  int max_events = 32;
  int ring_size = 64;
  int recv_size = 4096;
  int io_uring_buffers = 1024;
  uint16_t use_port = 0;
  bool client_only = false;
  bool server_only = false;
  SendOptions send_options;

  bool io_uring_supports_nonblock_accept = false;
  bool io_uring_false_limit_cqes = false;

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
    Config const& cfg,
    uint16_t port,
    bool const isv6,
    int extra_flags) {
  struct sockaddr_in serv_addr;
  struct sockaddr_in6 serv_addr6;
  int fd = checkedErrno(mkBasicSock(port, isv6, extra_flags));
  checkedErrno(listen(fd, cfg.backlog), "listen");
  vlog("made sock ", fd, " v6=", isv6, " port=", port);
  return fd;
}

struct io_uring mkIoUring(Config const& cfg) {
  struct io_uring_params params;
  struct io_uring ring;
  memset(&params, 0, sizeof(params));

  checkedErrno(
      io_uring_queue_init_params(cfg.ring_size, &ring, &params),
      "io_uring_queue_init_params");
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

class RunnerBase {
 public:
  virtual void loop(std::atomic<bool>* should_shutdown) = 0;
  virtual void stop() = 0;
  virtual ~RunnerBase() = default;

 protected:
  void didRead(int x) {
    // log("didRead: ", x);
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

 private:
  int socks_ = 0;
};

class NullRunner : public RunnerBase {
 public:
  void loop(std::atomic<bool>*) override {}
  void stop() override {}
};

class BufferProvider : private boost::noncopyable {
 public:
  static constexpr int kBgid = 1;

  explicit BufferProvider(size_t count, size_t size) : sizePerBuffer_(size) {
    buffers_.resize(count);
    for (size_t i = 0; i < buffers_.size(); i++) {
      buffers_[i].resize(size);
    }
  }

  size_t count() const {
    return buffers_.size();
  }

  size_t sizePerBuffer() const {
    return sizePerBuffer_;
  }

  void add(int i, struct io_uring_sqe* sqe) {
    io_uring_prep_provide_buffers(
        sqe, buffers_[i].data(), buffers_[i].size(), 1, kBgid, i);
  }

  char const* getData(int i) const {
    return buffers_.at(i).data();
  }

 private:
  size_t sizePerBuffer_;
  std::vector<std::vector<char>> buffers_;
};

template <size_t ReadSize, bool UseBufferProvider>
struct IOUringSockBase : private boost::noncopyable {
  explicit IOUringSockBase(int fd) : fd(fd) {}
  ~IOUringSockBase() {
    close(fd);
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
    do_send -= std::min(len, do_send);
  }

  void addRead(struct io_uring_sqe* sqe, BufferProvider& provider) {
    if (UseBufferProvider) {
      io_uring_prep_recv(sqe, fd, NULL, provider.sizePerBuffer(), 0);
      sqe->flags |= IOSQE_BUFFER_SELECT;
      sqe->buf_group = BufferProvider::kBgid;
    } else {
      io_uring_prep_recv(sqe, fd, &buff[0], sizeof(buff), 0);
    }
  }

  int fd;

 protected:
  void didRead(size_t n) {
    // normal read from buffer
    didRead(buff, n);
  }

  void didRead(size_t n, BufferProvider& provider, int idx) {
    // read from a provided buffer
    didRead(provider.getData(idx), n);
  }

 private:
  void didRead(char const* b, size_t n) {
    do_send += parser.consume(b, n);
  }
  char buff[ReadSize];
  ProtocolParser parser;
  uint32_t do_send = 0;
};

template <
    size_t ReadSize = 4096,
    bool ShouldLoop = true,
    bool UseBufferProvider = false>
struct BasicSock : IOUringSockBase<ReadSize, UseBufferProvider> {
  using TParent = IOUringSockBase<ReadSize, UseBufferProvider>;
  explicit BasicSock(int fd) : TParent(fd) {}

  int didRead(size_t size, BufferProvider& provider, struct io_uring_cqe* cqe) {
    // pull remaining data
    int res = size;
    int recycleBufferIdx = -1;
    if (UseBufferProvider) {
      recycleBufferIdx = cqe->flags >> 16;
      TParent::didRead(res, provider, recycleBufferIdx);
    } else {
      TParent::didRead(res);
    }
    while (ShouldLoop && res == size) {
      res = recv(this->fd, buff, sizeof(buff), MSG_NOSIGNAL);
      if (res > 0) {
        TParent::didRead(res);
      }
    }
    return recycleBufferIdx;
  }

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
};

template <class TSock>
struct IOUringRunner : public RunnerBase {
  explicit IOUringRunner(Config const& cfg)
      : cfg_(cfg),
        ring(mkIoUring(cfg)),
        buffers_(cfg.io_uring_buffers, cfg.recv_size) {
    cqes_.resize(cfg.max_events);
    for (size_t i = 0; i < buffers_.count(); i++) {
      addBuffer(i);
    }
    submit();
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

  void addBuffer(int i) {
    buffers_.add(i, get_sqe());
  }

  static constexpr int kAccept = 1;
  static constexpr int kRead = 2;
  static constexpr int kWrite = 3;
  static constexpr int kIgnore = 0;

  void addListenSock(int fd, bool v6, int concurrent_count = 1) {
    listeners_++;
    listenSocks_.push_back(std::make_unique<ListenSock>(fd, v6));
    for (int i = 0; i < concurrent_count; i++) {
      addAccept(listenSocks_.back().get());
    }
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
    io_uring_prep_accept(sqe, ls->fd, addr, &ls->client_len, SOCK_NONBLOCK);
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
      TSock* sock = new TSock(fd);
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
      if (cfg_.io_uring_supports_nonblock_accept) {
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
        buffers_.add(recycleBufferIdx, get_sqe());
      }
      if (uint32_t sends = sock->peekSend(); sends > 0) {
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
          log("unexpected read: ", amount);
        }
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
    struct __kernel_timespec timeout;
    timeout.tv_sec = 1;
    timeout.tv_nsec = 0;

    while (socks() || !stopping) {
      submit();

      if (!checkedErrno(
              io_uring_wait_cqe_timeout(&ring, &cqes_[0], &timeout),
              "wait_cqe_timeout")) {
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
      do {
        cqe_count = io_uring_peek_batch_cqe(&ring, cqes_.data(), cqes_.size());
        for (int i = 0; i < cqe_count; i++) {
          processCqe(cqes_[i]);
        }
        io_uring_cq_advance(&ring, cqe_count);
      } while (cqe_count > 0 && !cfg_.io_uring_false_limit_cqes);

      if (!cqe_count && stopping) {
        vlog("processed ", cqe_count, " socks()=", socks());
      }
    }
  }

  void stop() override {
    stopping = true;
    for (auto& l : listenSocks_) {
      l->close();
    }
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
  int expected = 0;
  bool stopping = false;
  struct io_uring ring;
  BufferProvider buffers_;
  std::vector<std::unique_ptr<ListenSock>> listenSocks_;
  std::vector<struct io_uring_cqe*> cqes_;
  int listeners_ = 0;
  uint32_t enobuffCount_ = 0;
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
  explicit EPollRunner(Config const& cfg) {
    epoll_fd = checkedErrno(epoll_create(cfg.max_events), "epoll_create");
    rcvbuff.resize(cfg.recv_size);
    events.resize(cfg.max_events);
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

  void addListenSock(int fd, bool v6) {
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
        ed->to_write += ed->parser.consume(rcvbuff.data(), res);
        reads++;
      }
    } while (res == rcvbuff.size());
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
    while (!should_shutdown->load() && !globalShouldShutdown.load()) {
      int nevents = checkedErrno(
          epoll_wait(epoll_fd, events.data(), events.size(), 1000),
          "epoll_wait");
      if (!nevents) {
        vlog("epoll: no events socks()=", socks());
      }
      for (size_t i = 0; i < nevents; ++i) {
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
    }

    vlog("epollrunner: done socks=", socks());
  }

  int epoll_fd;
  std::vector<struct epoll_event> events;
  std::vector<char> rcvbuff;
  std::vector<std::unique_ptr<EPollData>> listeners_;
  std::unordered_set<EPollData*> sockets_;
  size_t reads = 0;
};

uint16_t pickPort(Config const& config) {
  static uint16_t startPort = 10000 + rand() % 2000;
  bool v6 = config.send_options.ipv6;
  if (config.use_port) {
    return config.use_port;
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

template <class TSock = BasicSock<>>
std::unique_ptr<RunnerBase> prep_io_uring(Config const& cfg, uint16_t port) {
  auto runner = std::make_unique<IOUringRunner<TSock>>(cfg);
  // io_uring doesnt seem to like accepting on a nonblocking socket
  int flags = cfg.io_uring_supports_nonblock_accept ? SOCK_NONBLOCK : 0;
  runner->addListenSock(
      mkServerSock(cfg, port, cfg.send_options.ipv6, flags),
      cfg.send_options.ipv6);
  return runner;
}

std::unique_ptr<RunnerBase> prep_epoll(Config const& cfg, uint16_t port) {
  auto runner = std::make_unique<EPollRunner>(cfg);
  runner->addListenSock(
      mkServerSock(cfg, port, cfg.send_options.ipv6, SOCK_NONBLOCK),
      cfg.send_options.ipv6);
  return runner;
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
};

std::unordered_map<std::string, std::function<Receiver(Config const& cfg)>>
allRx() {
  return {
      {"epoll",
       [](Config const& cfg) -> Receiver {
         uint16_t port = pickPort(cfg);
         return Receiver{prep_epoll(cfg, port), port};
       }},
      {"epoll_d1",
       [](Config const& cfg) -> Receiver {
         auto c2 = cfg;
         c2.max_events = 1;
         uint16_t port = pickPort(c2);
         return Receiver{prep_epoll(c2, port), port};
       }},

      {"io_uring",
       [](Config const& cfg) -> Receiver {
         uint16_t port = pickPort(cfg);
         return Receiver{prep_io_uring<BasicSock<>>(cfg, port), port};
       }},
      {"io_uring_d1",
       [](Config const& cfg) -> Receiver {
         auto c2 = cfg;
         c2.max_events = 1;
         uint16_t port = pickPort(c2);
         return Receiver{prep_io_uring<BasicSock<>>(c2, port), port};
       }},
      {"io_uring_provided_buff",
       [](Config const& cfg) -> Receiver {
         uint16_t port = pickPort(cfg);
         return Receiver{
             prep_io_uring<BasicSock<4096, true, true>>(cfg, port), port};
       }},
      {"io_uring_plain_recv",
       [](Config const& cfg) -> Receiver {
         uint16_t port = pickPort(cfg);
         return Receiver{
             prep_io_uring<BasicSock<4096, false, false>>(cfg, port), port};
       }},
      {"io_uring_big_buff",
       [](Config const& cfg) -> Receiver {
         uint16_t port = pickPort(cfg);
         return Receiver{prep_io_uring<BasicSock<64000>>(cfg, port), port};
       }},

  };
}

Config parse(int argc, char** argv) {
  Config config;
  po::options_description desc;
  // clang-format off
desc.add_options()
("help", "produce help message")
("verbose", "verbose logging")
("io_uring_supports_nonblock_accept",
 po::value<bool>(&config.io_uring_supports_nonblock_accept))
("ring_size", po::value(&config.ring_size))
("recv_size", po::value(&config.recv_size))
("use_port", po::value(&config.use_port), "what target port")
("server_only", po::value(&config.server_only),
 "do not tx locally, wait for it")
("client_only", po::value(&config.client_only),
 "do not rx locally, only send requests")
("host", po::value(&config.send_options.host))
("v6", po::value(&config.send_options.ipv6))
("time", po::value(&config.send_options.run_seconds))
("send_threads", po::value(&config.send_options.threads))
("send_per_thread", po::value(&config.send_options.per_thread))
("tx", po::value<std::vector<std::string> >()->multitoken(),
 "tx scenarios to run (can be multiple)")
("rx", po::value<std::vector<std::string> >()->multitoken(),
 "rx engines to run (can be multiple)")
("send_small_size", po::value(&config.send_options.small_size))
("send_medium_size", po::value(&config.send_options.medium_size))
("send_large_size", po::value(&config.send_options.large_size))
("io_uring_buffers", po::value(&config.io_uring_buffers),
 "number of provided buffers")
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
    std::cerr << "rx options are:\n";
    for (auto rx : allRx()) {
      std::cerr << "    " << rx.first << "\n";
    }
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

  if (config.use_port && config.rx.size() > 1) {
    die("can only specify port with <= 1 rx, have ", config.rx.size());
  }

  if (config.server_only && config.client_only) {
    die("only one of server/client only please");
  }

  return config;
}

int main(int argc, char** argv) {
  Config const cfg = parse(argc, argv);
  signal(SIGINT, intHandler);
  std::vector<std::function<Receiver()>> receiver_factories;
  auto const all_rx = allRx();
  auto mk_recv_fact = [&cfg](auto it) {
    return [&cfg, name = it->first, fn = it->second]() -> Receiver {
      Receiver r = fn(cfg);
      r.name = name;
      return r;
    };
  };
  for (auto const& rx : cfg.rx) {
    if (rx == "all") {
      for (auto it = all_rx.begin(); it != all_rx.end(); ++it) {
        receiver_factories.push_back(mk_recv_fact(it));
      }
    } else if (auto it = all_rx.find(rx); it != all_rx.end()) {
      receiver_factories.push_back(mk_recv_fact(it));
    } else {
      die("no rx ", rx);
    }
  }

  if (cfg.client_only) {
    if (!cfg.use_port) {
      die("please specify port for client_only");
    }
    log("using given port ", cfg.use_port, ". not setting up local receivers");
    receiver_factories.clear();
    receiver_factories.push_back([&cfg]() -> Receiver {
      return Receiver{
          std::make_unique<NullRunner>(), cfg.use_port, "given_port"};
    });
  }

  std::vector<std::string> results;
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
        log("running ", tx, " for ", rcv.name);

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
        results.push_back(strcat(
            leftpad(strcat(tx, "_", rcv.name), 30), ": ", res.toString()));
      }
    }
    for (auto const& r : results) {
      log(r);
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
      log(r.name, ": port=", r.port);
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
