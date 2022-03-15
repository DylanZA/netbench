#include "sender.h"
#include <boost/thread/barrier.hpp>
#include <algorithm>
#include <deque>
#include <numeric>
#include <thread>

#include <arpa/inet.h>
#include <liburing.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>

enum class ActionOp { Unknown = 0, Connect, Disconnect, Recv, Send, Ready };
enum class SenderState { Preparing, WaitingForReady, Running, Closing, Closed };
int constexpr kPreludeSize = 4;

struct Action {
  Action() = default;
  Action(ActionOp o, uint64_t id, uint64_t param = 0)
      : op(o), id(id), param(param) {}
  ActionOp op;
  size_t id;
  uint64_t param;
};

class IBenchmarkScenario {
 public:
  virtual ~IBenchmarkScenario() = default;
  virtual bool getAction(Action& a) = 0;
  virtual void doneLast(uint64_t idx, ActionOp op) = 0;
  virtual void doneError(uint64_t idx, int error) = 0;
};

class ConnectSendLots : public IBenchmarkScenario {
 public:
  ConnectSendLots(uint64_t conns, uint64_t size)
      : conns_(conns), sendSize_(size) {
    for (uint64_t c = 1; c <= conns_; c++) {
      queue.emplace_back(Action(ActionOp::Connect, c));
    }
    queue.emplace_back(Action(ActionOp::Ready, 0));
  }

  bool getAction(Action& out) override {
    if (queue.empty())
      return false;
    out = std::move(queue.front());
    queue.pop_front();
    return true;
  }

  void doneLast(uint64_t idx, ActionOp op) override {
    switch (op) {
      case ActionOp::Send:
        queue.emplace_back(ActionOp::Recv, idx, 1);
        break;
      case ActionOp::Connect:
      case ActionOp::Recv:
        queue.emplace_back(ActionOp::Send, idx, sendSize_);
        break;
      default:
        break;
    };
  }

  void doneError(uint64_t idx, int error) override {}

 private:
  uint64_t conns_;
  uint64_t sendSize_;
  std::deque<Action> queue;
};

class ConnectSendDisconnect : public IBenchmarkScenario {
 public:
  ConnectSendDisconnect(uint64_t concurrent, uint64_t send)
      : concurrent_(concurrent), send_(send) {
    for (uint64_t c = 1; c <= concurrent_; c++) {
      queue.emplace_back(Action(ActionOp::Connect, nextIdx_++));
    }
    queue.emplace_back(Action(ActionOp::Ready, 0));
  }

  bool getAction(Action& out) override {
    if (queue.empty())
      return false;
    out = std::move(queue.front());
    queue.pop_front();
    return true;
  }

  void doneLast(uint64_t idx, ActionOp op) override {
    switch (op) {
      case ActionOp::Connect:
        queue.emplace_back(ActionOp::Send, idx, send_);
        break;
      case ActionOp::Disconnect:
        queue.emplace_back(ActionOp::Connect, nextIdx_++);
        break;
      case ActionOp::Send:
        queue.emplace_back(ActionOp::Recv, idx, 1);
        break;
      default:
        queue.emplace_back(ActionOp::Disconnect, idx);
        break;
    }
  }

  void doneError(uint64_t idx, int error) override {
    queue.emplace_back(ActionOp::Connect, nextIdx_++);
  }

 private:
  uint64_t nextIdx_ = 1;
  uint64_t concurrent_;
  uint64_t send_;
  std::deque<Action> queue;
};

std::vector<std::string> allScenarios() {
  return {
      "large",
      "small",
      "medium",
      "single_large",
      "single_small",
      "single_medium"};
}

std::unique_ptr<IBenchmarkScenario> makeScenario(
    std::string const& test,
    SendOptions const& options) {
  if (test == "large") {
    return std::make_unique<ConnectSendLots>(
        options.per_thread, options.large_size);
  } else if (test == "medium") {
    return std::make_unique<ConnectSendLots>(
        options.per_thread, options.medium_size);
  } else if (test == "small") {
    return std::make_unique<ConnectSendLots>(
        options.per_thread, options.small_size);
  } else if (test == "single_large") {
    return std::make_unique<ConnectSendDisconnect>(
        options.per_thread, options.large_size);
  } else if (test == "single_medium") {
    return std::make_unique<ConnectSendDisconnect>(
        options.per_thread, options.medium_size);
  } else if (test == "single_small") {
    return std::make_unique<ConnectSendDisconnect>(
        options.per_thread, options.small_size);
  }
  die("unknown test ", test);
  return {};
}

struct Connection {
  explicit Connection(uint64_t id) : id(id) {
    memset(&msg, 0, sizeof(struct msghdr));
    msg.msg_name = NULL;
    msg.msg_namelen = 0;
    msg.msg_iov = &iovs[0];
  }

  uint64_t id;
  int fd = -1;
  ActionOp current = ActionOp::Unknown;
  uint32_t remaining = 0;

  uint32_t whole_write = 0;
  void const* write_at = NULL;

  int connectRetries = 0;

  bool want_close = false;
  uint32_t small_buff;

  struct msghdr msg;
  struct iovec iovs[2];
};

struct SendBuffers {
  explicit SendBuffers(SendOptions const& options) {
    buff_.resize(std::max(
        options.small_size, std::max(options.medium_size, options.large_size)));
  }
  std::vector<char> const& buff() const {
    return buff_;
  }
  std::vector<char> buff_;
};

static int parse_ip6_addr(const char* str_addr, struct sockaddr_in6* sockaddr) {
  struct addrinfo hints;
  struct addrinfo* result;
  int ret;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET6;
  hints.ai_flags = AI_NUMERICHOST | AI_PASSIVE;
  ret = getaddrinfo(str_addr, NULL, &hints, &result);
  if (!ret) {
    *sockaddr = *(struct sockaddr_in6*)result->ai_addr;
    freeaddrinfo(result);
    return 0;
  }
  log("getaddrinfo(",
      str_addr,
      " ) failed with err=",
      ret,
      " : ",
      gai_strerror(ret));
  return -1;
}

class Sender {
 public:
  Sender(
      std::string const& test,
      SendOptions const& options,
      SendBuffers const& buffers,
      uint16_t port,
      boost::barrier& ready_barrier)
      : cfg_(options),
        buffers(buffers),
        scenario(makeScenario(test, options)),
        ready_barrier(ready_barrier) {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags = IORING_SETUP_CQSIZE;
    params.cq_entries = cfg_.maxOutstanding;
    checkedErrno(
        io_uring_queue_init_params(64, &ring_, &params),
        "io_uring_queue_init_params");

    std::string dest = cfg_.host;
    if (cfg_.ipv6) {
      struct sockaddr_in6* addr6 = (struct sockaddr_in6*)&addr_;
      addrLen_ = sizeof(*addr6);
      memset(addr6, 0, sizeof(*addr6));
      if (dest.empty()) {
        dest = "::1";
      }
      if (parse_ip6_addr(dest.c_str(), addr6)) {
        die("ipv6 parse error: ", dest);
      }
      addr6->sin6_family = PF_INET6;
      addr6->sin6_port = htons(port);
    } else {
      struct sockaddr_in* addr4 = (struct sockaddr_in*)&addr_;
      addrLen_ = sizeof(*addr4);
      memset(addr4, 0, sizeof(*addr4));
      addr4->sin_family = AF_INET;
      addr4->sin_port = htons(port);
      if (dest.empty()) {
        dest = "127.0.1.1";
      }
      if (inet_pton(AF_INET, dest.c_str(), &(addr4->sin_addr)) != 1) {
        die("ipv4 parse error:", dest);
      }
    }
  }

  ~Sender() {
    io_uring_queue_exit(&ring_);
  }

  bool queueOne() {
    if (outstanding_ >= cfg_.maxOutstanding) {
      return false;
    }
    if (state_ == SenderState::WaitingForReady) {
      if (outstanding_ > 0 || expected_ > 0) {
        // flush current
        return false;
      }
      ready_barrier.wait();
      end_ = std::chrono::steady_clock::now() +
          std::chrono::seconds(cfg_.run_seconds);
      state_ = SenderState::Running;
    }
    switch (state_) {
      case SenderState::Closed:
        return false;
      case SenderState::WaitingForReady:
        // waiting for things
        break;
      case SenderState::Closing:
        if (outstanding_) {
          // dont want to interfere
          return false;
        }
        if (toClose.size()) {
          runAction(Action{ActionOp::Disconnect, toClose.back()});
          toClose.pop_back();
          return true;
        } else {
          return false;
        }
      case SenderState::Preparing:
      case SenderState::Running: {
        Action a;
        if (scenario->getAction(a)) {
          runAction(a);
          return true;
        }
      } break;
    }
    return false;
  }

  Connection* tryGetConnection(uint64_t idx, bool create) {
    auto it = connections.find(idx);
    if (it == connections.end()) {
      if (!create) {
        return nullptr;
      }
      it = connections.emplace_hint(it, idx, std::make_unique<Connection>(idx));
    }
    return it->second.get();
  }

  void runAction(Action const& a) {
    if (a.op == ActionOp::Ready) {
      state_ = SenderState::WaitingForReady;
      vlog("waiting to be ready");
      return;
    }
    Connection* connection = tryGetConnection(a.id, a.op == ActionOp::Connect);
    if (!connection) {
      // drop this one
      return;
    }
    if (connection->current != ActionOp::Unknown) {
      // we only allow one action at a time,
      // the exception being shutdown which we like to allow at any time
      if (a.op == ActionOp::Disconnect) {
        connection->want_close = true;
      } else {
        die("bad current ", (int)connection->current, " want ", (int)a.op);
      }
    }
    connection->current = a.op;
    switch (a.op) {
      case ActionOp::Connect:
        queueConnect(connection);
        break;
      case ActionOp::Disconnect: {
        auto* sqe = get_sqe();
        io_uring_prep_close(sqe, connection->fd);
        io_uring_sqe_set_data(sqe, (void*)connection->id);
      } break;
      case ActionOp::Recv:
        queueRecv(connection, a.param);
        break;
      case ActionOp::Send:
        queueNewSend(connection, a.param);
        break;
      default:
        die("bad op ", (int)a.op);
    };
  }

  void submit() {
    while (expected_) {
      int got = io_uring_submit(&ring_);
      if (got != expected_) {
        // log("sender: expected to submit ", expected_, " but did ", got);
        if (got == 0) {
          die("literally sent nothing");
        }
      }
      outstanding_ += got;
      expected_ -= got;
    }
  }

  void queueConnect(Connection* connection) {
    if (connection->fd < 0) {
      int type = cfg_.ipv6 ? PF_INET6 : PF_INET;
      connection->fd = checkedErrno(socket(type, SOCK_STREAM, 0));
    }
    if (cfg_.zero_send_buf) {
      doSetSockOpt<int>(connection->fd, SOL_SOCKET, SO_SNDBUF, 0);
    }
    auto* sqe = get_sqe();
    io_uring_prep_connect(
        sqe, connection->fd, (struct sockaddr*)&addr_, addrLen_);
    io_uring_sqe_set_data(sqe, (void*)connection->id);
  }

  void queueSend(Connection* connection) {
    size_t idx = 0;
    connection->msg.msg_iovlen = 1;
    size_t to_send = connection->remaining;
    if (connection->whole_write - to_send <= kPreludeSize) {
      connection->msg.msg_iovlen = 2;
      idx = 1;

      // still need to send the first 4 bytes
      uint32_t offset = connection->whole_write - to_send;
      uint32_t prelude_send = kPreludeSize - offset;

      connection->iovs[0].iov_base =
          (void*)(((char const*)&connection->small_buff) + offset);
      connection->iovs[0].iov_len = prelude_send;
      to_send -= kPreludeSize;
    }
    auto* sqe = get_sqe();
    connection->iovs[idx].iov_base = (void*)connection->write_at;
    connection->iovs[idx].iov_len = to_send;
    io_uring_prep_sendmsg(sqe, connection->fd, &connection->msg, MSG_NOSIGNAL);
    io_uring_sqe_set_data(sqe, (void*)connection->id);
  }

  void queueNewSend(Connection* connection, uint32_t length) {
    connection->whole_write = connection->remaining = length + kPreludeSize;
    connection->write_at = buffers.buff().data();
    memcpy(&connection->small_buff, &length, sizeof(connection->small_buff));
    queueSend(connection);
  }

  void queueRecv(Connection* connection, uint32_t length) {
    if (length > sizeof(connection->small_buff)) {
      die(length, " too big");
    }
    connection->remaining = length;
    auto* sqe = get_sqe();
    io_uring_prep_recv(
        sqe, connection->fd, (void*)&connection->small_buff, length, 0);
    io_uring_sqe_set_data(sqe, (void*)connection->id);
  }

  void processRes(Connection* connection, int res) {
    bool finished = true;
    bool waserror = false;
    bool kill = false;
    auto was = connection->current;
    connection->current = ActionOp::Unknown;
    switch (was) {
      case ActionOp::Connect:
        if (res < 0) {
          waserror = true;
          kill = true;
          connectErrors_++;
          maybeTooManyConnectErrors();
        } else {
          // connected no problem
          successConnects_++;
        }
        break;
      case ActionOp::Recv:
        // we didn't change the old state
        if (res <= 0) {
          vlog("recv bad read: ", res);
          recvErrors_++;
          waserror = true;
        } else if ((size_t)res < connection->remaining) {
          connection->remaining -= res;
          queueRecv(connection, connection->remaining);
          finished = false;
        } else {
          // finished
        }
        break;
      case ActionOp::Send:
        if (res <= 0) {
          waserror = true;
          if (res != -ECONNRESET) {
            vlog(
                "sender: send res < 0: was=",
                (int)was,
                " fd=",
                connection->fd,
                " res=",
                res);
          }
          sendErrors_++;
          connection->remaining = 0;
        } else if (res > 0) {
          connection->remaining -= std::min<size_t>(connection->remaining, res);
          if (connection->remaining > 0) {
            // log("was was ", (int)was, " to write ", connection->remaining, "
            // res=", res);
            queueSend(connection);
            finished = false;
          } else {
            statsFinishedWrite(res);
          }
        }
        break;
      case ActionOp::Disconnect:
        // disconnected, kill the connection
        // don't really care if it failed
        kill = true;
        break;
      default:
        vlog("sender: weird current action ", (int)was, " res=", res);
        kill = true;
        break;
    }

    if (!finished) {
      // still running an op, don't do anything further
      connection->current = was;
      return;
    }

    if (waserror) {
      scenario->doneError(connection->id, res);
      connection->want_close = true;
    } else {
      scenario->doneLast(connection->id, was);
    }

    if (kill) {
      connections.erase(connection->id);
    } else if (connection->want_close) {
      // queue up a disconnect
      runAction(Action{ActionOp::Disconnect, connection->id});
    }
  }

  void processCqe(struct io_uring_cqe* cqe) {
    if (cqe->user_data == LIBURING_UDATA_TIMEOUT) {
      io_uring_cqe_seen(&ring_, cqe);
      return;
    }

    Connection* connection = tryGetConnection(cqe->user_data, false);
    int res = cqe->res;
    io_uring_cqe_seen(&ring_, cqe);
    outstanding_--;
    if (connection) {
      processRes(connection, res);
    } else {
      // fire and forget
    }
  }

  void processCompletions() {
    struct __kernel_timespec timeout;
    timeout.tv_sec = 1;
    timeout.tv_nsec = 0;
    struct io_uring_cqe* cqes[1024];
    checkedErrno(
        io_uring_wait_cqe_timeout(&ring_, &cqes[0], &timeout),
        "sender processCompletions");
    int cqe_count =
        io_uring_peek_batch_cqe(&ring_, cqes, sizeof(cqes) / sizeof(cqes[0]));
    if (!cqe_count) {
      vlog(
          "no cqe state=",
          (int)state_,
          " connections=",
          connections.size(),
          " outstanding=",
          outstanding_,
          " errorsend=",
          sendErrors_,
          " errorconn=",
          connectErrors_);
    }
    for (int i = 0; i < cqe_count; i++) {
      processCqe(cqes[i]);
    }
  }

  struct io_uring_sqe* get_sqe() {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
      submit();
      sqe = io_uring_get_sqe(&ring_);
      if (!sqe) {
        die("still not sqe!");
      }
    }
    ++expected_;
    return sqe;
  }

  SendResults go() {
    SendResults res;
    while (state_ != SenderState::Closed) {
      if (state_ == SenderState::Running &&
          std::chrono::steady_clock::now() > end_) {
        state_ = SenderState::Closing;
        // close all the connections
        for (auto const& kv : connections) {
          toClose.push_back(kv.first);
        }

        // make the results now, so it doesnt include cleanup
        res = {};
        res.packetsPerSecond = packetsSent_ / cfg_.run_seconds;
        res.bytesPerSecond = bytesSent_ / cfg_.run_seconds;
        res.sendErrors = sendErrors_;
        res.recvErrors = recvErrors_;
        res.connectErrors = connectErrors_;
        res.connects = successConnects_;
      }
      if (state_ == SenderState::Closing && connections.empty()) {
        state_ = SenderState::Closed;
        break;
      }

      while (1) {
        while (queueOne())
          ;
        if (expected_) {
          submit();
        } else {
          break;
        }
      }

      processCompletions();
    }

    return res;
  }

  void statsFinishedWrite(int size) {
    if (state_ != SenderState::Running) {
      return;
    }
    packetsSent_++;
    bytesSent_ += size;
  }

 private:
  void maybeTooManyConnectErrors() {
    // bail out early
    if (connectErrors_ > 100 && connectErrors_ > 100 * successConnects_) {
      die("too many connection errors: ",
          connectErrors_,
          " vs successes: ",
          successConnects_);
    }
  }

  SendOptions const cfg_;
  SendBuffers const& buffers;
  std::unique_ptr<IBenchmarkScenario> scenario;
  boost::barrier& ready_barrier;
  std::unordered_map<uint64_t, std::unique_ptr<Connection>> connections;
  int expected_ = 0;
  int outstanding_ = 0;
  struct io_uring ring_;
  SenderState state_ = SenderState::Preparing;
  struct sockaddr_storage addr_;
  socklen_t addrLen_;
  std::chrono::steady_clock::time_point end_;
  std::vector<uint64_t> toClose;

  size_t bytesSent_ = 0;
  size_t packetsSent_ = 0;
  size_t connectErrors_ = 0;
  size_t sendErrors_ = 0;
  size_t recvErrors_ = 0;
  size_t successConnects_ = 0;
};

SendResults
runSender(std::string const& test, SendOptions const& options, uint16_t port) {
  SendBuffers buffers{options};
  std::vector<SendResults> results;
  std::vector<std::thread> threads;
  boost::barrier ready_barrier{(unsigned int)options.threads + 1};
  results.resize(options.threads);
  for (int i = 0; i < options.threads; i++) {
    auto sender =
        std::make_unique<Sender>(test, options, buffers, port, ready_barrier);
    threads.push_back(std::thread{wrapThread(
        strcat("send", i), [i, s = std::move(sender), r = &results[i]]() {
          *r = s->go();
          vlog("test ", i, " done with ", r->toString());
        })});
  }

  ready_barrier.wait();
  vlog("sender started test");

  for (auto& t : threads) {
    t.join();
  }
  return std::accumulate(
      results.begin(), results.end(), SendResults{}, SendResults::merge);
}