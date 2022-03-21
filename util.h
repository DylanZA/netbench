#pragma once

#include <pthread.h>
#include <string.h>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include <sys/socket.h>
#include <sys/types.h>

float logTime();
void setVerbose();
bool isVerbose();

namespace {

template <typename... T>
inline std::string strcat(const T&... vals) {
  std::stringstream ss;
  (ss << ... << vals) << "";
  return ss.str();
}

template <typename... T>
inline void die(const T&... vals) {
  throw std::runtime_error(strcat(vals...));
}

template <typename... T>
inline void log(const T&... vals) {
  std::stringstream ss;
  ss << "[" << std::fixed << std::setw(9) << std::setprecision(3) << logTime()
     << "] " << strcat(vals...) << "\n";
  auto str = ss.str();
  fwrite(str.data(), 1, str.size(), stderr);
}

template <typename... T>
inline void _vlog(const T&... vals) {
  if (!isVerbose())
    return;
  log(vals...);
}

#define vlog(...) \
  _vlog(__FILE__, ":", __LINE__, ':', __FUNCTION__, ' ', __VA_ARGS__)

inline std::string leftpad(std::string x, size_t n) {
  if (x.size() >= n)
    return x;
  return std::string(n - x.size(), ' ') + x;
}

struct InterruptedException : std::exception {};

template <typename TResult, typename... T>
inline TResult checkedErrno(TResult res, const T&... vals) {
  static_assert(std::is_integral_v<TResult>, "result type should be integral!");
  if (res < 0) {
    int64_t error = res == -1 ? errno : -res;
    if (error == EINTR) {
      throw InterruptedException{};
    }
    if (error == ETIME) {
      return -ETIME; // probably fine
    }
    die(strcat(vals...),
        ": failed with ",
        res,
        " err=",
        strerror(error),
        "(",
        error,
        ")");
  }
  return res;
}

template <class T>
inline void doSetSockOpt(int fd, int t, int l, T val) {
  checkedErrno(setsockopt(fd, t, l, &val, sizeof(val)), "doSetSockOpt");
}

template <class FN>
inline auto wrapThread(std::string name, FN&& fn) {
  return [name, f = std::move(fn)]() mutable {
    if (name.size()) {
      pthread_setname_np(pthread_self(), name.c_str());
    }
    try {
      f();
    } catch (std::exception const& e) {
      log("uncaught thread exception: ", e.what());
      fflush(stderr);
      std::terminate();
    }
  };
}

} // namespace
