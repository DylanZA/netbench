

#include "socket.h"
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/times.h>

#include "util.h"

int parse_ip6_addr(const char* str_addr, struct sockaddr_in6* sockaddr) {
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

void getAddress(
    std::string dest,
    bool isv6,
    uint16_t port,
    struct sockaddr_storage* addr,
    socklen_t* addrLen) {
  if (isv6) {
    struct sockaddr_in6* addr6 = (struct sockaddr_in6*)addr;
    *addrLen = sizeof(*addr6);
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
    struct sockaddr_in* addr4 = (struct sockaddr_in*)addr;
    *addrLen = sizeof(*addr4);
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

int mkBasicSock(bool const isv6, int extra_flags) {
  int fd = checkedErrno(
      socket(isv6 ? AF_INET6 : AF_INET, SOCK_STREAM | extra_flags, 0),
      "make socket v6=",
      isv6);
  doSetSockOpt<int>(fd, SOL_SOCKET, SO_REUSEADDR, 1);
  if (isv6) {
    doSetSockOpt<int>(fd, IPPROTO_IPV6, IPV6_V6ONLY, 1);
  }
  return fd;
}

int mkBoundSock(uint16_t port, bool const isv6, int extra_flags) {
  struct sockaddr_in serv_addr;
  struct sockaddr_in6 serv_addr6;
  struct sockaddr* paddr;
  size_t paddrlen;
  int fd = mkBasicSock(isv6, extra_flags);
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
