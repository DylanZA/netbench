#pragma once

#include <cstdint>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string>

int mkBasicSock(bool isv6, int extra_flags = 0);
int mkBoundSock(uint16_t port, bool isv6, int extra_flags = 0);
int parse_ip6_addr(const char* str_addr, struct sockaddr_in6* sockaddr);
void getAddress(
    std::string dest,
    bool isv6,
    uint16_t port,
    struct sockaddr_storage* addr,
    socklen_t* addrLen);
