/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/* Consider this file as an extension to config.h, just that it contains
 * static text. The intention is to reduce the number of #ifdefs in the rest
 * of the source files without having to put all of them in AH_BOTTOM
 * in configure.ac.
 */
#ifndef CONFIG_STATIC_H
#define CONFIG_STATIC_H 1

#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

#if ((defined (__SUNPRO_C) || defined(__SUNPRO_CC)) || defined __GNUC__)
#define EXPORT_FUNCTION __attribute__ ((visibility("default")))
#else
#define EXPORT_FUNCTION
#endif

#if HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif

#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif

#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif

#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif

#ifdef HAVE_WINSOCK2_H
#include <winsock2.h>
#endif

#ifdef HAVE_WS2TCPIP_H
#include <ws2tcpip.h>
#endif

#ifdef WIN32
#define SOCKETPAIR_AF AF_INET
#else
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#define SOCKETPAIR_AF AF_UNIX
#endif

#ifndef HAVE_GETHRTIME
typedef uint64_t hrtime_t;

#ifdef __cplusplus
extern "C" {
#endif
    extern hrtime_t gethrtime(void);
#ifdef __cplusplus
}
#endif

#endif

#ifndef SQLITE_HAS_CODEC
#define SQLITE_HAS_CODEC 0
#endif

#ifdef HAVE_POLL_H
#include <poll.h>
#endif

#ifdef linux
#undef ntohs
#undef ntohl
#undef htons
#undef htonl
#endif

#ifndef HAVE_HTONLL
#ifdef __cplusplus
extern "C" {
#endif
    extern uint64_t htonll(uint64_t);
    extern uint64_t ntohll(uint64_t);
#ifdef __cplusplus
}
#endif
#endif

#endif
