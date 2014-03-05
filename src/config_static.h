/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/* Consider this file as an extension to config.h, just that it contains
 * static text. The intention is to reduce the number of #ifdefs in the rest
 * of the source files without having to put all of them in AH_BOTTOM
 * in configure.ac.
 */

#ifndef SRC_CONFIG_STATIC_H
#define SRC_CONFIG_STATIC_H 1

#include "config.h"

#define _FILE_OFFSET_BITS 64

#if ((defined (__SUNPRO_C) || defined(__SUNPRO_CC)) || defined __GNUC__)
#define EXPORT_FUNCTION __attribute__ ((visibility("default")))
#elif defined(_MSC_VER)
#define EXPORT_FUNCTION __declspec(dllexport)
#else
#define EXPORT_FUNCTION
#endif

#if HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif

#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif

#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif

#ifdef HAVE_POLL_H
#include <poll.h>
#endif

#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#include <sys/types.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef WIN32
#define NOMINMAX 1
#include <winsock2.h>
#include <ws2tcpip.h>
#undef NOMINMAX
#define SOCKETPAIR_AF AF_INET
#define getppid() 2

#include <io.h>
#define F_OK 0
#define W_OK 2
#define R_OK 4
#pragma warning(disable: 4291)
#pragma warning(disable: 4244)
#pragma warning(disable: 4267)
#pragma warning(disable: 4996)
#pragma warning(disable: 4800)
/*
#pragma warning(disable: )
#pragma warning(disable: )
*/

#define sched_yield() SwitchToThread()
#define snprintf _snprintf
#define sleep(a) Sleep(a * 1000)
#define random() (long)rand()

/* TROND FIXME */
#define IOV_MAX 1024
typedef unsigned int useconds_t;
#else
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#define SOCKETPAIR_AF AF_UNIX
#endif

#ifdef linux
#undef ntohs
#undef ntohl
#undef htons
#undef htonl
#endif

#if __cplusplus >= 201103L || _MSC_VER >= 1800
#define HAVE_CXX11_SUPPORT 1
#endif

#if !defined(HAVE_UNORDERED_MAP) || !defined(HAVE_ATOMIC) || !defined(HAVE_THREAD)
#undef HAVE_CXX11_SUPPORT
#endif

#ifdef HAVE_CXX11_SUPPORT
/* For now we'll allow to enable/disable separate features of the language */
#define USE_CXX11_ATOMICS 1
#endif

#ifdef HAVE_SCHED_H
#include <sched.h>
#endif

#include <platform/platform.h>
#endif /* SRC_CONFIG_STATIC_H */
