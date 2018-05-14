/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#cmakedefine HAVE_MEMALIGN 1
#cmakedefine HAVE_LIBNUMA 1
#cmakedefine HAVE_PKCS5_PBKDF2_HMAC 1
#cmakedefine HAVE_PKCS5_PBKDF2_HMAC_SHA1 1
#cmakedefine HAVE_SSL_OP_NO_TLSv1_1 1

#ifndef HAVE_SSL_OP_NO_TLSv1_1
/*
 * Some of our platforms use an old version of OpenSSL without
 * support for anything newer than TLSv1
 */
#define SSL_OP_NO_TLSv1_1 0L
#endif

#define COUCHBASE_MAX_NUM_BUCKETS 100
#define COUCHBASE_MAX_ITEM_PRIVILEGED_BYTES (1024 * 1024)

#ifdef WIN32
#define WIN32_LEAN_AND_MEAN 1

// We need to make sure that we include winsock2.h before
// ws2tcpip.h before windows.h... if we end up with windows.h
// before those files we're getting compile errors.
#include <winsock2.h>

#include <windows.h>

#include <io.h>

#ifndef F_OK
#define F_OK 0
#endif

typedef HANDLE pid_t;

#define EX_OSERR EXIT_FAILURE
#define EX_USAGE EXIT_FAILURE

#else // !WIN32

/* need this to get IOV_MAX on some platforms. */
#ifndef __need_IOV_MAX
#define __need_IOV_MAX
#endif

#ifndef _POSIX_PTHREAD_SEMANTICS
#define _POSIX_PTHREAD_SEMANTICS
#endif

#define HAVE_SIGIGNORE 1

#endif // WIN32

/* Common section */
#include <inttypes.h>

#ifndef IOV_MAX
#define IOV_MAX 1024
#endif

#define MEMCACHED_VERSION "${MEMCACHED_VERSION}"
#define PRODUCT_VERSION "${PRODUCT_VERSION}"
#define DESTINATION_ROOT "${CMAKE_INSTALL_PREFIX}"
#define SOURCE_ROOT "${Memcached_SOURCE_DIR}"
#define OBJECT_ROOT "${Memcached_BINARY_DIR}"

/* We don't use assert() for two main reasons:
 * 1) It's disabled on non-debug builds, which we don't want.
 * 2) cb_assert() prints extra info (backtraces).
 */
#undef assert
#define assert \
    #error "assert() is forbidden. Use cb_assert() from <platform/cbassert.h instead."

/*
 * Using the ntoh-methods on Linux thread sanitizer builder cause
 * compile warnings due to the macros is using the "register"
 * keyword. Just undefine the macros since we don't need the extra
 * performance optimization during the thread sanitizer run.
 */
#if defined(THREAD_SANITIZER) && defined(linux)
#undef ntohs
#undef ntohl
#undef htons
#undef htonl
#endif
