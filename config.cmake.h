/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#cmakedefine HAVE_MEMALIGN 1
#cmakedefine HAVE_LIBNUMA 1
#cmakedefine HAVE_PKCS5_PBKDF2_HMAC 1
#cmakedefine HAVE_PKCS5_PBKDF2_HMAC_SHA1 1

#define _FILE_OFFSET_BITS 64

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

#ifdef WIN32
// 'declaration' : no matching operator delete found; memory will not be
// freed if initialization throws an exception
#pragma warning(disable : 4291)
// 'conversion' conversion from 'type1' to 'type2', possible loss of data
#pragma warning(disable : 4244)
// 'var' : conversion from 'size_t' to 'type', possible loss of data
#pragma warning(disable : 4267)
// Turn of deprecated warnings
#pragma warning(disable : 4996)

#endif // WIN32
