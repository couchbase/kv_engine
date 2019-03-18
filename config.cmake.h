/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#cmakedefine HAVE_MEMALIGN 1
#cmakedefine HAVE_LIBNUMA 1
#cmakedefine HAVE_PKCS5_PBKDF2_HMAC 1
#cmakedefine HAVE_PKCS5_PBKDF2_HMAC_SHA1 1

#define COUCHBASE_MAX_NUM_BUCKETS 100
#define COUCHBASE_MAX_ITEM_PRIVILEGED_BYTES (1024 * 1024)

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
