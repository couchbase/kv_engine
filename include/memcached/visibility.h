/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

/**
 * VSC++ does not want extern to be specified for C++ classes exported
 * via shared libraries. GNU does not mind.
 * so MEMCACHED_PUBLIC_CLASS had to be added
 */
#if defined __GNUC__
#define MEMCACHED_PUBLIC_API __attribute__ ((visibility("default")))
#define MEMCACHED_PUBLIC_CLASS __attribute__((visibility("default")))
#elif defined(_MSC_VER)
#define MEMCACHED_PUBLIC_API extern __declspec(dllexport)
#define MEMCACHED_PUBLIC_CLASS __declspec(dllexport)
#else
#define MEMCACHED_PUBLIC_API
#define MEMCACHED_PUBLIC_CLASS
#endif
