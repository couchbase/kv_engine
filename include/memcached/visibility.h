/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_VISIBILITY_H
#define MEMCACHED_VISIBILITY_H 1

/**
 * VSC++ does not want extern to be specified for C++ classes exported
 * via shared libraries. GNU does not mind.
 * so MEMCACHED_PUBLIC_CLASS had to be added
 */

#if (defined(__SUNPRO_C) && (__SUNPRO_C >= 0x550)) || (defined(__SUNPRO_CC) && (__SUNPRO_CC >= 0x550))
#define MEMCACHED_PUBLIC_API __global
#define MEMCACHED_PUBLIC_CLASS __global
#elif defined __GNUC__
#define MEMCACHED_PUBLIC_API __attribute__ ((visibility("default")))
#define MEMCACHED_PUBLIC_CLASS __attribute__((visibility("default")))
#elif defined(_MSC_VER)
#define MEMCACHED_PUBLIC_API extern __declspec(dllexport)
#define MEMCACHED_PUBLIC_CLASS __declspec(dllexport)
#else
#define MEMCACHED_PUBLIC_API
#define MEMCACHED_PUBLIC_CLASS
#endif

#endif /* MEMCACHED_VISIBILITY_H */
