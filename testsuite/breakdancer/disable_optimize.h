/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef DISABLE_OPTIMIZE_H
#define DISABLE_OPTIMIZE_H 1

/* avoid wasting time trying to optimize those countless test functions */
#if defined(__clang__)


/*
 * Works for Alk since clang-3.5.
 * Unfortunately it looks like Apple have their own versioning scheme for
 * clang, because mine (Trond) reports itself as 5.1 and does not have
 * the pragma.
 */

#if ((__clang_major__ * 0x100 + __clang_minor) >= 0x305) && !defined(__APPLE__)
#pragma clang optimize off
#endif

#elif defined(__GNUC__)
/*
 * gcc docs indicate that pragma optimize is supported since 4.4. Earlier
 * versions will emit harmless warning.
 */
#if ((__GNUC__ * 0x100 + __GNUC_MINOR__) >= 0x0404)
#pragma GCC optimize ("O0")
#endif

#endif /* __GNUC__ */


#endif
