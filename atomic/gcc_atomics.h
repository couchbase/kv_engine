/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef ATOMIC_GCC_ATOMICS_H
#define ATOMIC_GCC_ATOMICS_H 1

#define ep_sync_add_and_fetch(a, b) __sync_add_and_fetch(a, b);
#define ep_sync_bool_compare_and_swap(a, b, c) __sync_bool_compare_and_swap(a, b, c)
#define ep_sync_fetch_and_add(a, b) __sync_fetch_and_add(a, b);
#define ep_sync_lock_release(a) __sync_lock_release(a)
#define ep_sync_lock_test_and_set(a, b) __sync_lock_test_and_set(a, b)
#define ep_sync_synchronize() __sync_synchronize()

#endif
