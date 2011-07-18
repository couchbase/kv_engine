/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef ATOMIC_LIBATOMIC_H
#define ATOMIC_LIBATOMIC_H 1

/**
 * atomic.h provides a function interface to the various atomic functions,
 * but it's a C API and not C++ so I can't use the preprocessor to map
 * each function. Let's use function overloading instead and let the compiler
 * pick the one it wants...
 */
#include <atomic.h>
#include <queue>

inline int ep_sync_lock_test_and_set(volatile int *dest, int value) {
    return atomic_swap_uint((volatile uint*)dest, value);
}

inline void ep_sync_lock_release(volatile int *dest) {
    atomic_swap_uint((volatile uint*)dest, 0);
}

inline void ep_sync_synchronize(void) {
    // I don't know how to add this yet...

}

inline rel_time_t ep_sync_add_and_fetch(volatile uint64_t *dest, uint64_t value) {
     if (value == 1) {
         return atomic_inc_64_nv(dest);
     } else {
         return atomic_add_64_nv(dest, value);
     }
}

inline rel_time_t ep_sync_add_and_fetch(volatile uint32_t *dest, uint32_t value) {
     if (value == 1) {
         return atomic_inc_32_nv(dest);
     } else {
         return atomic_add_32_nv(dest, value);
     }
}

inline int ep_sync_add_and_fetch(volatile int *dest, int value) {
    if (value == 1) {
        return atomic_inc_uint_nv((volatile uint_t*)dest);
    } else {
        return atomic_add_int_nv((volatile uint_t*)dest, value);
    }
}

inline uint8_t ep_sync_add_and_fetch(volatile uint8_t *dest, uint8_t value) {
    if (value == 1) {
        return atomic_inc_8_nv(dest);
    } else {
        return atomic_add_8_nv(dest, value);
    }
}

inline hrtime_t ep_sync_add_and_fetch(volatile hrtime_t *dest, hrtime_t value) {
    if (value == 1) {
        return atomic_inc_64_nv((volatile uint64_t*)dest);
    } else {
        return atomic_add_64_nv((volatile uint64_t*)dest, value);
    }
}

inline int ep_sync_fetch_and_add(volatile int *dest, int value) {
    size_t original = *dest;
    if (value == 1) {
        atomic_inc_uint((volatile uint_t*)dest);
    } else {
        atomic_add_int((volatile uint_t*)dest, value);
    }
    return original;
}

inline uint64_t ep_sync_fetch_and_add(volatile uint64_t *dest, uint64_t value) {
    uint64_t original = *dest;
    if (value == 1) {
        atomic_inc_64(dest);
    } else {
        atomic_add_64(dest, value);
    }

    return original;
}

inline uint32_t ep_sync_fetch_and_add(volatile uint32_t *dest, uint32_t value) {
    uint32_t original = *dest;
    if (value == 1) {
        atomic_inc_32(dest);
    } else {
        atomic_add_32(dest, value);
    }

    return original;
}

inline hrtime_t ep_sync_fetch_and_add(volatile hrtime_t *dest, hrtime_t value) {
    size_t original = *dest;
    if (value == 1) {
        atomic_inc_64((volatile uint64_t*)dest);
    } else {
        atomic_add_64((volatile uint64_t*)dest, value);
    }

    return original;
}

inline bool ep_sync_bool_compare_and_swap(volatile bool *dest, bool prev, bool next) {
    hrtime_t original = *dest;
    if (original == atomic_cas_8((volatile uint8_t*)dest, (uint8_t)prev, (uint8_t)next)) {
        return true;
    } else {
        return false;
    }
}

inline bool ep_sync_bool_compare_and_swap(volatile int *dest, int prev, int next) {
    hrtime_t original = *dest;
    if (original == atomic_cas_uint((volatile uint_t*)dest, (uint_t)prev, (uint_t)next)) {
        return true;
    } else {
        return false;
    }
}

inline bool ep_sync_bool_compare_and_swap(volatile hrtime_t *dest, hrtime_t prev, hrtime_t next) {
    hrtime_t original = *dest;
    if (original == atomic_cas_64((volatile uint64_t*)dest, (uint64_t)prev, (uint64_t)next)) {
        return true;
    } else {
        return false;
    }
}

inline bool ep_sync_bool_compare_and_swap(volatile size_t *dest, size_t prev, size_t next) {
    size_t original = *dest;

#ifdef _LP64
    if (original == atomic_cas_64((volatile uint64_t*)dest, (uint64_t)prev, (uint64_t)next)) {
        return true;
    } else {
        return false;
    }
#else
    if (original == atomic_cas_32((volatile uint32_t*)dest, (uint32_t)prev, (uint32_t)next)) {
        return true;
    } else {
        return false;
    }
#endif
}

/*
 * Unfortunately C++ isn't all that happy about assinging everything to/from a
 * void pointer without a cast, so we need to add extra functions.
 * Luckily we know that the size_t is big enough to keep a pointer, so
 * we can reuse the size_t function for we already defined
 */
class QueuedItem;
typedef std::queue<QueuedItem> ItemQueue;
typedef std::queue<int> IntQueue;
class VBucket;
class VBucketHolder;
class Doodad;
class Blob;

inline bool ep_sync_bool_compare_and_swap(ItemQueue * volatile *dest, ItemQueue *prev, ItemQueue *next) {
    return ep_sync_bool_compare_and_swap((size_t*)dest, (size_t)prev, (size_t)next);
}

inline bool ep_sync_bool_compare_and_swap(VBucket* volatile* dest, VBucket* prev, VBucket* next) {
    return ep_sync_bool_compare_and_swap((size_t*)dest, (size_t)prev, (size_t)next);
}

inline bool ep_sync_bool_compare_and_swap(Blob* volatile* dest, Blob* prev, Blob* next) {
    return ep_sync_bool_compare_and_swap((size_t*)dest, (size_t)prev, (size_t)next);
}

inline bool ep_sync_bool_compare_and_swap(QueuedItem* volatile* dest, QueuedItem* prev, QueuedItem* next) {
    return ep_sync_bool_compare_and_swap((size_t*)dest, (size_t)prev, (size_t)next);
}

inline bool ep_sync_bool_compare_and_swap(VBucketHolder* volatile* dest, VBucketHolder* prev, VBucketHolder* next) {
    return ep_sync_bool_compare_and_swap((size_t*)dest, (size_t)prev, (size_t)next);
}

inline bool ep_sync_bool_compare_and_swap(Doodad* volatile* dest, Doodad* prev, Doodad* next) {
    return ep_sync_bool_compare_and_swap((size_t*)dest, (size_t)prev, (size_t)next);
}

inline bool ep_sync_bool_compare_and_swap(IntQueue * volatile *dest, IntQueue *prev, IntQueue *next) {
    return ep_sync_bool_compare_and_swap((size_t*)dest, (size_t)prev, (size_t)next);
}


#endif
