/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#ifndef SRC_ATOMIC_H_
#define SRC_ATOMIC_H_ 1

#include "config.h"

#include <algorithm>
#include <queue>
#include <vector>

#ifdef USE_CXX11_ATOMICS
#include <atomic>
#include <thread>
#define AtomicValue std::atomic
using std::memory_order;
using std::memory_order_relaxed;
using std::memory_order_consume;
using std::memory_order_acquire;
using std::memory_order_release;
using std::memory_order_acq_rel;
using std::memory_order_seq_cst;
#else
#define AtomicValue CouchbaseAtomic
enum memory_order {
    memory_order_relaxed,
    memory_order_consume,
    memory_order_acquire,
    memory_order_release,
    memory_order_acq_rel,
    memory_order_seq_cst
};
#endif

#if defined(HAVE_GCC_ATOMICS)
#include "atomic/gcc_atomics.h"
#elif defined(HAVE_ATOMIC_H)
#include "atomic/libatomic.h"
#elif _MSC_VER
#define ep_sync_synchronize() MemoryBarrier()
#else
#error "Don't know how to use atomics on your target system!"
#endif

#include "callbacks.h"
#include "locks.h"

#ifndef _MSC_VER
/**
 * Holder of atomic values.
 */
template <typename T>
class CouchbaseAtomic {
public:

    CouchbaseAtomic(const T &initial = (T)0) {
        store(initial);
    }

    ~CouchbaseAtomic() {}

    T load(memory_order sync = memory_order_acq_rel) const {
        (void) sync;
        return ep_sync_fetch_and_add(const_cast<T*>(&value), T(0));
    }

    void store(const T &newValue, memory_order sync = memory_order_acq_rel) {
        (void) sync;
        value = newValue;
        ep_sync_synchronize();
    }


    bool compare_exchange_strong(T& expected, T val,
                                 memory_order sync = memory_order_acq_rel)  {
        (void) sync;
        if (ep_sync_bool_compare_and_swap(&value, expected, val)) {
            return true;
        } else {
            expected = load();
            return false;
        }
    }

    operator T() const {
        return load();
    }

    void operator =(const T &newValue) {
        store(newValue);
    }

    T operator ++() { // prefix
        return ep_sync_add_and_fetch(&value, 1);
    }

    T operator ++(int) { // postfix
        return ep_sync_fetch_and_add(&value, 1);
    }

    T operator --() { // prefix
        return ep_sync_add_and_fetch(&value, -1);
    }

    T operator --(int) { // postfix
        return ep_sync_fetch_and_add(&value, -1);
    }

    T fetch_add(const T &increment, memory_order sync = memory_order_acq_rel) {
        // Returns the old value
        (void) sync;
        return ep_sync_fetch_and_add(&value, increment);
    }

    T fetch_sub(const T &decrement, memory_order sync = memory_order_acq_rel) {
        (void) sync;
        return ep_sync_add_and_fetch(&value, -(signed)decrement);
    }

    T exchange(const T &newValue) {
        T rv;
        while (true) {
            rv = load();
            if (compare_exchange_strong(rv, newValue)) {
                break;
            }
        }
        return rv;
    }

    T swapIfNot(const T &badValue, const T &newValue) {
        T oldValue;
        while (true) {
            oldValue = load();
            if (oldValue != badValue) {
                if (compare_exchange_strong(oldValue, newValue)) {
                    break;
                }
            } else {
                break;
            }
        }
        return oldValue;
    }

private:

    volatile T value;
};

template <>
class CouchbaseAtomic<double> {
    typedef union doubleIntStore {
        double d;
        uint64_t i;
    } DoubleIntStore;

public:
    CouchbaseAtomic(const double& initial = 0) {
        store(initial);
    }

    double load(memory_order sync = memory_order_acq_rel) const {
        (void) sync;
        DoubleIntStore rv;
        rv.i = ep_sync_fetch_and_add((uint64_t*)&value.i, 0);
        return rv.d;
    }

    void store(const double& newValue, memory_order sync = memory_order_acq_rel) {
        (void) sync;
        DoubleIntStore input;
        input.d = newValue;
        (void)ep_sync_lock_test_and_set(&value.i, input.i);
    }

    operator double() const {
        return load();
    }

private:
    volatile DoubleIntStore value;
};

#endif

template <typename T>
void atomic_setIfBigger(AtomicValue<T> &obj, const T &newValue) {
    T oldValue = obj.load();
    while (newValue > oldValue) {
        if (obj.compare_exchange_strong(oldValue, newValue)) {
            break;
        }
        oldValue = obj.load();
    }
}

template <typename T>
void atomic_setIfLess(AtomicValue<T> &obj, const T &newValue) {
    T oldValue = obj.load();
    while (newValue < oldValue) {
        if (obj.compare_exchange_strong(oldValue, newValue)) {
            break;
        }
        oldValue = obj.load();
    }
}

/**
 * Atomic pointer.
 *
 * This does *not* make the item that's pointed to atomic.
 */
template <typename T>
class AtomicPtr : public AtomicValue<T*> {
public:
    AtomicPtr(T *initial = NULL) : AtomicValue<T*>(initial) {}

    ~AtomicPtr() {}

    T *operator ->() {
        return AtomicValue<T*>::load();
    }

    T &operator *() {
        return *AtomicValue<T*>::load();
    }

    operator bool() const {
        return AtomicValue<T*>::load() != NULL;
    }

    bool operator !() const {
        return AtomicValue<T*>::load() == NULL;
    }
};

/**
 * A lighter-weight, smaller lock than a mutex.
 *
 * This is primarily useful when contention is rare.
 */
class SpinLock {
public:
    // It seems like inlining the code caused the dtrace probe to
    // be optimized away ;)
    SpinLock();
    ~SpinLock();

    void acquire(void);
    void release(void);

private:
    bool tryAcquire(void);

#ifdef USE_CXX11_ATOMICS
    std::atomic_flag lock;
#else
    volatile int lock;
#endif
    DISALLOW_COPY_AND_ASSIGN(SpinLock);
};

/**
 * Safe LockHolder for SpinLock instances.
 */
class SpinLockHolder {
public:
    SpinLockHolder(SpinLock *theLock) : sl(theLock) {
        lock();
    }

    ~SpinLockHolder() {
        unlock();
    }

    void lock() {
        sl->acquire();
        locked = true;
    }

    void unlock() {
        if (locked) {
            sl->release();
            locked = false;
        }
    }
private:
    SpinLock *sl;
    bool locked;
};

template <class T> class RCPtr;
template <class S> class SingleThreadedRCPtr;

/**
 * A reference counted value (used by RCPtr and SingleThreadedRCPtr).
 */
class RCValue {
public:
    RCValue() : _rc_refcount(0) {}
    RCValue(const RCValue &) : _rc_refcount(0) {}
    ~RCValue() {}
private:
    template <class MyTT> friend class RCPtr;
    template <class MySS> friend class SingleThreadedRCPtr;
    int _rc_incref() const {
        return ++_rc_refcount;
    }

    int _rc_decref() const {
        return --_rc_refcount;
    }

    mutable AtomicValue<int> _rc_refcount;
};

/**
 * Concurrent reference counted pointer.
 */
template <class C>
class RCPtr {
public:
    RCPtr(C *init = NULL) : value(init) {
        if (init != NULL) {
            static_cast<RCValue*>(value)->_rc_incref();
        }
    }

    RCPtr(const RCPtr<C> &other) : value(other.gimme()) {}

    ~RCPtr() {
        if (value && static_cast<RCValue *>(value)->_rc_decref() == 0) {
            delete get();
        }
    }

    void reset(C *newValue = NULL) {
        if (newValue != NULL) {
            static_cast<RCValue *>(newValue)->_rc_incref();
        }
        swap(newValue);
    }

    void reset(const RCPtr<C> &other) {
        swap(other.gimme());
    }

    // safe for the lifetime of this instance
    C *get() const {
        return value;
    }

    RCPtr<C> & operator =(const RCPtr<C> &other) {
        reset(other);
        return *this;
    }

    C &operator *() const {
        return *value;
    }

    C *operator ->() const {
        return value;
    }

    bool operator! () const {
        return !value;
    }

    operator bool () const {
        return (bool)value;
    }

private:
    C *gimme() const {
        SpinLockHolder lh(&lock);
        if (value) {
            static_cast<RCValue *>(value)->_rc_incref();
        }
        return value;
    }

    void swap(C *newValue) {
        SpinLockHolder lh(&lock);
        C *tmp(value.exchange(newValue));
        lh.unlock();
        if (tmp != NULL && static_cast<RCValue *>(tmp)->_rc_decref() == 0) {
            delete tmp;
        }
    }

    AtomicPtr<C> value;
    mutable SpinLock lock; // exists solely for the purpose of implementing reset() safely
};

/**
 * Single-threaded reference counted pointer.
 * "Single-threaded" means that the reference counted pointer should be accessed
 * by only one thread at any time or accesses to the reference counted pointer
 * by multiple threads should be synchronized by the external lock.
 */
template <class T>
class SingleThreadedRCPtr {
public:
    SingleThreadedRCPtr(T *init = NULL) : value(init) {
        if (init != NULL) {
            static_cast<RCValue*>(value)->_rc_incref();
        }
    }

    SingleThreadedRCPtr(const SingleThreadedRCPtr<T> &other) : value(other.gimme()) {}

    ~SingleThreadedRCPtr() {
        if (value && static_cast<RCValue *>(value)->_rc_decref() == 0) {
            delete value;
        }
    }

    void reset(T *newValue = NULL) {
        if (newValue != NULL) {
            static_cast<RCValue *>(newValue)->_rc_incref();
        }
        swap(newValue);
    }

    void reset(const SingleThreadedRCPtr<T> &other) {
        swap(other.gimme());
    }

    // safe for the lifetime of this instance
    T *get() const {
        return value;
    }

    SingleThreadedRCPtr<T> & operator =(const SingleThreadedRCPtr<T> &other) {
        reset(other);
        return *this;
    }

    T &operator *() const {
        return *value;
    }

    T *operator ->() const {
        return value;
    }

    bool operator! () const {
        return !value;
    }

    operator bool () const {
        return (bool)value;
    }

private:
    T *gimme() const {
        if (value) {
            static_cast<RCValue *>(value)->_rc_incref();
        }
        return value;
    }

    void swap(T *newValue) {
        T *old = value;
        value = newValue;
        if (old != NULL && static_cast<RCValue *>(old)->_rc_decref() == 0) {
            delete old;
        }
    }

    T *value;
};

#endif  // SRC_ATOMIC_H_
