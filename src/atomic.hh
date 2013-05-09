/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef ATOMIC_HH
#define ATOMIC_HH

#include <pthread.h>
#include <queue>
#include <sched.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#include "callbacks.hh"
#include "locks.hh"

#define MAX_THREADS 100

#if defined(HAVE_GCC_ATOMICS)
#include "atomic/gcc_atomics.h"
#elif defined(HAVE_ATOMIC_H)
#include "atomic/libatomic.h"
#else
#error "Don't know how to use atomics on your target system!"
#endif

extern "C" {
   typedef void (*ThreadLocalDestructor)(void *);
}

/**
 * Container of thread-local data.
 */
template<typename T>
class ThreadLocal {
public:
    ThreadLocal(ThreadLocalDestructor destructor = NULL) {
        int rc = pthread_key_create(&key, destructor);
        if (rc != 0) {
            fprintf(stderr, "Failed to create a thread-specific key: %s\n", strerror(rc));
            abort();
        }
    }

    ~ThreadLocal() {
        pthread_key_delete(key);
    }

    void set(const T &newValue) {
        int rc = pthread_setspecific(key, newValue);
        if (rc != 0) {
            std::stringstream ss;
            ss << "Failed to store thread specific value: " << strerror(rc);
            throw std::runtime_error(ss.str().c_str());
        }
    }

    T get() const {
        return reinterpret_cast<T>(pthread_getspecific(key));
    }

    void operator =(const T &newValue) {
        set(newValue);
    }

    operator T() const {
        return get();
    }

private:
    pthread_key_t key;
};

/**
 * Holder of atomic values.
 */
template <typename T>
class Atomic {
public:

    Atomic(const T &initial = 0) {
        set(initial);
    }

    ~Atomic() {}

    T get() const {
        return value;
    }

    void set(const T &newValue) {
        value = newValue;
        ep_sync_synchronize();
    }

    operator T() const {
        return get();
    }

    void operator =(const T &newValue) {
        set(newValue);
    }

    bool cas(const T &oldValue, const T &newValue) {
        return ep_sync_bool_compare_and_swap(&value, oldValue, newValue);
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

    T operator +=(T increment) {
        // Returns the new value
        return ep_sync_add_and_fetch(&value, increment);
    }

    T operator -=(T decrement) {
        return ep_sync_add_and_fetch(&value, -decrement);
    }

    T incr(const T &increment) {
        // Returns the old value
        return ep_sync_fetch_and_add(&value, increment);
    }

    T decr(const T &decrement) {
        return ep_sync_add_and_fetch(&value, -decrement);
    }

    T swap(const T &newValue) {
        T rv;
        while (true) {
            rv = get();
            if (cas(rv, newValue)) {
                break;
            }
        }
        return rv;
    }

    T swapIfNot(const T &badValue, const T &newValue) {
        T oldValue;
        while (true) {
            oldValue = get();
            if (oldValue != badValue) {
                if (cas(oldValue, newValue)) {
                    break;
                }
            } else {
                break;
            }
        }
        return oldValue;
    }

   void setIfLess(const T &newValue) {
      T oldValue = get();

      while (newValue < oldValue) {
         if (cas(oldValue, newValue)) {
            break;
         }
         oldValue = get();
      }
   }

   void setIfBigger(const T &newValue) {
      T oldValue = get();

      while (newValue > oldValue) {
         if (cas(oldValue, newValue)) {
            break;
         }
         oldValue = get();
      }
   }

private:
    volatile T value;
};

/**
 * Atomic pointer.
 *
 * This does *not* make the item that's pointed to atomic.
 */
template <typename T>
class AtomicPtr : public Atomic<T*> {
public:
    AtomicPtr(T *initial = NULL) : Atomic<T*>(initial) {}

    ~AtomicPtr() {}

    T *operator ->() {
        return Atomic<T*>::get();
    }

    T &operator *() {
        return *Atomic<T*>::get();
    }

    operator bool() const {
        return Atomic<T*>::get() != NULL;
    }

    bool operator !() const {
        return  Atomic<T*>::get() == NULL;
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
    bool tryAcquire() {
       return ep_sync_lock_test_and_set(&lock, 1) == 0;
    }

    volatile int lock;
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

    mutable Atomic<int> _rc_refcount;
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
            delete value;
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

    bool cas(RCPtr<C> &oldValue, RCPtr<C> &newValue) {
        SpinLockHolder lh(&lock);
        if (value == oldValue.get()) {
            C *tmp = value;
            value = newValue.gimme();
            if (tmp != NULL &&
                static_cast<RCValue *>(tmp)->_rc_decref() == 0) {
                lh.unlock();
                delete tmp;
            }
            return true;
        }
        return false;
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
        C *tmp(value.swap(newValue));
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

#endif // ATOMIC_HH
