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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#include <algorithm>
#include <queue>
#include <vector>

#if __cplusplus >= 201103L || _MSC_VER >= 1800
#define USE_CXX11_ATOMICS 1
#include <atomic>
#endif


// #if __cplusplus >= 201103L || _MSC_VER >= 1800
//
// This block is currently commented out because the work is not
// complete yet. It will however be hard to keep the patch "in sync"
// if we wait too long before merging it...
//
// The current status for the Atomic class is that it "almost" maps
// to the std::atomic<> in C++11. We've still got too many methods
// in there that needs to be moved out..
//
// #include <atomic>
// using std::memory_order;
// using std::memory_order_seq_cst;

// #define Atomic std::atomic

// #else
enum memory_order {
    memory_order_seq_cst
};

#define Atomic CouchbaseAtomic

#if defined(HAVE_GCC_ATOMICS)
#include "atomic/gcc_atomics.h"
#elif defined(HAVE_ATOMIC_H)
#include "atomic/libatomic.h"
#elif _MSC_VER >= 1800
#include "atomic/msvc_atomics.h"
#else
#error "Don't know how to use atomics on your target system!"
#endif

// #endif



#include "callbacks.h"
#include "locks.h"

#define MAX_THREADS 100

/**
 * Container of thread-local data.
 */
template<typename T>
class ThreadLocal {
public:
    ThreadLocal() {
        int rc = pthread_key_create(&key, NULL);
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
  * Container for a thread-local pointer.
  */
template <typename T>
class ThreadLocalPtr : public ThreadLocal<T*> {
public:
    ThreadLocalPtr() : ThreadLocal<T*>() {}

    ~ThreadLocalPtr() {}

    T *operator ->() {
        return ThreadLocal<T*>::get();
    }

    T operator *() {
        return *ThreadLocal<T*>::get();
    }

    void operator =(T *newValue) {
        this->set(newValue);
    }
};

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

    T load(memory_order sync = memory_order_seq_cst) const {
        (void) sync;
        return value;
    }

    void store(const T &newValue, memory_order sync = memory_order_seq_cst) {
        (void) sync;
        value = newValue;
        ep_sync_synchronize();
    }


    bool compare_exchange_strong(T& expected, T val,
                                 memory_order sync = memory_order_seq_cst)  {
        (void) sync;
        return ep_sync_bool_compare_and_swap(&value, expected, val);
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

    T fetch_add(const T &increment, memory_order sync = memory_order_seq_cst) {
        // Returns the old value
        (void) sync;
        return ep_sync_fetch_and_add(&value, increment);
    }

    T fetch_sub(const T &decrement, memory_order sync = memory_order_seq_cst) {
        (void) sync;
        return ep_sync_add_and_fetch(&value, -(signed)decrement);
    }

    T swap(const T &newValue) {
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

template <typename T>
void atomic_setIfBigger(Atomic<T> &obj, const T &newValue) {
    T oldValue = obj.load();
    while (newValue > oldValue) {
        if (obj.compare_exchange_strong(oldValue, newValue)) {
            break;
        }
        oldValue = obj.load();
    }
}

template <typename T>
void atomic_setIfLess(Atomic<T> &obj, const T &newValue) {
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
class AtomicPtr : public CouchbaseAtomic<T*> {
public:
    AtomicPtr(T *initial = NULL) : CouchbaseAtomic<T*>(initial) {}

    ~AtomicPtr() {}

    T *operator ->() {
        return CouchbaseAtomic<T*>::load();
    }

    T &operator *() {
        return *CouchbaseAtomic<T*>::load();
    }

    operator bool() const {
        return CouchbaseAtomic<T*>::load() != NULL;
    }

    bool operator !() const {
        return CouchbaseAtomic<T*>::load() == NULL;
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

#ifdef _MSC_VER

#include <queue>
#include <thread>
#include <mutex>

/**
 * Create a simple version of the AtomicQueue for windows right now to
 * avoid the threadlocal usage which is currently using pthreads
 */
template <typename T>
class AtomicQueue {
public:
    void push(T &value) {
        std::lock_guard<std::mutex> lock(mutex);
        queue.push(value);
    }

    void getAll(std::queue<T> &outQueue) {
        std::lock_guard<std::mutex> lock(mutex);
        while (!queue.empty()) {
            outQueue.push(queue.front());
            queue.pop();
        }
    }

    bool empty() {
        std::lock_guard<std::mutex> lock(mutex);
        return queue.empty();
    }

private:
    std::queue<T> queue;
    std::mutex mutex;
};

#else

/**
 * Efficient approximate-FIFO queue optimize for concurrent writers.
 */
template <typename T>
class AtomicQueue {
public:
    AtomicQueue() : counter(0), numItems(0) {}

    ~AtomicQueue() {
        size_t i;
        for (i = 0; i < counter; ++i) {
            delete queues[i];
        }
    }

    /**
     * Place an item in the queue.
     */
    void push(T &value) {
        std::queue<T> *q = swapQueue(); // steal our queue
        q->push(value);
        ++numItems;
        q = swapQueue(q);
    }

    /**
     * Grab all items from this queue an place them into the provided
     * output queue.
     *
     * @param outQueue a destination queue to fill
     */
    void getAll(std::queue<T> &outQueue) {
        std::queue<T> *q(swapQueue()); // Grab my own queue
        std::queue<T> *newQueue(NULL);
        int count(0);

        // Will start empty unless this thread is adding stuff
        while (!q->empty()) {
            outQueue.push(q->front());
            q->pop();
            ++count;
        }

        size_t c(counter);
        for (size_t i = 0; i < c; ++i) {
            // Swap with another thread
            newQueue = queues[i].swapIfNot(NULL, q);
            // Empty the queue
            if (newQueue != NULL) {
                q = newQueue;
                while (!q->empty()) {
                    outQueue.push(q->front());
                    q->pop();
                    ++count;
                }
            }
        }

        q = swapQueue(q);
        numItems.fetch_sub(count);
    }

    /**
     * True if this queue is empty.
     */
    bool empty() const {
        return size() == 0;
    }

    /**
     * Return the number of queued items.
     */
    size_t size() const {
        return numItems;
    }
private:
    AtomicPtr<std::queue<T> > *initialize() {
        std::queue<T> *q = new std::queue<T>;
        size_t i(counter++);
        queues[i] = q;
        threadQueue = &queues[i];
        return &queues[i];
    }

    std::queue<T> *swapQueue(std::queue<T> *newQueue = NULL) {
        AtomicPtr<std::queue<T> > *qPtr(threadQueue);
        if (qPtr == NULL) {
            qPtr = initialize();
        }
        return qPtr->swap(newQueue);
    }

    ThreadLocalPtr<AtomicPtr<std::queue<T> > > threadQueue;
    AtomicPtr<std::queue<T> > queues[MAX_THREADS];
    Atomic<size_t> counter;
    Atomic<size_t> numItems;
    DISALLOW_COPY_AND_ASSIGN(AtomicQueue);
};
#endif


#endif  // SRC_ATOMIC_H_
