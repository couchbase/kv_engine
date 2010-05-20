#ifndef ATOMIC_HH
#define ATOMIC_HH

#include <pthread.h>
#include <queue>
#include <sched.h>

#include "callbacks.hh"
#include "locks.hh"

#define MAX_THREADS 100

template<typename T>
class ThreadLocal {
public:
    ThreadLocal(void (*destructor)(void *) = NULL) {
        int rc = pthread_key_create(&key, destructor);
        if (rc != 0) {
            throw std::runtime_error("Failed to create a thread-specific key");
        }
    }

    ~ThreadLocal() {
        pthread_key_delete(key);
    }

    void set(const T &newValue) {
        pthread_setspecific(key, newValue);
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

template <typename T>
class ThreadLocalPtr : public ThreadLocal<T*> {
public:
    ThreadLocalPtr(void (*destructor)(void *) = NULL) : ThreadLocal<T*>(destructor) {}

    ~ThreadLocalPtr() {}

    T *operator ->() {
        return ThreadLocal<T*>::get();
    }

    T operator *() {
        return *ThreadLocal<T*>::get();
    }

    void operator =(T *newValue) {
        set(newValue);
    }
};

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
        __sync_synchronize();
    }

    operator T() const {
        return get();
    }

    void operator =(const T &newValue) {
        set(newValue);
    }

    bool cas(const T &oldValue, const T &newValue) {
        return __sync_bool_compare_and_swap(&value, oldValue, newValue);
    }

    T operator ++() { // prefix
        return __sync_add_and_fetch(&value, 1);
    }

    T operator ++(int ignored) { // postfix
        (void)ignored;
        return __sync_fetch_and_add(&value, 1);
    }

    T operator --() { // prefix
        return __sync_add_and_fetch(&value, -1);
    }

    T operator --(int ignored) { // postfix
        (void)ignored;
        return __sync_fetch_and_add(&value, -1);
    }

    T operator +=(T increment) {
        // Returns the new value
        return __sync_add_and_fetch(&value, increment);
    }

    T operator -=(T decrement) {
        return __sync_add_and_fetch(&value, -decrement);
    }

    T incr(const T &increment) {
        // Returns the old value
        return __sync_fetch_and_add(&value, increment);
    }

    T decr(const T &decrement) {
        return __sync_add_and_fetch(&value, -decrement);
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
private:
    volatile T value;
};


template <typename T>
class AtomicPtr : public Atomic<T*> {
public:
    AtomicPtr(T *initial = NULL) : Atomic<T*>(initial) {}

    ~AtomicPtr() {}

    T *operator ->() {
        return Atomic<T*>::get();
    }

    T operator *() {
        return *Atomic<T*>::get();
    }
};

class SpinLock {
public:
    SpinLock() : lock(0) {}

    bool tryAcquire() {
        return __sync_lock_test_and_set(&lock, 1) == 0;
    }

    void acquire() {
        while (!tryAcquire()) {
            sched_yield();
        }
    }

    void release() {
        __sync_lock_release(&lock);
    }

private:
    volatile int lock;
    DISALLOW_COPY_AND_ASSIGN(SpinLock);
};

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

class RCValue {
public:
    RCValue() : _rc_refcount(0) {}
    ~RCValue() {}
private:
    template <class TT> friend class RCPtr;
    int _rc_incref() {
        return ++_rc_refcount;
    }

    int _rc_decref() {
        return --_rc_refcount;
    }

    Atomic<int> _rc_refcount;
};

template <class C>
class RCPtr {
public:
    RCPtr(C *init = NULL) : value(init) {
        if (init != NULL) {
            static_cast<RCValue *>(value)->_rc_incref();
            assert(static_cast<RCValue *>(value)->_rc_refcount > 0);
        }
    }

    RCPtr(RCPtr<C> &other) : value(other.gimme()) {}

    ~RCPtr() {
        if (value != NULL && static_cast<RCValue *>(value)->_rc_decref() == 0) {
            delete value;
        }
    }

    void reset(C *newValue = NULL) {
        if (newValue != NULL) {
            static_cast<RCValue *>(newValue)->_rc_incref();
        }
        swap(newValue);
    }

    void reset(RCPtr<C> &other) {
        swap(other.gimme());
    }

    bool cas(C *oldValue, RCPtr<C> &newValue) {
        SpinLockHolder lh(&lock);
        if (value == oldValue) {
            value = newValue.gimme();
            if (oldValue != NULL &&
                static_cast<RCValue *>(oldValue)->_rc_decref() == 0) {
                lh.unlock();
                delete oldValue;
            }
            return true;
        }
        return false;
    }

    // safe for the lifetime of this instance
    C *get() {
        return value;
    }

    C *operator =(RCPtr<C> &other) {
        reset(other);
    }

    C &operator *() const {
        return *value;
    }

    C *operator ->() const {
        return value;
    }
private:
    C *gimme() {
        SpinLockHolder lh(&lock);
        if (value != NULL) {
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
    SpinLock lock; // exists solely for the purpose of implementing reset() safely
};

template <typename T>
class AtomicQueue {
public:
    AtomicQueue() : counter((size_t)0), numItems((size_t)0) {}

    ~AtomicQueue() {
        size_t i;
        for (i = 0; i < counter; ++i) {
            delete queues[i];
        }
    }

    void push(T value) {
        std::queue<T> *q = swapQueue(); // steal our queue
        q->push(value);
        ++numItems;
        q = swapQueue(q);
    }

    void pushQueue(std::queue<T> &inQueue) {
        std::queue<T> *q = swapQueue(); // steal our queue
        numItems.incr(inQueue.size());
        while (!inQueue.empty()) {
            q->push(inQueue.front());
            inQueue.pop();
        }
        q = swapQueue(q);
    }

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
        numItems -= count;
    }

    size_t getNumQueues() const {
        return counter;
    }

    bool empty() const {
        return size() == 0;
    }

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

#endif // ATOMIC_HH
