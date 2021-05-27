/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "utility.h"
#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>

template <typename T>
void atomic_setIfBigger(std::atomic<T> &obj, const T &newValue) {
    T oldValue = obj.load();
    while (newValue > oldValue) {
        if (obj.compare_exchange_strong(oldValue, newValue)) {
            break;
        }
        oldValue = obj.load();
    }
}

template <typename T>
void atomic_setIfLess(std::atomic<T> &obj, const T &newValue) {
    T oldValue = obj.load();
    while (newValue < oldValue) {
        if (obj.compare_exchange_strong(oldValue, newValue)) {
            break;
        }
        oldValue = obj.load();
    }
}

template <typename T>
T atomic_swapIfNot(std::atomic<T> &obj, const T &badValue, const T &newValue) {
    T oldValue;
    while (true) {
        oldValue = obj.load();
        if (oldValue != badValue) {
            if (obj.compare_exchange_strong(oldValue, newValue)) {
                break;
            }
        } else {
            break;
        }
    }
    return oldValue;
}

/**
 * Atomic pointer.
 *
 * This does *not* make the item that's pointed to atomic.
 */
template <typename T>
class AtomicPtr : public std::atomic<T*> {
public:
    explicit AtomicPtr(T* initial = nullptr) : std::atomic<T*>(initial) {
    }

    ~AtomicPtr() = default;

    T *operator ->() const noexcept {
        return std::atomic<T*>::load();
    }

    T &operator *() const noexcept {
        return *std::atomic<T*>::load();
    }

    explicit operator bool() const {
        return std::atomic<T*>::load() != NULL;
    }

    bool operator !() const {
        return std::atomic<T*>::load() == NULL;
    }
};

template <class S, class Pointer, class Deleter>
class SingleThreadedRCPtr;

/**
 * A reference count, for use by SingleThreadedRCPtr.
 *
 * Exposes increment / decrement methods with the appropariate (and
 * optimal) memory ordering for correct reference-counting (see
 * https://en.cppreference.com/w/cpp/atomic/memory_order):
 *
 * Increments of count can use relaxed memory ordering:
 *
 *     "Typical use for relaxed memory ordering is incrementing
 *     counters, such as the reference counters of std::shared_ptr,
 *     since this only requires atomicity, but not ordering or
 *     synchronization"
 *
 * Decrements of count must use acquire-release, as a decrment could
 * reduce ref-count to zero when object must be destroyed exactly
 * once:
 *
 *     "... note that decrementing the shared_ptr counters requires
 *     acquire-release synchronization with the destructor".
 *
 * Reads of the count must use acquire ordering to ensure any (relaxed
 * increments) from other threads are observed.
 */
class RefCount {
public:
    RefCount(int value = 0) {
        count.store(value, std::memory_order_relaxed);
    }

    int operator++() noexcept {
        return count.fetch_add(1, std::memory_order_relaxed) + 1;
    }

    int operator--() noexcept {
        return count.fetch_sub(1, std::memory_order_acq_rel) - 1;
    }

    int load() const {
        return count.load(std::memory_order_acquire);
    }

private:
    mutable std::atomic<int> count;
};

/**
 * A reference counted value (used by SingleThreadedRCPtr).
 */
class RCValue {
public:
    RCValue() : _rc_refcount(0) {}
    RCValue(const RCValue &) : _rc_refcount(0) {}
    ~RCValue() = default;

private:
    template <class MySS, class Pointer, class Deleter>
    friend class SingleThreadedRCPtr;

    mutable RefCount _rc_refcount;
};

/**
 * Single-threaded reference counted pointer.
 * "Single-threaded" means that the reference counted pointer should be accessed
 * by only one thread at any time or accesses to the reference counted pointer
 * by multiple threads should be synchronized by the external lock.
 *
 * Takes the following template parameters:
 * @tparam class T the class that the SingleThreadedRCPtr is pointing to.
 * @tparam class Pointer the pointer type that the SingleThreadedRCPtr
 * maintains a reference counter for.  It is defaulted to a T*, however can also
 * be a TaggedPtr<T>.
 * @tparam class Deleter the deleter function for deleting the object pointed to
 * by SingleThreadedRCPtr.  It defaults to the std::default_delete function
 * templated on class T.  However when the pointer type is a TaggedPtr<T> a
 * specialised delete function must be provided.
 */
template <class T, class Pointer = T*, class Deleter = std::default_delete<T>>
class SingleThreadedRCPtr {
public:
    SingleThreadedRCPtr() : value(Pointer()){};

    explicit SingleThreadedRCPtr(Pointer init) : value(init) {
        if (init != nullptr) {
            ++value->_rc_refcount;
        }
    }

    // Copy construction - increases ref-count on object by 1.
    SingleThreadedRCPtr(const SingleThreadedRCPtr& other)
        : value(other.gimme()) {
    }

    // Move construction - reference count is unchanged.
    SingleThreadedRCPtr(SingleThreadedRCPtr&& other) noexcept
        : value(other.value) {
        other.value = nullptr;
    }

    template <typename Y, typename P>
    explicit SingleThreadedRCPtr(
            const SingleThreadedRCPtr<Y, P, Deleter>& other)
        : value(other.gimme()) {
    }

    explicit SingleThreadedRCPtr(std::unique_ptr<T>&& other)
        : SingleThreadedRCPtr(other.release()) {
    }

    ~SingleThreadedRCPtr() {
        if (value != nullptr && --value->_rc_refcount == 0) {
// Hide the deleter function from clang analyzer as sometimes it dose not fully
// understand how our ref counting works in RCValue
#ifndef __clang_analyzer__
            Deleter()(value);
#endif
        }
    }

    void reset() {
        swap(Pointer());
    }

    void reset(Pointer newValue) {
        if (newValue != nullptr) {
            ++newValue->_rc_refcount;
        }
        swap(newValue);
    }

    void reset(const SingleThreadedRCPtr& other) {
        swap(other.gimme());
    }

    // Swap - reference count is unchanged on each pointed-to object.
    void swap(SingleThreadedRCPtr& other) {
        std::swap(this->value, other.value);
    }

    int refCount() const {
        return value->_rc_refcount.load();
    }

    // safe for the lifetime of this instance
    Pointer get() const {
        return value;
    }

    /**
     * Returns a reference to the owned pointer.
     *
     * WARNING WARNING WARNING
     *
     * This function is inheritly unsafe; as it exposes the internal
     * managed pointer. Incorrect use of this could lead to memory
     * leaks, crashes etc.  Unless you really know what you're doing
     * don't use this!
     */
    Pointer& unsafeGetPointer() {
        return value;
    }

    SingleThreadedRCPtr& operator=(const SingleThreadedRCPtr& other) {
        reset(other);
        return *this;
    }

    // Move-assignment - reference count is unchanged of incoming item.
    SingleThreadedRCPtr& operator=(SingleThreadedRCPtr&& other) noexcept {
        swap(other.value);
        other.value = nullptr;
        return *this;
    }

    T &operator *() const {
        return *value;
    }

    Pointer operator ->() const {
        return value;
    }

    bool operator! () const {
        return value == nullptr;
    }

    explicit operator bool() const {
        return value != nullptr;
    }

    bool operator==(const SingleThreadedRCPtr& other) const {
        return value == other.value;
    }

private:
    template <typename Y, typename P, typename D>
    friend class SingleThreadedRCPtr;

    Pointer gimme() const {
        if (value != nullptr) {
            ++value->_rc_refcount;
        }
        return value;
    }

    void swap(Pointer newValue) {
        Pointer old = value;
        value = newValue;
        if (old != nullptr && --old->_rc_refcount == 0) {
// Hide the deleter function from clang analyzer as sometimes it dose not fully
// understand how our ref counting works in RCValue
#ifndef __clang_analyzer__
            Deleter()(old);
#endif
        }
    }

    Pointer value;
};

template <class T,
          class Pointer = T*,
          class Deleter = std::default_delete<T>,
          class... Args>
SingleThreadedRCPtr<T, Pointer, Deleter> make_STRCPtr(Args&&... args) {
    return SingleThreadedRCPtr<T, Pointer, Deleter>(
            new T(std::forward<Args>(args)...));
}

// Makes SingleThreadedRCPtr support Swappable
template <typename T, typename Pointer, typename Deleter>
void swap(SingleThreadedRCPtr<T, Pointer, Deleter>& a,
          SingleThreadedRCPtr<T, Pointer, Deleter>& b) {
    a.swap(b);
}

/**
 * Debugging wrapper around std::atomic which print all accesses to the atomic
 * value to stderr.
 */
template <typename T>
class LoggedAtomic {
public:
    explicit LoggedAtomic(T initial) : value(initial) {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        std::cerr << "LoggedAtomic[" << this << "]::LoggedAtomic: "
                  << value.load() << std::endl;
    }

    T operator=(T desired) {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        value.store(desired);
        std::cerr << "LoggedAtomic[" << this << "]::operator=: "
                  << value.load() << std::endl;
        return value.load();
    }

    T load() const {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        auto result = value.load();
        std::cerr << "LoggedAtomic[" << this << "]::load: " << result
                  << std::endl;
        return result;
    }

    void store(T desired) {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        value.store(desired);
        std::cerr << "LoggedAtomic[" << this << "]::store: " << value.load()
                  << std::endl;
    }

    explicit operator T() const {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        auto result = value.load();
        std::cerr << "LoggedAtomic[" << this << "]::operator T: " << result
                  << std::endl;
        return result;
    }

    T exchange(T desired, std::memory_order order = std::memory_order_seq_cst) {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        std::cerr << "LoggedAtomic[" << this << "]::exchange("
                  << "desired:" << desired << ") = ";
        auto result = value.exchange(desired, order);
        std::cerr << result << std::endl;
        return result;
    }

    bool compare_exchange_strong(T& expected, T desired,
                                 std::memory_order order =
                                      std::memory_order_seq_cst ) {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        std::cerr << "LoggedAtomic[" << this << "]::compare_exchange_strong("
                  << "expected:" << expected << ", desired:) = " << desired;
        auto result = value.compare_exchange_strong(expected, desired, order);
        std::cerr << result << std::endl;
        return result;
    }

    T fetch_add(T arg,
                std::memory_order order = std::memory_order_seq_cst ) {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        T result = value.fetch_add(arg, order);
        std::cerr << "LoggedAtomic[" << this << "]::fetch_add(" << arg
                  << "): " << result << std::endl;
        return value.load();
    }

    T fetch_sub(T arg,
                std::memory_order order = std::memory_order_seq_cst ) {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        T result = value.fetch_sub(arg, order);
        std::cerr << "LoggedAtomic[" << this << "]::fetch_sub(" << arg
                  << "): " << result << std::endl;
        return value.load();
    }

    T operator++() {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        ++value;
        std::cerr << "LoggedAtomic[" << this << "]::pre-increment: "
                  << value << std::endl;
        return value;
    }

    T operator--() {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        --value;
        std::cerr << "LoggedAtomic[" << this << "]::pre-decrement: " << value
                  << std::endl;
        return value;
    }

protected:
    mutable std::mutex stderr_mutex;
    std::atomic<T> value;
};
