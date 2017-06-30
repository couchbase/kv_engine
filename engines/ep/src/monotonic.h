/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#pragma once

#include <atomic>
#include <limits>
#include <stdexcept>
#include <string>

/// Policy class for handling non-monotonic updates by simply ignoring them.
template <class T>
struct IgnorePolicy {
    void nonMonotonic(const T& curValue, const T& newValue) {
        // Ignore the update.
    }
};

/// Policy class for handling non-monotonic updates by throwing std::logic_error
template <class T>
struct ThrowExceptionPolicy {
    void nonMonotonic(const T& curValue, const T& newValue) {
        throw std::logic_error("Monotonic<> invariant failed: new value (" +
                               std::to_string(newValue) +
                               ") is not greater than current value (" +
                               std::to_string(curValue) + ")");
    }
};

/**
 * Monotonic is a class template for simple types T. It provides guarantee
 * of updating the class objects by only values that are greater than what is
 * contained at the time of update - i.e. the object must be
 * Strictly Increasing.
 *
 * Note: This is not atomic/thread-safe. If you need thread-safely, see
 * AtomicMonotonic.
 *
 * @tparam T value type used to represent the value.
 * @tparam OrderReversePolicy Policy class which controls the behaviour if
 *         an operation would break the monotonic invariant.
 */
template <typename T,
          template <class> class OrderReversedPolicy =
                  IgnorePolicy>
class Monotonic : public OrderReversedPolicy<T> {
public:
    Monotonic(const T val = std::numeric_limits<T>::min()) : val(val) {
    }

    Monotonic(const Monotonic<T>& other) : val(other.val) {
    }

    Monotonic& operator=(const Monotonic<T>& other) {
        if (val < other.val) {
            val = other.val;
        } else {
            OrderReversedPolicy<T>::nonMonotonic(val, other);
        }
        return *this;
    }

    Monotonic& operator=(const T& v) {
        if (val < v) {
            val = v;
        } else {
            OrderReversedPolicy<T>::nonMonotonic(val, v);
        }
        return *this;
    }

    operator T() const {
        return val;
    }

    /* Can be used to lower the value */
    void reset(T desired) {
        val = desired;
    }

private:
    T val;
};

/**
 * Variant of the Monotonic class, except that the type T is wrapped in
 * std::atomic, so all updates are atomic. T must be TriviallyCopyable.
 */
template <typename T, template <class> class OrderReversedPolicy = IgnorePolicy>
class AtomicMonotonic : public OrderReversedPolicy<T> {
public:
    AtomicMonotonic(T val = std::numeric_limits<T>::min()) : val(val) {
    }

    AtomicMonotonic(const AtomicMonotonic<T>& other) : val(other.val) {
    }

    AtomicMonotonic& operator=(T desired) {
        while (true) {
            T current = val.load();
            if (current < desired) {
                if (val.compare_exchange_weak(current, desired)) {
                    break;
                }
            } else {
                OrderReversedPolicy<T>::nonMonotonic(current, desired);
                break;
            }
        }
        return *this;
    }

    operator T() const {
        return val;
    }

    T load() const {
        return val;
    }

    void store(T desired) {
        operator=(desired);
    }

    /* Can be used to lower the value */
    void reset(T desired) {
        val = desired;
    }

private:
    std::atomic<T> val;
};
