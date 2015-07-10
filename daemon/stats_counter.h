/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

/**
 * The StatsCounter is used to track stat values by using an
 * std::atomic<> but use relaxed memory ordering.
 */
template <typename T>
class StatsCounter {
public:
    StatsCounter() {
        value.store(0, std::memory_order_relaxed);
    }
    StatsCounter(const StatsCounter &other) {
        value.store(other.value.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
    }

    operator T() const {
        return value.load(std::memory_order_relaxed);
    }

    T load() const {
        return value.load(std::memory_order_relaxed);
    }

    StatsCounter & operator = (const StatsCounter &rhs) {
        value.store(rhs.load(), std::memory_order_relaxed);
        return *this;
    }

    StatsCounter & operator += (const T rhs) {
        value.fetch_add(rhs, std::memory_order_relaxed);
        return *this;
    }

    StatsCounter & operator += (const StatsCounter &rhs) {
        value.fetch_add(rhs.value.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
        return *this;
    }

    void operator++() {
        value.fetch_add(1, std::memory_order_relaxed);
    }

    void operator++(int) {
        value.fetch_add(1, std::memory_order_relaxed);
    }

    StatsCounter & operator = (T val) {
        value.store(val, std::memory_order_relaxed);
        return *this;
    }

    void reset() {
        value.store(0, std::memory_order_relaxed);
    }

    void setIfGreater(const T &val) {
        do {
            T currval = value.load(std::memory_order_relaxed);
            if (val > currval) {
                if (value.compare_exchange_weak(currval, val, std::memory_order_relaxed)) {
                    break;
                }
            } else {
                break;
            }
        } while (true);
    }

    void setIfGreater(const StatsCounter &val) {
        setIfGreater(val.load());
    }

private:
    std::atomic<T> value;
};
