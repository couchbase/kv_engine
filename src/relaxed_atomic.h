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

namespace Couchbase {

    /**
     * The RelaxedAtomic class wraps std::atomic<> and operates with
     * relaxed memory ordering.
     */
    template<typename T>
    class RelaxedAtomic {
    public:
        RelaxedAtomic() {
            value.store(0, std::memory_order_relaxed);
        }

        RelaxedAtomic(const T& initial) {
            value.store(initial, std::memory_order_relaxed);
        }

        explicit RelaxedAtomic(const RelaxedAtomic& other) {
            value.store(other.value.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
        }

        operator T() const {
            return value.load(std::memory_order_relaxed);
        }

        T load() const {
            return value.load(std::memory_order_relaxed);
        }

        RelaxedAtomic& operator=(const RelaxedAtomic& rhs) {
            value.store(rhs.load(), std::memory_order_relaxed);
            return *this;
        }

        RelaxedAtomic& operator+=(const T rhs) {
            value.fetch_add(rhs, std::memory_order_relaxed);
            return *this;
        }

        RelaxedAtomic& operator+=(const RelaxedAtomic& rhs) {
            value.fetch_add(rhs.value.load(std::memory_order_relaxed),
                            std::memory_order_relaxed);
            return *this;
        }

        RelaxedAtomic& operator-=(const T rhs) {
            value.fetch_sub(rhs, std::memory_order_relaxed);
            return *this;
        }

        RelaxedAtomic& operator-=(const RelaxedAtomic& rhs) {
            value.fetch_sub(rhs.value.load(std::memory_order_relaxed),
                            std::memory_order_relaxed);
            return *this;
        }

        T operator++() {
            return value.fetch_add(1, std::memory_order_relaxed) + 1;
        }

        T operator++(int) {
            return value.fetch_add(1, std::memory_order_relaxed);
        }

        T operator--() {
            return value.fetch_sub(1, std::memory_order_relaxed) - 1;
        }

        T operator--(int) {
            return value.fetch_sub(1, std::memory_order_relaxed);
        }

        RelaxedAtomic& operator=(T val) {
            value.store(val, std::memory_order_relaxed);
            return *this;
        }

        void reset() {
            value.store(0, std::memory_order_relaxed);
        }

        void setIfGreater(const T& val) {
            do {
                T currval = value.load(std::memory_order_relaxed);
                if (val > currval) {
                    if (value.compare_exchange_weak(currval, val,
                                                    std::memory_order_relaxed)) {
                        break;
                    }
                } else {
                    break;
                }
            } while (true);
        }

        void setIfGreater(const RelaxedAtomic& val) {
            setIfGreater(val.load());
        }

    private:
        std::atomic <T> value;
    };
}
