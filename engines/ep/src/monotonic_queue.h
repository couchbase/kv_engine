/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "monotonic.h"

#include <queue>

/**
 * MonotonicQueue enforces a parameterisable invariant on values added to a
 * queue e.g., values added to the queue must be greater than or equal to the
 * preceding value
 *
 * @tparam T value type used in queue.
 * @tparam OrderReversePolicy Policy class which controls the behaviour if
 *         an operation would break the monotonic invariant.
 * @tparam Invariant The invariant to maintain across pushes.
 */
template <class T,
          template <class> class OrderReversedPolicy =
                  DefaultOrderReversedPolicy,
          template <class> class Invariant = cb::greater>
class MonotonicQueue {
public:
    const T& front() const {
        return queue.front();
    }
    T& front() {
        return queue.front();
    }
    const T& back() const {
        return queue.back();
    }
    T& back() {
        return queue.back();
    }
    bool empty() const {
        return queue.empty();
    }
    size_t size() const {
        return queue.size();
    }

    void push(const T& value) {
        latestValue = value;
        queue.push(value);
    }
    void push(T&& value) {
        latestValue = value;
        queue.push(std::move(value));
    }
    void pop() {
        queue.pop();
    }

private:
    std::queue<T> queue;
    Monotonic<T, OrderReversedPolicy, Invariant> latestValue;
};