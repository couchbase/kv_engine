/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <platform/monotonic.h>

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