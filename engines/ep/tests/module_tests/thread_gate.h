/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <condition_variable>

/** Object which is used to synchronise the execution of a number of threads.
 *  Each thread calls threadUp(), and until all threads have called this
 *  they are all blocked.
 */
class ThreadGate {
public:
    ThreadGate() : n_threads(0) {
    }

    /** Create a ThreadGate.
     *  @param n_threads Total number of threads to wait for.
     */
    explicit ThreadGate(size_t n_threads_) : n_threads(n_threads_) {
    }

    /*
     * atomically increment a threadCount
     * if the calling thread is the last one up, notify_all
     * if the calling thread is not the last one up, wait (in the function)
     */
    void threadUp() {
        std::unique_lock<std::mutex> lh(m);
        if (++thread_count < n_threads) {
            cv.wait(lh, [this, &lh]() { return isComplete(lh); });
        } else {
            cv.notify_all(); // all threads accounted for, begin
        }
    }

    template <typename Rep, typename Period>
    void waitFor(std::chrono::duration<Rep, Period> timeout) {
        std::unique_lock<std::mutex> lh(m);
        cv.wait_for(lh, timeout, [this, &lh]() { return isComplete(lh); });
    }

    size_t getCount() {
        std::unique_lock<std::mutex> lh(m);
        return thread_count;
    }

    bool isComplete() {
        std::unique_lock<std::mutex> lh(m);
        return isComplete(lh);
    }

private:
    bool isComplete(const std::unique_lock<std::mutex>&) {
        return thread_count >= n_threads;
    }

    const size_t n_threads;
    size_t thread_count{0};
    std::mutex m;
    std::condition_variable cv;
};
