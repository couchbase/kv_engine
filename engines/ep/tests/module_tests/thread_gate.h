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
    ThreadGate(size_t n_threads_) : n_threads(n_threads_) {
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
        return thread_count == n_threads;
    }

    const size_t n_threads;
    size_t thread_count{0};
    std::mutex m;
    std::condition_variable cv;
};
