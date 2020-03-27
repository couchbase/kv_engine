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
#pragma once

#include "locks.h"
#include "syncobject.h"
#include "utility.h"

#include <platform/cbassert.h>
#include <platform/platform_thread.h>

#include <algorithm>
#include <iostream>
#include <vector>


template <typename T>
class Generator {
public:
    virtual ~Generator() {}
    virtual T operator()() = 0;
};

class CountDownLatch {
public:

    CountDownLatch(int n=1) : count(n) {}

    void decr() {
        std::unique_lock<std::mutex> lh(so);
        --count;
        so.notify_all();
    }

    void wait() {
        std::unique_lock<std::mutex> lh(so);
        so.wait(lh, [this] { return count == 0; });
    }

private:
    int count;
    SyncObject so;

    DISALLOW_COPY_AND_ASSIGN(CountDownLatch);
};

template <typename T> class SyncTestThread;

template <typename T>
static void launch_sync_test_thread(void *arg) {
    auto *stt = static_cast<SyncTestThread<T>*>(arg);
    stt->run();
}

extern "C" {
   typedef void (*CB_THREAD_MAIN)(void *);
}

template <typename T>
class SyncTestThread {
public:

    SyncTestThread(CountDownLatch *s, CountDownLatch *p, Generator<T> *testGen) :
        startingLine(s), pistol(p), gen(testGen) {}

    SyncTestThread(const SyncTestThread &other) :
        startingLine(other.startingLine),
        pistol(other.pistol),
        gen(other.gen) {}

    void start() {
        if (cb_create_thread(&thread, (CB_THREAD_MAIN)( launch_sync_test_thread<T> ), this, 0) != 0) {
            throw std::runtime_error("Error initializing thread");
        }
    }

    void run() {
        startingLine->decr();
        pistol->wait();
        result = (*gen)();
    }

    void join() {
        if (cb_join_thread(thread) != 0) {
            throw std::runtime_error("Failed to join.");
        }
    }

    const T getResult() const { return result; };

private:
    CountDownLatch *startingLine;
    CountDownLatch *pistol;
    Generator<T>   *gen;

    T         result;
    cb_thread_t thread;
};

template <typename T>
static void starter(SyncTestThread<T> &t) { t.start(); }

template <typename T>
static void waiter(SyncTestThread<T> &t) { t.join(); }

template <typename T>
std::vector<T> getCompletedThreads(size_t n, Generator<T> *gen) {
    CountDownLatch startingLine(n), pistol(1);

    SyncTestThread<T> proto(&startingLine, &pistol, gen);
    std::vector<SyncTestThread<T> > threads(n, proto);
    cb_assert(threads.size() == n);
    std::for_each(threads.begin(), threads.end(), starter<T>);

    startingLine.wait();
    pistol.decr();

    std::for_each(threads.begin(), threads.end(), waiter<T>);

    std::vector<T> results;
    typename std::vector<SyncTestThread<T> >::iterator it;
    for (it = threads.begin(); it != threads.end(); ++it) {
        results.push_back(it->getResult());
    }

    return results;
}
