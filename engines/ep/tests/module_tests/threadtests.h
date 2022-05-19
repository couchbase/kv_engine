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
#include <platform/syncobject.h>

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
    explicit CountDownLatch(int n = 1) : count(n) {
    }

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
        thread = create_thread([this]() { launch_sync_test_thread<T>(this); },
                               "thread");
    }

    void run() {
        startingLine->decr();
        pistol->wait();
        result = (*gen)();
    }

    void join() {
        thread.join();
    }

    const T getResult() const { return result; };

private:
    CountDownLatch *startingLine;
    CountDownLatch *pistol;
    Generator<T>   *gen;

    T         result;
    std::thread thread;
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
