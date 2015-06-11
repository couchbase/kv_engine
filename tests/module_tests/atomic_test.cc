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

#include "config.h"

#ifdef _MSC_VER
#define alarm(a)
#endif

#include <algorithm>
#include <vector>

#include "atomic.h"
#include "threadtests.h"

const size_t numThreads    = 100;
const size_t numIterations = 1000;

class AtomicIntTest : public Generator<int> {
public:

    AtomicIntTest(int start=0) : i(start) {}

    int operator()() {
        for (size_t j = 0; j < numIterations-1; j++) {
           ++i;
        }
        return ++i;
    }

    int latest(void) { return i.load(); }

private:
    AtomicValue<int> i;
};

static void testAtomicInt() {
    AtomicIntTest intgen;
    // Run this test with 100 concurrent threads.
    std::vector<int> r(getCompletedThreads<int>(numThreads, &intgen));

    // We should have 100 distinct numbers.
    std::sort(r.begin(), r.end());
    std::unique(r.begin(), r.end());
    cb_assert(r.size() == numThreads);

    // And the last number should be (numThreads * numIterations)
    cb_assert(intgen.latest() == (numThreads * numIterations));
}

static void testSetIfLess() {
    AtomicValue<int> x;

    x.store(842);
    atomic_setIfLess(x, 924);
    cb_assert(x.load() == 842);
    atomic_setIfLess(x, 813);
    cb_assert(x.load() == 813);
}

static void testSetIfBigger() {
    AtomicValue<int> x;

    x.store(842);
    atomic_setIfBigger(x, 13);
    cb_assert(x.load() == 842);
    atomic_setIfBigger(x, 924);
    cb_assert(x.load() == 924);
}

static void testAtomicDouble(void) {
    AtomicValue<double> d = 991.5;
    cb_assert(d == 991.5);
}

int testAtomicCompareExchangeStrong(void) {
    AtomicValue<bool> x(true);
    bool expected = false;

    int returncode = 0;

    if (x.compare_exchange_strong(expected, false)) {
        std::cerr << "ERROR. this is supposed to be true "
                  << "and we're comparing with false" << std::endl;
        ++returncode;
    }

    if (!x) {
        std::cerr << "Expected value to be updated" << std::endl;
        ++returncode;
    }

    if (!expected) {
        std::cerr << "Expected should be set from compare_exchange_strong"
                  << std::endl;
        ++returncode;
    }

    if (!x.compare_exchange_strong(expected, false)) {
        std::cerr << "ERROR. this is supposed to be true "
                  << "and we're comparing with true" << std::endl;
        ++returncode;
    }

    if (x) {
        std::cerr << "Expected value to be updated" << std::endl;
        ++returncode;
    }

    return returncode;
}

int main() {
    alarm(60);
    testAtomicInt();
    testSetIfLess();
    testSetIfBigger();
    testAtomicDouble();
    return testAtomicCompareExchangeStrong();
}
