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
#include "atomic.h"
#include "locks.h"
#include "threadtests.h"

#include <platform/cbassert.h>

#define NUM_THREADS 50
#define NUM_TIMES 10000

class Doodad : public RCValue {
public:
    Doodad() {
        numInstances++;
    }

    Doodad(const Doodad& src) : RCValue(src) {
        numInstances++;
    }

    ~Doodad() {
        numInstances--;
    }

    static int getNumInstances() {
        return numInstances;
    }

private:
    static std::atomic<int> numInstances;
};

std::atomic<int> Doodad::numInstances(0);

class AtomicPtrTest : public Generator<bool> {
public:
    explicit AtomicPtrTest(RCPtr<Doodad>* p) : ptr(p) {
    }

    bool operator()() {
        for (int i = 0; i < NUM_TIMES; ++i) {
            switch (rand() % 7) {
            case 0:
                ptr->reset(new Doodad);
                break;
            case 1:
                {
                    RCPtr<Doodad> d(new Doodad);
                    ptr->reset(d);
                }
                break;
            case 2:
                {
                    RCPtr<Doodad> d(new Doodad);
                    d.reset();
                }
                break;
            case 3:
                {
                    RCPtr<Doodad> d(*ptr);
                    d.reset();
                }
                break;
            case 4:
                // CAS is removed due to it wasn't used elsewhere in our
                // codebase
                break;
            case 5:
                ptr->reset(new Doodad);
                break;
            case 6:
                {
                    RCPtr<Doodad> d(*ptr);
                    d.reset(new Doodad);
                }
                break;
            default:
                cb_assert(false);
            }
        }

        return true;
    }

private:
    RCPtr<Doodad>* ptr;
};

static void testAtomicPtr() {
    // Just do a bunch.
    RCPtr<Doodad> dd;
    auto* testGen = new AtomicPtrTest(&dd);

    getCompletedThreads<bool>(NUM_THREADS, testGen);

    delete testGen;
    dd.reset();
    cb_assert(Doodad::getNumInstances() == 0);
}

static void testOperators() {
    RCPtr<Doodad> dd;
    cb_assert(!dd);
    dd.reset(new Doodad);
    cb_assert(dd);
    dd.reset();
    cb_assert(!dd);

    auto* d = new Doodad;
    dd.reset(d);
    cb_assert((void*)(d) == (void*)(&(*dd)));
    dd.reset();

    cb_assert(Doodad::getNumInstances() == 0);
}

struct TrackingInt {
    TrackingInt(int v) : value(v){};
    ~TrackingInt() = default;
    int operator++() {
        auto newRC = ++value;
        history.push_back(newRC);
        return newRC;
    }
    int operator--() {
        auto newRC = --value;
        history.push_back(newRC);
        return newRC;
    }
    bool operator==(const TrackingInt& rhs) {
        return this->value == rhs.value;
    }
    bool operator==(const int& rhs) {
        return this->value == rhs;
    }
    int value = 0;
    // history of what values the recount has been.
    std::vector<int> history;
};

/// Class which records whenever it's refcount changes.
struct TrackingRCValue {
    /// Current reference count.
    TrackingInt _rc_refcount = 0;
};

// Test that move semantics work correctly and refcounts are not unnecessarily
// modified.
static void testMove1() {
    RCPtr<TrackingRCValue> ptr(new TrackingRCValue());
    // Check result - history just contains initial increment; rc is 1.
    cb_assert(ptr->_rc_refcount.history.size() == 1);
    cb_assert(ptr->_rc_refcount == 1);
}

// Transfer ownership to new pointer via move (rvalue-reference).
static void testMove2() {
    RCPtr<TrackingRCValue> ptr1(new TrackingRCValue());
    RCPtr<TrackingRCValue> ptr2(std::move(ptr1));

    // No changes in refcount should have occurred.
    cb_assert(ptr2->_rc_refcount.history.size() == 1);
    cb_assert(ptr2->_rc_refcount == 1);

    // clang will warn about using ptr1 after its been moved as ptr1 could be
    // left in an undefined state. However, as this is a test and we've defined
    // the move operator suppress this warning.
#ifndef __clang_analyzer__
    // Moved-from pointer should be empty.
    cb_assert(ptr1.get() == nullptr);
#endif
}

int main() {
    testOperators();
    testAtomicPtr();
    testMove1();
    testMove2();
}
