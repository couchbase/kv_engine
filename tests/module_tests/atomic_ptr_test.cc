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

#include "atomic.h"
#include "locks.h"
#include "threadtests.h"

#ifdef _MSC_VER
#define alarm(a)
#endif

#define NUM_THREADS 50
#define NUM_TIMES 10000

// Clang analyzer doesn't really understand
// our custom smart-pointers so we'll skip compiling
// this test under the clang analyzer
#ifndef __clang_analyzer__

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
    static AtomicValue<int> numInstances;
};

AtomicValue<int> Doodad::numInstances(0);

class AtomicPtrTest : public Generator<bool> {
public:

    AtomicPtrTest(RCPtr<Doodad> *p) : ptr(p) {}

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
    RCPtr<Doodad> *ptr;
};

static void testAtomicPtr() {
    // Just do a bunch.
    RCPtr<Doodad> dd;
    AtomicPtrTest *testGen = new AtomicPtrTest(&dd);

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

    Doodad *d = new Doodad;
    dd.reset(d);
    cb_assert((void*)(d) == (void*)(&(*dd)));
    dd.reset();

    cb_assert(Doodad::getNumInstances() == 0);
}

int main() {
    alarm(60);
    testOperators();
    testAtomicPtr();
}
#else
int main() {}
#endif
