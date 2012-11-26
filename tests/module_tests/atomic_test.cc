#include "config.h"

#include <algorithm>
#include <cassert>
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

    int latest(void) { return i.get(); }

private:
    Atomic<int> i;
};

static void testAtomicInt() {
    AtomicIntTest intgen;
    // Run this test with 100 concurrent threads.
    std::vector<int> r(getCompletedThreads<int>(numThreads, &intgen));

    // We should have 100 distinct numbers.
    std::sort(r.begin(), r.end());
    std::unique(r.begin(), r.end());
    assert(r.size() == numThreads);

    // And the last number should be (numThreads * numIterations)
    assert(intgen.latest() == (numThreads * numIterations));
}

static void testSetIfLess() {
    Atomic<int> x;

    x.set(842);
    x.setIfLess(924);
    assert(x.get() == 842);
    x.setIfLess(813);
    assert(x.get() == 813);
}

static void testSetIfBigger() {
    Atomic<int> x;

    x.set(842);
    x.setIfBigger(13);
    assert(x.get() == 842);
    x.setIfBigger(924);
    assert(x.get() == 924);
}

int main() {
    alarm(60);
    testAtomicInt();
    testSetIfLess();
    testSetIfBigger();
}
