#include <cassert>
#include <vector>
#include <algorithm>

#include "atomic.hh"
#include "threadtests.hh"

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

int main() {
    testAtomicInt();
}
