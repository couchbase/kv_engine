#include <assert.h>
#include <pthread.h>

#include "atomic.hh"
#include "locks.hh"
#include "threadtests.hh"

#define NUM_THREADS 50
#define NUM_TIMES 1000000

class Doodad : public RCValue {
public:
    Doodad() {
        numInstances++;
    }

    ~Doodad() {
        numInstances--;
    }

    static int getNumInstances() {
        return numInstances;
    }

private:
    static Atomic<int> numInstances;
};

Atomic<int> Doodad::numInstances(0);

class AtomicPtrTest : public Generator<bool> {
public:

    AtomicPtrTest(RCPtr<Doodad> p) : ptr(p) {}

    bool operator()() {
        for (int i = 0; i < NUM_TIMES; ++i) {
            switch (rand() % 4) {
            case 0:
                ptr.reset(new Doodad);
                break;
            case 1:
                {
                    RCPtr<Doodad> d(new Doodad);
                    ptr.reset(d);
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
                    RCPtr<Doodad> d(ptr);
                    d.reset();
                }
                break;
            default:
                assert(false);
            }
        }

        return true;
    }

private:
    RCPtr<Doodad> ptr;
};

static void testAtomicPtr() {
    // Just do a bunch.
    RCPtr<Doodad> dd;
    AtomicPtrTest *testGen = new AtomicPtrTest(dd);

    getCompletedThreads<bool>(NUM_THREADS, testGen);

    delete testGen;
    dd.reset();
    assert(Doodad::getNumInstances() == 0);
}

int main() {
    testAtomicPtr();
}
