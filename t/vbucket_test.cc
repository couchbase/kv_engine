/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <pthread.h>
#include <signal.h>

#include <cassert>
#include <vector>
#include <algorithm>

#include "vbucket.hh"
#include "threadtests.hh"

static const size_t numThreads = 10;
static const size_t vbucketsEach = 100;

class VBucketGenerator {
public:
    VBucketGenerator(int start = 1) : i(start) {}
    VBucket *operator()() {
        return new VBucket(i++, active);
    }
private:
    int i;
};

static void assertVBucket(const VBucketMap& vbm, int id) {
    RCPtr<VBucket> v = vbm.getBucket(id);
    assert(v);
    assert(v->getId() == id);
}

static void testVBucketLookup() {
    VBucketGenerator vbgen;
    std::vector<VBucket*> bucketList(3);
    std::generate_n(bucketList.begin(), bucketList.capacity(), vbgen);

    VBucketMap vbm;
    vbm.addBuckets(bucketList);

    assert(!vbm.getBucket(4));
    assertVBucket(vbm, 1);
    assertVBucket(vbm, 2);
    assertVBucket(vbm, 3);
}

class AtomicUpdater : public Generator<bool> {
public:
    AtomicUpdater(VBucketMap *m) : vbm(m), i(0) {}
    bool operator()() {
        for (size_t j = 0; j < vbucketsEach; j++) {
            int newId = ++i;

            RCPtr<VBucket> v(new VBucket(newId, active));
            vbm->addBucket(v);
            assert(vbm->getBucket(newId) == v);

            if (newId % 2 == 0) {
                vbm->removeBucket(newId);
            }
        }

        return true;
    }
private:
    VBucketMap *vbm;
    Atomic<int> i;
};

static void testConcurrentUpdate(void) {
    VBucketMap vbm;
    AtomicUpdater au(&vbm);
    getCompletedThreads<bool>(numThreads, &au);

    // We remove half of the buckets in the test
    assert(vbm.getBuckets().size() == ((numThreads * vbucketsEach) / 2));
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    alarm(10);

    testVBucketLookup();
    testConcurrentUpdate();
}
