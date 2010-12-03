/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <cassert>
#include <vector>
#include <algorithm>

#include "vbucket.hh"
#include "stats.hh"
#include "threadtests.hh"

static const size_t numThreads = 10;
static const size_t vbucketsEach = 100;

EPStats global_stats;

class VBucketGenerator {
public:
    VBucketGenerator(EPStats &s, int start = 1) : st(s), i(start) {}
    VBucket *operator()() {
        return new VBucket(i++, vbucket_state_active, st);
    }
private:
    EPStats &st;
    int i;
};

extern "C" {

static const char* test_get_logger_name(void) {
    return "dispatcher_test";
}

static void test_get_logger_log(EXTENSION_LOG_LEVEL severity,
                                const void* client_cookie,
                                const char *fmt, ...) {
    (void)severity;
    (void)client_cookie;
    (void)fmt;
    // ignore
}

}

EXTENSION_LOGGER_DESCRIPTOR* getLogger() {
    static EXTENSION_LOGGER_DESCRIPTOR logger;
    logger.get_name = test_get_logger_name;
    logger.log = test_get_logger_log;
    return &logger;
}

static void assertVBucket(const VBucketMap& vbm, int id) {
    RCPtr<VBucket> v = vbm.getBucket(id);
    assert(v);
    assert(v->getId() == id);
}

static void testVBucketLookup() {
    VBucketGenerator vbgen(global_stats);
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

            RCPtr<VBucket> v(new VBucket(newId, vbucket_state_active, global_stats));
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

static void testVBucketFilter() {
    VBucketFilter empty;

    assert(empty(0));
    assert(empty(1));
    assert(empty(2));

    std::vector<uint16_t> v;

    VBucketFilter emptyTwo(v);
    assert(emptyTwo(0));
    assert(emptyTwo(1));
    assert(emptyTwo(2));

    v.push_back(2);

    VBucketFilter hasOne(v);
    assert(!hasOne(0));
    assert(!hasOne(1));
    assert(hasOne(2));

    v.push_back(0);

    VBucketFilter hasTwo(v);
    assert(hasTwo(0));
    assert(!hasTwo(1));
    assert(hasTwo(2));

    v.push_back(1);

    VBucketFilter hasThree(v);
    assert(hasThree(0));
    assert(hasThree(1));
    assert(hasThree(2));
    assert(!hasThree(3));
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    HashTable::setDefaultNumBuckets(5);
    HashTable::setDefaultNumLocks(1);

    alarm(60);

    testVBucketLookup();
    testConcurrentUpdate();
    testVBucketFilter();
}
