/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#include <signal.h>

#include <algorithm>
#include <vector>

#include "configuration.h"
#include "stats.h"
#include "threadtests.h"
#include "vbucket.h"
#include "vbucketmap.h"

static const size_t numThreads = 10;
static const size_t vbucketsEach = 100;

EPStats global_stats;
CheckpointConfig checkpoint_config;

extern "C" {
    static rel_time_t basic_current_time(void) {
        return 0;
    }

    rel_time_t (*ep_current_time)() = basic_current_time;

    time_t ep_real_time() {
        return time(NULL);
    }
}

class VBucketGenerator {
public:
    VBucketGenerator(EPStats &s, CheckpointConfig &c, int start = 1)
        : st(s), config(c), i(start) {}
    VBucket *operator()() {
        return new VBucket(i++, vbucket_state_active, st, config, NULL);
    }
private:
    EPStats &st;
    CheckpointConfig &config;
    int i;
};

static void assertVBucket(const VBucketMap& vbm, int id) {
    RCPtr<VBucket> v = vbm.getBucket(id);
    cb_assert(v);
    cb_assert(v->getId() == id);
}

static void testVBucketLookup() {
    VBucketGenerator vbgen(global_stats, checkpoint_config);
    std::vector<VBucket*> bucketList(3);
    std::generate_n(bucketList.begin(), bucketList.capacity(), vbgen);

    Configuration config;
    VBucketMap vbm(config);
    vbm.addBuckets(bucketList);

    cb_assert(!vbm.getBucket(4));
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

            RCPtr<VBucket> v(new VBucket(newId, vbucket_state_active,
                                         global_stats, checkpoint_config, NULL));
            vbm->addBucket(v);
            cb_assert(vbm->getBucket(newId) == v);

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
    Configuration config;
    config.setMaxNumShards((size_t)1);
    VBucketMap vbm(config);
    AtomicUpdater au(&vbm);
    getCompletedThreads<bool>(numThreads, &au);

    // We remove half of the buckets in the test
    std::vector<int> rv;
    vbm.getBuckets(rv);
    cb_assert(rv.size() == ((numThreads * vbucketsEach) / 2));
}

static void testVBucketFilter() {
    VBucketFilter empty;

    cb_assert(empty(0));
    cb_assert(empty(1));
    cb_assert(empty(2));

    std::vector<uint16_t> v;

    VBucketFilter emptyTwo(v);
    cb_assert(emptyTwo(0));
    cb_assert(emptyTwo(1));
    cb_assert(emptyTwo(2));

    v.push_back(2);

    VBucketFilter hasOne(v);
    cb_assert(!hasOne(0));
    cb_assert(!hasOne(1));
    cb_assert(hasOne(2));

    v.push_back(0);

    VBucketFilter hasTwo(v);
    cb_assert(hasTwo(0));
    cb_assert(!hasTwo(1));
    cb_assert(hasTwo(2));

    v.push_back(1);

    VBucketFilter hasThree(v);
    cb_assert(hasThree(0));
    cb_assert(hasThree(1));
    cb_assert(hasThree(2));
    cb_assert(!hasThree(3));
}

static void assertFilterTxt(const VBucketFilter &filter, const std::string &res)
{
    std::stringstream ss;
    ss << filter;
    if (ss.str() != res) {
        std::cerr << "Filter mismatch: "
                  << ss.str() << " != " << res << std::endl;
        std::cerr.flush();
        abort();
    }
}

static void testVBucketFilterFormatter(void) {
    std::set<uint16_t> v;

    VBucketFilter filter(v);
    assertFilterTxt(filter, "{ empty }");
    v.insert(1);
    filter.assign(v);
    assertFilterTxt(filter, "{ 1 }");

    for (uint16_t ii = 2; ii < 100; ++ii) {
        v.insert(ii);
    }
    filter.assign(v);
    assertFilterTxt(filter, "{ [1,99] }");

    v.insert(101);
    v.insert(102);
    filter.assign(v);
    assertFilterTxt(filter, "{ [1,99], 101, 102 }");

    v.insert(103);
    filter.assign(v);
    assertFilterTxt(filter, "{ [1,99], [101,103] }");

    v.insert(100);
    filter.assign(v);
    assertFilterTxt(filter, "{ [1,103] }");
}

static void testGetVBucketsByState(void) {
    Configuration config;
    VBucketMap vbm(config);

    int st = vbucket_state_dead;
    for (int id = 0; id < 4; id++, st--) {
        RCPtr<VBucket> v(new VBucket(id, (vbucket_state_t)st, global_stats,
                                     checkpoint_config, NULL));
        vbm.addBucket(v);
        cb_assert(vbm.getBucket(id) == v);
    }
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    putenv(strdup("ALLOW_NO_STATS_UPDATE=yeah"));

    HashTable::setDefaultNumBuckets(5);
    HashTable::setDefaultNumLocks(1);

    alarm(60);

    testVBucketLookup();
    testConcurrentUpdate();
    testVBucketFilter();
    testVBucketFilterFormatter();
    testGetVBucketsByState();
}
