/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#include "defragmenter_visitor.h"

#include <gtest/gtest.h>
#include <iomanip>
#include <locale>

static time_t start_time;

static time_t mock_abstime(const rel_time_t exptime) {
    return start_time + exptime;
}

static rel_time_t mock_current_time(void) {
    rel_time_t result = (rel_time_t)(time(NULL) - start_time);
    return result;
}

/**
 * Dummy callback to replace the flusher callback.
 */
class DummyCB: public Callback<uint16_t> {
public:
    DummyCB() {}

    void callback(uint16_t &dummy) {
        (void) dummy;
    }
};

/* Fill the bucket with the given number of docs. Returns the rate at which
 * items were added.
 */
static size_t populateVbucket(VBucket& vbucket, size_t ndocs) {

    /* Set the hashTable to a sensible size */
    vbucket.ht.resize(ndocs);

    /* Store items */
    char value[256];
    hrtime_t start = gethrtime();
    for (size_t i = 0; i < ndocs; i++) {
        std::stringstream ss;
        ss << "key" << i;
        const std::string key = ss.str();
        Item item(key.c_str(), key.length(), 0, 0, value, sizeof(value));
        vbucket.ht.add(item, VALUE_ONLY);
    }
    hrtime_t end = gethrtime();

    // Let hashTable set itself to correct size, post-fill
    vbucket.ht.resize();

    double duration_s = (end - start) / double(1000 * 1000 * 1000);
    return size_t(ndocs / duration_s);
}

/* Measure the rate at which the defragmenter can defragment documents, using
 * the given age threshold.
 *
 * Setup a Defragmenter, then time how long it takes to visit them all
 * documents in the given vbucket, npasses times.
 */
static size_t benchmarkDefragment(VBucket& vbucket, size_t passes,
                                  uint8_t age_threshold,
                                  size_t chunk_duration_ms) {
    // Create and run visitor for the specified number of iterations, with
    // the given age.
    DefragmentVisitor visitor(age_threshold);
    hrtime_t start = gethrtime();
    for (size_t i = 0; i < passes; i++) {
        // Loop until we get to the end; this may take multiple chunks depending
        // on the chunk_duration.
        HashTable::Position pos;
        while (pos != vbucket.ht.endPosition()) {
            visitor.setDeadline(gethrtime() +
                                 (chunk_duration_ms * 1000 * 1000));
            pos = vbucket.ht.pauseResumeVisit(visitor, pos);
        }
    }
    hrtime_t end = gethrtime();
    size_t visited = visitor.getVisitedCount();

    double duration_s = (end - start) / double(1000 * 1000 * 1000);
    return size_t(visited / duration_s);
}

class DefragmenterBenchmarkTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        /* Create the vbucket */
        std::shared_ptr<Callback<uint16_t> > cb(new DummyCB());
        vbucket.reset(new VBucket(0, vbucket_state_active, stats, config,
                                  nullptr, 0, 0, 0, nullptr, cb));
    }

    static void TearDownTestCase() {
        vbucket.reset();
    }

    static EPStats stats;
    static CheckpointConfig config;
    static std::unique_ptr<VBucket> vbucket;
};

EPStats DefragmenterBenchmarkTest::stats;
CheckpointConfig DefragmenterBenchmarkTest::config;
std::unique_ptr<VBucket> DefragmenterBenchmarkTest::vbucket;


TEST_F(DefragmenterBenchmarkTest, Populate) {
    size_t populateRate = populateVbucket(*vbucket, 500000);
    RecordProperty("items_per_sec", populateRate);
}

TEST_F(DefragmenterBenchmarkTest, Visit) {
    const size_t one_minute = 60 * 1000;
    size_t visit_rate = benchmarkDefragment(*vbucket, 1,
                                            std::numeric_limits<uint8_t>::max(),
                                            one_minute);
    RecordProperty("items_per_sec", visit_rate);
}

TEST_F(DefragmenterBenchmarkTest, DefragAlways) {
    const size_t one_minute = 60 * 1000;
    size_t defrag_always_rate = benchmarkDefragment(*vbucket, 1, 0,
                                                    one_minute);
    RecordProperty("items_per_sec", defrag_always_rate);
}

TEST_F(DefragmenterBenchmarkTest, DefragAge10) {
    const size_t one_minute = 60 * 1000;
    size_t defrag_age10_rate = benchmarkDefragment(*vbucket, 1, 10,
                                                   one_minute);
    RecordProperty("items_per_sec", defrag_age10_rate);
}

TEST_F(DefragmenterBenchmarkTest, DefragAge10_20ms) {
    size_t defrag_age10_20ms_rate = benchmarkDefragment(*vbucket, 1, 10, 20);
    RecordProperty("items_per_sec", defrag_age10_20ms_rate);
}


static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=1";

int main(int argc, char** argv) {
    /* Setup mock time functions */
    start_time = time(0);
    ep_abs_time = mock_abstime;
    ep_current_time = mock_current_time;

    putenv(allow_no_stats_env);

    // Set number of hashtable locks equal to current JSON config default.
    HashTable::setDefaultNumLocks(47);

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
