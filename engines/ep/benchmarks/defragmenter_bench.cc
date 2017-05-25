/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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
#include "tests/module_tests/defragmenter_test.h"
#include "tests/module_tests/test_helpers.h"

#include <gtest/gtest.h>
#include <valgrind/valgrind.h>


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


class DefragmenterBenchmarkTest : public DefragmenterTest {
protected:
    /* Fill the bucket with the given number of docs. Returns the rate at which
     * items were added.
     */
    size_t populateVbucket() {
        // How many items to create in the VBucket. Use a large number for
        // normal runs when measuring performance, but a very small number
        // (enough for functional testing) when running under Valgrind
        // where there's no sense in measuring performance.
        const size_t ndocs = RUNNING_ON_VALGRIND ? 10 : 500000;

        /* Set the hashTable to a sensible size */
        vbucket->ht.resize(ndocs);

        /* Store items */
        char value[256];
        hrtime_t start = gethrtime();
        for (size_t i = 0; i < ndocs; i++) {
            std::string key = "key" + std::to_string(i);
            Item item(makeStoredDocKey(key), 0, 0, value, sizeof(value));
            public_processSet(item, 0);
        }
        hrtime_t end = gethrtime();

        // Let hashTable set itself to correct size, post-fill
        vbucket->ht.resize();

        double duration_s = (end - start) / double(1000 * 1000 * 1000);
        return size_t(ndocs / duration_s);
    }

};

TEST_P(DefragmenterBenchmarkTest, Populate) {
    size_t populateRate = populateVbucket();
    RecordProperty("items_per_sec", populateRate);
}

TEST_P(DefragmenterBenchmarkTest, Visit) {
    populateVbucket();
    const size_t one_minute = 60 * 1000;
    size_t visit_rate = benchmarkDefragment(*vbucket, 1,
                                            std::numeric_limits<uint8_t>::max(),
                                            one_minute);
    RecordProperty("items_per_sec", visit_rate);
}

TEST_P(DefragmenterBenchmarkTest, DefragAlways) {
    populateVbucket();
    const size_t one_minute = 60 * 1000;
    size_t defrag_always_rate = benchmarkDefragment(*vbucket, 1, 0,
                                                    one_minute);
    RecordProperty("items_per_sec", defrag_always_rate);
}

TEST_P(DefragmenterBenchmarkTest, DefragAge10) {
    populateVbucket();
    const size_t one_minute = 60 * 1000;
    size_t defrag_age10_rate = benchmarkDefragment(*vbucket, 1, 10,
                                                   one_minute);
    RecordProperty("items_per_sec", defrag_age10_rate);
}

TEST_P(DefragmenterBenchmarkTest, DefragAge10_20ms) {
    populateVbucket();
    size_t defrag_age10_20ms_rate = benchmarkDefragment(*vbucket, 1, 10, 20);
    RecordProperty("items_per_sec", defrag_age10_20ms_rate);
}

INSTANTIATE_TEST_CASE_P(
        FullAndValueEviction,
        DefragmenterBenchmarkTest,
        ::testing::Values(VALUE_ONLY, FULL_EVICTION),
        [](const ::testing::TestParamInfo<item_eviction_policy_t>& info) {
            if (info.param == VALUE_ONLY) {
                return "VALUE_ONLY";
            } else {
                return "FULL_EVICTION";
            }
        });
