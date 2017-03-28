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

/**
 * Tests specific to Ephemeral VBuckets.
 */

#include "config.h"

#include "ephemeral_vb.h"
#include "failover-table.h"
#include "test_helpers.h"
#include "seqlist.h"
#include "vbucket_test.h"

class EphemeralVBucketTest : public VBucketTest {
protected:
    void SetUp() {
        vbucket.reset(new EphemeralVBucket(0,
                                        vbucket_state_active,
                                        global_stats,
                                        checkpoint_config,
                                        /*kvshard*/ nullptr,
                                        /*lastSeqno*/ 1000,
                                        /*lastSnapStart*/ 0,
                                        /*lastSnapEnd*/ 0,
                                        /*table*/ nullptr,
                                        /*newSeqnoCb*/ nullptr,
                                        config,
                                        VALUE_ONLY));
    }

    void TearDown() {
        vbucket.reset();
    }

    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
};

// Verify that attempting to pageOut an item twice has no effect the second
// time.
TEST_F(EphemeralVBucketTest, DoublePageOut) {
    auto key = makeStoredDocKey("key");
    addOne(key, AddStatus::Success);
    ASSERT_EQ(1, vbucket->getNumItems());

    auto lock_sv = lockAndFind(key);
    auto* storedVal = lock_sv.second;
    ASSERT_FALSE(storedVal->isDeleted());

    // Page out the item (once).
    EXPECT_TRUE(vbucket->pageOut(lock_sv.first, storedVal));
    EXPECT_EQ(0, vbucket->getNumItems());
    EXPECT_TRUE(storedVal->isDeleted());

    // Attempt to page out again - should not be possible.
    EXPECT_FALSE(vbucket->pageOut(lock_sv.first, storedVal));
    EXPECT_EQ(0, vbucket->getNumItems());
    EXPECT_TRUE(storedVal->isDeleted());
}
