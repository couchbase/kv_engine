/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "evp_store_single_threaded_test.h"

#include "collections/collections_test_helpers.h"
#include "item.h"
#include "kv_bucket.h"
#include "test_helpers.h"
#include "test_manifest.h"
#include "vbucket.h"

#include <folly/portability/GTest.h>

class PiTRTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        // Testing some PiTR specific compaction things here so we want to
        // actually be able to compact things during the test.
        config_string += "pitr_max_history_age=1;pitr_granularity=1";
        STParameterizedBucketTest::SetUp();
    }
};

TEST_P(PiTRTest, PitrCompactionPurgesCollection) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    CollectionsManifest cm(CollectionEntry::dairy);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));

    auto key = makeStoredDocKey("milk", CollectionEntry::dairy);
    store_item(vbid, key, "value");
    flushVBucketToDiskIfPersistent(vbid, 2);

    cm.remove(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // @TODO Sleeping in tests is pretty rubbish, but the couchstore side of
    // PiTR uses the system clock so we don't have much choice right now...
    std::this_thread::sleep_for(std::chrono::seconds(1));
    runCompaction(vbid);

    auto [status, dropped] = store->getVBucket(vbid)
                                     ->getShard()
                                     ->getRWUnderlying()
                                     ->getDroppedCollections(vbid);
    ASSERT_TRUE(status);
    EXPECT_TRUE(dropped.empty());
}

INSTANTIATE_TEST_SUITE_P(PiTRTest,
                         PiTRTest,
                         STParameterizedBucketTest::pitrEnabledConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);