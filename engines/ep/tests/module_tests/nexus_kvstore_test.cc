/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "evp_store_single_threaded_test.h"

#include "collections/collections_test_helpers.h"

#include "kv_bucket.h"
#include "test_helpers.h"
#include "test_manifest.h"
#include "vbucket.h"

// GTest warns about uninstantiated tests and currently the only variants of
// Nexus tests are EE only as they require magma. NexusKVStore supports other
// variants though which aren't necessarily EE, but we don't currently test.
// Instead of making this entire test suite EE only, just supress the warning.
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(NexusKVStoreTest);

/**
 * Test fixture for the NexusKVStore test harness
 */
class NexusKVStoreTest : public STParamPersistentBucketTest {
public:
    static auto couchstoreMagmaVariants() {
        using namespace std::string_literals;
        return ::testing::Values(
#ifdef EP_USE_MAGMA
                std::make_tuple("persistent_nexus_couchstore_magma"s,
                                "value_only"s),
                std::make_tuple("persistent_nexus_couchstore_magma"s,
                                "full_eviction"s),
                std::make_tuple("persistent_nexus_magma_couchstore"s,
                                "value_only"s),
                std::make_tuple("persistent_nexus_magma_couchstore"s,
                                "full_eviction"s)
#endif
        );
    }
};

TEST_P(NexusKVStoreTest, DropCollectionMidFlush) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBucket(vbid);

    CollectionsManifest cm;
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::meat)));
    flushVBucketToDiskIfPersistent(vbid, 1);
    store_item(vbid,
               makeStoredDocKey("keyA", CollectionEntry::meat.getId()),
               "biggerValues",
               0 /*exptime*/,
               {cb::engine_errc::success} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES);

    // Previously the post commit checks would fail to find the dropped
    // collection in the manifest but assumed it was there and segfaulted
    auto* kvstore = store->getRWUnderlying(vbid);
    kvstore->setPostFlushHook([this, &vb, &cm]() {
        vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::meat)));
    });

    flushVBucketToDiskIfPersistent(vbid, 1);
}

INSTANTIATE_TEST_SUITE_P(Nexus,
                         NexusKVStoreTest,
                         NexusKVStoreTest::couchstoreMagmaVariants(),
                         STParameterizedBucketTest::PrintToStringParamName);
