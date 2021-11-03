/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * Tests for Scope Data Limits
 */

#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "collections_dcp_test.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/module_tests/collections/collections_test.h"
#include "tests/module_tests/test_helpers.h"

#include <utilities/test_manifest.h>

#include <folly/portability/GMock.h>

// Test that when only replica vbuckets exist, and a scope with a limit exists
// that any promotion from replica->active will set the data limit on the newly
// active vbucket.
//
// The test harness creates two vbuckets, vb:0 (vbid) and vb:1 (replicaVB)
// Harness sets vb:0 active and vb:1 is replica.
//
// This test will start with both vbuckets as replicas
//
// * On the test manifest create scope 'shop1' with a limit
// * On the test manifest create collection in shop1
// * Set that manifest - no active vbuckets to update.
// * Next use consumer to create shop1 then fruit on vb:0
// * Expect that the vb:0 vbucket does know shop1 but not the limit
// * Set vb:0 to active
// * Expect that the vbucket knows the limit
TEST_P(CollectionsDcpParameterizedTest,
       replica_create_scope_with_limit_to_active) {
    store->setVBucketState(vbid, vbucket_state_replica);

    // No active vbuckets, so shop1 isn't applied anywhere yet
    const size_t limit = 10;
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1,
           limit * store->getEPEngine().getConfiguration().getMaxVbuckets());
    cm.add(CollectionEntry::fruit, ScopeEntry::shop1);
    setCollections(cookie, cm);

    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0));

    ASSERT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(/*opaque*/ 2,
                                       vbid,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ 2,
                                       /*flags*/ 0,
                                       /*HCS*/ {},
                                       /*maxVisibleSeqno*/ {}));

    // Replicate the scope and collection
    Collections::ManifestUid muid{1};
    createScopeOnConsumer(vbid, 2, muid, ScopeEntry::shop1, 1);
    muid++;
    createCollectionOnConsumer(
            vbid, 2, muid, ScopeEntry::shop1, CollectionEntry::fruit, 2);

    auto vb = store->getVBucket(vbid);
    // No no, no no no no...
    EXPECT_FALSE(vb->getManifest().lock().getDataLimit(ScopeEntry::shop1));

    // Will trigger a modification of the shop1 scope to update the limit
    store->setVBucketState(vbid, vbucket_state_active);

    // The limit is as expected
    EXPECT_TRUE(vb->getManifest().lock().getDataLimit(ScopeEntry::shop1));
    EXPECT_EQ(limit,
              vb->getManifest().lock().getDataLimit(ScopeEntry::shop1).value());

    // Check we fail limits on the active vbucket
    store_item(vbid,
               StoredDocKey{"k1", CollectionEntry::fruit},
               std::string(limit + 1, 'a'),
               0,
               {cb::engine_errc::scope_size_limit_exceeded});
    store_item(vbid,
               StoredDocKey{"k1", CollectionEntry::fruit},
               std::string(limit, 'a'));

    // for completeness - the other vb will reject for  other reasons
    store_item(replicaVB,
               StoredDocKey{"k1", CollectionEntry::fruit},
               std::string(limit + 1, 'a'),
               0,
               {cb::engine_errc::not_my_vbucket});

    // And check when active it rejects
    store->setVBucketState(replicaVB, vbucket_state_active);

    store_item(replicaVB,
               StoredDocKey{"k1", CollectionEntry::fruit},
               std::string(limit + 1, 'a'),
               0,
               {cb::engine_errc::scope_size_limit_exceeded});
    // but ok for at the limit
    store_item(replicaVB,
               StoredDocKey{"k1", CollectionEntry::fruit},
               std::string(limit, 'a'));
}

// Test that when an active vbucket has already applied the scope with limit
// that any replicas automatically get the limit even before switching to active
//
// The test harness creates two vbuckets, vb:0 (vbid) and vb:1 (replicaVB)
// Harness sets vb:0 active and vb:1 is replica.
//
// * On the test manifest create scope 'shop1' with a limit
// * On the test manifest create collection in shop1
// * Set that manifest - vb:0 is active and updates.
// * Expect that vb:0 knows shop1 and the limit
// * Next use consumer to create shop1 then fruit on vb:1
// * Expect that vb:1 knows shop1 and the limit
// * Set vb:1 to active and check it still knows the limit
TEST_P(CollectionsDcpParameterizedTest,
       replica_gets_scope_with_limit_from_active) {
    CollectionsManifest cm;
    const size_t limit = 10;
    cm.add(ScopeEntry::shop1,
           limit * store->getEPEngine().getConfiguration().getMaxVbuckets());
    cm.add(CollectionEntry::fruit, ScopeEntry::shop1);
    setCollections(cookie, cm); // will update vb:0

    // vb:0 knows shop1 and the limit
    auto vb = store->getVBucket(vbid);
    EXPECT_TRUE(vb->getManifest().lock().getDataLimit(ScopeEntry::shop1));
    EXPECT_EQ(limit,
              vb->getManifest().lock().getDataLimit(ScopeEntry::shop1).value());

    // Now drive the replica vbucket with the shop1/fruit setup
    ASSERT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(/*opaque*/ 1,
                                       replicaVB,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ 2,
                                       /*flags*/ 0,
                                       /*HCS*/ {},
                                       /*maxVisibleSeqno*/ {}));

    // Replicate the scope and collection
    Collections::ManifestUid muid{1};
    createScopeOnConsumer(replicaVB, 1, muid, ScopeEntry::shop1, 1);
    muid++;
    createCollectionOnConsumer(
            replicaVB, 1, muid, ScopeEntry::shop1, CollectionEntry::fruit, 2);
    auto replica = store->getVBucket(replicaVB);

    // 'inherited' the limit from active
    EXPECT_TRUE(replica->getManifest().lock().getDataLimit(ScopeEntry::shop1));
    EXPECT_EQ(limit,
              replica->getManifest()
                      .lock()
                      .getDataLimit(ScopeEntry::shop1)
                      .value());

    // Set vb:1 to active
    store->setVBucketState(replicaVB, vbucket_state_active);

    // The limit is as expected
    EXPECT_TRUE(vb->getManifest().lock().getDataLimit(ScopeEntry::shop1));
    EXPECT_EQ(limit,
              vb->getManifest().lock().getDataLimit(ScopeEntry::shop1).value());

    // Check we fail limits on both vbuckets if the size is right.
    // If we have the scopeWithDataLimitExists code wrong, the limits wouldn't
    // be checked
    store_item(vbid,
               StoredDocKey{"k1", CollectionEntry::fruit},
               std::string(limit + 1, 'a'),
               0,
               {cb::engine_errc::scope_size_limit_exceeded});

    store_item(replicaVB,
               StoredDocKey{"k1", CollectionEntry::fruit},
               std::string(limit + 1, 'a'),
               0,
               {cb::engine_errc::scope_size_limit_exceeded});

    // But can write upto the limit
    store_item(replicaVB,
               StoredDocKey{"k1", CollectionEntry::fruit},
               std::string(limit, 'a'));
    store_item(vbid,
               StoredDocKey{"k1", CollectionEntry::fruit},
               std::string(limit, 'a'));
}

// Test that when only replicas exist and they are the first to create a scope
// with a limit, ahead of the bucket manifest discovering the scope. That any
// active vbuckets created later, will correct the data limit once the correct
// manifest is set on the node.
// This exposes a window where an active vbucket can exist with the scope but
// not the data limit (theoretical hole as we expect the node to be updated
// before vbucket changes)
//
// The test harness creates two vbuckets, vb:0 (vbid) and vb:1 (replicaVB)
// Harness sets vb:0 active and vb:1 is replica.
//
// This test will start with both vbuckets as replicas
// * Use consumer to create shop1 then fruit on vb:0
// * Expect that vb:0 knows shop1 but the limit
// * Set vb:0 to active and check it still does not know the limit
// * Create a manifest with shop1 + limit and set the manifest
// * Expect that vb:0 now knows the limit
TEST_P(CollectionsDcpParameterizedTest, active_updates_limit) {
    store->setVBucketState(vbid, vbucket_state_replica);
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0));
    // Now drive the replica vbucket with the shop1/fruit setup
    ASSERT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(/*opaque*/ 2,
                                       vbid,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ 2,
                                       /*flags*/ 0,
                                       /*HCS*/ {},
                                       /*maxVisibleSeqno*/ {}));

    // Replicate the scope and collection
    Collections::ManifestUid muid{1};
    createScopeOnConsumer(vbid, 2, muid, ScopeEntry::shop1, 1);
    muid++;
    createCollectionOnConsumer(
            vbid, 2, muid, ScopeEntry::shop1, CollectionEntry::fruit, 2);

    auto vb = store->getVBucket(vbid);
    auto replica = store->getVBucket(replicaVB);

    // vb:0 knows shop1 but not the limit
    EXPECT_FALSE(vb->getManifest().lock().getDataLimit(ScopeEntry::shop1));

    // Switch both to active.
    store->setVBucketState(vbid, vbucket_state_active);
    store->setVBucketState(replicaVB, vbucket_state_active);

    // vb:0 knows shop1 but not the limit
    EXPECT_FALSE(vb->getManifest().lock().getDataLimit(ScopeEntry::shop1));
    // vb:1 does not know shop1
    EXPECT_FALSE(replica->getManifest().lock().isScopeValid(ScopeEntry::shop1));

    // Now set shop1 with limit
    CollectionsManifest cm;
    const size_t limit = 10;
    cm.add(ScopeEntry::shop1,
           limit * store->getEPEngine().getConfiguration().getMaxVbuckets());
    cm.add(CollectionEntry::fruit, ScopeEntry::shop1);
    setCollections(cookie, cm); // vb:1 now receives scope/collection

    // Now the limit is as expected
    EXPECT_TRUE(vb->getManifest().lock().getDataLimit(ScopeEntry::shop1));
    EXPECT_EQ(limit,
              vb->getManifest().lock().getDataLimit(ScopeEntry::shop1).value());

    EXPECT_TRUE(replica->getManifest().lock().getDataLimit(ScopeEntry::shop1));
    EXPECT_EQ(limit,
              replica->getManifest()
                      .lock()
                      .getDataLimit(ScopeEntry::shop1)
                      .value());

    // Check we fail limits on both vbuckets if the size is right.
    // If we have the scopeWithDataLimitExists code wrong, the limits wouldn't
    // be checked
    store_item(vbid,
               StoredDocKey{"k1", CollectionEntry::fruit},
               std::string(limit + 1, 'a'),
               0,
               {cb::engine_errc::scope_size_limit_exceeded});

    store_item(replicaVB,
               StoredDocKey{"k1", CollectionEntry::fruit},
               std::string(limit + 1, 'a'),
               0,
               {cb::engine_errc::scope_size_limit_exceeded});

    // But can write upto the limit
    store_item(replicaVB,
               StoredDocKey{"k1", CollectionEntry::fruit},
               std::string(limit, 'a'));
    store_item(vbid,
               StoredDocKey{"k1", CollectionEntry::fruit},
               std::string(limit, 'a'));
}

TEST_F(CollectionsTest, ScopeWithManyCollectionsWarmup) {
    auto vb = store->getVBucket(vbid);

    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1);
    cm.add(CollectionEntry::fruit, ScopeEntry::shop1);
    cm.add(CollectionEntry::vegetable, ScopeEntry::shop1);
    cm.add(CollectionEntry::dairy, ScopeEntry::shop1);
    setCollections(cookie, cm);

    ASSERT_TRUE(store_items(
            2, vbid, makeStoredDocKey("f", CollectionEntry::fruit), "value"));
    ASSERT_TRUE(store_items(2,
                            vbid,
                            makeStoredDocKey("v", CollectionEntry::vegetable),
                            "value"));
    ASSERT_TRUE(store_items(
            2, vbid, makeStoredDocKey("d", CollectionEntry::dairy), "value"));

    EXPECT_EQ(0, vb->getManifest().lock().getDataSize(ScopeEntry::shop1));

    flushVBucketToDiskIfPersistent(vbid, 4 + 6);
    auto ds = vb->getManifest().lock().getDataSize(ScopeEntry::shop1);
    EXPECT_NE(0, ds);
    vb.reset();
    resetEngineAndWarmup();

    // Data size comes back the same value
    EXPECT_EQ(ds,
              store->getVBucket(vbid)->getManifest().lock().getDataSize(
                      ScopeEntry::shop1));
}
