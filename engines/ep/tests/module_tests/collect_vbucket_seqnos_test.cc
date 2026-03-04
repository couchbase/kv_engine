/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "evp_store_single_threaded_test.h"
#include "test_helpers.h"
#include "vbucket.h"

#include <memcached/vbucket.h>
#include <utilities/test_manifest.h>
#include <cstdint>

class CollectVBucketSeqnosTest : public SingleThreadedKVBucketTest {
public:
    CollectVBucketSeqnosTest()
        : key1(makeStoredDocKey("key1")),
          key2(makeStoredDocKey("key2", CollectionEntry::fruit)),
          key3(makeStoredDocKey("key3", CollectionEntry::vegetable)) {
    }

    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();

        setVBucketState(vbid0, vbucket_state_active);
        setVBucketState(vbid1, vbucket_state_replica);
        setVBucketState(vbid2, vbucket_state_active);
        setVBucketState(vbid3, vbucket_state_active);
        setVBucketState(vbid3, vbucket_state_dead);

        // Create collections and scopes for testing the filtering.
        // Only the active vbuckets will get the collections setup
        CollectionsManifest cm;
        cm.add(ScopeEntry::shop1);
        cm.add(CollectionEntry::fruit, ScopeEntry::shop1);
        cm.add(CollectionEntry::vegetable);
        setCollections(cookie, cm);
        flushVBucketToDiskIfPersistent(vbid0, 3);
        flushVBucketToDiskIfPersistent(vbid2, 3);

        // Take vbid0 to high-seqno + 3
        store_item(vbid0, key1, ""); // default collection
        store_item(vbid0, key2, ""); // collection 'fruit' in scope 'shop1'
        store_item(vbid0, key3, ""); // vegetable collection in default scope
        flushVBucketToDiskIfPersistent(vbid0, 3);

        // Take vbid2 to high-seqno + 1 (default collection only)
        store_item(vbid2, key1, "");
        flushVBucketToDiskIfPersistent(vbid2, 1);

        vbid0HS = engine->getVBucket(vbid0)->getHighSeqno();
        vbid1HS = engine->getVBucket(vbid1)->getHighSeqno();
        vbid2HS = engine->getVBucket(vbid2)->getHighSeqno();
    }

    void TearDown() override {
        SingleThreadedKVBucketTest::TearDown();
    }

    StoredDocKey key1;
    StoredDocKey key2;
    StoredDocKey key3;
    Vbid vbid0{0};
    Vbid vbid1{1};
    Vbid vbid2{2};
    Vbid vbid3{3};
    uint64_t vbid0HS{0};
    uint64_t vbid1HS{0};
    uint64_t vbid2HS{0};
};

TEST_F(CollectVBucketSeqnosTest, bucket_seqnos) {
    // Test collecting sequence numbers with no collection filter
    auto kvbucket = engine->getKVBucket();
    auto vbuckets = kvbucket->getVBuckets().getBuckets();

    std::vector<std::pair<Vbid, uint64_t>> vbSeqnosInfo;

    engine->collectVBucketSequenceNumbers(
            aliveVBStates,
            {},
            {},
            [&vbSeqnosInfo](Vbid vbid, uint64_t seqno) {
                vbSeqnosInfo.emplace_back(vbid, seqno);
            },
            true,
            true);

    EXPECT_FALSE(vbSeqnosInfo.empty());
    // 3 vbuckets should have valid seqnos
    EXPECT_EQ(3, vbSeqnosInfo.size());

    // Check each vbid0, vbid1 and vbid2 and check the sequence numbers are as
    // expected
    auto it0 = std::find_if(vbSeqnosInfo.begin(),
                            vbSeqnosInfo.end(),
                            [this](const std::pair<Vbid, uint64_t> info) {
                                return info.first == vbid0;
                            });
    ASSERT_NE(it0, vbSeqnosInfo.end());
    EXPECT_EQ(it0->second, vbid0HS);

    auto it1 = std::find_if(vbSeqnosInfo.begin(),
                            vbSeqnosInfo.end(),
                            [this](const std::pair<Vbid, uint64_t> info) {
                                return info.first == vbid1;
                            });
    ASSERT_NE(it1, vbSeqnosInfo.end());
    // We can expect the seqno to be 1.
    EXPECT_EQ(it1->second, vbid1HS);

    auto it2 = std::find_if(vbSeqnosInfo.begin(),
                            vbSeqnosInfo.end(),
                            [this](const std::pair<Vbid, uint64_t> info) {
                                return info.first == vbid2;
                            });
    ASSERT_NE(it2, vbSeqnosInfo.end());
    // We can expect the seqno to be 0 as no items were stored in vbid2.
    EXPECT_EQ(it2->second, vbid2HS);
}

TEST_F(CollectVBucketSeqnosTest, active_vbucket_seqnos) {
    // Test collecting sequence numbers with no collection filter
    auto kvbucket = engine->getKVBucket();
    auto vbuckets = kvbucket->getVBuckets().getBuckets();

    // Create std::pair<Vbid, uint64_t vector
    std::vector<std::pair<Vbid, uint64_t>> vbSeqnosInfo;

    // active vbuckets only (no collection filter)
    engine->collectVBucketSequenceNumbers(
            vbucket_state_active,
            {},
            {},
            [&vbSeqnosInfo](Vbid vbid, uint64_t seqno) {
                vbSeqnosInfo.emplace_back(vbid, seqno);
            },
            true,
            true);

    EXPECT_FALSE(vbSeqnosInfo.empty());
    // 2 active vbuckets should have valid seqnos
    EXPECT_EQ(2, vbSeqnosInfo.size());

    // Lookup each vbid0/vbid2 and check the sequence numbers are as expected
    auto it0 = std::find_if(vbSeqnosInfo.begin(),
                            vbSeqnosInfo.end(),
                            [this](const std::pair<Vbid, uint64_t> info) {
                                return info.first == vbid0;
                            });
    ASSERT_NE(it0, vbSeqnosInfo.end());
    EXPECT_EQ(it0->second, vbid0HS);

    auto it2 = std::find_if(vbSeqnosInfo.begin(),
                            vbSeqnosInfo.end(),
                            [this](const std::pair<Vbid, uint64_t> info) {
                                return info.first == vbid2;
                            });
    ASSERT_NE(it2, vbSeqnosInfo.end());
    EXPECT_EQ(it2->second, vbid2HS);
}

TEST_F(CollectVBucketSeqnosTest, collection_filtering) {
    // Create a test vbucket
    auto kvbucket = engine->getKVBucket();
    auto vb = kvbucket->getVBucket(vbid);
    ASSERT_TRUE(vb);

    // Create std::pair<Vbid, uint64_t vector
    auto vbuckets = kvbucket->getVBuckets().getBuckets();
    std::vector<std::pair<Vbid, uint64_t>> vbSeqnosInfo;

    // Test collecting sequence numbers with default collection filter
    engine->collectVBucketSequenceNumbers(
            aliveVBStates,
            CollectionEntry::fruit,
            {},
            [&vbSeqnosInfo](Vbid vbid, uint64_t seqno) {
                vbSeqnosInfo.emplace_back(vbid, seqno);
            },
            true,
            true);

    EXPECT_FALSE(vbSeqnosInfo.empty());
    // Count vbuckets with valid seqnos
    EXPECT_EQ(2, vbSeqnosInfo.size());

    auto it0 = std::find_if(vbSeqnosInfo.begin(),
                            vbSeqnosInfo.end(),
                            [this](const std::pair<Vbid, uint64_t> info) {
                                return info.first == vbid0;
                            });
    ASSERT_NE(it0, vbSeqnosInfo.end());
    EXPECT_EQ(it0->second, vbid0HS - 1);

    auto it2 = std::find_if(vbSeqnosInfo.begin(),
                            vbSeqnosInfo.end(),
                            [this](const std::pair<Vbid, uint64_t> info) {
                                return info.first == vbid2;
                            });
    ASSERT_NE(it2, vbSeqnosInfo.end());
    EXPECT_EQ(it2->second,
              engine->getVBucket(vbid2)
                      ->getManifest()
                      .lock(CollectionEntry::fruit)
                      .getHighSeqno());
}

// Some coverage for the max-visible seqno case
TEST_F(CollectVBucketSeqnosTest, sync_writes) {
    setVBucketStateAndRunPersistTask(
            vbid0,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    // Add an extra pending write
    store_pending_item(vbid0, key1, "");

    // Test with sync writes enabled (collections is also off)
    auto kvbucket = engine->getKVBucket();
    auto vbuckets = kvbucket->getVBuckets().getBuckets();

    std::vector<std::pair<Vbid, uint64_t>> vbSeqnosInfo;

    engine->collectVBucketSequenceNumbers(
            aliveVBStates,
            {},
            {},
            [&vbSeqnosInfo](Vbid vbid, uint64_t seqno) {
                vbSeqnosInfo.emplace_back(vbid, seqno);
            },
            false,
            false);

    EXPECT_FALSE(vbSeqnosInfo.empty());
    // 3 vbuckets should have valid seqnos
    EXPECT_EQ(3, vbSeqnosInfo.size());

    // Lookup each vbid0, vbid1 and vbid2 and check the sequence numbers are as
    // expected
    auto it0 = std::find_if(vbSeqnosInfo.begin(),
                            vbSeqnosInfo.end(),
                            [this](const std::pair<Vbid, uint64_t> info) {
                                return info.first == vbid0;
                            });
    ASSERT_NE(it0, vbSeqnosInfo.end());
    // Expect vbid0 highseqno to no longer match vbid0HS
    EXPECT_NE(engine->getVBucket(vbid0)->getHighSeqno(), vbid0HS);
    // But the value from collect is the old highseqno before the pending write
    EXPECT_EQ(it0->second, vbid0HS);

    auto it1 = std::find_if(vbSeqnosInfo.begin(),
                            vbSeqnosInfo.end(),
                            [this](const std::pair<Vbid, uint64_t> info) {
                                return info.first == vbid1;
                            });
    ASSERT_NE(it1, vbSeqnosInfo.end());
    // We can expect the seqno to be 1.
    EXPECT_EQ(it1->second, vbid1HS);

    auto it2 = std::find_if(vbSeqnosInfo.begin(),
                            vbSeqnosInfo.end(),
                            [this](const std::pair<Vbid, uint64_t> info) {
                                return info.first == vbid2;
                            });
    ASSERT_NE(it2, vbSeqnosInfo.end());
    // We can expect the seqno to be 0 as no items were stored in vbid2.
    EXPECT_EQ(it2->second, vbid2HS);

    // Collect again with sync writes enabled and see we get vbid0HS + 1 for
    // vbid0
    // Reset the vector - clear it instead of just resetting seqnos
    vbSeqnosInfo.clear();

    engine->collectVBucketSequenceNumbers(
            aliveVBStates,
            {},
            {},
            [&vbSeqnosInfo](Vbid vbid, uint64_t seqno) {
                vbSeqnosInfo.emplace_back(vbid, seqno);
            },
            true,
            true);
    EXPECT_FALSE(vbSeqnosInfo.empty());
    EXPECT_EQ(3, vbSeqnosInfo.size());
    it0 = std::find_if(vbSeqnosInfo.begin(),
                       vbSeqnosInfo.end(),
                       [this](const std::pair<Vbid, uint64_t> info) {
                           return info.first == vbid0;
                       });
    ASSERT_NE(it0, vbSeqnosInfo.end());
    EXPECT_EQ(it0->second, vbid0HS + 1);
}

TEST_F(CollectVBucketSeqnosTest, scope_filtering) {
    // Create a test vbucket
    auto kvbucket = engine->getKVBucket();
    auto vb = kvbucket->getVBucket(vbid);
    ASSERT_TRUE(vb);

    // Test collecting sequence numbers with default collection filter
    auto vbuckets = kvbucket->getVBuckets().getBuckets();
    std::vector<std::pair<Vbid, uint64_t>> vbSeqnosInfo;

    engine->collectVBucketSequenceNumbers(
            aliveVBStates,
            {},
            ScopeEntry::shop1,
            [&vbSeqnosInfo](Vbid vbid, uint64_t seqno) {
                vbSeqnosInfo.emplace_back(vbid, seqno);
            },
            true,
            true);

    EXPECT_FALSE(vbSeqnosInfo.empty());
    // 3 vbuckets in map, but replica has no collections, so returns nothing.
    EXPECT_EQ(2, vbSeqnosInfo.size());

    auto it = std::find_if(vbSeqnosInfo.begin(),
                           vbSeqnosInfo.end(),
                           [this](const std::pair<Vbid, uint64_t> info) {
                               return info.first == vbid0;
                           });
    ASSERT_NE(it, vbSeqnosInfo.end());
    EXPECT_EQ(it->second, vbid0HS - 1);

    it = std::find_if(vbSeqnosInfo.begin(),
                      vbSeqnosInfo.end(),
                      [this](const std::pair<Vbid, uint64_t> info) {
                          return info.first == vbid2;
                      });
    ASSERT_NE(it, vbSeqnosInfo.end());
    EXPECT_EQ(it->second,
              engine->getVBucket(vbid2)
                      ->getManifest()
                      .lock(CollectionEntry::fruit)
                      .getHighSeqno());

    vbSeqnosInfo.clear();
    engine->collectVBucketSequenceNumbers(
            aliveVBStates,
            {},
            ScopeID::Default,
            [&vbSeqnosInfo](Vbid vbid, uint64_t seqno) {
                vbSeqnosInfo.emplace_back(vbid, seqno);
            },
            true,
            true);

    EXPECT_FALSE(vbSeqnosInfo.empty());
    // 3 vbuckets in map, and default collection exists everywhere by default...
    EXPECT_EQ(3, vbSeqnosInfo.size());

    it = std::find_if(vbSeqnosInfo.begin(),
                      vbSeqnosInfo.end(),
                      [this](const std::pair<Vbid, uint64_t> info) {
                          return info.first == vbid0;
                      });
    ASSERT_NE(it, vbSeqnosInfo.end());
    EXPECT_EQ(it->second, vbid0HS);

    it = std::find_if(vbSeqnosInfo.begin(),
                      vbSeqnosInfo.end(),
                      [this](const std::pair<Vbid, uint64_t> info) {
                          return info.first == vbid2;
                      });
    ASSERT_NE(it, vbSeqnosInfo.end());
    EXPECT_EQ(it->second,
              engine->getVBucket(vbid2)
                      ->getManifest()
                      .lock(CollectionID::Default)
                      .getHighSeqno());

    it = std::find_if(vbSeqnosInfo.begin(),
                      vbSeqnosInfo.end(),
                      [this](const std::pair<Vbid, uint64_t> info) {
                          return info.first == vbid1;
                      });
    ASSERT_NE(it, vbSeqnosInfo.end());
    EXPECT_EQ(it->second, 0);
}