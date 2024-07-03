/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp_hlc_invalid_strategy_test.h"
#include "vbucket_utils.h"

#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_stream.h"
#include <folly/portability/GTest.h>

// Verify behaviour for DCP setWithMeta with dcp_hlc_invalid_strategy=ignore
TEST_P(PassiveStreamHlcInvalidStrategyTest, dcpSetWithMetaInvalidCasIgnore) {
    auto vb = engine->getVBucket(vbid);

    // create commited item with poisoned cas
    auto key = makeStoredDocKey("setWithMeta");
    auto poisonedCas = std::numeric_limits<int64_t>::max() & ~0xffffull;
    auto item = makeCommittedItem(key, "value", vbid);
    item->setBySeqno(1);
    item->setCas(poisonedCas);

    // setWithMeta succeeds as invalid cas is ignored
    EXPECT_EQ(cb::engine_errc::success,
              testDcpReplicationWithInvalidCas(std::move(item)));
    EXPECT_EQ(1, store->getEPEngine().getEpStats().numInvalidCas);
    EXPECT_EQ(0, store->getEPEngine().getEpStats().numCasRegenerated);

    vb->notifyReplication();

    EXPECT_EQ(1, vb->getNumItems());
    EXPECT_EQ(1, vb->ht.getNumItems());

    auto prepareSeqno = 1;
    uint64_t cas;
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::CommittedViaMutation,
                  sv.storedValue->getCommitted());
        ASSERT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
        cas = sv.storedValue->getCas();
    }
    // item is stored with initial poisoned cas
    EXPECT_EQ(poisonedCas, cas);
}

// Verify behaviour for DCP setWithMeta with dcp_hlc_invalid_strategy=replace
TEST_P(PassiveStreamHlcInvalidStrategyTest, dcpSetWithMetaInvalidCasReplace) {
    config_string += ";dcp_hlc_invalid_strategy=replace";
    reinitialise(config_string);
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto vb = engine->getVBucket(vbid);

    // create commited item with poisoned cas
    auto key = makeStoredDocKey("setWithMeta");
    auto poisonedCas = std::numeric_limits<int64_t>::max() & ~0xffffull;
    auto item = makeCommittedItem(key, "value", vbid);
    item->setBySeqno(1);
    item->setCas(poisonedCas);

    // setWithMeta succeeds as the poisoned cas is regenerated
    EXPECT_EQ(cb::engine_errc::success,
              testDcpReplicationWithInvalidCas(std::move(item)));
    EXPECT_EQ(1, store->getEPEngine().getEpStats().numInvalidCas);
    EXPECT_EQ(1, store->getEPEngine().getEpStats().numCasRegenerated);

    vb->notifyReplication();

    EXPECT_EQ(1, vb->getNumItems());
    EXPECT_EQ(1, vb->ht.getNumItems());

    auto prepareSeqno = 1;
    uint64_t cas;
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::CommittedViaMutation,
                  sv.storedValue->getCommitted());
        ASSERT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
        cas = sv.storedValue->getCas();
    }
    // item is stored with a new generated cas value
    EXPECT_LT(cas, poisonedCas);
}

// Verify behaviour for DCP setWithMeta with dcp_hlc_invalid_strategy=error
TEST_P(PassiveStreamHlcInvalidStrategyTest, dcpSetWithMetaInvalidCasError) {
    config_string += ";dcp_hlc_invalid_strategy=error";
    reinitialise(config_string);
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto vb = engine->getVBucket(vbid);

    // create commited item with poisoned cas
    auto key = makeStoredDocKey("setWithMeta");
    auto poisonedCas = std::numeric_limits<int64_t>::max() & ~0xffffull;
    auto item = makeCommittedItem(key, "value", vbid);
    item->setBySeqno(1);
    item->setCas(poisonedCas);

    // setWithMeta fails due to invalid cas
    EXPECT_EQ(cb::engine_errc::cas_value_invalid,
              testDcpReplicationWithInvalidCas(std::move(item)));
    EXPECT_EQ(1, store->getEPEngine().getEpStats().numInvalidCas);
    EXPECT_EQ(0, store->getEPEngine().getEpStats().numCasRegenerated);
}

// Verify behaviour for DCP deleteWithMeta with dcp_hlc_invalid_strategy=ignore
TEST_P(PassiveStreamHlcInvalidStrategyTest, dcpDeleteWithMetaInvalidCasIgnore) {
    auto vb = engine->getVBucket(vbid);

    // create deleted item with poisoned cas
    auto key = makeStoredDocKey("deleteWithMeta");
    auto poisonedCas = std::numeric_limits<int64_t>::max() & ~0xffffull;
    auto item = makeDeletedItem(key);
    item->setBySeqno(1);
    item->setCas(poisonedCas);

    // deleteWithMeta succeeds as invalid cas is ignored
    EXPECT_EQ(cb::engine_errc::success,
              testDcpReplicationWithInvalidCas(std::move(item)));
    EXPECT_EQ(1, store->getEPEngine().getEpStats().numInvalidCas);
    EXPECT_EQ(0, store->getEPEngine().getEpStats().numCasRegenerated);

    vb->notifyReplication();

    EXPECT_EQ(0, vb->getNumItems());
    EXPECT_EQ(1, vb->ht.getNumItems());

    auto prepareSeqno = 1;
    uint64_t cas;
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::CommittedViaMutation,
                  sv.storedValue->getCommitted());
        ASSERT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
        cas = sv.storedValue->getCas();
    }
    // item is stored with initial poisoned cas
    EXPECT_EQ(poisonedCas, cas);
}

// Verify behaviour for DCP deleteWithMeta with dcp_hlc_invalid_strategy=replace
TEST_P(PassiveStreamHlcInvalidStrategyTest,
       dcpDeleteWithMetaInvalidCasReplace) {
    config_string += ";dcp_hlc_invalid_strategy=replace";
    reinitialise(config_string);
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto vb = engine->getVBucket(vbid);

    // create deleted item with poisoned cas
    auto key = makeStoredDocKey("deleteWithMeta");
    auto poisonedCas = std::numeric_limits<int64_t>::max() & ~0xffffull;
    auto item = makeDeletedItem(key);
    item->setBySeqno(1);
    item->setCas(poisonedCas);

    // deleteWithMeta succeeds as the invalid cas is regenerated
    EXPECT_EQ(cb::engine_errc::success,
              testDcpReplicationWithInvalidCas(std::move(item)));
    EXPECT_EQ(1, store->getEPEngine().getEpStats().numInvalidCas);
    EXPECT_EQ(1, store->getEPEngine().getEpStats().numCasRegenerated);

    vb->notifyReplication();

    EXPECT_EQ(0, vb->getNumItems());
    EXPECT_EQ(1, vb->ht.getNumItems());

    auto prepareSeqno = 1;
    uint64_t cas;
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::CommittedViaMutation,
                  sv.storedValue->getCommitted());
        ASSERT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
        cas = sv.storedValue->getCas();
    }
    // item is stored with a new generated cas value
    EXPECT_LT(cas, poisonedCas);
}

// Verify behaviour for DCP delteWithMeta with dcp_hlc_invalid_strategy=error
TEST_P(PassiveStreamHlcInvalidStrategyTest, dcpDeleteWithMetaInvalidCasError) {
    config_string += ";dcp_hlc_invalid_strategy=error";
    reinitialise(config_string);
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto vb = engine->getVBucket(vbid);

    // create deleted item with poisoned cas
    auto key = makeStoredDocKey("deleteWithMeta");
    auto poisonedCas = std::numeric_limits<int64_t>::max() & ~0xffffull;
    auto item = makeDeletedItem(key);
    item->setBySeqno(1);
    item->setCas(poisonedCas);

    // deleteWithMeta fails due to invalid cas
    EXPECT_EQ(cb::engine_errc::cas_value_invalid,
              testDcpReplicationWithInvalidCas(std::move(item)));
    EXPECT_EQ(1, store->getEPEngine().getEpStats().numInvalidCas);
    EXPECT_EQ(0, store->getEPEngine().getEpStats().numCasRegenerated);
}

// Verify behaviour for DCP prepare with dcp_hlc_invalid_strategy=ignore
TEST_P(PassiveStreamHlcInvalidStrategyTest, dcpPrepareWithInvalidCasIgnore) {
    auto vb = engine->getVBucket(vbid);

    // create pending item with poisoned cas
    using namespace cb::durability;
    auto key = makeStoredDocKey("prepare");
    auto poisonedCas = std::numeric_limits<int64_t>::max() & ~0xffffull;
    auto item = makePendingItem(
            key, "value", {Level::Majority, Timeout::Infinity()});
    item->setBySeqno(1);
    item->setCas(poisonedCas);

    // Prepare succeeds as invalid cas is ignored
    EXPECT_EQ(cb::engine_errc::success,
              testDcpReplicationWithInvalidCas(std::move(item)));
    EXPECT_EQ(1, store->getEPEngine().getEpStats().numInvalidCas);
    EXPECT_EQ(0, store->getEPEngine().getEpStats().numCasRegenerated);

    vb->notifyReplication();
    // We don't account Prepares in VB stats
    EXPECT_EQ(0, vb->getNumItems());
    // We do in HT stats
    EXPECT_EQ(1, vb->ht.getNumItems());

    auto prepareSeqno = 1;
    uint64_t cas;
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::Pending, sv.storedValue->getCommitted());
        ASSERT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
        cas = sv.storedValue->getCas();
    }
    // item is stored with initial poisoned cas
    EXPECT_EQ(poisonedCas, cas);
}

// Verify behaviour for DCP prepare with dcp_hlc_invalid_strategy=replace
TEST_P(PassiveStreamHlcInvalidStrategyTest, dcpPrepareWithInvalidCasReplace) {
    config_string += ";dcp_hlc_invalid_strategy=replace";
    reinitialise(config_string);
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto vb = engine->getVBucket(vbid);

    // create pending item with poisoned cas
    using namespace cb::durability;
    auto key = makeStoredDocKey("prepare");
    auto poisonedCas = std::numeric_limits<int64_t>::max() & ~0xffffull;
    auto item = makePendingItem(
            key, "value", {Level::Majority, Timeout::Infinity()});
    item->setBySeqno(1);
    item->setCas(poisonedCas);

    // Prepare succeeds as the invalid cas is regenerated
    EXPECT_EQ(cb::engine_errc::success,
              testDcpReplicationWithInvalidCas(std::move(item)));
    EXPECT_EQ(1, store->getEPEngine().getEpStats().numInvalidCas);
    EXPECT_EQ(1, store->getEPEngine().getEpStats().numCasRegenerated);

    vb->notifyReplication();
    // We don't account Prepares in VB stats
    EXPECT_EQ(0, vb->getNumItems());
    // We do in HT stats
    EXPECT_EQ(1, vb->ht.getNumItems());

    auto prepareSeqno = 1;
    uint64_t cas;
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::Pending, sv.storedValue->getCommitted());
        ASSERT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
        cas = sv.storedValue->getCas();
    }
    // item is stored with a new generated cas value
    EXPECT_LT(cas, poisonedCas);
}

// Verify behaviour for DCP prepare with dcp_hlc_invalid_strategy=error
TEST_P(PassiveStreamHlcInvalidStrategyTest, dcpPrepareWithInvalidCasError) {
    config_string += ";dcp_hlc_invalid_strategy=error";
    reinitialise(config_string);
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto vb = engine->getVBucket(vbid);

    // create pending item with poisoned cas
    using namespace cb::durability;
    auto key = makeStoredDocKey("prepare");
    auto poisonedCas = std::numeric_limits<int64_t>::max() & ~0xffffull;
    auto item = makePendingItem(
            key, "value", {Level::Majority, Timeout::Infinity()});
    item->setBySeqno(1);
    item->setCas(poisonedCas);

    // Prepare fails due to invalid cas
    EXPECT_EQ(cb::engine_errc::cas_value_invalid,
              testDcpReplicationWithInvalidCas(std::move(item)));
    EXPECT_EQ(1, store->getEPEngine().getEpStats().numInvalidCas);
    EXPECT_EQ(0, store->getEPEngine().getEpStats().numCasRegenerated);
}

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         PassiveStreamHlcInvalidStrategyTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
