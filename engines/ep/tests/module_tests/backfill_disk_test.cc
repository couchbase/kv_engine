/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
 * Tests for DCPBackfillDisk class.
 */

#include "dcp/backfill-manager.h"
#include "dcp/response.h"
#include "evp_store_single_threaded_test.h"
#include "test_helpers.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_kvstore.h"
#include "tests/mock/mock_stream.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/vbucket_utils.h"

#include <kv_bucket.h>

using namespace ::testing;

/// Test fixture for DCPBackfillDisk class tests.
class DCPBackfillDiskTest : public SingleThreadedEPBucketTest {
protected:
    void backfillGetDriver(IncludeValue incVal,
                           IncludeXattrs incXattr,
                           IncludeDeletedUserXattrs incDeletedXattr,
                           int expectedGetCalls);

    void testDiskBackfillHoldsVBStateLock();
};

/**
 * Regression test for MB-47790 - if a backfill fails during the scan() phase
 * due to disk issues, the stream should be closed (and not left stuck at
 * the last read seqno).
 */
TEST_F(DCPBackfillDiskTest, ScanDiskError) {
    // Store an items, create new checkpoint and flush so we have something to
    // backfill from disk
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    store_item(vbid, makeStoredDocKey("key1"), "value");
    flushAndRemoveCheckpoints(vbid);

    // Setup expectations on mock KVStore - expect to initialise the scan
    // context, then a scan() call which we cause to fail, followed by destroy
    // of scan context.
    auto& mockKVStore = MockKVStore::replaceRWKVStoreWithMock(*store, 0);
    EXPECT_CALL(mockKVStore, initBySeqnoScanContext(_, _, _, _, _, _, _, _))
            .Times(1);
    EXPECT_CALL(mockKVStore, scan(An<BySeqnoScanContext&>()))
            .WillOnce(Return(ScanStatus::Failed));

    // Create producer now we have items only on disk.
    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test-producer", 0 /*flags*/, false /*startTask*/);

    auto stream =
            std::make_shared<MockActiveStream>(engine.get(),
                                               producer,
                                               DCP_ADD_STREAM_FLAG_DISKONLY,
                                               0,
                                               *engine->getVBucket(vbid),
                                               0,
                                               1,
                                               0,
                                               0,
                                               0,
                                               IncludeValue::Yes,
                                               IncludeXattrs::Yes,
                                               IncludeDeletedUserXattrs::No,
                                               std::string{});
    stream->setActive();
    ASSERT_TRUE(stream->isBackfilling());

    // Initialise the backfill of this VBucket (performs initial scan but
    // doesn't read any data yet).
    auto& bfm = producer->getBFM();
    ASSERT_EQ(backfill_success, bfm.backfill());
    auto backfillRemaining = stream->getNumBackfillItemsRemaining();
    ASSERT_TRUE(backfillRemaining);

    // Test - run backfill scan step. Backfill should fail early as scan()
    // has been configured to return scan_failed.
    bfm.backfill();

    // Verify state - stream should have been marked dead, and EndStreamResponse
    // added to ready queue indicating the disk backfill failed.
    EXPECT_FALSE(stream->isActive());
    ASSERT_EQ(1, stream->public_readyQSize());
    auto response = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::StreamEnd, response->getEvent());
    auto* streamEndResp = dynamic_cast<StreamEndResponse*>(response.get());
    ASSERT_NE(nullptr, streamEndResp);
    EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::BackfillFail,
              streamEndResp->getFlags());

    // Replace the MockKVStore with the real one so we can tidy up correctly
    MockKVStore::restoreOriginalRWKVStore(*store);
}

void DCPBackfillDiskTest::backfillGetDriver(
        IncludeValue incVal,
        IncludeXattrs incXattr,
        IncludeDeletedUserXattrs incDeletedXattr,
        int expectedGetCalls) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // Create item, checkpoint and flush so item is to be backfilled from disk
    store_item(vbid, makeStoredDocKey("key"), "value");
    flushAndRemoveCheckpoints(vbid);

    // Set up and define expectations for the hook that is called in the body of
    // CacheCallback::get(), i.e., when an item's value is retrieved from cache
    auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    testing::StrictMock<testing::MockFunction<void()>> getInternalHook;
    VBucketTestIntrospector::setIsCalledHook(vb,
                                             getInternalHook.AsStdFunction());
    EXPECT_CALL(getInternalHook, Call()).Times(expectedGetCalls);

    // Items now only on disk, create producer
    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test-producer", 0 /*flags*/, false /*startTask*/);

    auto stream = std::make_shared<MockActiveStream>(
            engine.get(),
            producer,
            0, /* flags */
            0, /* opaque */
            vb, /* vbucket */
            0, /* start seqNo */
            1, /* end seqNo */
            0, /* vbucket uuid */
            0, /* snapshot start seqNo */
            0, /* snapshot end seqNo */
            incVal, /* includeValue */
            incXattr, /* includeXattrs */
            incDeletedXattr, /* includeDeletedUserXattrs */
            std::string{}); /* jsonFilter */

    stream->setActive();
    ASSERT_TRUE(stream->isBackfilling());

    auto& bfm = producer->getBFM();
    ASSERT_EQ(backfill_success, bfm.backfill()); // initialize backfill

    auto backfillRemaining = stream->getNumBackfillItemsRemaining();
    ASSERT_TRUE(backfillRemaining);

    ASSERT_EQ(2, stream->public_readyQSize());

    auto response = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, response->getEvent());

    // Ensure an item mutation is in the DCP stream
    response = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::Mutation, response->getEvent());

    // Ensure this item has the correct key, and value (if IncludeValue::Yes)
    MutationResponse mutResponse = dynamic_cast<MutationResponse&>(*response);
    SingleThreadedRCPtr item = mutResponse.getItem();
    EXPECT_EQ(item->getKey(), makeStoredDocKey("key"));

    if (!(stream->isKeyOnly())) {
        EXPECT_EQ(item->getValue()->to_s(), "value");
    }
}

void DCPBackfillDiskTest::testDiskBackfillHoldsVBStateLock() {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // Create item which expires in 10s, checkpoint and flush so item is to be
    // backfilled from disk
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value");
    flushAndRemoveCheckpoints(vbid);

    auto& vb = *engine->getKVBucket()->getVBucket(vbid);

    // Set up and define expectations for the hook that is called in the body of
    // CacheCallback::get(), i.e., when an item's value is retrieved from cache
    testing::StrictMock<testing::MockFunction<void()>> getInternalHook;
    VBucketTestIntrospector::setIsCalledHook(vb,
                                             getInternalHook.AsStdFunction());
    EXPECT_CALL(getInternalHook, Call()).Times(1);

    // Set up a hook to run before we expire items and check whether an
    // exclusive lock can be obtained on the vbucket state.
    VBucketTestIntrospector::setFetchValidValueHook(
            vb, {[&](folly::SharedMutex& vbStateLock) {
                bool didManageToLock = vb.getStateLock().try_lock();
                if (didManageToLock) {
                    vb.getStateLock().unlock();
                }
                EXPECT_FALSE(didManageToLock);
            }});

    // Items now only on disk, create producer
    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test-producer", 0 /*flags*/, false /*startTask*/);

    auto stream = std::make_shared<MockActiveStream>(
            engine.get(),
            producer,
            0, /* flags */
            0, /* opaque */
            vb, /* vbucket */
            0, /* start seqNo */
            1, /* end seqNo */
            0, /* vbucket uuid */
            0, /* snapshot start seqNo */
            0, /* snapshot end seqNo */
            IncludeValue::Yes, /* includeValue */
            IncludeXattrs::No, /* includeXattrs */
            IncludeDeletedUserXattrs::No, /* includeDeletedUserXattrs */
            std::string{}); /* jsonFilter */

    stream->setActive();
    ASSERT_TRUE(stream->isBackfilling());

    auto& bfm = producer->getBFM();
    ASSERT_EQ(backfill_success, bfm.backfill()); // initialize backfill

    auto backfillRemaining = stream->getNumBackfillItemsRemaining();
    ASSERT_TRUE(backfillRemaining);

    ASSERT_EQ(2, stream->public_readyQSize());

    auto response = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, response->getEvent());

    // Ensure an item mutation is in the DCP stream
    response = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::Mutation, response->getEvent());
}

// Tests that the vbstate lock is held during disk backfill.
TEST_F(DCPBackfillDiskTest, DiskBackfillHoldsVBStateLock) {
    testDiskBackfillHoldsVBStateLock();
}

// Tests that CacheCallback::get is never called when a stream is keyOnly.
TEST_F(DCPBackfillDiskTest, KeyOnlyBackfillSkipsScan) {
    DCPBackfillDiskTest::backfillGetDriver(IncludeValue::No,
                                           IncludeXattrs::No,
                                           IncludeDeletedUserXattrs::No,
                                           0);
}

// Complement to KeyOnlyBackfillSkipGet. Other tests already cover all cases,
// but not using the hook. This test validates the hook, and thus
// KeyOnlyBackfillSkipGet itself, is performing correctly and can be trusted.
TEST_F(DCPBackfillDiskTest, ValueBackfillRegressionTest) {
    DCPBackfillDiskTest::backfillGetDriver(IncludeValue::Yes,
                                           IncludeXattrs::Yes,
                                           IncludeDeletedUserXattrs::Yes,
                                           1);
}
