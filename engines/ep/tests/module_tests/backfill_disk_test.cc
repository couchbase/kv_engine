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
#include <kv_bucket.h>

using namespace ::testing;

/// Test fixture for DCPBackfillDisk class tests.
class DCPBackfillDiskTest : public SingleThreadedEPBucketTest {};

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
