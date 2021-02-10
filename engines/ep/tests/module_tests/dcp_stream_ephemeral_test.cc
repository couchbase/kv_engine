/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc
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

#include "dcp_stream_ephemeral_test.h"

#include "ephemeral_bucket.h"
#include "ephemeral_vb.h"

#include "../mock/mock_dcp_consumer.h"

void STPassiveStreamEphemeralTest::SetUp() {
    config_string = "ephemeral_metadata_purge_age=0";
    SingleThreadedPassiveStreamTest::SetUp();
}

/**
 * The test verifies that the SeqList::numDeletedItems counter is correctly
 * incremented when a Replica vbucket receives multiple deletions for the same
 * key in a row. The test verifies both normal and sync deletions.
 */
void STPassiveStreamEphemeralTest::test_MB_44139(
        const std::optional<cb::durability::Requirements>& durReqs) {
    // Test relies on that the HTCleaner does its work when it runs
    ASSERT_EQ(0, engine->getConfiguration().getEphemeralMetadataPurgeAge());

    auto& vb = dynamic_cast<EphemeralVBucket&>(*store->getVBucket(vbid));
    ASSERT_EQ(0, vb.getHighSeqno());
    ASSERT_EQ(0, vb.getSeqListNumItems());
    ASSERT_EQ(0, vb.getSeqListNumDeletedItems());

    // Receive marker for a Disk snapshot
    // Note: For the SyncDel test replica receives a single Snap{Disk, 1, 5},
    // while for the NormalDel test Snap{Disk, 1, 2} + Snap{Memory, 3, 5}
    const uint32_t opaque = 1;
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       1 /*start*/,
                                       durReqs ? 5 : 2 /*end*/,
                                       MARKER_FLAG_CHK | MARKER_FLAG_DISK,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));

    const auto keyA = DocKey("keyA", DocKeyEncodesCollectionId::No);
    if (durReqs) {
        // Receive SyncDelete:1
        const std::string value("value");
        cb::const_byte_buffer valueBuf{
                reinterpret_cast<const uint8_t*>(value.data()), value.size()};
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->prepare(opaque,
                                    keyA,
                                    valueBuf,
                                    0 /*priv_bytes*/,
                                    PROTOCOL_BINARY_RAW_BYTES,
                                    0 /*cas*/,
                                    vbid,
                                    0 /*flags*/,
                                    1 /*bySeqno*/,
                                    0 /*revSeqno*/,
                                    0 /*exp*/,
                                    0 /*lockTime*/,
                                    0 /*nru*/,
                                    DocumentState::Deleted,
                                    durReqs->getLevel()));
        EXPECT_EQ(1, vb.getHighSeqno());
        EXPECT_EQ(1, vb.getSeqListNumItems());
        EXPECT_EQ(1, vb.getSeqListNumDeletedItems());
    } else {
        // Receive DEL:2
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->deletion(opaque,
                                     keyA,
                                     {} /*value*/,
                                     0 /*priv_bytes*/,
                                     PROTOCOL_BINARY_RAW_BYTES,
                                     0 /*cas*/,
                                     vbid,
                                     2 /*seqno*/,
                                     0 /*revSeqno*/,
                                     {} /*meta*/));
        EXPECT_EQ(2, vb.getHighSeqno());
        EXPECT_EQ(1, vb.getSeqListNumItems());
        EXPECT_EQ(1, vb.getSeqListNumDeletedItems());
    }

    // Note: In the NormalDel test replica receives 2 DELs in a row, so by logic
    // the second one must be in a second snapshot.
    if (!durReqs) {
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->snapshotMarker(opaque,
                                           vbid,
                                           3 /*start*/,
                                           5 /*end*/,
                                           MARKER_FLAG_CHK | MARKER_FLAG_MEMORY,
                                           {} /*HCS*/,
                                           {} /*maxVisibleSeqno*/));
    }

    // Receive DEL:4 while there is a range-read in place (eg, TombstonePurger
    // is running).
    // Note: For replica is legal to receive 2 DELs in a row for the same
    // key, as mutations in the middle may have been deduplicated
    {
        const auto range = vb.makeRangeIterator(false /*backfill*/);

        EXPECT_EQ(cb::engine_errc::success,
                  consumer->deletion(opaque,
                                     keyA,
                                     {} /*value*/,
                                     0 /*priv_bytes*/,
                                     PROTOCOL_BINARY_RAW_BYTES,
                                     0 /*cas*/,
                                     vbid,
                                     4 /*seqno*/,
                                     0 /*revSeqno*/,
                                     {} /*meta*/));
    }

    EXPECT_EQ(4, vb.getHighSeqno());
    EXPECT_EQ(2, vb.getSeqListNumItems());
    // Core check: Before the fix num-deleted-items stays 1.
    EXPECT_EQ(2, vb.getSeqListNumDeletedItems());

    // Receive MUT:5 for a different key.
    // This step is necessary for the TombstonePurger to touch DEL:4, as it
    // would skip it if it's the latest item in the SeqList
    const auto keyB = DocKey("keyB", DocKeyEncodesCollectionId::No);
    const std::string value("value");
    cb::const_byte_buffer valueBuf{
            reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 keyB,
                                 valueBuf,
                                 0,
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 5 /*seqno*/,
                                 0,
                                 0,
                                 0,
                                 {},
                                 0));
    EXPECT_EQ(5, vb.getHighSeqno());
    EXPECT_EQ(3, vb.getSeqListNumItems());
    EXPECT_EQ(2, vb.getSeqListNumDeletedItems());

    // Before the fix this throws as we try to decrement num-deleted-item to -1
    runEphemeralHTCleaner();

    EXPECT_EQ(5, vb.getHighSeqno());
    EXPECT_EQ(1, vb.getSeqListNumItems());
    EXPECT_EQ(0, vb.getSeqListNumDeletedItems());
}

TEST_P(STPassiveStreamEphemeralTest, MB_44139_NormalDel) {
    test_MB_44139({});
}

TEST_P(STPassiveStreamEphemeralTest, MB_44139_SyncDel) {
    test_MB_44139(cb::durability::Requirements());
}

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         STPassiveStreamEphemeralTest,
                         STParameterizedBucketTest::ephConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);