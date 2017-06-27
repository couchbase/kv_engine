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

#include "ephemeral_bucket_test.h"

#include "test_helpers.h"

#include "../mock/mock_dcp_consumer.h"
#include "dcp/dcpconnmap.h"
/*
 * Test statistics related to an individual VBucket's sequence list.
 */

void EphemeralBucketStatTest::addDocumentsForSeqListTesting(uint16_t vb) {
    // Add some documents to the vBucket to use to test the stats.
    store_item(vb, makeStoredDocKey("deleted"), "value");
    delete_item(vb, makeStoredDocKey("deleted"));
    store_item(vb, makeStoredDocKey("doc"), "value");
    store_item(vb, makeStoredDocKey("doc"), "value 2");
}

TEST_F(EphemeralBucketStatTest, VBSeqlistStats) {
    // Check preconditions.
    auto stats = get_stat("vbucket-details 0");
    ASSERT_EQ("0", stats.at("vb_0:seqlist_high_seqno"));

    // Add some documents to the vBucket to use to test the stats.
    addDocumentsForSeqListTesting(vbid);

    stats = get_stat("vbucket-details 0");

    EXPECT_EQ("0", stats.at("vb_0:auto_delete_count"));
    EXPECT_EQ("2", stats.at("vb_0:seqlist_count"))
        << "Expected both current and deleted documents";
    EXPECT_EQ("1", stats.at("vb_0:seqlist_deleted_count"));
    EXPECT_EQ("4", stats.at("vb_0:seqlist_high_seqno"));
    EXPECT_EQ("4", stats.at("vb_0:seqlist_highest_deduped_seqno"));
    EXPECT_EQ("0", stats.at("vb_0:seqlist_range_read_begin"));
    EXPECT_EQ("0", stats.at("vb_0:seqlist_range_read_end"));
    EXPECT_EQ("0", stats.at("vb_0:seqlist_range_read_count"));
    EXPECT_EQ("0", stats.at("vb_0:seqlist_stale_count"));
    EXPECT_EQ("0", stats.at("vb_0:seqlist_stale_value_bytes"));
    EXPECT_EQ("0", stats.at("vb_0:seqlist_stale_metadata_bytes"));

    // Trigger the "automatic" deletion of an item by paging it out.
    auto vb = store->getVBucket(vbid);
    auto key = makeStoredDocKey("doc");
    auto lock = vb->ht.getLockedBucket(key);
    auto* value = vb->fetchValidValue(
            lock, key, WantsDeleted::No, TrackReference::Yes, QueueExpired::No);
    ASSERT_TRUE(vb->pageOut(lock, value));

    stats = get_stat("vbucket-details 0");
    EXPECT_EQ("1", stats.at("vb_0:auto_delete_count"));
    EXPECT_EQ("2", stats.at("vb_0:seqlist_deleted_count"));
    EXPECT_EQ("5", stats.at("vb_0:seqlist_high_seqno"));
}

TEST_F(SingleThreadedEphemeralBackfillTest, RangeIteratorVBDeleteRaceTest) {
    /* The destructor of RangeIterator attempts to release locks in the
     * seqList, which is owned by the Ephemeral VB. If the evb is
     * destructed before the iterator, unexepected behaviour will arise.
     * In MB-24631 the destructor spun trying to acquire a lock which
     * was now garbage data after the memory was reused.
     *
     * Due to the variable results of this, the test alone does not
     * confirm the absence of this issue, but AddressSanitizer should
     * report heap-use-after-free.
     */

    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());

    // prep data
    store_item(vbid, makeStoredDocKey("key1"), "value");
    store_item(vbid, makeStoredDocKey("key2"), "value");

    auto& ckpt_mgr = vb->checkpointManager;
    ASSERT_EQ(1, ckpt_mgr.getNumCheckpoints());

    // make checkpoint to cause backfill later rather than straight to in-memory
    ckpt_mgr.createNewCheckpoint();
    bool new_ckpt_created;
    ASSERT_EQ(2, ckpt_mgr.removeClosedUnrefCheckpoints(*vb, new_ckpt_created));

    // Create a Mock Dcp producer
    mock_dcp_producer_t producer = new MockDcpProducer(*engine,
                                                       cookie,
                                                       "test_producer",
                                                       /*flags*/ 0,
                                                       {/*no json*/});
    // Create a Mock Active Stream
    stream_t stream = new MockActiveStream(
            static_cast<EventuallyPersistentEngine*>(engine.get()),
            producer,
            /*flags*/ 0,
            /*opaque*/ 0,
            *vb,
            /*st_seqno*/ 0,
            /*en_seqno*/ ~0,
            /*vb_uuid*/ 0xabcd,
            /*snap_start_seqno*/ 0,
            /*snap_end_seqno*/ ~0,
            IncludeValue::Yes,
            IncludeXattrs::Yes);

    MockActiveStream* mock_stream =
            static_cast<MockActiveStream*>(stream.get());

    ASSERT_TRUE(mock_stream->isPending()) << "stream state should be Pending";

    mock_stream->transitionStateToBackfilling();

    ASSERT_TRUE(mock_stream->isBackfilling())
            << "stream state should have transitioned to Backfilling";

    size_t byteLimit = engine->getConfiguration().getDcpScanByteLimit();

    auto& manager = producer->getBFM();

    /* Hack to make DCPBackfillMemoryBuffered::create construct the range
     * iterator, but DCPBackfillMemoryBuffered::scan /not/ complete the
     * backfill immediately - we pretend the buffer is full. This is
     * reset in manager->backfill() */
    manager.bytesCheckAndRead(byteLimit + 1);

    // Directly run backfill once, to create the range iterator
    manager.backfill();

    const char* vbDeleteTaskName = "Removing (dead) vb:0 from memory";
    ASSERT_FALSE(
            task_executor->isTaskScheduled(NONIO_TASK_IDX, vbDeleteTaskName));

    /* Bin the vbucket. This will eventually lead to the destruction of
     * the seqList. If the vb were to be destroyed *now*,
     * AddressSanitizer would report heap-use-after-free when the
     * DCPBackfillMemoryBuffered is destroyed (it owns a range iterator)
     * This should no longer happen, as the backfill now hold a
     * shared_ptr to the evb.
     */
    store->deleteVBucket(vbid, nullptr);
    vb.reset();

    // vb can't yet be deleted, there is a range iterator over it still!
    EXPECT_FALSE(
            task_executor->isTaskScheduled(NONIO_TASK_IDX, vbDeleteTaskName));

    // Now bin the producer
    engine->getDcpConnMap().shutdownAllConnections();
    stream.reset();
    producer.reset();

    // run the backfill task so the backfill can reach state
    // backfill_finished and be destroyed destroying the range iterator
    // in the process
    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");

    // Now the backfill is gone, the evb can be deleted
    EXPECT_TRUE(
            task_executor->isTaskScheduled(NONIO_TASK_IDX, vbDeleteTaskName));
}
