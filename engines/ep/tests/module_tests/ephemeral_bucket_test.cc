/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ephemeral_bucket_test.h"

#include <chrono>
#include <utility>

#include "checkpoint_manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/backfill-manager.h"
#include "dcp/backfill_memory.h"
#include "dcp/dcpconnmap.h"
#include "dcp/response.h"
#include "ephemeral_bucket.h"
#include "ephemeral_mem_recovery.h"
#include "ephemeral_tombstone_purger.h"
#include "ephemeral_vb.h"
#include "test_helpers.h"

#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_stream.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include <folly/portability/GMock.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <statistics/labelled_collector.h>
#include <statistics/tests/mock/mock_stat_collector.h>

/*
 * Test statistics related to an individual VBucket's sequence list.
 */

void EphemeralBucketStatTest::addDocumentsForSeqListTesting(Vbid vb) {
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
    EXPECT_EQ("0", stats.at("vb_0:seqlist_range_read_begin"));
    EXPECT_EQ("0", stats.at("vb_0:seqlist_range_read_end"));
    EXPECT_EQ("0", stats.at("vb_0:seqlist_range_read_count"));
    EXPECT_EQ("0", stats.at("vb_0:seqlist_stale_count"));
    EXPECT_EQ("0", stats.at("vb_0:seqlist_stale_value_bytes"));
    EXPECT_EQ("0", stats.at("vb_0:seqlist_stale_metadata_bytes"));

    // Trigger the "automatic" deletion of an item by paging it out.
    auto vb = store->getVBucket(vbid);
    auto key = makeStoredDocKey("doc");

    // Test visitor which pages out our key
    struct Visitor : public HashTableVisitor {
        Visitor(VBucket& vb, StoredDocKey key) : vb(vb), key(std::move(key)) {
        }

        bool visit(const HashTable::HashBucketLock& lh,
                   StoredValue& v) override {
            if (v.getKey() == key) {
                StoredValue* vPtr = &v;
                EXPECT_TRUE(
                        vb.pageOut(vbStateLock, readHandle, lh, vPtr, false));
            }
            return true;
        }

        bool setUpHashBucketVisit() override {
            // Need to lock the vbucket state before collections.
            vbStateLock =
                    std::shared_lock<folly::SharedMutex>(vb.getStateLock());
            // Need to lock collections before we visit each SV.
            readHandle = vb.lockCollections();
            return true;
        }

        void tearDownHashBucketVisit() override {
            readHandle.unlock();
            vbStateLock.unlock();
        }

        VBucket& vb;
        StoredDocKey key;
        std::shared_lock<folly::SharedMutex> vbStateLock;
        Collections::VB::ReadHandle readHandle;
    };

    // Invoke the visitor so the item gets paged out.
    Visitor visitor(*vb, key);
    vb->ht.visit(visitor);

    stats = get_stat("vbucket-details 0");
    EXPECT_EQ("1", stats.at("vb_0:auto_delete_count"));
    EXPECT_EQ("2", stats.at("vb_0:seqlist_deleted_count"));
    EXPECT_EQ("5", stats.at("vb_0:seqlist_high_seqno"));
}

TEST_F(EphemeralBucketStatTest, InctiveMemoryTracking) {
    // test that inactiveHTMemory is correctly updated for
    // inserts/updates/deletes/tombstone removal.
    auto replicaVB = Vbid(0);
    setVBucketStateAndRunPersistTask(replicaVB, vbucket_state_replica);

    auto cookie = create_mock_cookie();

    auto& stats = engine->getEpStats();
    EXPECT_EQ(0, stats.inactiveHTMemory);

    auto key = makeStoredDocKey("item2");

    std::string value = "value";
    auto item = make_item(replicaVB, key, value);

    // Store an item in a replica VB and confirm inactiveHTMemory increases
    item.setCas(1);
    uint64_t seqno;
    ASSERT_EQ(cb::engine_errc::success,
              store->setWithMeta(std::ref(item),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_replica},
                                 CheckConflicts::No,
                                 /*allowExisting*/ true));

    // avoids checking exact values to be resilient to changes (e.g.) in stored
    // value size.
    auto smallItemMem = stats.inactiveHTMemory;
    EXPECT_GT(smallItemMem, 80);

    // Replace the existing item with a _larger_ item and confirm
    // inactiveHTMemory increases further
    std::string largerValue = "valuevaluevaluevaluevaluevalue";
    auto largerItem = make_item(replicaVB, key, largerValue);
    largerItem.setCas(1);
    ASSERT_EQ(cb::engine_errc::success,
              store->setWithMeta(std::ref(largerItem),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_replica},
                                 CheckConflicts::No,
                                 /*allowExisting*/ true));

    auto largerItemMem = smallItemMem + largerValue.size() - value.size();
    EXPECT_EQ(largerItemMem, stats.inactiveHTMemory);

    // Delete the item, confirm inactiveHTMemory decreases (tombstone
    // remains).
    ItemMetaData meta;
    uint64_t cas = 1;
    meta.cas = cas;
    ASSERT_EQ(cb::engine_errc::success,
              store->deleteWithMeta(
                      key,
                      cas,
                      nullptr,
                      replicaVB,
                      cookie,
                      {vbucket_state_replica},
                      CheckConflicts::No,
                      meta,
                      GenerateBySeqno::Yes,
                      GenerateCas::No,
                      store->getVBucket(replicaVB)->getHighSeqno() + 1,
                      nullptr /* extended metadata */,
                      DeleteSource::Explicit,
                      EnforceMemCheck::Yes));

    EXPECT_LT(stats.inactiveHTMemory, largerItemMem);
    EXPECT_GT(stats.inactiveHTMemory, 0);

    // now remove the tombstone and confirm the inactiveHTMemory is now 0
    auto& replica = *store->getVBucket(replicaVB);

    EphemeralVBucket::HTTombstonePurger purger(
            0 /* remove tombstones of any age */);
    purger.setCurrentVBucket(replica);
    replica.ht.visit(purger);

    EXPECT_EQ(0, stats.inactiveHTMemory);

    destroy_mock_cookie(cookie);
}

TEST_F(EphemeralBucketStatTest, InactiveMemoryTrackingNotUpdatedForActive) {
    // inactiveHTMemory should not be updated by storing items in active
    // vbuckets
    auto activeVB = Vbid(0);
    setVBucketStateAndRunPersistTask(activeVB, vbucket_state_active);

    auto& stats = engine->getEpStats();
    EXPECT_EQ(0, stats.inactiveHTMemory);
    EXPECT_EQ(0, stats.inactiveCheckpointOverhead);

    // Confirm inactiveHTMemory is _not_ affected by storing an item to an
    // active vb.
    store_item(activeVB, makeStoredDocKey("item"), "value");
    EXPECT_EQ(0, stats.inactiveHTMemory);
    EXPECT_EQ(0, stats.inactiveCheckpointOverhead);
}

void EphemeralBucketStatTest::inactiveMemoryTrackingTestSetup() {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.inactiveHTMemory);
    ASSERT_EQ(0, stats.inactiveCheckpointOverhead);

    auto key = makeStoredDocKey("item");
    store_item(vbid, key, "value");

    ASSERT_EQ(0, stats.inactiveHTMemory);
    ASSERT_EQ(0, stats.inactiveCheckpointOverhead);
    ASSERT_EQ(1, store->getVBucket(vbid)->getNumItems());

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    // check that the mem usage has gone up by some amount - not
    // checking it is an exact value to avoid a brittle test
    ASSERT_GT(stats.inactiveHTMemory, 0);
    ASSERT_GT(stats.inactiveCheckpointOverhead, 0);
    ASSERT_EQ(1, store->getVBucket(vbid)->getNumItems());
}

TEST_F(EphemeralBucketStatTest, InactiveMemoryTrackingStateChange) {
    {
        CB_SCOPED_TRACE("");
        inactiveMemoryTrackingTestSetup();
    }

    // changing back to active should return inactiveHTMemory to 0
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto& stats = engine->getEpStats();
    EXPECT_EQ(0, stats.inactiveHTMemory);
    EXPECT_EQ(0, stats.inactiveCheckpointOverhead);

    // Now check what happens when we delete a vBucket, we first need to change
    // back to replica though to start tracking memory against the replica
    // counters again.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    // Check that the replica mem usage has gone up by some amount - not
    // checking it is an exact value to avoid a brittle test
    EXPECT_GT(stats.inactiveHTMemory, 80);
    EXPECT_GT(stats.inactiveCheckpointOverhead, 80);

    // Deleting a vBucket should also reset the tracked value.
    EXPECT_EQ(cb::engine_errc::success,
              engine->deleteVBucket(*cookie, vbid, false));

    // Dead vBucket should still be accounted until it is removed
    EXPECT_GT(stats.inactiveHTMemory, 80);
    EXPECT_GT(stats.inactiveCheckpointOverhead, 80);

    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
    runNextTask(lpNonioQ, "Removing (dead) vb:0 from memory");

    EXPECT_EQ(0, stats.inactiveHTMemory);
    EXPECT_EQ(0, stats.inactiveCheckpointOverhead);
}

TEST_F(EphemeralBucketStatTest, InactiveMemoryTrackingStats) {
    {
        CB_SCOPED_TRACE("");
        inactiveMemoryTrackingTestSetup();
    }

    using namespace ::testing;
    using namespace std::literals::string_view_literals;
    // Now check what happens when we delete a vBucket, we first need to change
    // back to replica though to start tracking memory against the replica
    // counters again.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto& stats = engine->getEpStats();

    NiceMock<MockStatCollector> collector;
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("ephemeral_vb_ht_memory_bytes"),
                        Matcher<int64_t>(stats.inactiveHTMemory),
                        Contains(Pair("state"sv, "replica"))));
    EXPECT_CALL(
            collector,
            addStat(StatDefNameMatcher(
                            "ephemeral_vb_checkpoint_memory_overhead_bytes"),
                    Matcher<int64_t>(stats.inactiveCheckpointOverhead),
                    Contains(Pair("state"sv, "replica"))));

    store->getAggregatedVBucketStats(collector.forBucket("foobar"),
                                     cb::prometheus::MetricGroup::High);
}

TEST_F(EphemeralBucketStatTest, AutoDeleteCountResetOnStateChange) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto key = makeStoredDocKey("item");
    auto vbucket = store->getVBucket(vbid);
    ASSERT_TRUE(vbucket);
    auto& vb = dynamic_cast<EphemeralVBucket&>(*vbucket);
    store_item(vbid, key, "value");
    {
        std::shared_lock rlh(vb.getStateLock());
        auto readHandle = vb.lockCollections();

        auto result = vb.ht.findForUpdate(key);

        auto* storedVal = result.committed;
        ASSERT_NE(nullptr, storedVal);
        ASSERT_FALSE(storedVal->isDeleted());

        // Page out the item (once).
        ASSERT_TRUE(
                vb.pageOut(rlh, readHandle, result.getHBL(), storedVal, false));
    }
    ASSERT_EQ(1, vb.getAutoDeleteCount());
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    EXPECT_EQ(0, vb.getAutoDeleteCount());
}

TEST_F(EphemeralBucketStatTest, InactiveMemoryTrackingRollback) {
    {
        CB_SCOPED_TRACE("");
        inactiveMemoryTrackingTestSetup();
    }

    store->rollback(vbid, 0 /* rollbackSeqno */);

    auto& nonIO = *task_executor->getLpTaskQ(TaskType::NonIO);
    runNextTask(nonIO, "Removing (dead) vb:0 from memory");

    // Rollback reset the vBucket and HashTable
    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(0, vb->getNumItems());
    ASSERT_EQ(0, vb->ht.getItemMemory());

    // Now the inactive memory stats should reflect the current state
    auto& stats = engine->getEpStats();
    EXPECT_EQ(0, stats.inactiveHTMemory);
    EXPECT_EQ(vb->checkpointManager->getMemOverhead(),
              stats.inactiveCheckpointOverhead);
}

TEST_F(EphemeralBucketStatTest, InactiveCheckpointMemoryTracking) {
    // test that inactiveCheckpointOverhead is correctly updated
    auto replicaVB = Vbid(0);
    setVBucketStateAndRunPersistTask(replicaVB, vbucket_state_replica);

    auto cookie = create_mock_cookie();

    auto& replica = *store->getVBucket(replicaVB);
    auto& cpm = *replica.checkpointManager;

    // remove the checkpoint containing the set vbstate to get a clean
    // baseline memory usage
    cpm.createNewCheckpoint();

    auto& stats = engine->getEpStats();
    const auto initialMem = stats.inactiveCheckpointOverhead;

    const auto keyA = makeStoredDocKey("itemA");
    const auto keyB = makeStoredDocKey("itemB");

    const std::string value = "value";
    auto item1 = make_item(replicaVB, keyA, value);

    // Store an item in a replica VB and confirm inactiveCheckpointOverhead
    // increases
    item1.setCas(1);
    uint64_t seqno;
    ASSERT_EQ(cb::engine_errc::success,
              store->setWithMeta(std::ref(item1),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_replica},
                                 CheckConflicts::No,
                                 /*allowExisting*/ true));

    // avoids checking exact values to be resilient to changes (e.g.) in stored
    // value size.
    const auto item1Mem = stats.inactiveCheckpointOverhead;
    EXPECT_GT(item1Mem, initialMem + 20);

    // Store the item again and confirm inactiveCheckpointOverhead
    // _does not increase_. This matches existing checkpoint memory tracking;
    // in the event of an existing item, checkpoint memory usage is _not_
    // adjusted, even though the old and new item could be of different sizes
    const std::string largerValue = "valuevaluevaluevaluevaluevaluevaluevalue";
    auto item2 = make_item(replicaVB, keyA, value);
    item2.setCas(1);
    ASSERT_EQ(cb::engine_errc::success,
              store->setWithMeta(std::ref(item2),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_replica},
                                 CheckConflicts::No,
                                 /*allowExisting*/ true));
    // tracked memory unchanged
    EXPECT_EQ(item1Mem, stats.inactiveCheckpointOverhead);

    // Store an item with a different key, confirm checkpoint mem increases
    auto item3 = make_item(replicaVB, keyB, value);
    item3.setCas(1);
    ASSERT_EQ(cb::engine_errc::success,
              store->setWithMeta(std::ref(item3),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_replica},
                                 CheckConflicts::No,
                                 /*allowExisting*/ true));

    const auto item3Mem = stats.inactiveCheckpointOverhead;
    EXPECT_GT(item3Mem, item1Mem);

    // now remove the checkpoint and confirm the inactiveCheckpointOverhead is
    // now back to the initial value.
    cpm.createNewCheckpoint();

    EXPECT_EQ(initialMem, stats.inactiveCheckpointOverhead);

    destroy_mock_cookie(cookie);
}

TEST_F(SingleThreadedEphemeralTest, RangeIteratorVBDeleteRaceTest) {
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

    auto& ckpt_mgr =
            *(static_cast<MockCheckpointManager*>(vb->checkpointManager.get()));
    ASSERT_EQ(1, ckpt_mgr.getNumCheckpoints());

    // make checkpoint to cause backfill later rather than straight to in-memory
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.itemsRemovedFromCheckpoints);
    ckpt_mgr.createNewCheckpoint();
    ASSERT_EQ(4, stats.itemsRemovedFromCheckpoints);

    // Create a Mock Dcp producer
    const std::string testName("test_producer");
    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, testName, cb::mcbp::DcpOpenFlag::None);

    // Since we are creating a mock active stream outside of
    // DcpProducer::streamRequest(), and we want the checkpt processor task,
    // create it explicitly here
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    // Create a Mock Active Stream
    auto mock_stream = std::make_shared<MockActiveStream>(
            static_cast<EventuallyPersistentEngine*>(engine.get()),
            producer,
            /*flags*/ cb::mcbp::DcpAddStreamFlag::None,
            /*opaque*/ 0,
            *vb,
            /*st_seqno*/ 0,
            /*en_seqno*/ ~0,
            /*vb_uuid*/ 0xabcd,
            /*snap_start_seqno*/ 0,
            /*snap_end_seqno*/ ~0,
            IncludeValue::Yes,
            IncludeXattrs::Yes);

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
            task_executor->isTaskScheduled(TaskType::NonIO, vbDeleteTaskName));

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
            task_executor->isTaskScheduled(TaskType::NonIO, vbDeleteTaskName));

    // Now bin the producer
    producer->cancelCheckpointCreatorTask();
    /* Checkpoint processor task finishes up and releases its producer
       reference */
    auto& lpNonIoQ = *task_executor->getLpTaskQ(TaskType::NonIO);
    runNextTask(lpNonIoQ, "Process checkpoint(s) for DCP producer " + testName);

    engine->getDcpConnMap().shutdownAllConnections();
    mock_stream.reset();
    producer.reset();

    // run the backfill task so the backfill can reach state
    // backfill_finished and be destroyed destroying the range iterator
    // in the process
    auto& lpAuxioQ = *task_executor->getLpTaskQ(TaskType::AuxIO);
    runNextTask(lpAuxioQ, "Backfilling items for MockDcpBackfillManager");

    // Now the backfill is gone, the evb can be deleted
    EXPECT_TRUE(
            task_executor->isTaskScheduled(TaskType::NonIO, vbDeleteTaskName));
}

TEST_F(SingleThreadedEphemeralTest, Commit_RangeRead) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // prepare:1 + commit:2
    auto key = makeStoredDocKey("key");
    store_item(vbid,
               key,
               "value",
               0 /*exptime*/,
               {cb::engine_errc::sync_write_pending},
               PROTOCOL_BINARY_RAW_BYTES,
               cb::durability::Requirements(),
               false /*deleted*/);
    auto& vb = *store->getVBuckets().getBucket(vbid);
    auto& ht = vb.ht;
    {
        auto res = ht.findForUpdate(key);
        ASSERT_TRUE(res.pending);
        ASSERT_EQ(1, res.pending->getBySeqno());
        ASSERT_FALSE(res.committed);
    }
    {
        std::shared_lock rlh(vb.getStateLock());
        ASSERT_EQ(cb::engine_errc::success,
                  vb.commit(rlh,
                            key,
                            1,
                            {},
                            CommitType::Majority,
                            vb.lockCollections(key)));
    }
    {
        auto res = ht.findForUpdate(key);
        ASSERT_TRUE(res.pending);
        ASSERT_EQ(1, res.pending->getBySeqno());
        ASSERT_EQ(CommittedState::PrepareCommitted,
                  res.pending->getCommitted());
        ASSERT_TRUE(res.committed);
        ASSERT_EQ(2, res.committed->getBySeqno());
        ASSERT_EQ(CommittedState::CommittedViaPrepare,
                  res.committed->getCommitted());
    }

    // Prepare:3
    store_item(vbid,
               key,
               "value",
               0 /*exptime*/,
               {cb::engine_errc::sync_write_pending},
               PROTOCOL_BINARY_RAW_BYTES,
               cb::durability::Requirements(),
               false /*deleted*/);
    {
        auto res = ht.findForUpdate(key);
        ASSERT_TRUE(res.pending);
        ASSERT_EQ(3, res.pending->getBySeqno());
        ASSERT_TRUE(res.committed);
        ASSERT_EQ(2, res.committed->getBySeqno());
    }

    /*
     * Simulate a stream-req that ends up in a backfill
     */

    // Remove all the closed checkpoints to cause a backfill
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(4, stats.itemsRemovedFromCheckpoints);
    auto& manager =
            *(static_cast<MockCheckpointManager*>(vb.checkpointManager.get()));
    manager.createNewCheckpoint();
    ASSERT_EQ(7, stats.itemsRemovedFromCheckpoints);

    // Create producer and stream, enable SyncRepl
    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test_producer", cb::mcbp::DcpOpenFlag::None);
    producer->setSyncReplication(SyncReplication::SyncReplication);
    auto stream = std::make_shared<MockActiveStream>(
            static_cast<EventuallyPersistentEngine*>(engine.get()),
            producer,
            cb::mcbp::DcpAddStreamFlag::None,
            /*opaque*/ 0,
            vb,
            /*st_seqno*/ 0,
            /*en_seqno*/ ~0,
            /*vb_uuid*/ 0xabcd,
            /*snap_start_seqno*/ 0,
            /*snap_end_seqno*/ ~0,
            IncludeValue::Yes,
            IncludeXattrs::Yes);
    ASSERT_TRUE(stream->public_supportSyncReplication());
    ASSERT_TRUE(stream->isPending()) << "Stream state should be Pending";
    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling())
            << "Stream state should be Backfilling";

    // Manually drive a backfill
    auto& bfMgr = producer->getBFM();
    // Create the range iterator
    ASSERT_EQ(backfill_success, bfMgr.backfill());
    // SnapMarker in the readyQ
    auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(3, readyQ.size());
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, readyQ.front()->getEvent());

    {
        std::shared_lock rlh(vb.getStateLock());
        // Commit:4
        ASSERT_EQ(cb::engine_errc::success,
                  vb.commit(rlh,
                            key,
                            3,
                            {},
                            CommitType::Majority,
                            vb.lockCollections(key)));
    }
    {
        auto res = ht.findForUpdate(key);
        ASSERT_TRUE(res.pending);
        ASSERT_EQ(3, res.pending->getBySeqno());
        ASSERT_EQ(CommittedState::PrepareCommitted,
                  res.pending->getCommitted());
        ASSERT_TRUE(res.committed);
        ASSERT_EQ(4, res.committed->getBySeqno());
        ASSERT_EQ(CommittedState::CommittedViaPrepare,
                  res.committed->getCommitted());
    }

    // Verify that the RR snapshot contains only commit:2 and prepare:3 (ie, not
    // prepare:1)
    // Note: Before http://review.couchbase.org/c/kv_engine/+/109841 we would
    //  end up sending also prepare:1, which means same key twice in a snapshot.
    //  Side effect would be (1) breaking deduplication and (2) failing with
    //  QueueDirtyStatus::FailureDuplicateItem status at replica
    ASSERT_EQ(3, readyQ.size());
    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    ASSERT_EQ(2, readyQ.size());
    resp = stream->public_nextQueuedItem(*producer);
    // Note: commit:2 sent as mutation in a backfill snapshot
    ASSERT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    ASSERT_EQ(2, resp->getBySeqno());
    ASSERT_EQ(1, readyQ.size());
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    ASSERT_EQ(3, resp->getBySeqno());
}

TEST_F(SingleThreadedEphemeralTest, SeqListHasHighPreparedSeqno) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto cookie = create_mock_cookie();
    auto& vb = *store->getVBucket(vbid);
    auto key = makeStoredDocKey("key");
    // auto item = make_item(vbid, key, "value");

    store_item(vbid,
               key,
               "value",
               0 /*exptime*/,
               {cb::engine_errc::sync_write_pending},
               PROTOCOL_BINARY_RAW_BYTES,
               cb::durability::Requirements(),
               false /*deleted*/);

    {
        auto& ht = vb.ht;
        auto res = ht.findForUpdate(key);
        ASSERT_TRUE(res.pending);
        ASSERT_EQ(1, res.pending->getBySeqno());
        ASSERT_FALSE(res.committed);
    }

    {
        auto& ephVb = static_cast<EphemeralVBucket&>(vb);
        auto itr = ephVb.makeRangeIterator(true /*backfill*/);
        // HPS updated for a Prepare
        EXPECT_EQ(1, itr->getHighPreparedSeqno());
    }
    destroy_mock_cookie(cookie);
}

TEST_F(SingleThreadedEphemeralTest, no_prepare_snapshot) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    nlohmann::json manifest;
    EXPECT_EQ(cb::engine_errc::not_supported,
              engine->prepare_snapshot(
                      *cookie, vbid, [&manifest](auto& m) { manifest = m; }));
}

void SingleThreadedEphemeralTest::testEphemeralMemRecoverySwitching() {
    auto& ephemeralBucket = dynamic_cast<EphemeralBucket&>(*store);
    auto& config = engine->getConfiguration();
    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);

    // ItemPager only scheduled for auto_delete bucket
    if (!ephemeralFailNewData()) {
        ephemeralBucket.enableItemPager();
    }

    // Enable EphemeralMemRecovery and disable ItemPager periodic execution
    config.setEphemeralMemRecoveryEnabled(true);
    ephemeralBucket.useEphemeralMemRecovery(true);
    ASSERT_EQ(std::chrono::seconds(INT_MAX), getItemPagerWakeTime());
    ASSERT_FALSE(isEphemeralMemRecoveryTaskDead());

    // attempToFreeMemory will wakeup EphemeralMemRecovery
    ephemeralBucket.attemptToFreeMemory();
    runNextTask(lpNonioQ, "Ephemeral Memory Recovery");
    if (ephemeralFailNewData()) {
        // Also wakes up the expiry pager for fail_new_data bucket
        runNextTask(lpNonioQ, "Paging expired items.");
    }

    // Disable EphemeralMemRecovery and enable ItemPager periodic execution
    config.setEphemeralMemRecoveryEnabled(false);
    ephemeralBucket.useEphemeralMemRecovery(false);
    ASSERT_NE(std::chrono::seconds(INT_MAX), getItemPagerWakeTime());
    ASSERT_TRUE(isEphemeralMemRecoveryTaskDead());
    // Cancelling wakes up the task once more to set rescheduled to false
    runNextTask(lpNonioQ, "Ephemeral Memory Recovery");

    ephemeralBucket.attemptToFreeMemory();
    if (ephemeralFailNewData()) {
        // fail_new_data bucket should run expiry pager
        runNextTask(lpNonioQ, "Paging expired items.");
    } else {
        // auto_delete should run item pager
        runNextTask(lpNonioQ, "Paging out items.");
    }
}

TEST_F(SingleThreadedEphemeralTest, useEphemeralMemRecovery) {
    testEphemeralMemRecoverySwitching();
}

TEST_F(SingleThreadedEphemeralTestFailNewData, useEphemeralMemRecovery) {
    testEphemeralMemRecoverySwitching();
}

/**
 * Test that the EphemeralMemRecovery task will run again even if the
 * checkpoint removers complete before the task has run to completion.
 */
TEST_F(SingleThreadedEphemeralTest, earlyCheckpointRemoverCompletion) {
    auto& ephemeralBucket = dynamic_cast<EphemeralBucket&>(*store);
    auto& config = engine->getConfiguration();
    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);

    // Use the EphemeralMemRecovery task.
    ephemeralBucket.useEphemeralMemRecovery(true);

    auto task = getEphemeralMemRecoveryTask();
    ASSERT_TRUE(task);
    ASSERT_FALSE(task->isdead());

    auto numChkRemovers = config.getCheckpointRemoverTaskCount();

    // Set a hook to run within the task's run method after we schedule the chk
    // removers. We will signal the task immediately to simulate the removers
    // completing before the task execution completes.
    task->chkRemoversScheduledHook = [taskPtr = task.get(), numChkRemovers]() {
        for (size_t i = 0; i < numChkRemovers; i++) {
            taskPtr->signal();
        }
    };

    // Set the task to sleep indefinitely after it runs.
    task->updateSleepTime(std::chrono::seconds(INT_MAX));
    // Wakeup using the notifiable task interface.
    task->wakeup();
    // Run the task with manual wakeup to force recovery.
    runNextTask(lpNonioQ, "Ephemeral Memory Recovery");

    // Expect to need to run again soon, since we completed the removers.
    auto nextRunSeconds =
            std::chrono::duration_cast<std::chrono::seconds>(
                    task->getWaketime() - cb::time::steady_clock::now())
                    .count();
    EXPECT_LT(nextRunSeconds, 30);
}

class SingleThreadedEphemeralPurgerTest : public SingleThreadedKVBucketTest {
protected:
    void SetUp() override {
        config_string +=
                "bucket_type=ephemeral;"
                "max_vbuckets=" + std::to_string(numVbs) + ";"
                "ephemeral_metadata_purge_age=0;"
                "ephemeral_metadata_purge_stale_chunk_duration=0";
        SingleThreadedKVBucketTest::SetUp();

        /* Set up 4 vbuckets */
        for (Vbid::id_type vbid = 0; vbid < numVbs; ++vbid) {
            setVBucketStateAndRunPersistTask(Vbid(vbid), vbucket_state_active);
        }

        // 'vbid' used for some durability related test
        setVBucketStateAndRunPersistTask(
                vbid,
                vbucket_state_active,
                {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    }

    void TearDown() override {
        // Destroy before engine.
        stream.reset();
        producer.reset();
        SingleThreadedKVBucketTest::TearDown();
    }

    bool checkAllPurged(uint64_t expPurgeUpto) {
        for (Vbid::id_type vbid = 0; vbid < numVbs; ++vbid) {
            if (store->getVBucket(Vbid(vbid))->getPurgeSeqno() < expPurgeUpto) {
                return false;
            }
        }
        return true;
    }

    EphemeralVBucket& getVBucket(Vbid vbid) {
        return dynamic_cast<EphemeralVBucket&>(*store->getVBucket(vbid));
    }

    std::shared_ptr<MockActiveStream> createStream(Vbid vbid) {
        using namespace cb::mcbp;
        if (!producer) {
            /// Create producer, stream, backfill.
            producer = std::make_shared<MockDcpProducer>(*engine,
                                                         cookie,
                                                         "test_producer",
                                                         DcpOpenFlag::None,
                                                         /*startTask*/ false);
        }
        stream = std::make_shared<MockActiveStream>(engine.get(),
                                                    producer,
                                                    DcpAddStreamFlag::None,
                                                    /*opaque*/ 0,
                                                    getVBucket(vbid),
                                                    /*st_seqno*/ 0,
                                                    /*en_seqno*/ ~0,
                                                    /*vb_uuid*/ 0xabcd,
                                                    /*snap_start_seqno*/ 0,
                                                    /*snap_end_seqno*/ ~0,
                                                    IncludeValue::No,
                                                    IncludeXattrs::No);
        return stream;
    }

    const int numVbs = 4;
    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<MockActiveStream> stream;
};

TEST_F(SingleThreadedEphemeralPurgerTest, PurgeAcrossAllVbuckets) {
    /* Set 100 item in all vbuckets. We need hundred items atleast because
       our ProgressTracker checks whether to pause only after
       INITIAL_VISIT_COUNT_CHECK = 100 */
    const int numItems = 100;
    for (Vbid::id_type vbid = 0; vbid < numVbs; ++vbid) {
        for (int i = 0; i < numItems; ++i) {
            const std::string key("key" + std::to_string(vbid) +
                                  std::to_string(i));
            store_item(Vbid(vbid), makeStoredDocKey(key), "value");
        }
    }

    /* Add and delete an item in every vbucket */
    for (Vbid::id_type vbid = 0; vbid < numVbs; ++vbid) {
        const std::string key("keydelete" + std::to_string(vbid));
        storeAndDeleteItem(Vbid(vbid), makeStoredDocKey(key), "value");
    }

    /* We have added an item at seqno 100 and deleted it immediately */
    const uint64_t expPurgeUpto = numItems + 2;

    /* Add another item as we do not purge last element in the list */
    for (Vbid::id_type vbid = 0; vbid < numVbs; ++vbid) {
        const std::string key("afterdelete" + std::to_string(vbid));
        store_item(Vbid(vbid), makeStoredDocKey(key), "value");
    }

    /* Run the HTCleaner task, so that we can wake up the stale item deleter */
    auto* bucket = dynamic_cast<EphemeralBucket*>(store);
    bucket->enableTombstonePurgerTask();
    bucket->attemptToFreeMemory(); // this wakes up the HTCleaner task

    auto& lpNonIoQ = *task_executor->getLpTaskQ(TaskType::NonIO);
    /* Run the HTCleaner and EphTombstoneStaleItemDeleter tasks. We expect
       pause and resume of EphTombstoneStaleItemDeleter atleast once and we run
       until all the deleted items across all the vbuckets are purged */
    int numPaused = 0;
    while (!checkAllPurged(expPurgeUpto)) {
        runNextTask(lpNonIoQ);
        ++numPaused;
    }
    EXPECT_GT(numPaused, 2 /* 1 run of 'HTCleaner' and more than 1 run of
                              'EphTombstoneStaleItemDeleter' */);
}

TEST_F(SingleThreadedEphemeralPurgerTest, HTCleanerSkipsPrepares) {
    // Test relies on that the HTCleaner does its work when it runs
    ASSERT_EQ(0, engine->getConfiguration().getEphemeralMetadataPurgeAge());

    // Store a SyncDelete
    auto key = makeStoredDocKey("key");
    store_item(vbid,
               key,
               "value",
               0 /*exptime*/,
               {cb::engine_errc::sync_write_pending},
               PROTOCOL_BINARY_RAW_BYTES,
               cb::durability::Requirements(),
               true /*deleted*/);

    auto& vb = *store->getVBucket(vbid);
    {
        auto res = vb.ht.findForUpdate(key);
        ASSERT_TRUE(res.pending);
        ASSERT_TRUE(res.pending->isDeleted());
        ASSERT_EQ(1, res.pending->getBySeqno());
        ASSERT_FALSE(res.committed);
    }

    // Run the HTCleaner
    auto* bucket = dynamic_cast<EphemeralBucket*>(store);
    bucket->enableTombstonePurgerTask();
    bucket->attemptToFreeMemory(); // This wakes up the HTCleaner
    auto& queue = *task_executor->getLpTaskQ(TaskType::NonIO);
    const std::string expectedTaskName = "Eph tombstone hashtable cleaner";
    runNextTask(queue, expectedTaskName);

    // Core of the test: Verify Prepare still in the HT
    {
        auto res = vb.ht.findForUpdate(key);
        ASSERT_TRUE(res.pending);
        ASSERT_EQ(CommittedState::Pending, res.pending->getCommitted());
        ASSERT_TRUE(res.pending->isDeleted());
        ASSERT_EQ(1, res.pending->getBySeqno());
        ASSERT_FALSE(res.committed);
    }

    {
        std::shared_lock rlh(vb.getStateLock());
        // Proceed with checking that everything behaves as expected at Prepare
        // completion.
        ASSERT_EQ(cb::engine_errc::success,
                  vb.commit(rlh,
                            key,
                            1 /*prepareSeqno*/,
                            {},
                            CommitType::Majority,
                            vb.lockCollections(key)));
    }

    // Verify Prepare and Commit in the HT
    {
        auto res = vb.ht.findForUpdate(key);
        ASSERT_TRUE(res.pending);
        ASSERT_EQ(CommittedState::PrepareCommitted,
                  res.pending->getCommitted());
        ASSERT_TRUE(res.pending->isDeleted());
        ASSERT_EQ(1, res.pending->getBySeqno());
        ASSERT_TRUE(res.committed);
        ASSERT_EQ(CommittedState::CommittedViaPrepare,
                  res.committed->getCommitted());
        ASSERT_TRUE(res.committed->isDeleted());
        ASSERT_EQ(2, res.committed->getBySeqno());
    }

    {
        auto& ephVb = static_cast<EphemeralVBucket&>(vb);
        auto itr = ephVb.makeRangeIterator(true /*backfill*/);

        // HCS updated for commit
        EXPECT_EQ(1, itr->getHighCompletedSeqno());
    }

    // Run the StaleItemDeleter (scheduled by the first run of the HTCleaner)
    runNextTask(queue, "Eph tombstone stale item deleter");
    // Run the HTCleaner again
    bucket->scheduleTombstonePurgerTask();
    bucket->attemptToFreeMemory();
    runNextTask(queue, expectedTaskName);

    // Verify that the HTCleaner behaves as expected:
    // - Prepare removed as Committed
    // - Committed removed as it is a tombstone
    {
        auto res = vb.ht.findForUpdate(key);
        ASSERT_FALSE(res.pending);
        ASSERT_FALSE(res.committed);
    }
}

TEST_F(SingleThreadedEphemeralPurgerTest, MB_42568) {
    // Test relies on that the HTCleaner does its work when it runs
    ASSERT_EQ(0, engine->getConfiguration().getEphemeralMetadataPurgeAge());

    auto& vb = dynamic_cast<EphemeralVBucket&>(*store->getVBucket(vbid));
    ASSERT_EQ(0, vb.getHighSeqno());
    ASSERT_EQ(0, vb.getSeqListNumItems());
    ASSERT_EQ(0, vb.getSeqListNumDeletedItems());

    // keyA - SyncDelete and Commit
    const auto keyA = makeStoredDocKey("keyA");
    const std::string value = "value";
    store_item(vbid,
               keyA,
               value,
               0 /*exptime*/,
               {cb::engine_errc::sync_write_pending},
               PROTOCOL_BINARY_RAW_BYTES,
               cb::durability::Requirements(),
               true /*deleted*/);
    EXPECT_EQ(1, vb.getHighSeqno());
    EXPECT_EQ(1, vb.getSeqListNumItems());
    EXPECT_EQ(1, vb.getSeqListNumDeletedItems());
    {
        std::shared_lock rlh(vb.getStateLock());
        EXPECT_EQ(cb::engine_errc::success,
                  vb.commit(rlh,
                            keyA,
                            1 /*prepareSeqno*/,
                            {},
                            CommitType::Majority,
                            vb.lockCollections(keyA)));
    }
    EXPECT_EQ(2, vb.getHighSeqno());
    EXPECT_EQ(2, vb.getSeqListNumItems());
    EXPECT_EQ(2, vb.getSeqListNumDeletedItems());

    // Cover seqno 1 and 2 with a range-read, then queue a SyncWrite for keyA.
    // Seqno 1 is PrepareCommitted and we could just move that OSV to the end of
    // the SeqList and update it to store the new Pending. But we cannot touch
    // seqno:1 because of the range-read, so we append a new OSV to the SeqList.
    {
        const auto rangeIt = vb.makeRangeIterator(true /*isBackfill*/);
        ASSERT_TRUE(rangeIt);

        // keyA - SyncWrite
        store_item(vbid,
                   keyA,
                   value,
                   0 /*exptime*/,
                   {cb::engine_errc::sync_write_pending},
                   PROTOCOL_BINARY_RAW_BYTES,
                   cb::durability::Requirements(),
                   false /*deleted*/);
    }
    EXPECT_EQ(3, vb.getHighSeqno());
    // Note: This would be 2 with no range-read
    EXPECT_EQ(3, vb.getSeqListNumItems());

    // Core check: We have not updated the deleted at seqno:1 with the alive at
    // seqno:3, we have just appended seqno:3. So, num-deleted-items must not
    // change. Before the fix this was decremented to 1.
    EXPECT_EQ(2, vb.getSeqListNumDeletedItems());

    // As a side effect, before the fix this step throws with
    // ThrowExceptionUnderflowPolicy, as we try to remove seqno 1 and 2 (ie,
    // (two deleted items) from the SeqList and we try to decrement
    // num-deleted-items to -1.
    runEphemeralHTCleaner();

    EXPECT_EQ(3, vb.getHighSeqno());
    EXPECT_EQ(1, vb.getSeqListNumItems());
    EXPECT_EQ(0, vb.getSeqListNumDeletedItems());
}

// Verify that the backfill create uses the LinkedList purgeSeqno when the
// ReadRange is created. The purgeSeqno returned by VBucket::getPurgeSeqno is
// backed by a separate field which may be out of date.
TEST_F(SingleThreadedEphemeralPurgerTest,
       bufferedMemoryBackfillPurgeGreaterThanStart) {
    auto& vb = getVBucket(vbid);

    {
        store_item(vbid, makeStoredDocKey("key1"), ""); // seqno 1
        auto item2 = store_item(vbid, makeStoredDocKey("key2"), ""); // seqno 2

        // Need range lock for the tombstone to be created.
        auto itr = vb.makeRangeIterator(/*isBackfill*/ true);
        ASSERT_TRUE(itr);

        uint64_t cas = 0;
        mutation_descr_t mutation_descr;
        EXPECT_EQ(cb::engine_errc::success,
                  engine->getKVBucket()->deleteItem(item2.getDocKey(),
                                                    cas,
                                                    vbid,
                                                    cookie,
                                                    {},
                                                    /*itemMeta*/ nullptr,
                                                    mutation_descr));

        store_item(vbid, makeStoredDocKey("key3"), ""); // seqno 4
        store_item(vbid, makeStoredDocKey("key4"), ""); // seqno 5
    }

    // Purge deletes.
    runEphemeralHTCleaner();

    ASSERT_EQ(5, vb.getHighSeqno());
    ASSERT_EQ(3, vb.getPurgeSeqno());

    /// Create producer, stream, backfill.
    auto stream = createStream(vbid);
    DCPBackfillMemoryBuffered dcpbfm(
            std::dynamic_pointer_cast<EphemeralVBucket>(
                    store->getVBucket(vbid)),
            stream,
            2, // purgeTombstones() has just purged seqno 3
            ~0);

    dcpbfm.run();

    EXPECT_TRUE(stream->isDead()) << "Expected to have set the stream to "
                                     "dead as we need to rollback";
}

// Verify that the backfill create uses the LinkedList purgeSeqno when the
// ReadRange is created. The purgeSeqno returned by VBucket::getPurgeSeqno is
// backed by a separate field which may be out of date.
TEST_F(SingleThreadedEphemeralPurgerTest,
       bufferedMemoryBackfillCreateDuringPurgeRace) {
    auto& vb = getVBucket(vbid);

    {
        store_item(vbid, makeStoredDocKey("key1"), ""); // seqno 1
        auto item2 = store_item(vbid, makeStoredDocKey("key2"), ""); // seqno 2

        // Need range lock for the tombstone to be created.
        auto itr = vb.makeRangeIterator(/*isBackfill*/ true);
        ASSERT_TRUE(itr);

        uint64_t cas = 0;
        mutation_descr_t mutation_descr;
        EXPECT_EQ(cb::engine_errc::success,
                  engine->getKVBucket()->deleteItem(item2.getDocKey(),
                                                    cas,
                                                    vbid,
                                                    cookie,
                                                    {},
                                                    /*itemMeta*/ nullptr,
                                                    mutation_descr));

        store_item(vbid, makeStoredDocKey("key3"), ""); // seqno 4
        store_item(vbid, makeStoredDocKey("key4"), ""); // seqno 5
    }

    ASSERT_EQ(5, vb.getHighSeqno());
    ASSERT_EQ(5, vb.getSeqListNumItems());
    ASSERT_EQ(1, vb.getSeqListNumDeletedItems())
            << "Expected to have 1 deleted item in the SeqList";
    ASSERT_EQ(0, vb.getPurgeSeqno());

    bool purgeHookRan = false;
    // Use the postPurgeTombstonesHook which runs after purgeTombstones() but
    // before we've updated the VBucket::setPurgeSeqno.
    vb.postPurgeTombstonesHook = [&]() {
        auto stream = createStream(vbid);
        DCPBackfillMemoryBuffered dcpbfm(
                std::dynamic_pointer_cast<EphemeralVBucket>(
                        store->getVBucket(vbid)),
                stream,
                2, // purgeTombstones() has just purged seqno 3
                ~0);

        // Run the backfill - it should request rollback.
        dcpbfm.run();

        ASSERT_EQ(0, vb.getSeqListNumDeletedItems())
                << "Expected to have purged the deleted item";
        ASSERT_EQ(0, vb.getPurgeSeqno())
                << "Did not expect the vBucket stat to be updated yet";

        EXPECT_TRUE(stream->isDead()) << "Expected to have set the stream to "
                                         "dead as we need to rollback";

        purgeHookRan = true;
    };

    // Purge deletes.
    runEphemeralHTCleaner();

    ASSERT_EQ(3, vb.getPurgeSeqno());
    ASSERT_TRUE(purgeHookRan);
}
