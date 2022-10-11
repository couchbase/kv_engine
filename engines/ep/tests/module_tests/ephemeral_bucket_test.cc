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

#include "checkpoint_manager.h"
#include "dcp/backfill-manager.h"
#include "dcp/dcpconnmap.h"
#include "dcp/response.h"
#include "ephemeral_bucket.h"
#include "ephemeral_tombstone_purger.h"
#include "ephemeral_vb.h"
#include "test_helpers.h"

#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_stream.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include <programs/engine_testapp/mock_server.h>

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
        Visitor(VBucket& vb, StoredDocKey key) : vb(vb), key(key) {
        }

        bool visit(const HashTable::HashBucketLock& lh,
                   StoredValue& v) override {
            if (v.getKey() == key) {
                StoredValue* vPtr = &v;
                EXPECT_TRUE(vb.pageOut(readHandle, lh, vPtr));
            }
            return true;
        }

        void setUpHashBucketVisit() override {
            // Need to lock collections before we visit each SV.
            readHandle = vb.lockCollections();
        }

        void tearDownHashBucketVisit() override {
            readHandle.unlock();
        }

        VBucket& vb;
        StoredDocKey key;
        Collections::VB::Manifest::ReadHandle readHandle;
    };

    // Invoke the visitor so the item gets paged out.
    Visitor visitor(*vb, key);
    vb->ht.visit(visitor);

    stats = get_stat("vbucket-details 0");
    EXPECT_EQ("1", stats.at("vb_0:auto_delete_count"));
    EXPECT_EQ("2", stats.at("vb_0:seqlist_deleted_count"));
    EXPECT_EQ("5", stats.at("vb_0:seqlist_high_seqno"));
}

TEST_F(EphemeralBucketStatTest, ReplicaMemoryTracking) {
    // test that replicaHTMemory is correctly updated for
    // inserts/updates/deletes/tombstone removal.
    auto replicaVB = Vbid(0);
    setVBucketStateAndRunPersistTask(replicaVB, vbucket_state_replica);

    auto cookie = create_mock_cookie();

    auto& stats = engine->getEpStats();
    EXPECT_EQ(0, stats.replicaHTMemory);

    auto key = makeStoredDocKey("item2");

    std::string value = "value";
    auto item = make_item(replicaVB, key, value);

    // Store an item in a replica VB and confirm replicaHTMemory increases
    item.setCas(1);
    uint64_t seqno;
    ASSERT_EQ(ENGINE_SUCCESS,
              store->setWithMeta(std::ref(item),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_replica},
                                 CheckConflicts::No,
                                 /*allowExisting*/ true));

    // avoids checking exact values to be resilient to changes (e.g.) in stored
    // value size.
    auto smallItemMem = stats.replicaHTMemory;
    EXPECT_GT(smallItemMem, 80);

    // Replace the existing item with a _larger_ item and confirm
    // replicaHTMemory increases further
    std::string largerValue = "valuevaluevaluevaluevaluevalue";
    auto largerItem = make_item(replicaVB, key, largerValue);
    largerItem.setCas(1);
    ASSERT_EQ(ENGINE_SUCCESS,
              store->setWithMeta(std::ref(largerItem),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_replica},
                                 CheckConflicts::No,
                                 /*allowExisting*/ true));

    auto largerItemMem = smallItemMem + largerValue.size() - value.size();
    EXPECT_EQ(largerItemMem, stats.replicaHTMemory);

    // Delete the item, confirm replicaHTMemory decreases (tombstone
    // remains).
    ItemMetaData meta;
    uint64_t cas = 1;
    meta.cas = cas;
    ASSERT_EQ(ENGINE_SUCCESS,
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
                      DeleteSource::Explicit));

    EXPECT_LT(stats.replicaHTMemory, largerItemMem);
    EXPECT_GT(stats.replicaHTMemory, 0);

    // now remove the tombstone and confirm the replicaHTMemory is now 0
    auto& replica = *store->getVBucket(replicaVB);

    EphemeralVBucket::HTTombstonePurger purger(
            0 /* remove tombstones of any age */);
    purger.setCurrentVBucket(replica);
    replica.ht.visit(purger);

    EXPECT_EQ(0, stats.replicaHTMemory);

    destroy_mock_cookie(cookie);
}

TEST_F(EphemeralBucketStatTest, ReplicaMemoryTrackingNotUpdatedForActive) {
    // replicaHTMemory should not be updated by storing items in active
    // vbuckets
    auto activeVB = Vbid(0);
    setVBucketStateAndRunPersistTask(activeVB, vbucket_state_active);

    auto& stats = engine->getEpStats();
    EXPECT_EQ(0, stats.replicaHTMemory);
    EXPECT_EQ(0, stats.replicaCheckpointOverhead);

    // Confirm replicaHTMemory is _not_ affected by storing an item to an
    // active vb.
    store_item(activeVB, makeStoredDocKey("item"), "value");
    EXPECT_EQ(0, stats.replicaHTMemory);
    EXPECT_EQ(0, stats.replicaCheckpointOverhead);
}

TEST_F(EphemeralBucketStatTest, ReplicaMemoryTrackingStateChange) {
    // Check that replicaHTMemory is increased/decreased as vbuckets change
    // state to/from replica
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto key = makeStoredDocKey("item");

    auto& stats = engine->getEpStats();
    EXPECT_EQ(0, stats.replicaHTMemory);
    EXPECT_EQ(0, stats.replicaCheckpointOverhead);

    store_item(vbid, key, "value");

    EXPECT_EQ(0, stats.replicaHTMemory);
    EXPECT_EQ(0, stats.replicaCheckpointOverhead);

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    // check that the mem usage has gone up by some amount - not
    // checking it is an exact value to avoid a brittle test
    EXPECT_GT(stats.replicaHTMemory, 80);
    EXPECT_GT(stats.replicaCheckpointOverhead, 80);

    // changing back to active should return replicaHTMemory to 0
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    EXPECT_EQ(0, stats.replicaHTMemory);
    EXPECT_EQ(0, stats.replicaCheckpointOverhead);
}

TEST_F(EphemeralBucketStatTest, ReplicaCheckpointMemoryTracking) {
    // test that replicaCheckpointOverhead is correctly updated
    auto replicaVB = Vbid(0);
    setVBucketStateAndRunPersistTask(replicaVB, vbucket_state_replica);

    auto cookie = create_mock_cookie();

    auto& replica = *store->getVBucket(replicaVB);
    auto& cpm = *replica.checkpointManager;

    // remove the checkpoint containing the set vbstate to get a clean
    // baseline memory usage
    cpm.createNewCheckpoint(true /*force*/);
    bool newCkptCreated = false;
    cpm.removeClosedUnrefCheckpoints(replica, newCkptCreated);

    auto& stats = engine->getEpStats();
    const auto initialMem = stats.replicaCheckpointOverhead;

    const auto keyA = makeStoredDocKey("itemA");
    const auto keyB = makeStoredDocKey("itemB");

    const std::string value = "value";
    auto item1 = make_item(replicaVB, keyA, value);

    // Store an item in a replica VB and confirm replicaCheckpointOverhead
    // increases
    item1.setCas(1);
    uint64_t seqno;
    ASSERT_EQ(ENGINE_SUCCESS,
              store->setWithMeta(std::ref(item1),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_replica},
                                 CheckConflicts::No,
                                 /*allowExisting*/ true));

    // avoids checking exact values to be resilient to changes (e.g.) in stored
    // value size.
    const auto item1Mem = stats.replicaCheckpointOverhead;
    EXPECT_GT(item1Mem, initialMem + 20);

    // Store the item again and confirm replicaCheckpointOverhead
    // _does not increase_. This matches existing checkpoint memory tracking;
    // in the event of an existing item, checkpoint memory usage is _not_
    // adjusted, even though the old and new item could be of different sizes
    const std::string largerValue = "valuevaluevaluevaluevaluevaluevaluevalue";
    auto item2 = make_item(replicaVB, keyA, value);
    item2.setCas(1);
    ASSERT_EQ(ENGINE_SUCCESS,
              store->setWithMeta(std::ref(item2),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_replica},
                                 CheckConflicts::No,
                                 /*allowExisting*/ true));
    // tracked memory unchanged
    EXPECT_EQ(item1Mem, stats.replicaCheckpointOverhead);

    // Store an item with a different key, confirm checkpoint mem increases
    auto item3 = make_item(replicaVB, keyB, value);
    item3.setCas(1);
    ASSERT_EQ(ENGINE_SUCCESS,
              store->setWithMeta(std::ref(item3),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_replica},
                                 CheckConflicts::No,
                                 /*allowExisting*/ true));

    const auto item3Mem = stats.replicaCheckpointOverhead;
    EXPECT_GT(item3Mem, item1Mem);

    // now remove the checkpoint and confirm the replicaCheckpointOverhead is
    // now back to the initial value.
    cpm.createNewCheckpoint();
    cpm.removeClosedUnrefCheckpoints(replica, newCkptCreated);

    EXPECT_EQ(initialMem, stats.replicaCheckpointOverhead);

    destroy_mock_cookie(cookie);
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

    auto& ckpt_mgr =
            *(static_cast<MockCheckpointManager*>(vb->checkpointManager.get()));
    ASSERT_EQ(1, ckpt_mgr.getNumCheckpoints());

    // make checkpoint to cause backfill later rather than straight to in-memory
    ckpt_mgr.createNewCheckpoint();
    bool new_ckpt_created;
    ASSERT_EQ(2, ckpt_mgr.removeClosedUnrefCheckpoints(*vb, new_ckpt_created));

    // Create a Mock Dcp producer
    const std::string testName("test_producer");
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      testName,
                                                      /*flags*/ 0);

    // Since we are creating a mock active stream outside of
    // DcpProducer::streamRequest(), and we want the checkpt processor task,
    // create it explicitly here
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    // Create a Mock Active Stream
    auto mock_stream = std::make_shared<MockActiveStream>(
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
    producer->cancelCheckpointCreatorTask();
    /* Checkpoint processor task finishes up and releases its producer
       reference */
    auto& lpNonIoQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    runNextTask(lpNonIoQ, "Process checkpoint(s) for DCP producer " + testName);

    engine->getDcpConnMap().shutdownAllConnections();
    mock_stream.reset();
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
        for (int vbid = 0; vbid < numVbs; ++vbid) {
            setVBucketStateAndRunPersistTask(Vbid(vbid), vbucket_state_active);
        }

        // 'vbid' used for some durability related test
        setVBucketStateAndRunPersistTask(
                vbid,
                vbucket_state_active,
                {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    }

    bool checkAllPurged(uint64_t expPurgeUpto) {
        for (int vbid = 0; vbid < numVbs; ++vbid) {
            if (store->getVBucket(Vbid(vbid))->getPurgeSeqno() < expPurgeUpto) {
                return false;
            }
        }
        return true;
    }
    const int numVbs = 4;
};

TEST_F(SingleThreadedEphemeralPurgerTest, PurgeAcrossAllVbuckets) {
    /* Set 100 item in all vbuckets. We need hundred items atleast because
       our ProgressTracker checks whether to pause only after
       INITIAL_VISIT_COUNT_CHECK = 100 */
    const int numItems = 100;
    for (int vbid = 0; vbid < numVbs; ++vbid) {
        for (int i = 0; i < numItems; ++i) {
            const std::string key("key" + std::to_string(vbid) +
                                  std::to_string(i));
            store_item(Vbid(vbid), makeStoredDocKey(key), "value");
        }
    }

    /* Add and delete an item in every vbucket */
    for (int vbid = 0; vbid < numVbs; ++vbid) {
        const std::string key("keydelete" + std::to_string(vbid));
        storeAndDeleteItem(Vbid(vbid), makeStoredDocKey(key), "value");
    }

    /* We have added an item at seqno 100 and deleted it immediately */
    const uint64_t expPurgeUpto = numItems + 2;

    /* Add another item as we do not purge last element in the list */
    for (int vbid = 0; vbid < numVbs; ++vbid) {
        const std::string key("afterdelete" + std::to_string(vbid));
        store_item(Vbid(vbid), makeStoredDocKey(key), "value");
    }

    /* Run the HTCleaner task, so that we can wake up the stale item deleter */
    EphemeralBucket* bucket = dynamic_cast<EphemeralBucket*>(store);
    bucket->enableTombstonePurgerTask();
    bucket->attemptToFreeMemory(); // this wakes up the HTCleaner task

    auto& lpNonIoQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
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
    auto& queue = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
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

    // Proceed with checking that everything behaves as expected at Prepare
    // completion.
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key, 1 /*prepareSeqno*/, {}, vb.lockCollections(key)));

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
