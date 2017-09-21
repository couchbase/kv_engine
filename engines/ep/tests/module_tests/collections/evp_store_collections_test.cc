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

/**
 * Tests for Collection functionality in EPStore.
 */
#include "bgfetcher.h"
#include "checkpoint.h"
#include "dcp/dcpconnmap.h"
#include "kvstore.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_global_task.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/evp_store_test.h"
#include "tests/module_tests/test_helpers.h"
#include "tests/module_tests/thread_gate.h"

#include <boost/optional/optional.hpp>

#include <functional>
#include <thread>

class CollectionsTest : public SingleThreadedKVBucketTest {
public:
    void SetUp() override {
        // Enable collections (which will enable namespace persistence).
        config_string += "collections_prototype_enabled=true";
        SingleThreadedKVBucketTest::SetUp();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active, false);
    }

    std::string getManifest(uint16_t vb) const {
        return store->getVBucket(vb)
                ->getShard()
                ->getRWUnderlying()
                ->getCollectionsManifest(vbid);
    }
};

// This test stores a key which matches what collections internally uses, but
// in a different namespace.
TEST_F(CollectionsTest, namespace_separation) {
    // Use the event factory to get an event which we'll borrow the key from
    auto se = SystemEventFactory::make(
            SystemEvent::Collection, "::", "meat", 0, {});
    DocKey key(se->getKey().data(),
               se->getKey().size(),
               DocNamespace::DefaultCollection);

    store_item(vbid, key, "value");
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the meat collection
    vb->updateFromManifest({R"({"separator":"::",
                 "collections":[{"name":"$default", "uid":"0"},
                                {"name":"meat", "uid":"1"}]})"});
    // Trigger a flush to disk. Flushes the meat create event and 1 item
    flush_vbucket_to_disk(vbid, 2);

    // evict and load - should not see the system key for create collections
    evict_key(vbid, key);
    get_options_t options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    GetValue gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    // Manually run the BGFetcher task; to fetch the two outstanding
    // requests (for the same key).
    runBGFetcherTask();

    gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ(0, strncmp("value", gv.item->getData(), gv.item->getNBytes()));
}

TEST_F(CollectionsTest, collections_basic) {
    // Default collection is open for business
    store_item(vbid, {"key", DocNamespace::DefaultCollection}, "value");
    store_item(vbid,
               {"meat::beef", DocNamespace::Collections},
               "value",
               0,
               {cb::engine_errc::unknown_collection});

    VBucketPtr vb = store->getVBucket(vbid);

    // Add the meat collection
    vb->updateFromManifest({R"({"separator":"::",
                 "collections":[{"name":"$default", "uid":"0"},
                                {"name":"meat", "uid":"1"}]})"});

    // Trigger a flush to disk. Flushes the meat create event and 1 item
    flush_vbucket_to_disk(vbid, 2);

    // Now we can write to beef
    store_item(vbid, {"meat::beef", DocNamespace::Collections}, "value");

    flush_vbucket_to_disk(vbid, 1);

    // And read a document from beef
    get_options_t options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

    GetValue gv = store->get(
            {"meat::beef", DocNamespace::Collections}, vbid, cookie, options);
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());

    // A key in meat that doesn't exist
    gv = store->get({"meat::sausage", DocNamespace::Collections},
                    vbid,
                    cookie,
                    options);
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    // Begin the deletion
    vb->updateFromManifest({R"({"separator":"::",
                 "collections":[{"name":"$default", "uid":"0"}]})"});

    // We should have deleted the create marker
    flush_vbucket_to_disk(vbid, 1);

    // Access denied (although the item still exists)
    gv = store->get(
            {"meat::beef", DocNamespace::Collections}, vbid, cookie, options);
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION, gv.getStatus());
}

// Test demonstrates issue logged as MB_25344, when we delete a collection
// and then happen to perform a mutation against a new rev of the collection
// we may encounter the key which is pending deletion and then fail when we
// shouldn't. In this test the final add should logically work, but fails as the
// old key is found.
TEST_F(CollectionsTest, DISABLED_MB_25344) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    vb->updateFromManifest({R"({"separator":"::",
                 "collections":[{"name":"$default", "uid":"0"},
                                {"name":"dairy", "uid":"1"}]})"});
    // Trigger a flush to disk. Flushes the dairy create event.
    flush_vbucket_to_disk(vbid, 1);

    auto item = make_item(vbid, {"dairy::milk", DocNamespace::Collections}, "creamy", 0, 0);
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item, nullptr));
    flush_vbucket_to_disk(vbid, 1);

    // Now delete the dairy collection
    vb->updateFromManifest({R"({"separator":"::",
                 "collections":[{"name":"$default", "uid":"0"}]})"});

    // Re-add the dairy collection
    vb->updateFromManifest({R"({"separator":"::",
                 "collections":[{"name":"$default", "uid":"0"},
                                {"name":"dairy", "uid":"2"}]})"});
    // Trigger a flush to disk. Flushes the dairy create event.
    flush_vbucket_to_disk(vbid, 1);

    // Should be able to add the key again
    // @todo when BG collection deletion exists, it should be disabled for this
    // test, otherwise the test will be racey in whether the old key is in the
    // cache.
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item, nullptr));
}

class CollectionsFlushTest : public CollectionsTest {
public:
    void SetUp() override {
        CollectionsTest::SetUp();
    }

    void collectionsFlusher(int items);

private:
    std::string createCollectionAndFlush(const std::string& json,
                                         const std::string& collection,
                                         int items);
    std::string deleteCollectionAndFlush(const std::string& json,
                                         const std::string& collection,
                                         int items);
    std::string completeDeletionAndFlush(const std::string& collection,
                                         Collections::uid_t uid,
                                         int items);

    void storeItems(const std::string& collection, DocNamespace ns, int items);

    /**
     * Create manifest object from jsonManifest and validate if we can write to
     * the collection.
     * @param jsonManifest - A JSON VB manifest
     * @param collection - a collection name to test for writing
     *
     * @return true if the collection can be written
     */
    static bool canWrite(const std::string& jsonManifest,
                         const std::string& collection);

    /**
     * Create manifest object from jsonManifest and validate if we cannot write
     * to the collection.
     * @param jsonManifest - A JSON VB manifest
     * @param collection - a collection name to test for writing
     *
     * @return true if the collection cannot be written
     */
    static bool cannotWrite(const std::string& jsonManifest,
                            const std::string& collection);
};

void CollectionsFlushTest::storeItems(const std::string& collection,
                                      DocNamespace ns,
                                      int items) {
    for (int ii = 0; ii < items; ii++) {
        std::string key = collection + "::" + std::to_string(ii);
        store_item(vbid, {key, ns}, "value");
    }
}

std::string CollectionsFlushTest::createCollectionAndFlush(
        const std::string& json, const std::string& collection, int items) {
    VBucketPtr vb = store->getVBucket(vbid);
    vb->updateFromManifest(json);
    storeItems(collection, DocNamespace::Collections, items);
    flush_vbucket_to_disk(vbid, 1 + items); // create event + items
    return getManifest(vbid);
}

std::string CollectionsFlushTest::deleteCollectionAndFlush(
        const std::string& json, const std::string& collection, int items) {
    VBucketPtr vb = store->getVBucket(vbid);
    storeItems(collection, DocNamespace::Collections, items);
    vb->updateFromManifest(json);
    flush_vbucket_to_disk(vbid, 1 + items); // del(create event) + items
    return getManifest(vbid);
}

std::string CollectionsFlushTest::completeDeletionAndFlush(
        const std::string& collection, Collections::uid_t uid, int items) {
    VBucketPtr vb = store->getVBucket(vbid);
    vb->completeDeletion({collection, uid});
    storeItems("defaultcollection", DocNamespace::DefaultCollection, items);
    flush_vbucket_to_disk(vbid, items); // just the items
    return getManifest(vbid);
}

bool CollectionsFlushTest::canWrite(const std::string& jsonManifest,
                                    const std::string& collection) {
    Collections::VB::Manifest manifest(jsonManifest);
    return manifest.lock().doesKeyContainValidCollection(
            {collection + "::", DocNamespace::Collections});
}

bool CollectionsFlushTest::cannotWrite(const std::string& jsonManifest,
                                       const std::string& collection) {
    return !canWrite(jsonManifest, collection);
}

/**
 * Drive manifest state changes through the test's vbucket
 *  1. Validate the flusher flushes the expected items
 *  2. Validate the updated collections manifest changes
 *  3. Use a validator function to check if a collection is (or is not)
 *     writeable
 */
void CollectionsFlushTest::collectionsFlusher(int items) {
    struct testFuctions {
        std::function<std::string()> function;
        std::function<bool(const std::string&)> validator;
    };

    using std::placeholders::_1;
    // Setup the test using a vector of functions to run
    std::vector<testFuctions> test{
            // First 3 steps - add,delete,complete for the meat collection
            {// 0
             std::bind(&CollectionsFlushTest::createCollectionAndFlush,
                       this,
                       R"({"separator":"::",
                         "collections":[{"name":"$default", "uid":"0"},
                                        {"name":"meat", "uid":"1"}]})",
                       "meat",
                       items),
             std::bind(&CollectionsFlushTest::canWrite, _1, "meat")},

            {// 1
             std::bind(&CollectionsFlushTest::deleteCollectionAndFlush,
                       this,
                       R"({"separator":"::",
                         "collections":[{"name":"$default", "uid":"0"}]})",
                       "meat",
                       items),
             std::bind(&CollectionsFlushTest::cannotWrite, _1, "meat")},
            {// 2
             std::bind(&CollectionsFlushTest::completeDeletionAndFlush,
                       this,
                       "meat",
                       2,
                       items),
             std::bind(&CollectionsFlushTest::cannotWrite, _1, "meat")},

            // Final 4 steps - add,delete,add,complete for the fruit collection
            {// 3
             std::bind(&CollectionsFlushTest::createCollectionAndFlush,
                       this,
                       R"({"separator":"::",
                         "collections":[{"name":"$default", "uid":"0"},
                                        {"name":"fruit", "uid":"3"}]})",
                       "fruit",
                       items),
             std::bind(&CollectionsFlushTest::canWrite, _1, "fruit")},
            {// 4
             std::bind(&CollectionsFlushTest::deleteCollectionAndFlush,
                       this,
                       R"({"separator":"::",
                         "collections":[{"name":"$default", "uid":"0"}]})",
                       "fruit",
                       items),
             std::bind(&CollectionsFlushTest::cannotWrite, _1, "fruit")},
            {// 5
             std::bind(&CollectionsFlushTest::createCollectionAndFlush,
                       this,
                       R"({"separator":"::",
                         "collections":[{"name":"$default", "uid":"0"},
                                        {"name":"fruit", "uid":"5"}]})",
                       "fruit",
                       items),
             std::bind(&CollectionsFlushTest::canWrite, _1, "fruit")},
            {// 6
             std::bind(&CollectionsFlushTest::completeDeletionAndFlush,
                       this,
                       "fruit",
                       4,
                       items),
             std::bind(&CollectionsFlushTest::canWrite, _1, "fruit")}};

    std::string m1;
    int step = 0;
    for (auto& f : test) {
        auto m2 = f.function();
        // The manifest should change for each step
        EXPECT_NE(m1, m2);
        EXPECT_TRUE(f.validator(m2))
                << "Failed step " + std::to_string(step) + " validating " + m2;
        m1 = m2;
        step++;
    }
}

TEST_F(CollectionsFlushTest, collections_flusher_no_items) {
    collectionsFlusher(0);
}

TEST_F(CollectionsFlushTest, collections_flusher_with_items) {
    collectionsFlusher(3);
}

class CollectionsWarmupTest : public SingleThreadedKVBucketTest {
public:
    void SetUp() override {
        // Enable collections (which will enable namespace persistence).
        config_string += "collections_prototype_enabled=true";
        SingleThreadedKVBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    }
};

//
// Create a collection then create a second engine which will warmup from the
// persisted collection state and should have the collection accessible.
//
TEST_F(CollectionsWarmupTest, warmup) {
    {
        auto vb = store->getVBucket(vbid);

        // Add the meat collection *and* change the separator
        vb->updateFromManifest({R"({"separator":"-+-",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"meat","uid":"1"}]})"});

        // Trigger a flush to disk. Flushes the meat create event and a separator
        // changed event.
        flush_vbucket_to_disk(vbid, 2);

        // Now we can write to beef
        store_item(vbid, {"meat-+-beef", DocNamespace::Collections}, "value");
        // But not dairy
        store_item(vbid,
                   {"dairy-+-milk", DocNamespace::Collections},
                   "value",
                   0,
                   {cb::engine_errc::unknown_collection});

        flush_vbucket_to_disk(vbid, 1);
    } // VBucketPtr scope ends

    resetEngineAndWarmup();

    {
        Item item({"meat-+-beef", DocNamespace::Collections},
                  /*flags*/ 0,
                  /*exp*/ 0,
                  "rare",
                  sizeof("rare"));
        item.setVBucketId(vbid);
        uint64_t cas;
        EXPECT_EQ(ENGINE_SUCCESS,
                  engine->store(nullptr, &item, &cas, OPERATION_SET));
    }
    {
        Item item({"dairy-+-milk", DocNamespace::Collections},
                  /*flags*/ 0,
                  /*exp*/ 0,
                  "skimmed",
                  sizeof("skimmed"));
        item.setVBucketId(vbid);
        uint64_t cas;
        EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION,
                  engine->store(nullptr, &item, &cas, OPERATION_SET));
    }
}

// When a collection is deleted - an event enters the checkpoint which does not
// enter the persisted seqno index - hence at the end of this test when we warm
// up, expect the highSeqno to be less than before the warmup.
TEST_F(CollectionsWarmupTest, MB_25381) {
    int64_t highSeqno = 0;
    {
        auto vb = store->getVBucket(vbid);

        // Add the dairy collection *and* change the separator
        vb->updateFromManifest({R"({"separator":"@",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"dairy","uid":"1"}]})"});

        // Trigger a flush to disk. Flushes the dairy create event and a separator
        // changed event.
        flush_vbucket_to_disk(vbid, 2);

        // Now we can write to dairy
        store_item(vbid, {"dairy@milk", DocNamespace::Collections}, "creamy");

        // Now delete the dairy collection
        vb->updateFromManifest({R"({"separator":"@",
              "collections":[{"name":"$default", "uid":"0"}]})"});

        flush_vbucket_to_disk(vbid, 2);

        // This pushes an Item which doesn't flush but has consumed a seqno
        vb->completeDeletion({"dairy", 1});

        flush_vbucket_to_disk(vbid, 0); // 0 items but has written _local

        highSeqno = vb->getHighSeqno();
    } // VBucketPtr scope ends
    resetEngineAndWarmup();

    auto vb = store->getVBucket(vbid);
    EXPECT_GT(highSeqno, vb->getHighSeqno());
}

TEST_F(CollectionsTest, test_dcp_consumer) {
    const void* cookie = create_mock_cookie();

    SingleThreadedRCPtr<MockDcpConsumer> consumer(
            new MockDcpConsumer(*engine, cookie, "test_consumer"));

    store->setVBucketState(vbid, vbucket_state_replica, false);
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0));

    std::string collection = "meat";

    Collections::uid_t uid = 4;
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(/*opaque*/ 1,
                                       vbid,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ 100,
                                       /*flags*/ 0));

    VBucketPtr vb = store->getVBucket(vbid);

    EXPECT_FALSE(vb->lockCollections().doesKeyContainValidCollection(
            {"meat::bacon", DocNamespace::Collections}));

    // Call the consumer function for handling DCP events
    // create the meat collection
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->systemEvent(
                      /*opaque*/ 1,
                      vbid,
                      mcbp::systemevent::id::CreateCollection,
                      /*seqno*/ 1,
                      {reinterpret_cast<const uint8_t*>(collection.data()),
                       collection.size()},
                      {reinterpret_cast<const uint8_t*>(&uid), sizeof(uid)}));

    // We can now access the collection
    EXPECT_TRUE(vb->lockCollections().doesKeyContainValidCollection(
            {"meat::bacon", DocNamespace::Collections}));

    // Call the consumer function for handling DCP events
    // delete the meat collection
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->systemEvent(
                      /*opaque*/ 1,
                      vbid,
                      mcbp::systemevent::id::DeleteCollection,
                      /*seqno*/ 2,
                      {reinterpret_cast<const uint8_t*>(collection.data()),
                       collection.size()},
                      {reinterpret_cast<const uint8_t*>(&uid), sizeof(uid)}));

    // It's gone!
    EXPECT_FALSE(vb->lockCollections().doesKeyContainValidCollection(
            {"meat::bacon", DocNamespace::Collections}));

    consumer->closeAllStreams();
    destroy_mock_cookie(cookie);
    consumer->cancelTask();
}

extern uint8_t dcp_last_op;
extern std::string dcp_last_key;

class CollectionsDcpTest : public CollectionsTest {
public:
    CollectionsDcpTest()
        : cookieC(create_mock_cookie()), cookieP(create_mock_cookie()) {
    }

    // Setup a producer/consumer ready for the test
    void SetUp() override {
        CollectionsTest::SetUp();
        producers = get_dcp_producers(
                reinterpret_cast<ENGINE_HANDLE*>(engine.get()),
                reinterpret_cast<ENGINE_HANDLE_V1*>(engine.get()));
        createDcpObjects({/*no filter*/}, true /*collections on*/);
    }

    void createDcpObjects(const std::string& filter, bool dcpCollectionAware) {
        CollectionsDcpTest::consumer =
                new MockDcpConsumer(*engine, cookieC, "test_consumer");

        int flags = DCP_OPEN_INCLUDE_XATTRS;
        if (dcpCollectionAware) {
            flags |= DCP_OPEN_COLLECTIONS;
        }
        producer = new MockDcpProducer(
                *engine,
                cookieP,
                "test_producer",
                flags,
                {reinterpret_cast<const uint8_t*>(filter.data()),
                 filter.size()},
                false /*startTask*/);

        // Create the task object, but don't schedule
        producer->createCheckpointProcessorTask();

        // Need to enable NOOP for XATTRS (and collections).
        producer->setNoopEnabled(true);

        store->setVBucketState(replicaVB, vbucket_state_replica, false);
        ASSERT_EQ(ENGINE_SUCCESS,
                  consumer->addStream(/*opaque*/ 0,
                                      replicaVB,
                                      /*flags*/ 0));
        uint64_t rollbackSeqno;
        ASSERT_EQ(ENGINE_SUCCESS,
                  producer->streamRequest(
                          0, // flags
                          1, // opaque
                          vbid,
                          0, // start_seqno
                          ~0ull, // end_seqno
                          0, // vbucket_uuid,
                          0, // snap_start_seqno,
                          0, // snap_end_seqno,
                          &rollbackSeqno,
                          &CollectionsDcpTest::dcpAddFailoverLog));

        // Patch our local callback into the handlers
        producers->system_event = &CollectionsDcpTest::sendSystemEvent;

        // Setup a snapshot on the consumer
        ASSERT_EQ(ENGINE_SUCCESS,
                  consumer->snapshotMarker(/*opaque*/ 1,
                                           /*vbucket*/ replicaVB,
                                           /*start_seqno*/ 0,
                                           /*end_seqno*/ 100,
                                           /*flags*/ 0));
    }

    void TearDown() override {
        teardown();
        SingleThreadedKVBucketTest::TearDown();
    }

    void teardown() {
        destroy_mock_cookie(cookieC);
        destroy_mock_cookie(cookieP);
        consumer->closeAllStreams();
        consumer->cancelTask();
        producer->closeAllStreams();
        producer.reset();
        consumer.reset();
    }

    void notifyAndStepToCheckpoint(bool fromMemory = true) {
        auto vb = store->getVBucket(vbid);
        ASSERT_NE(nullptr, vb.get());

        if (fromMemory) {
            producer->notifySeqnoAvailable(vbid, vb->getHighSeqno());

            // Step which will notify the snapshot task
            EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

            EXPECT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());

            // Now call run on the snapshot task to move checkpoint into DCP
            // stream
            producer->getCheckpointSnapshotTask().run();
        } else {
            // Run a backfill
            auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
            // backfill:create()
            runNextTask(lpAuxioQ);
            // backfill:scan()
            runNextTask(lpAuxioQ);
            // backfill:complete()
            runNextTask(lpAuxioQ);
            // backfill:finished()
            runNextTask(lpAuxioQ);
        }

        // Next step which will process a snapshot marker and then the caller
        // should now be able to step through the checkpoint
        EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
        EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER, dcp_last_op);
    }

    void testDcpCreateDelete(int expectedCreates,
                             int expectedDeletes,
                             bool fromMemory = true);

    void resetEngineAndWarmup() {
        teardown();
        SingleThreadedKVBucketTest::resetEngineAndWarmup();
        producers = get_dcp_producers(
                reinterpret_cast<ENGINE_HANDLE*>(engine.get()),
                reinterpret_cast<ENGINE_HANDLE_V1*>(engine.get()));
        cookieC = create_mock_cookie();
        cookieP = create_mock_cookie();
    }

    static const uint16_t replicaVB{1};
    static SingleThreadedRCPtr<MockDcpConsumer> consumer;
    static mcbp::systemevent::id dcp_last_system_event;

    /*
     * DCP callback method to push SystemEvents on to the consumer
     */
    static ENGINE_ERROR_CODE sendSystemEvent(const void* cookie,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             mcbp::systemevent::id event,
                                             uint64_t bySeqno,
                                             cb::const_byte_buffer key,
                                             cb::const_byte_buffer eventData) {
        (void)cookie;
        (void)vbucket; // ignored as we are connecting VBn to VBn+1
        dcp_last_op = PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT;
        dcp_last_key.assign(reinterpret_cast<const char*>(key.data()),
                            key.size());
        dcp_last_system_event = event;
        return consumer->systemEvent(
                opaque, replicaVB, event, bySeqno, key, eventData);
    }

    static ENGINE_ERROR_CODE dcpAddFailoverLog(vbucket_failover_t* entry,
                                               size_t nentries,
                                               const void* cookie) {
        return ENGINE_SUCCESS;
    }

    const void* cookieC;
    const void* cookieP;
    std::unique_ptr<dcp_message_producers> producers;
    SingleThreadedRCPtr<MockDcpProducer> producer;
};

SingleThreadedRCPtr<MockDcpConsumer> CollectionsDcpTest::consumer;
mcbp::systemevent::id CollectionsDcpTest::dcp_last_system_event;

/*
 * test_dcp connects a producer and consumer to test that collections created
 * on the producer are transferred to the consumer
 *
 * The test replicates VBn to VBn+1
 */
TEST_F(CollectionsDcpTest, test_dcp) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Add a collection, then remove it. This generated events into the CP which
    // we'll manually replicate with calls to step
    vb->updateFromManifest({R"({"separator":"::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"meat","uid":"1"}]})"});

    notifyAndStepToCheckpoint();

    VBucketPtr replica = store->getVBucket(replicaVB);

    // 1. Replica does not know about meat
    EXPECT_FALSE(replica->lockCollections().doesKeyContainValidCollection(
            {"meat::bacon", DocNamespace::Collections}));

    // Now step the producer to transfer the collection creation
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // 1. Replica now knows the collection
    EXPECT_TRUE(replica->lockCollections().doesKeyContainValidCollection(
            {"meat::bacon", DocNamespace::Collections}));

    // remove meat
    vb->updateFromManifest({R"({"separator":"::",
              "collections":[{"name":"$default", "uid":"0"}]})"});

    notifyAndStepToCheckpoint();

    // Now step the producer to transfer the collection deletion
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // 3. Replica now blocking access to meat
    EXPECT_FALSE(replica->lockCollections().doesKeyContainValidCollection(
            {"meat::bacon", DocNamespace::Collections}));

    // Now step the producer, no more collection events
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
}

void CollectionsDcpTest::testDcpCreateDelete(int expectedCreates,
                                             int expectedDeletes,
                                             bool fromMemory) {
    notifyAndStepToCheckpoint(fromMemory);

    int creates = 0, deletes = 0;
    // step until done
    while (ENGINE_WANT_MORE == producer->step(producers.get())) {
        if (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT) {
            switch (dcp_last_system_event) {
            case mcbp::systemevent::id::CreateCollection:
                creates++;
                break;
            case mcbp::systemevent::id::DeleteCollection:
                deletes++;
                break;
            case mcbp::systemevent::id::CollectionsSeparatorChanged: {
                EXPECT_FALSE(true);
                break;
            }
            }
        }
    }

    EXPECT_EQ(expectedCreates, creates);
    EXPECT_EQ(expectedDeletes, deletes);

    // Finally check that the active and replica have the same manifest, our
    // BeginDeleteCollection should of contained enough information to form
    // an equivalent manifest
    EXPECT_EQ(getManifest(vbid), getManifest(vbid + 1));
}

// Test that a create/delete don't dedup (collections creates new checkpoints)
TEST_F(CollectionsDcpTest, test_dcp_create_delete) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Create dairy
    vb->updateFromManifest({R"({"separator":"::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"fruit","uid":"1"},
                             {"name":"dairy","uid":"1"}]})"});

    // Mutate dairy
    const int items = 3;
    for (int ii = 0; ii < items; ii++) {
        std::string key = "dairy::" + std::to_string(ii);
        store_item(vbid, {key, DocNamespace::Collections}, "value");
    }

    // Delete dairy
    vb->updateFromManifest({R"({"separator":"::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"fruit","uid":"1"}]})"});

    // Persist everything ready for warmup and check.
    // Flusher will merge create/delete and we only flush the delete
    flush_vbucket_to_disk(0, items + 2);

    // We will see create fruit/dairy and delete dairy (from another CP)
    testDcpCreateDelete(2, 1);

    /*
        @todo enable disk part of this test. Logically deleted collections are
        still streaming from backfill, meaning we send delete with no create to
        replica triggering an exception.

        resetEngineAndWarmup();

        createDcpObjects({}, true); // from disk

        // Streamed from disk, one create and one delete
        testDcpCreateDelete(1, 1, false);
    */
}

// Test that a create/delete don't dedup (collections creates new checkpoints)
TEST_F(CollectionsDcpTest, test_dcp_create_delete_create) {
    {
        VBucketPtr vb = store->getVBucket(vbid);
        // Create dairy
        vb->updateFromManifest({R"({"separator":"::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"dairy","uid":"1"}]})"});

        // Mutate dairy
        const int items = 3;
        for (int ii = 0; ii < items; ii++) {
            std::string key = "dairy::" + std::to_string(ii);
            store_item(vbid, {key, DocNamespace::Collections}, "value");
        }

        // Delete dairy
        vb->updateFromManifest(
                {R"({"separator":"::","collections":[{"name":"$default", "uid":"0"}]})"});

        // Create dairy (new uid)
        vb->updateFromManifest({R"({"separator":"::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"dairy","uid":"2"}]})"});

        // Persist everything ready for warmup and check.
        // Flusher will merge create/delete and we only flush the delete
        flush_vbucket_to_disk(0, items + 1);

        // Should see 2x create dairy and 1x delete dairy
        testDcpCreateDelete(2, 1);
    }

    resetEngineAndWarmup();

    createDcpObjects({}, true /* from disk*/);

    // Streamed from disk, we won't see the 2x create event (only 1) or the
    // intermediate delete
    testDcpCreateDelete(1, 0, false);
}

// Test that a create/delete/create don't dedup
TEST_F(CollectionsDcpTest, test_dcp_create_delete_create2) {
    {
        VBucketPtr vb = store->getVBucket(vbid);
        // Create dairy
        vb->updateFromManifest({R"({"separator":"::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"dairy","uid":"1"}]})"});

        // Mutate dairy
        const int items = 3;
        for (int ii = 0; ii < items; ii++) {
            std::string key = "dairy::" + std::to_string(ii);
            store_item(vbid, {key, DocNamespace::Collections}, "value");
        }

        // Delete dairy/create dairy in one update
        vb->updateFromManifest({R"({"separator":"::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"dairy","uid":"2"}]})"});

        // Persist everything ready for warmup and check.
        // Flusher will merge create/delete and we only flush the delete
        flush_vbucket_to_disk(0, items + 1);

        testDcpCreateDelete(2, 1);
    }

    resetEngineAndWarmup();

    createDcpObjects({}, true /* from disk*/);

    // Streamed from disk, we won't see the first create or delete
    testDcpCreateDelete(1, 0, false);
}

TEST_F(CollectionsDcpTest, test_dcp_separator) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Change the separator
    vb->updateFromManifest({R"({"separator":"@@",
              "collections":[{"name":"$default", "uid":"0"}]})"});

    // Add a collection
    vb->updateFromManifest({R"({"separator":"@@",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"meat","uid":"1"}]})"});

    // The producer should start with the old separator
    EXPECT_EQ("::", producer->getCurrentSeparatorForStream(vbid));

    notifyAndStepToCheckpoint();

    VBucketPtr replica = store->getVBucket(replicaVB);

    // The replica should have the :: separator
    EXPECT_EQ("::", replica->lockCollections().getSeparator());

    // Now step the producer to transfer the separator
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // The producer should now have the new separator
    EXPECT_EQ("@@", producer->getCurrentSeparatorForStream(vbid));

    // The replica should now have the new separator
    EXPECT_EQ("@@", replica->lockCollections().getSeparator());

    // Now step the producer to transfer the collection
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // Collection should now be live on the replica
    EXPECT_TRUE(replica->lockCollections().doesKeyContainValidCollection(
            {"meat@@bacon", DocNamespace::Collections}));

    // And done
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
}

TEST_F(CollectionsDcpTest, test_dcp_separator_many) {
    auto vb = store->getVBucket(vbid);

    // Change the separator
    vb->updateFromManifest({R"({"separator": "@@",
              "collections":[{"name":"$default", "uid":"0"}]})"});
    // Change the separator
    vb->updateFromManifest({R"({"separator": ":",
              "collections":[{"name":"$default", "uid":"0"}]})"});
    // Change the separator
    vb->updateFromManifest({R"({"separator": ",",
              "collections":[{"name":"$default", "uid":"0"}]})"});
    // Add a collection
    vb->updateFromManifest({R"({"separator": ",",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"meat", "uid":"1"}]})"});

    // All the changes will be collapsed into one update and we will expect
    // to see , as the separator once DCP steps through the checkpoint

    // The producer should start with the initial separator
    EXPECT_EQ("::", producer->getCurrentSeparatorForStream(vbid));

    notifyAndStepToCheckpoint();

    auto replica = store->getVBucket(replicaVB);

    // The replica should have the :: separator
    EXPECT_EQ("::", replica->lockCollections().getSeparator());

    std::array<std::string, 3> expectedData = {{"@@", ":", ","}};
    for (auto expected : expectedData) {
        // Now step the producer to transfer the separator
        EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

        // The producer should now have the new separator
        EXPECT_EQ(expected, producer->getCurrentSeparatorForStream(vbid));

        // The replica should now have the new separator
        EXPECT_EQ(expected, replica->lockCollections().getSeparator());
    }

    // Now step the producer to transfer the create "meat"
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // Collection should now be live on the replica with the final separator
    EXPECT_TRUE(replica->lockCollections().doesKeyContainValidCollection(
            {"meat,bacon", DocNamespace::Collections}));

    // And done
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
}

class CollectionsManagerTest : public CollectionsTest {};

/**
 * Test checks that setCollections propagates the collection data to active
 * vbuckets.
 */
TEST_F(CollectionsManagerTest, basic) {
    // Add some more VBuckets just so there's some iteration happening
    const int extraVbuckets = 2;
    for (int vb = vbid + 1; vb <= (vbid + extraVbuckets); vb++) {
        store->setVBucketState(vb, vbucket_state_active, false);
    }

    store->setCollections({R"({"separator": "@@",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"meat", "uid":"1"}]})"});

    // Check all vbuckets got the collections
    for (int vb = vbid; vb <= (vbid + extraVbuckets); vb++) {
        auto vbp = store->getVBucket(vb);
        EXPECT_EQ("@@", vbp->lockCollections().getSeparator());
        EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                {"meat@@bacon", DocNamespace::Collections}));
        EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                {"anykey", DocNamespace::DefaultCollection}));
    }
}

/**
 * Test checks that setCollections propagates the collection data to active
 * vbuckets and not the replicas
 */
TEST_F(CollectionsManagerTest, basic2) {
    // Add some more VBuckets just so there's some iteration happening
    const int extraVbuckets = 2;
    // Add active and replica
    for (int vb = vbid + 1; vb <= (vbid + extraVbuckets); vb++) {
        if (vb & 1) {
            store->setVBucketState(vb, vbucket_state_active, false);
        } else {
            store->setVBucketState(vb, vbucket_state_replica, false);
        }
    }

    store->setCollections({R"({"separator": "@@",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"meat", "uid":"1"}]})"});

    // Check all vbuckets got the collections
    for (int vb = vbid; vb <= (vbid + extraVbuckets); vb++) {
        auto vbp = store->getVBucket(vb);
        if (vbp->getState() == vbucket_state_active) {
            EXPECT_EQ("@@", vbp->lockCollections().getSeparator());
            EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                    {"meat@@bacon", DocNamespace::Collections}));
            EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                    {"anykey", DocNamespace::DefaultCollection}));
        } else {
            // Replica will be in default constructed settings
            EXPECT_EQ("::", vbp->lockCollections().getSeparator());
            EXPECT_FALSE(vbp->lockCollections().doesKeyContainValidCollection(
                    {"meat@@bacon", DocNamespace::Collections}));
            EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                    {"anykey", DocNamespace::DefaultCollection}));
        }
    }
}

class CollectionsFilteredDcpErrorTest : public CollectionsTest {
public:
    CollectionsFilteredDcpErrorTest() : cookieP(create_mock_cookie()) {
    }
    void SetUp() override {
        CollectionsTest::SetUp();
    }

    void TearDown() override {
        destroy_mock_cookie(cookieP);
        producer.reset();
        SingleThreadedKVBucketTest::TearDown();
    }

protected:
    SingleThreadedRCPtr<MockDcpProducer> producer;
    const void* cookieP;
};

TEST_F(CollectionsFilteredDcpErrorTest, error1) {
    // Set some collections
    store->setCollections({R"({"separator": "@@",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"meat", "uid":"1"},
                             {"name":"dairy", "uid":"2"}]})"});

    std::string filter = R"({"collections":["fruit"]})";
    cb::const_byte_buffer buffer{
            reinterpret_cast<const uint8_t*>(filter.data()), filter.size()};
    // Can't create a filter for unknown collections
    EXPECT_THROW(std::make_unique<MockDcpProducer>(*engine,
                                                   cookieP,
                                                   "test_producer",
                                                   DCP_OPEN_COLLECTIONS,
                                                   buffer,
                                                   false /*startTask*/),
                 std::invalid_argument);
}

TEST_F(CollectionsFilteredDcpErrorTest, error2) {
    // Set some collections
    store->setCollections({R"({"separator": "::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"meat", "uid":"1"},
                             {"name":"dairy", "uid":"2"}]})"});

    std::string filter = R"({"collections":["meat"]})";
    cb::const_byte_buffer buffer{
            reinterpret_cast<const uint8_t*>(filter.data()), filter.size()};
    // Can't create a filter for unknown collections
    producer = std::make_unique<MockDcpProducer>(*engine,
                                                 cookieP,
                                                 "test_producer",
                                                 DCP_OPEN_COLLECTIONS,
                                                 buffer,
                                                 false /*startTask*/);
    producer->setNoopEnabled(true);

    // Remove meat
    store->setCollections({R"({"separator": "::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"dairy", "uid":"2"}]})"});

    // Now should be prevented from creating a new stream
    uint64_t rollbackSeqno = 0;
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION,
              producer->streamRequest(0, // flags
                                      1, // opaque
                                      vbid,
                                      0, // start_seqno
                                      ~0ull, // end_seqno
                                      0, // vbucket_uuid,
                                      0, // snap_start_seqno,
                                      0, // snap_end_seqno,
                                      &rollbackSeqno,
                                      &CollectionsDcpTest::dcpAddFailoverLog));
}

class CollectionsFilteredDcpTest : public CollectionsDcpTest {
public:
    CollectionsFilteredDcpTest() : CollectionsDcpTest() {

    }

    void SetUp() override {
        CollectionsTest::SetUp();
        producers = get_dcp_producers(
                reinterpret_cast<ENGINE_HANDLE*>(engine.get()),
                reinterpret_cast<ENGINE_HANDLE_V1*>(engine.get()));
    }
};

TEST_F(CollectionsFilteredDcpTest, filtering) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy via the bucket level (filters are
    // worked out from the bucket manifest)
    store->setCollections({R"({"separator": "::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"meat", "uid":"1"},
                             {"name":"dairy", "uid":"2"}]})"});
    // Setup filtered DCP
    createDcpObjects(R"({"collections":["dairy"]})", true);

    // MB-24572, this notify an step gets us to an empty checkpoint as a create
    // was dropped.
    notifyAndStepToCheckpoint();

    // MB-24572, an extra step is needed to get past the next empty checkpoint
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER, dcp_last_op);

    // SystemEvent createCollection
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT, dcp_last_op);
    EXPECT_EQ("dairy", dcp_last_key);

    // Store collection documents
    std::array<std::string, 2> expectedKeys = {{"dairy::one", "dairy::two"}};
    store_item(vbid, {"meat::one", DocNamespace::Collections}, "value");
    store_item(vbid, {expectedKeys[0], DocNamespace::Collections}, "value");
    store_item(vbid, {"meat::two", DocNamespace::Collections}, "value");
    store_item(vbid, {expectedKeys[1], DocNamespace::Collections}, "value");
    store_item(vbid, {"meat::three", DocNamespace::Collections}, "value");

    auto vb0Stream = producer->findStream(0);
    ASSERT_NE(nullptr, vb0Stream.get());

    notifyAndStepToCheckpoint();

    // Now step DCP to transfer keys, only two keys are expected as all "meat"
    // keys are filtered
    for (auto& key : expectedKeys) {
        EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
        EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_MUTATION, dcp_last_op);
        EXPECT_EQ(key, dcp_last_key);
    }
    // And no more
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
}

TEST_F(CollectionsFilteredDcpTest, default_only) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy via the bucket level (filters are
    // worked out from the bucket manifest)
    store->setCollections({R"({"separator": "::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"meat", "uid":"1"},
                             {"name":"dairy", "uid":"2"}]})"});
    // Setup DCP
    createDcpObjects({/*no filter*/}, false /*don't know about collections*/);

    // MB-24572, this notify an step gets us to an empty checkpoint as a create
    // was dropped.
    notifyAndStepToCheckpoint();

    // MB-24572, an extra step is needed to get past the next empty checkpoint
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER, dcp_last_op);

    // Store collection documents and one default collection document
    store_item(vbid, {"meat::one", DocNamespace::Collections}, "value");
    store_item(vbid, {"dairy::one", DocNamespace::Collections}, "value");
    store_item(vbid, {"anykey", DocNamespace::DefaultCollection}, "value");
    store_item(vbid, {"dairy::two", DocNamespace::Collections}, "value");
    store_item(vbid, {"meat::three", DocNamespace::Collections}, "value");

    auto vb0Stream = producer->findStream(0);
    ASSERT_NE(nullptr, vb0Stream.get());

    // Now step into the items of which we expect to see only anykey
    notifyAndStepToCheckpoint();

    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_MUTATION, dcp_last_op);
    EXPECT_EQ("anykey", dcp_last_key);

    // And no more
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
}

TEST_F(CollectionsFilteredDcpTest, stream_closes) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat via the bucket level (filters are worked out
    // from the bucket manifest)
    store->setCollections({R"({"separator": "::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"meat", "uid":"1"}]})"});
    // Setup filtered DCP
    createDcpObjects(R"({"collections":["meat"]})", true);

    auto vb0Stream = producer->findStream(0);
    ASSERT_NE(nullptr, vb0Stream.get());

    notifyAndStepToCheckpoint();

    // Now step DCP to transfer system events. We expect that the stream will
    // close once we transfer DeleteCollection

    // Now step the producer to transfer the collection creation
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // Not dead yet...
    EXPECT_TRUE(vb0Stream->isActive());

    // Perform a delete of meat via the bucket level (filters are worked out
    // from the bucket manifest)
    store->setCollections({R"({"separator": "::",
              "collections":[{"name":"$default", "uid":"0"}]})"});

    notifyAndStepToCheckpoint();

    // Now step the producer to transfer the collection deletion
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // Done... collection deletion of meat has closed the stream
    EXPECT_FALSE(vb0Stream->isActive());

    // Now step the producer to transfer the close stream
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // And no more
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
}
