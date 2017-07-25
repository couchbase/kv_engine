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
#include "dcp/dcpconnmap.h"
#include "kvstore.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_global_task.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/evp_store_test.h"
#include "tests/module_tests/thread_gate.h"

#include <boost/optional/optional.hpp>

#include <functional>
#include <thread>

class CollectionsTest : public EPBucketTest {
public:
    void SetUp() override {
        // Enable collections (which will enable namespace persistence).
        config_string += "collections_prototype_enabled=true";
        EPBucketTest::SetUp();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active, false);
    }
};

TEST_F(CollectionsTest, namespace_separation) {
    store_item(vbid,
               {"$collections::create:meat1", DocNamespace::DefaultCollection},
               "value");
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the meat collection
    vb->updateFromManifest(
            {R"({"revision":1,)"
             R"("separator":"::","collections":["$default","meat"]})"});
    // Trigger a flush to disk. Flushes the meat create event and 1 item
    flush_vbucket_to_disk(vbid, 2);

    // evict and load - should not see the system key for create collections
    evict_key(vbid,
              {"$collections::create:meat1", DocNamespace::DefaultCollection});
    get_options_t options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    GetValue gv = store->get(
            {"$collections::create:meat1", DocNamespace::DefaultCollection},
            vbid,
            cookie,
            options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    // Manually run the BGFetcher task; to fetch the two outstanding
    // requests (for the same key).
    runBGFetcherTask();

    gv = store->get(
            {"$collections::create:meat1", DocNamespace::DefaultCollection},
            vbid,
            cookie,
            options);
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
    vb->updateFromManifest(
            {"{\"revision\":1, "
             "\"separator\":\"::\",\"collections\":[\"$default\",\"meat\"]}"});

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
    vb->updateFromManifest(
            {"{\"revision\":2, "
             "\"separator\":\"::\",\"collections\":[\"$default\"]}"});

    // Note that nothing is flushed because a begin delete doesn't generate
    // an Item.
    flush_vbucket_to_disk(vbid, 0);

    // Access denied (although the item still exists)
    gv = store->get(
            {"meat::beef", DocNamespace::Collections}, vbid, cookie, options);
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION, gv.getStatus());
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
                                         int revision,
                                         int items);

    std::string getManifest();

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
    return getManifest();
}

std::string CollectionsFlushTest::deleteCollectionAndFlush(
        const std::string& json, const std::string& collection, int items) {
    VBucketPtr vb = store->getVBucket(vbid);
    storeItems(collection, DocNamespace::Collections, items);
    vb->updateFromManifest(json);
    flush_vbucket_to_disk(vbid, items); // only flush items
    return getManifest();
}

std::string CollectionsFlushTest::completeDeletionAndFlush(
        const std::string& collection, int revision, int items) {
    VBucketPtr vb = store->getVBucket(vbid);
    vb->completeDeletion(collection, revision);
    storeItems("defaultcollection", DocNamespace::DefaultCollection, items);
    flush_vbucket_to_disk(vbid, 1 + items); // delete event + items
    return getManifest();
}

std::string CollectionsFlushTest::getManifest() {
    VBucketPtr vb = store->getVBucket(vbid);
    return vb->getShard()->getRWUnderlying()->getCollectionsManifest(vbid);
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
             std::bind(
                     &CollectionsFlushTest::createCollectionAndFlush,
                     this,
                     R"({"revision":1,"separator":"::","collections":["$default","meat"]})",
                     "meat",
                     items),
             std::bind(&CollectionsFlushTest::canWrite, _1, "meat")},

            {// 1
             std::bind(
                     &CollectionsFlushTest::deleteCollectionAndFlush,
                     this,
                     R"({"revision":2,"separator":"::","collections":["$default"]})",
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
             std::bind(
                     &CollectionsFlushTest::createCollectionAndFlush,
                     this,
                     R"({"revision":3,"separator":"::","collections":["$default","fruit"]})",
                     "fruit",
                     items),
             std::bind(&CollectionsFlushTest::canWrite, _1, "fruit")},
            {// 4
             std::bind(
                     &CollectionsFlushTest::deleteCollectionAndFlush,
                     this,
                     R"({"revision":4,"separator":"::","collections":["$default"]})",
                     "fruit",
                     items),
             std::bind(&CollectionsFlushTest::cannotWrite, _1, "fruit")},
            {// 5
             std::bind(
                     &CollectionsFlushTest::createCollectionAndFlush,
                     this,
                     R"({"revision":5,"separator":"::","collections":["$default","fruit"]})",
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

class CollectionsThreadTest {
public:
    CollectionsThreadTest(CollectionsTest& t,
                          VBucket& vbucket,
                          int sets,
                          int collectionLoops)
        : test(t),
          vb(vbucket),
          setCount(sets),
          createDeleteCount(collectionLoops),
          threadGate(2) {
    }

    // Create and delete a collection over and over
    void createDeleteCollection() {
        threadGate.threadUp();
        int revision = 1;
        for (int iterations = 0; iterations < createDeleteCount; iterations++) {
            vb.updateFromManifest({R"({"revision":)" +
                                   std::to_string(revision) +
                                   R"(,"separator":"::",)"
                                   R"("collections":["fruit"]})"});

            revision++;

            vb.updateFromManifest({R"({"revision":)" +
                                   std::to_string(revision) +
                                   R"(,"separator":"::",)"
                                   R"("collections":[]})"});
        }
    }

    // Keep setting documents in the collection, expect SUCCESS or
    // UNKNOWN_COLLECTION
    void setDocuments() {
        threadGate.threadUp();
        for (int iterations = 0; iterations < setCount; iterations++) {
            StoredDocKey key("fruit::key" + std::to_string(iterations),
                             DocNamespace::Collections);
            test.store_item(vb.getId(),
                            key,
                            "value",
                            0,
                            {cb::engine_errc::success,
                             cb::engine_errc::unknown_collection});
        }
    }

    void run() {
        t1 = std::thread(&CollectionsThreadTest::createDeleteCollection, this);
        t2 = std::thread(&CollectionsThreadTest::setDocuments, this);
        t1.join();
        t2.join();
    }

private:
    CollectionsTest& test;
    VBucket& vb;
    int setCount;
    int createDeleteCount;
    ThreadGate threadGate;
    std::thread t1;
    std::thread t2;
};

//
// Test that a vbucket's checkpoint is correctly ordered with collection
// events and documents. I.e. a document must never be found before the create
// or after a delete.
//
TEST_F(CollectionsTest, checkpoint_consistency) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsThreadTest threadTest(*this, *vb, 256, 256);
    threadTest.run();

    // Now get the VB checkpoint and validate the collection/item ordering
    std::vector<queued_item> items;
    vb->checkpointManager.getAllItemsForCursor(CheckpointManager::pCursorName,
                                               items);

    ASSERT_FALSE(items.empty());
    bool open = false;
    boost::optional<int64_t> seqno{};
    for (const auto& item : items) {
        if (!(item->getOperation() == queue_op::system_event ||
              item->getOperation() == queue_op::set)) {
            // Ignore all the checkpoint start/end stuff
            continue;
        }
        if (seqno) {
            EXPECT_LT(seqno.value(), item->getBySeqno());
        }
        // If this is a CreateCollection on fruit, open = true
        if (item->getOperation() == queue_op::system_event &&
            SystemEvent::CreateCollection == SystemEvent(item->getFlags()) &&
            std::strstr(item->getKey().c_str(), "fruit")) {
            open = true;
        }
        // If this is a BeginDeleteCollection on fruit, open = false (i.e.
        // ignore delete of $default)
        if (item->getOperation() == queue_op::system_event &&
            SystemEvent::BeginDeleteCollection ==
                    SystemEvent(item->getFlags()) &&
            std::strstr(item->getKey().c_str(), "fruit")) {
            open = false;
        }
        if (item->getOperation() == queue_op::set) {
            EXPECT_TRUE(open);
        }
        seqno = item->getBySeqno();
    }
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
        vb->updateFromManifest(
            {R"({"revision":1,"separator":"-+-","collections":["$default","meat"]})"});

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

TEST_F(CollectionsTest, test_dcp_consumer) {
    const void* cookie = create_mock_cookie();

    SingleThreadedRCPtr<MockDcpConsumer> consumer(
            new MockDcpConsumer(*engine, cookie, "test_consumer"));

    store->setVBucketState(vbid, vbucket_state_replica, false);
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0));

    std::string collection = "meat";

    uint32_t revision = 4;
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
                      {reinterpret_cast<const uint8_t*>(&revision),
                       sizeof(uint32_t)}));

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
                      {reinterpret_cast<const uint8_t*>(&revision),
                       sizeof(uint32_t)}));

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
        destroy_mock_cookie(cookieC);
        destroy_mock_cookie(cookieP);
        consumer->closeAllStreams();
        consumer->cancelTask();
        producer->closeAllStreams();
        producer.reset();
        consumer.reset();
        EPBucketTest::TearDown();
    }

    static const uint16_t replicaVB{1};
    static SingleThreadedRCPtr<MockDcpConsumer> consumer;

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
    vb->updateFromManifest(
            {R"({"revision":1,"separator":"::","collections":["$default","meat"]})"});

    vb->updateFromManifest(
            {R"({"revision":2,"separator":"::","collections":["$default"]})"});

    vb->completeDeletion("meat", 2);

    producer->notifySeqnoAvailable(vb->getId(), vb->getHighSeqno());

    // Step which will notify the snapshot task
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    EXPECT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());

    // Now call run on the snapshot task to move checkpoint into DCP stream
    producer->getCheckpointSnapshotTask().run();

    // Next step which will process a snapshot marker
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    VBucketPtr replica = store->getVBucket(replicaVB);

    // 1. Replica does not know about meat
    EXPECT_FALSE(vb->lockCollections().doesKeyContainValidCollection(
            {"meat::bacon", DocNamespace::Collections}));

    // Now step the producer to transfer the collection creation
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // 1. Replica now knows the collection
    EXPECT_TRUE(replica->lockCollections().doesKeyContainValidCollection(
            {"meat::bacon", DocNamespace::Collections}));

    // Now step the producer to transfer the collection deletion
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // 3. Replica does now blocking access to meat
    EXPECT_FALSE(replica->lockCollections().doesKeyContainValidCollection(
            {"meat::bacon", DocNamespace::Collections}));

    // Now step the producer, no more collection events
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
}

TEST_F(CollectionsDcpTest, test_dcp_separator) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Change the separator
    vb->updateFromManifest(
            {R"({"revision":1,"separator":"@@","collections":["$default"]})"});

    // Add a collection
    vb->updateFromManifest(
            {R"({"revision":2,"separator":"@@","collections":["$default","meat"]})"});

    producer->notifySeqnoAvailable(vb->getId(), vb->getHighSeqno());

    // Step which will notify the snapshot task
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    // The producer should start with the old separator
    EXPECT_EQ("::", producer->getCurrentSeparatorForStream(vbid));

    EXPECT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());

    // Now call run on the snapshot task to move checkpoint into DCP stream
    // this will trigger the stream to update the separator
    producer->getCheckpointSnapshotTask().run();

    // Next step which should process a snapshot marker
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

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
    vb->updateFromManifest(
            {R"({"revision":1,"separator":"@@","collections":["$default"]})"});
    // Change the separator
    vb->updateFromManifest(
            {R"({"revision":2,"separator":":","collections":["$default"]})"});
    // Change the separator
    vb->updateFromManifest(
            {R"({"revision":3,"separator":",","collections":["$default"]})"});
    // Add a collection
    vb->updateFromManifest(
            {R"({"revision":4,"separator":",","collections":["$default","meat"]})"});

    // All the changes will be collapsed into one update and we will expect
    // to see , as the separator once DCP steps through the checkpoint
    producer->notifySeqnoAvailable(vb->getId(), vb->getHighSeqno());

    // Step which will notify the snapshot task
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    // The producer should start with the initial separator
    EXPECT_EQ("::", producer->getCurrentSeparatorForStream(vbid));

    EXPECT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());

    // Now call run on the snapshot task to move checkpoint into DCP stream
    // this will trigger the stream to update the separator
    producer->getCheckpointSnapshotTask().run();

    // Next step which should process a snapshot marker
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

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

    store->setCollections(
            {R"({"revision":1,"separator":"@@","collections":["$default", "meat"]})"});

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

    store->setCollections(
            {R"({"revision":1,"separator":"@@","collections":["$default", "meat"]})"});

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
        EPBucketTest::TearDown();
    }

protected:
    SingleThreadedRCPtr<MockDcpProducer> producer;
    const void* cookieP;
};

TEST_F(CollectionsFilteredDcpErrorTest, error1) {
    // Set some collections
    store->setCollections(
            {R"({"revision":1,"separator":"::","collections":["$default", "meat", "dairy"]})"});

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
    store->setCollections(
            {R"({"revision":1,"separator":"::","collections":["$default", "meat", "dairy"]})"});

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

    // Remove dairy
    store->setCollections(
            {R"({"revision":2,"separator":"::","collections":["$default", "dairy"]})"});

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
    store->setCollections(
            {R"({"revision":1,"separator":"::","collections":["$default","meat", "dairy"]})"});

    // Setup filtered DCP
    createDcpObjects(R"({"collections":["dairy"]})", true);

    // Store collection documents
    std::array<std::string, 2> expectedKeys = {{"dairy::one", "dairy::two"}};
    store_item(vbid, {"meat::one", DocNamespace::Collections}, "value");
    store_item(vbid, {expectedKeys[0], DocNamespace::Collections}, "value");
    store_item(vbid, {"meat::two", DocNamespace::Collections}, "value");
    store_item(vbid, {expectedKeys[1], DocNamespace::Collections}, "value");
    store_item(vbid, {"meat::three", DocNamespace::Collections}, "value");

    auto vb0Stream = producer->findStream(0);
    ASSERT_NE(nullptr, vb0Stream.get());

    producer->notifySeqnoAvailable(vb->getId(), vb->getHighSeqno());

    // Step which will notify the snapshot task
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    EXPECT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());

    // Now call run on the snapshot task to move checkpoint into DCP stream
    producer->getCheckpointSnapshotTask().run();

    // snapshot marker
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER, dcp_last_op);

    // SystemEvent createCollection
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT, dcp_last_op);
    EXPECT_EQ("dairy", dcp_last_key);

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
    store->setCollections(
            {R"({"revision":1,"separator":"::","collections":["$default","meat", "dairy"]})"});

    // Setup DCP
    createDcpObjects({/*no filter*/}, false /*don't know about collections*/);

    // Store collection documents and one default collection document
    store_item(vbid, {"meat::one", DocNamespace::Collections}, "value");
    store_item(vbid, {"dairy::one", DocNamespace::Collections}, "value");
    store_item(vbid, {"anykey", DocNamespace::DefaultCollection}, "value");
    store_item(vbid, {"dairy::two", DocNamespace::Collections}, "value");
    store_item(vbid, {"meat::three", DocNamespace::Collections}, "value");

    auto vb0Stream = producer->findStream(0);
    ASSERT_NE(nullptr, vb0Stream.get());

    producer->notifySeqnoAvailable(vb->getId(), vb->getHighSeqno());

    // Step which will notify the snapshot task
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    EXPECT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());

    // Now call run on the snapshot task to move checkpoint into DCP stream
    producer->getCheckpointSnapshotTask().run();

    // snapshot marker
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER, dcp_last_op);

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
    store->setCollections(
            {R"({"revision":1,"separator":"::","collections":["$default","meat"]})"});

    // Setup filtered DCP
    createDcpObjects(R"({"collections":["meat"]})", true);

    auto vb0Stream = producer->findStream(0);
    ASSERT_NE(nullptr, vb0Stream.get());

    // Perform a delete of meat via the bucket level (filters are worked out
    // from the bucket manifest)
    store->setCollections(
            {R"({"revision":2,"separator":"::","collections":["$default"]})"});

    producer->notifySeqnoAvailable(vb->getId(), vb->getHighSeqno());

    // Now step DCP to transfer system events. We expect that the stream will
    // close once we transfer DeleteCollection

    EXPECT_NE(nullptr, consumer->getVbucketStream(1).get());

    // Step which will notify the snapshot task
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    EXPECT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());

    // Now call run on the snapshot task to move checkpoint into DCP stream
    producer->getCheckpointSnapshotTask().run();

    // Next step which will process a snapshot marker
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // Now step the producer to transfer the collection creation
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // Not dead yet...
    EXPECT_TRUE(vb0Stream->isActive());

    // Now step the producer to transfer the collection deletion
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // Done... collection deletion of meat has closed the stream
    EXPECT_FALSE(vb0Stream->isActive());

    // Now step the producer to transfer the close stream
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    // And no more
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
}
