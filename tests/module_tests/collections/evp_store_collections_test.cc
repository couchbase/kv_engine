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
#include "programs/engine_testapp/mock_server.h"
#include "tests/mock/mock_global_task.h"
#include "tests/module_tests/evp_store_test.h"
#include "tests/module_tests/thread_gate.h"

#include <boost/optional/optional.hpp>

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
    RCPtr<VBucket> vb = store->getVBucket(vbid);
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
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

    gv = store->get(
            {"$collections::create:meat1", DocNamespace::DefaultCollection},
            vbid,
            cookie,
            options);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ(0,
              strncmp("value",
                      gv.getValue()->getData(),
                      gv.getValue()->getNBytes()));
    delete gv.getValue();
}

TEST_F(CollectionsTest, collections_basic) {
    // Default collection is open for business
    store_item(vbid, {"key", DocNamespace::DefaultCollection}, "value");
    store_item(vbid,
               {"meat::beef", DocNamespace::Collections},
               "value",
               0,
               {cb::engine_errc::unknown_collection});

    RCPtr<VBucket> vb = store->getVBucket(vbid);

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
    delete gv.getValue();

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
    void collectionsFlusher(int items);

private:
    void createCollectionAndFlush(const std::string& json,
                                  const std::string& collection,
                                  int items);
    void deleteCollectionAndFlush(const std::string& json,
                                  const std::string& collection,
                                  int items);
    void completeDeletionAndFlush(const std::string& collection,
                                  int revision,
                                  int items);
    void storeItems(const std::string& collection, DocNamespace ns, int items);
};

void CollectionsFlushTest::storeItems(const std::string& collection,
                                      DocNamespace ns,
                                      int items) {
    for (int ii = 0; ii < items; ii++) {
        std::string key = collection + "::" + std::to_string(ii);
        store_item(vbid, {key, ns}, "value");
    }
}

void CollectionsFlushTest::createCollectionAndFlush(
        const std::string& json, const std::string& collection, int items) {
    RCPtr<VBucket> vb = store->getVBucket(vbid);
    vb->updateFromManifest(json);
    storeItems(collection, DocNamespace::Collections, items);
    flush_vbucket_to_disk(vbid, 1 + items); // create event + items
}

void CollectionsFlushTest::deleteCollectionAndFlush(
        const std::string& json, const std::string& collection, int items) {
    RCPtr<VBucket> vb = store->getVBucket(vbid);
    storeItems(collection, DocNamespace::Collections, items);
    vb->updateFromManifest(json);
    flush_vbucket_to_disk(vbid, items); // only flush items
}

void CollectionsFlushTest::completeDeletionAndFlush(
        const std::string& collection, int revision, int items) {
    RCPtr<VBucket> vb = store->getVBucket(vbid);
    vb->completeDeletion(collection, revision);
    storeItems("defaultcollection", DocNamespace::DefaultCollection, items);
    flush_vbucket_to_disk(vbid, 1 + items); // delete event + items
}

// Drive collection manifest changes and count how many items get flushed
void CollectionsFlushTest::collectionsFlusher(int items) {
    createCollectionAndFlush(
            {R"({"revision":1,"separator":"::","collections":["$default","meat"]})"},
            "meat",
            items);

    deleteCollectionAndFlush(
            {R"({"revision":2,"separator":"::","collections":["$default"]})"},
            "meat",
            items);

    completeDeletionAndFlush("meat", 2, items);

    createCollectionAndFlush(
            {R"({"revision":3,"separator":"::","collections":["$default","fruit"]})"},
            "fruit",
            items);

    deleteCollectionAndFlush(
            {R"({"revision":4,"separator":"::","collections":["$default"]})"},
            "fruit",
            items);

    createCollectionAndFlush(
            {R"({"revision":5,"separator":"::","collections":["$default","fruit"]})"},
            "fruit",
            items);

    completeDeletionAndFlush("fruit", 4, items);
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
    RCPtr<VBucket> vb = store->getVBucket(vbid);
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

//
// Create a collection then create a second engine which will warmup from the
// persisted collection state and should have the collection accessible.
//
TEST_F(CollectionsTest, warmup) {
    RCPtr<VBucket> vb = store->getVBucket(vbid);

    // Add the meat collection
    vb->updateFromManifest(
        {R"({"revision":1,"separator":"-+-","collections":["$default","meat"]})"});

    // Trigger a flush to disk. Flushes the meat create event and 1 item
    flush_vbucket_to_disk(vbid, 1);

    // Now we can write to beef
    store_item(vbid, {"meat-+-beef", DocNamespace::Collections}, "value");
    // But not dairy
    store_item(vbid,
               {"dairy-+-milk", DocNamespace::Collections},
               "value",
               0,
               {cb::engine_errc::unknown_collection});

    flush_vbucket_to_disk(vbid, 1);

    // Create a second engine and warmup - should come up with the collection
    // enabled as it loads the manifest from disk

    // Note we now create an EventuallyPersistentEngine as we need warmup to
    // complete. This engine will manage the warmup tasks itself.
    ENGINE_HANDLE* h;
    EXPECT_EQ(ENGINE_SUCCESS, create_instance(1, get_mock_server_api, &h))
            << "Failed to create ep engine instance";
    EXPECT_EQ(1, h->interface) << "Unexpected engine handle version";

    std::unique_ptr<EventuallyPersistentEngine> engine2(
            reinterpret_cast<EventuallyPersistentEngine*>(h));
    ObjectRegistry::onSwitchThread(engine2.get());

    // Add dbname to config string.
    std::string config = config_string;
    if (config.size() > 0) {
        config += ";";
    }
    config += "dbname=" + std::string(test_dbname);
    EXPECT_EQ(ENGINE_SUCCESS, engine2->initialize(config.c_str()))
            << "Failed to initialize engine2.";

    // Wait for warmup to complete.
    while (engine2->getKVBucket()->isWarmingUp()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    {
        Item item({"meat-+-beef", DocNamespace::Collections},
                  /*flags*/ 0,
                  /*exp*/ 0,
                  "rare",
                  sizeof("rare"));
        item.setVBucketId(vbid);
        uint64_t cas;
        EXPECT_EQ(ENGINE_SUCCESS,
                  engine2->store(nullptr, &item, &cas, OPERATION_SET));
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
                  engine2->store(nullptr, &item, &cas, OPERATION_SET));
    }

    // Because we manually created a new engine we must manually bring it down
    engine2->destroy(true);

    // Force the callbacks whilst engine2 is still valid
    destroy_mock_event_callbacks();

    ObjectRegistry::onSwitchThread(nullptr);
}
