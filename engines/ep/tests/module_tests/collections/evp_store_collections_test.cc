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
#include "ep_time.h"
#include "kvstore.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/mock/mock_global_task.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/evp_store_test.h"
#include "tests/module_tests/test_helpers.h"

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
TEST_F(CollectionsTest, MB_25344) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    vb->updateFromManifest({R"({"separator":"::",
                 "collections":[{"name":"$default", "uid":"0"},
                                {"name":"dairy", "uid":"1"}]})"});
    // Trigger a flush to disk. Flushes the dairy create event.
    flush_vbucket_to_disk(vbid, 1);

    auto item1 = make_item(
            vbid, {"dairy::milk", DocNamespace::Collections}, "creamy", 0, 0);
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item1, nullptr));
    flush_vbucket_to_disk(vbid, 1);

    auto item2 = make_item(
            vbid, {"dairy::cream", DocNamespace::Collections}, "creamy", 0, 0);
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item2, nullptr));
    flush_vbucket_to_disk(vbid, 1);

    // Delete the dairy collection (so all dairy keys become logically deleted)
    vb->updateFromManifest({R"({"separator":"::",
                 "collections":[{"name":"$default", "uid":"0"}]})"});

    // Re-add the dairy collection
    vb->updateFromManifest({R"({"separator":"::",
                 "collections":[{"name":"$default", "uid":"0"},
                                {"name":"dairy", "uid":"2"}]})"});

    // Trigger a flush to disk. Flushes the dairy create event.
    flush_vbucket_to_disk(vbid, 1);

    // Expect that we can add item1 again, even though it is still present
    item1.setCas(0);
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item1, nullptr));

    // Replace should fail
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->replace(item2, nullptr));

    // Delete should fail
    uint64_t cas = 0;
    EXPECT_EQ(ENGINE_KEY_ENOENT,
              store->deleteItem(
                      item2.getKey(), cas, vbid, nullptr, nullptr, nullptr));

    // Unlock should fail enoent rather than an unlock error
    EXPECT_EQ(ENGINE_KEY_ENOENT,
              store->unlockKey(item2.getKey(), vbid, 0, ep_current_time()));
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
        const std::string& collection, int items) {
    VBucketPtr vb = store->getVBucket(vbid);
    vb->completeDeletion(collection);
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
        vb->completeDeletion("dairy");

        flush_vbucket_to_disk(vbid, 0); // 0 items but has written _local

        highSeqno = vb->getHighSeqno();
    } // VBucketPtr scope ends
    resetEngineAndWarmup();

    auto vb = store->getVBucket(vbid);
    EXPECT_GT(highSeqno, vb->getHighSeqno());
}

//
// Create a collection then create a second engine which will warmup from the
// persisted collection state and should have the collection accessible.
//
TEST_F(CollectionsWarmupTest, warmupIgnoreLogicallyDeleted) {
    {
        auto vb = store->getVBucket(vbid);

        // Add the meat collection
        vb->updateFromManifest({R"({"separator":"::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"meat","uid":"1"}]})"});

        // Trigger a flush to disk. Flushes the meat create event
        flush_vbucket_to_disk(vbid, 1);
        const int nitems = 10;
        for (int ii = 0; ii < nitems; ii++) {
            // Now we can write to beef
            std::string key = "meat::" + std::to_string(ii);
            store_item(vbid, {key, DocNamespace::Collections}, "value");
        }

        flush_vbucket_to_disk(vbid, nitems);

        // Remove the meat collection
        vb->updateFromManifest({R"({"separator":"::",
              "collections":[{"name":"$default", "uid":"0"}]})"});

        flush_vbucket_to_disk(vbid, 1);

        EXPECT_EQ(nitems, vb->ht.getNumInMemoryItems());
    } // VBucketPtr scope ends

    resetEngineAndWarmup();

    EXPECT_EQ(0, store->getVBucket(vbid)->ht.getNumInMemoryItems());
}

//
// Create a collection then create a second engine which will warmup from the
// persisted collection state and should have the collection accessible.
//
TEST_F(CollectionsWarmupTest, warmupIgnoreLogicallyDeletedDefault) {
    {
        auto vb = store->getVBucket(vbid);

        // Add the meat collection
        vb->updateFromManifest({R"({"separator":"::",
              "collections":[{"name":"$default", "uid":"0"},
                             {"name":"meat","uid":"1"}]})"});

        // Trigger a flush to disk. Flushes the meat create event
        flush_vbucket_to_disk(vbid, 1);
        const int nitems = 10;
        for (int ii = 0; ii < nitems; ii++) {
            std::string key = "key" + std::to_string(ii);
            store_item(vbid, {key, DocNamespace::DefaultCollection}, "value");
        }

        flush_vbucket_to_disk(vbid, nitems);

        // Remove the default collection
        vb->updateFromManifest({R"({"separator":"::",
              "collections":[{"name":"meat", "uid":"1"}]})"});

        flush_vbucket_to_disk(vbid, 1);

        EXPECT_EQ(nitems, vb->ht.getNumInMemoryItems());
    } // VBucketPtr scope ends

    resetEngineAndWarmup();

    EXPECT_EQ(0, store->getVBucket(vbid)->ht.getNumInMemoryItems());
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