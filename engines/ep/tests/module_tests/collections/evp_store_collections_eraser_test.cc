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

#include "tests/module_tests/evp_store_single_threaded_test.h"

class CollectionsEraserTest
        : public SingleThreadedKVBucketTest,
          public ::testing::WithParamInterface<std::string> {
public:
    void SetUp() override {
        // Enable collections (which will enable namespace persistence).
        config_string += "collections_prototype_enabled=true;";
        config_string += GetParam();
        SingleThreadedKVBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
        vb = store->getVBucket(vbid);
    }

    void TearDown() override {
        vb.reset();
        SingleThreadedKVBucketTest::TearDown();
    }

    void runEraser() {
        runCompaction();
    }

    bool isFullEviction() const {
        return GetParam().find("item_eviction_policy=full_eviction") !=
               std::string::npos;
    }

    VBucketPtr vb;
};

// Small numbers of items for easier debug
TEST_P(CollectionsEraserTest, basic) {
    // add a collection
    vb->updateFromManifest({R"({"separator":":","uid":"0",)"
                            R"("collections":[{"name":"$default", "uid":"0"},)"
                            R"(               {"name":"dairy","uid":"1"}]})"});

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    // add some items
    store_item(vbid, {"dairy:milk", DocNamespace::Collections}, "nice");
    store_item(vbid, {"dairy:butter", DocNamespace::Collections}, "lovely");

    flush_vbucket_to_disk(vbid, 2 /* 2 x items */);

    EXPECT_EQ(2, vb->getNumItems());

    // Evict one of the keys, we should still erase it
    evict_key(vbid, {"dairy:butter", DocNamespace::Collections});

    // delete the collection
    vb->updateFromManifest(
            {R"({"separator":":","uid":"0",)"
             R"("collections":[{"name":"$default", "uid":"0"}]})"});

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    // Deleted, but still exists in the manifest
    EXPECT_TRUE(vb->lockCollections().exists("dairy"));

    runEraser();

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_FALSE(vb->lockCollections().exists("dairy"));
}

TEST_P(CollectionsEraserTest, basic_2_collections) {
    // add a collection
    vb->updateFromManifest({R"({"separator":":","uid":"0",)"
                            R"("collections":[{"name":"$default", "uid":"0"},)"
                            R"(               {"name":"fruit","uid":"1"},)"
                            R"(               {"name":"dairy","uid":"1"}]})"});

    flush_vbucket_to_disk(vbid, 2 /* 2 x system */);

    // add some items
    store_item(vbid, {"dairy:milk", DocNamespace::Collections}, "nice");
    store_item(vbid, {"dairy:butter", DocNamespace::Collections}, "lovely");
    store_item(vbid, {"fruit:apple", DocNamespace::Collections}, "nice");
    store_item(vbid, {"fruit:apricot", DocNamespace::Collections}, "lovely");

    flush_vbucket_to_disk(vbid, 4);

    EXPECT_EQ(4, vb->getNumItems());

    // delete the collections
    vb->updateFromManifest(
            {R"({"separator":":","uid":"0",)"
             R"("collections":[{"name":"$default", "uid":"0"}]})"});

    // Deleted, but still exists in the manifest
    EXPECT_TRUE(vb->lockCollections().exists("dairy"));
    EXPECT_TRUE(vb->lockCollections().exists("fruit"));

    flush_vbucket_to_disk(vbid, 2 /* 2 x system */);

    runEraser();

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_FALSE(vb->lockCollections().exists("dairy"));
    EXPECT_FALSE(vb->lockCollections().exists("fruit"));
}

TEST_P(CollectionsEraserTest, basic_3_collections) {
    // add a collection
    vb->updateFromManifest({R"({"separator":":","uid":"0",)"
                            R"("collections":[{"name":"$default", "uid":"0"},)"
                            R"(               {"name":"fruit","uid":"1"},)"
                            R"(               {"name":"dairy","uid":"1"}]})"});

    flush_vbucket_to_disk(vbid, 2 /* 1x system */);

    // add some items
    store_item(vbid, {"dairy:milk", DocNamespace::Collections}, "nice");
    store_item(vbid, {"dairy:butter", DocNamespace::Collections}, "lovely");
    store_item(vbid, {"fruit:apple", DocNamespace::Collections}, "nice");
    store_item(vbid, {"fruit:apricot", DocNamespace::Collections}, "lovely");

    flush_vbucket_to_disk(vbid, 4 /* 2x items */);

    EXPECT_EQ(4, vb->getNumItems());

    // delete one of the 3 collections
    vb->updateFromManifest({R"({"separator":":","uid":"0",)"
                            R"("collections":[{"name":"$default", "uid":"0"},)"
                            R"(               {"name":"dairy","uid":"1"}]})"});

    // Deleted, but still exists in the manifest
    EXPECT_TRUE(vb->lockCollections().exists("dairy"));
    EXPECT_TRUE(vb->lockCollections().exists("fruit"));

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    runEraser();

    EXPECT_EQ(2, vb->getNumItems());

    EXPECT_TRUE(vb->lockCollections().exists("dairy"));
    EXPECT_FALSE(vb->lockCollections().exists("fruit"));
}

TEST_P(CollectionsEraserTest, basic_4_collections) {
    // add a collection
    vb->updateFromManifest({R"({"separator":":","uid":"0",)"
                            R"("collections":[{"name":"$default", "uid":"0"},)"
                            R"(               {"name":"fruit","uid":"1"},)"
                            R"(               {"name":"dairy","uid":"1"}]})"});

    flush_vbucket_to_disk(vbid, 2 /* 1x system */);

    // add some items
    store_item(vbid, {"dairy:milk", DocNamespace::Collections}, "nice");
    store_item(vbid, {"dairy:butter", DocNamespace::Collections}, "lovely");
    store_item(vbid, {"fruit:apple", DocNamespace::Collections}, "nice");
    store_item(vbid, {"fruit:apricot", DocNamespace::Collections}, "lovely");

    flush_vbucket_to_disk(vbid, 4 /* 2x items */);

    // delete the collection and re-add a new dairy
    vb->updateFromManifest({R"({"separator":":","uid":"0",)"
                            R"("collections":[{"name":"$default", "uid":"0"},)"
                            R"(               {"name":"dairy","uid":"2"}]})"});

    // Deleted, but still exists in the manifest
    EXPECT_TRUE(vb->lockCollections().exists("dairy"));
    EXPECT_TRUE(vb->lockCollections().exists("fruit"));

    flush_vbucket_to_disk(vbid, 2 /* 1x system */);

    runEraser();

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_TRUE(vb->lockCollections().exists("dairy"));
    EXPECT_FALSE(vb->lockCollections().exists("fruit"));
}

TEST_P(CollectionsEraserTest, default_Destroy) {
    // add some items
    store_item(vbid, {"dairy:milk", DocNamespace::DefaultCollection}, "nice");
    store_item(
            vbid, {"dairy:butter", DocNamespace::DefaultCollection}, "lovely");
    store_item(vbid, {"fruit:apple", DocNamespace::DefaultCollection}, "nice");
    store_item(
            vbid, {"fruit:apricot", DocNamespace::DefaultCollection}, "lovely");

    flush_vbucket_to_disk(vbid, 4);

    EXPECT_EQ(4, vb->getNumItems());

    // delete the default collection
    vb->updateFromManifest({R"({"separator":":","uid":"0",)"
                            R"("collections":[]})"});

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    runEraser();

    EXPECT_EQ(0, vb->getNumItems());

    // Add default back - so we don't get collection unknown errors
    vb->updateFromManifest(
            {R"({"separator":":","uid":"0",)"
             R"("collections":[{"name":"$default", "uid":"0"}]})"});

    get_options_t options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

    GetValue gv = store->get({"dairy:milk", DocNamespace::DefaultCollection},
                             vbid,
                             cookie,
                             options);
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());
}

// Demonstrate MB_26455. Here we trigger collection erasing after a separator
// change, the code processing the old items can no longer determine which
// collection they belong too and fails to remove the items
TEST_P(CollectionsEraserTest, MB_26455) {
    const int items = 3;
    std::vector<std::string> separators = {
            "::", "*", "**", "***", "-=-=-=-=-="};

    for (size_t n = 0; n < separators.size(); n++) {
        // change sep
        std::string manifest =
                R"({"separator":")" + separators.at(n) +
                R"(","uid":"0","collections":[{"name":"$default", "uid":"0"}]})";
        vb->updateFromManifest({manifest});

        // add fruit
        manifest =
                R"({"separator":")" + separators.at(n) +
                R"(","uid":"0","collections":[{"name":"$default", "uid":"0"},)" +
                R"({"name":"fruit", "uid":")" + std::to_string(n) +
                R"("}]})";

        vb->updateFromManifest({manifest});

        // Mutate fruit
        const int items = 3;
        for (int ii = 0; ii < items; ii++) {
            std::string key = "fruit" + separators.at(n) + std::to_string(ii);
            store_item(vbid, {key, DocNamespace::Collections}, "value");
        }

        // expect change_separator + create_collection + items
        flush_vbucket_to_disk(vbid, 2 + items);

        // Drop fruit
        manifest =
                R"({"separator":")" + separators.at(n) +
                R"(","uid":"0","collections":[{"name":"$default", "uid":"0"}]})";
        vb->updateFromManifest({manifest});

        flush_vbucket_to_disk(vbid, 1);
    }

    EXPECT_EQ(items * separators.size(), vb->getNumItems());

    // Eraser will fail to delete keys of the original fruit as it will be using
    // the new separator and will never split the key correctly.
    runEraser();

    // All items erased
    EXPECT_EQ(0, vb->getNumItems());

    // Expected items on disk (the separator change keys)
    EXPECT_EQ(separators.size(),
              store->getROUnderlying(vbid)->getItemCount(vbid));

    // Eraser should of generated some deletes of the now defunct separator
    // change keys
    flush_vbucket_to_disk(vbid, separators.size() - 1);

    // Expect 1 item on disk (the last separator change key)
    EXPECT_EQ(1, store->getROUnderlying(vbid)->getItemCount(vbid));
}

struct PrintTestName {
    std::string operator()(
            const ::testing::TestParamInfo<std::string>& info) const {
        if ("bucket_type=persistent;item_eviction_policy=value_only" ==
            info.param) {
            return "PersistentVE";
        } else if (
                "bucket_type=persistent;item_eviction_policy=full_eviction" ==
                info.param) {
            return "PersistentFE";
        } else if ("bucket_type=ephemeral" == info.param) {
            return "Ephemeral";
        } else {
            throw std::invalid_argument("PrintTestName::Unknown info.param:" +
                                        info.param);
        }
    }
};

// @todo add ephemeral config
INSTANTIATE_TEST_CASE_P(
        CollectionsEraserTests,
        CollectionsEraserTest,
        ::testing::Values(
                "bucket_type=persistent;item_eviction_policy=value_only",
                "bucket_type=persistent;item_eviction_policy=full_eviction"),
        PrintTestName());