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
    vb->updateFromManifest({R"({"uid":"0",)"
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
            {R"({"uid":"0",)"
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
    vb->updateFromManifest({R"({"uid":"0",)"
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
            {R"({"uid":"0",)"
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
    vb->updateFromManifest({R"({"uid":"0",)"
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
    vb->updateFromManifest({R"({"uid":"0",)"
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
    vb->updateFromManifest({R"({"uid":"0",)"
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
    vb->updateFromManifest({R"({"uid":"0",)"
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
    vb->updateFromManifest({R"({"uid":"0",)"
                            R"("collections":[]})"});

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    runEraser();

    EXPECT_EQ(0, vb->getNumItems());

    // Add default back - so we don't get collection unknown errors
    vb->updateFromManifest(
            {R"({"uid":"0",)"
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