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

/*
 * Unit tests for the KVBucket class.
 */

#include "kv_bucket_test.h"

#include "../mock/mock_dcp_producer.h"
#include "bgfetcher.h"
#include "checkpoint.h"
#include "checkpoint_remover.h"
#include "dcp/dcpconnmap.h"
#include "dcp/flow-control-manager.h"
#include "ep_engine.h"
#include "flusher.h"
#include "replicationthrottle.h"
#include "tapconnmap.h"
#include "tasks.h"
#include "tests/mock/mock_global_task.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucketmemorydeletiontask.h"

#include <platform/dirutils.h>
#include <chrono>
#include <thread>

void KVBucketTest::SetUp() {
    // Paranoia - kill any existing files in case they are left over
    // from a previous run.
    cb::io::rmrf(test_dbname);

    // Add dbname to config string.
    std::string config = config_string;
    if (config.size() > 0) {
        config += ";";
    }
    config += "dbname=" + std::string(test_dbname);

    engine.reset(new SynchronousEPEngine(config));
    ObjectRegistry::onSwitchThread(engine.get());

    engine->setKVBucket(engine->public_makeBucket(engine->getConfiguration()));
    store = engine->getKVBucket();

    store->chkTask = new ClosedUnrefCheckpointRemoverTask(
            engine.get(),
            engine->getEpStats(),
            engine->getConfiguration().getChkRemoverStime());

    // Ensure that EPEngine is hold about necessary server callbacks
    // (client disconnect, bucket delete).
    engine->public_initializeEngineCallbacks();

    // Need to initialize ep_real_time and friends.
    initialize_time_functions(get_mock_server_api()->core);

    cookie = create_mock_cookie();
}

void KVBucketTest::TearDown() {
    destroy_mock_cookie(cookie);
    destroy_mock_event_callbacks();
    engine->getDcpConnMap().manageConnections();
    ObjectRegistry::onSwitchThread(nullptr);
    engine.reset();

    // Shutdown the ExecutorPool singleton (initialized when we create
    // an EPBucket object). Must happen after engine
    // has been destroyed (to allow the tasks the engine has
    // registered a chance to be unregistered).
    ExecutorPool::shutdown();
}

Item KVBucketTest::store_item(uint16_t vbid,
                              const StoredDocKey& key,
                              const std::string& value,
                              uint32_t exptime,
                              const std::vector<cb::engine_errc>& expected,
                              protocol_binary_datatype_t datatype) {
    auto item = make_item(vbid, key, value, exptime, datatype);
    auto returnCode = store->set(item, nullptr);
    EXPECT_NE(expected.end(),
              std::find(expected.begin(),
                        expected.end(),
                        cb::engine_errc(returnCode)));
    return item;
}

::testing::AssertionResult KVBucketTest::store_items(
        int nitems,
        uint16_t vbid,
        const DocKey& key,
        const std::string& value,
        uint32_t exptime,
        protocol_binary_datatype_t datatype) {
    for (int ii = 0; ii < nitems; ii++) {
        auto keyii = makeStoredDocKey(
                std::string(reinterpret_cast<const char*>(key.data()),
                            key.size()) +
                        std::to_string(ii),
                key.getDocNamespace());
        auto item = make_item(vbid, keyii, value, exptime, datatype);
        auto err = store->set(item, nullptr);
        if (ENGINE_SUCCESS != err) {
            return ::testing::AssertionFailure()
                   << "Failed to store " << keyii.data() << " error:" << err;
        }
    }
    return ::testing::AssertionSuccess();
}

void KVBucketTest::flush_vbucket_to_disk(uint16_t vbid, int expected) {
    int result;
    const auto time_limit = std::chrono::seconds(10);
    const auto deadline = std::chrono::steady_clock::now() + time_limit;

    // Need to retry as warmup may not have completed.
    bool flush_successful = false;
    do {
        result = store->flushVBucket(vbid);
        if (result != RETRY_FLUSH_VBUCKET) {
            flush_successful = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    } while (std::chrono::steady_clock::now() < deadline);

    ASSERT_TRUE(flush_successful)
            << "Hit timeout (" << time_limit.count()
            << " seconds) waiting for "
               "warmup to complete while flushing VBucket.";

    ASSERT_EQ(expected, result) << "Unexpected items in flush_vbucket_to_disk";
}

void KVBucketTest::delete_item(uint16_t vbid, const StoredDocKey& key) {
    uint64_t cas = 0;
    EXPECT_EQ(ENGINE_SUCCESS,
              store->deleteItem(key,
                                cas,
                                vbid,
                                cookie,
                                /*Item*/ nullptr,
                                /*itemMeta*/ nullptr,
                                /*mutation_descr_t*/ nullptr));
}

void KVBucketTest::evict_key(uint16_t vbid, const StoredDocKey& key) {
    const char* msg;
    EXPECT_EQ(ENGINE_SUCCESS, store->evictKey(key, vbid, &msg));
    EXPECT_STREQ("Ejected.", msg);
}

GetValue KVBucketTest::getInternal(const StoredDocKey& key,
                                   uint16_t vbucket,
                                   const void* cookie,
                                   vbucket_state_t allowedState,
                                   get_options_t options) {
    return store->getInternal(key, vbucket, cookie, allowedState, options);
}

void KVBucketTest::createAndScheduleItemPager() {
    store->itemPagerTask = new ItemPager(engine.get(), engine->getEpStats());
    ExecutorPool::get()->schedule(store->itemPagerTask);
}

void KVBucketTest::initializeExpiryPager() {
    store->initializeExpiryPager(engine->getConfiguration());
}

// getKeyStats tests //////////////////////////////////////////////////////////

// Check that keystats on resident items works correctly.
TEST_P(KVBucketParamTest, GetKeyStatsResident) {
    key_stats kstats;

    // Should start with key not existing.
    EXPECT_EQ(ENGINE_KEY_ENOENT,
              store->getKeyStats(makeStoredDocKey("key"),
                                 0,
                                 cookie,
                                 kstats,
                                 WantsDeleted::No));

    store_item(0, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS,
              store->getKeyStats(makeStoredDocKey("key"),
                                 0,
                                 cookie,
                                 kstats,
                                 WantsDeleted::No))
            << "Expected to get key stats on existing item";
    EXPECT_EQ(vbucket_state_active, kstats.vb_state);
    EXPECT_FALSE(kstats.logically_deleted);
}

// Create then delete an item, checking we get keyStats reporting the item as
// deleted.
TEST_P(KVBucketParamTest, GetKeyStatsDeleted) {
    auto& kvbucket = *engine->getKVBucket();
    key_stats kstats;

    store_item(0, makeStoredDocKey("key"), "value");
    delete_item(vbid, makeStoredDocKey("key"));

    // Should get ENOENT if we don't ask for deleted items.
    EXPECT_EQ(ENGINE_KEY_ENOENT,
              kvbucket.getKeyStats(makeStoredDocKey("key"),
                                   0,
                                   cookie,
                                   kstats,
                                   WantsDeleted::No));

    // Should get success (and item flagged as deleted) if we ask for deleted
    // items.
    EXPECT_EQ(ENGINE_SUCCESS,
              kvbucket.getKeyStats(makeStoredDocKey("key"),
                                   0,
                                   cookie,
                                   kstats,
                                   WantsDeleted::Yes));
    EXPECT_EQ(vbucket_state_active, kstats.vb_state);
    EXPECT_TRUE(kstats.logically_deleted);
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(KVBucketParamTest, GetKeyStatsNMVB) {
    auto& kvbucket = *engine->getKVBucket();
    key_stats kstats;

    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET,
              kvbucket.getKeyStats(makeStoredDocKey("key"),
                                   1,
                                   cookie,
                                   kstats,
                                   WantsDeleted::No));
}

// Replace tests //////////////////////////////////////////////////////////////

// Test replace against a non-existent key.
TEST_P(KVBucketParamTest, ReplaceENOENT) {
    // Should start with key not existing (and hence cannot replace).
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->replace(item, cookie));
}

// Create then delete an item, checking replace reports ENOENT.
TEST_P(KVBucketParamTest, ReplaceDeleted) {
    store_item(vbid, makeStoredDocKey("key"), "value");
    delete_item(vbid, makeStoredDocKey("key"));

    // Replace should fail.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->replace(item, cookie));
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(KVBucketParamTest, ReplaceNMVB) {
    auto item = make_item(vbid + 1, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, store->replace(item, cookie));
}

// Check pending vbucket returns EWOULDBLOCK.
TEST_P(KVBucketParamTest, ReplacePendingVB) {
    store->setVBucketState(vbid, vbucket_state_pending, false);
    auto item = make_item(vbid, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(ENGINE_EWOULDBLOCK, store->replace(item, cookie));
}

// Set tests //////////////////////////////////////////////////////////////////

// Test CAS set against a non-existent key
TEST_P(KVBucketParamTest, SetCASNonExistent) {
    // Create an item with a non-zero CAS.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    item.setCas();
    ASSERT_NE(0, item.getCas());

    // Should get ENOENT as we should immediately know (either from metadata
    // being resident, or by bloomfilter) that key doesn't exist.
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->set(item, cookie));
}

// Add tests //////////////////////////////////////////////////////////////////

// Test successful add
TEST_P(KVBucketParamTest, Add) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item, nullptr));
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(KVBucketParamTest, AddNMVB) {
    auto item = make_item(vbid + 1, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, store->add(item, cookie));
}

// SetWithMeta tests //////////////////////////////////////////////////////////

// Test basic setWithMeta
TEST_P(KVBucketParamTest, SetWithMeta) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    item.setCas();
    uint64_t seqno;
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setWithMeta(item,
                                 0,
                                 &seqno,
                                 cookie,
                                 /*force*/ false,
                                 /*allowExisting*/ false));
}

// Test setWithMeta with a conflict with an existing item.
TEST_P(KVBucketParamTest, SetWithMeta_Conflicted) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));

    uint64_t seqno;
    // Attempt to set with the same rev Seqno - should get EEXISTS.
    EXPECT_EQ(ENGINE_KEY_EEXISTS,
              store->setWithMeta(item,
                                 item.getCas(),
                                 &seqno,
                                 cookie,
                                 /*force*/ false,
                                 /*allowExisting*/ true));
}

// Test setWithMeta replacing existing item
TEST_P(KVBucketParamTest, SetWithMeta_Replace) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));

    // Increase revSeqno so conflict resolution doesn't fail.
    item.setRevSeqno(item.getRevSeqno() + 1);
    uint64_t seqno;
    // Should get EEXISTS if we don't force (and use wrong CAS).
    EXPECT_EQ(ENGINE_KEY_EEXISTS,
              store->setWithMeta(item,
                                 item.getCas() + 1,
                                 &seqno,
                                 cookie,
                                 /*force*/ false,
                                 /*allowExisting*/ true));

    // Should succeed with correct CAS, and different RevSeqno.
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setWithMeta(item,
                                 item.getCas(),
                                 &seqno,
                                 cookie,
                                 /*force*/ false,
                                 /*allowExisting*/ true));
}

// Test forced setWithMeta
TEST_P(KVBucketParamTest, SetWithMeta_Forced) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    item.setCas();
    uint64_t seqno;
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setWithMeta(item,
                                 0,
                                 &seqno,
                                 cookie,
                                 /*force*/ true,
                                 /*allowExisting*/ false));
}

// MB and test was raised because a few commits back this was broken but no
// existing test covered the case. I.e. run this test  against 0810540 and it
// fails, but now fixed
TEST_P(KVBucketParamTest, mb22824) {
    auto key = makeStoredDocKey("key");

    // Store key and force expiry
    store_item(0, key, "value", 1);
    TimeTraveller docBrown(20);

    uint32_t deleted = false;
    ItemMetaData itemMeta1;
    uint8_t datatype = PROTOCOL_BINARY_RAW_BYTES;
    EXPECT_EQ(ENGINE_SUCCESS,
              store->getMetaData(
                      key, vbid, cookie, false, itemMeta1, deleted, datatype));

    uint64_t cas = 0;
    ItemMetaData itemMeta2;
    EXPECT_EQ(ENGINE_KEY_ENOENT,
              store->deleteItem(key,
                                cas,
                                vbid,
                                cookie,
                                /*Item*/ nullptr,
                                &itemMeta2,
                                /*mutation_descr_t*/ nullptr));

    // Should be getting the same CAS from the failed delete as getMetaData
    EXPECT_EQ(itemMeta1.cas, itemMeta2.cas);
}

// Test cases which run for EP (Full and Value eviction) and Ephemeral
INSTANTIATE_TEST_CASE_P(EphemeralOrPersistent,
                        KVBucketParamTest,
                        ::testing::Values("item_eviction_policy=value_only",
                                          "item_eviction_policy=full_eviction",
                                          "bucket_type=ephemeral"),
                        [](const ::testing::TestParamInfo<std::string>& info) {
                            return info.param.substr(info.param.find('=') + 1);
                        });

const char KVBucketTest::test_dbname[] = "ep_engine_ep_unit_tests_db";
