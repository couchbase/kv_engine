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
#include "access_scanner.h"
#include "bgfetcher.h"
#include "checkpoint.h"
#include "checkpoint_remover.h"
#include "dcp/dcpconnmap.h"
#include "dcp/flow-control-manager.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "flusher.h"
#include "globaltask.h"
#include "lambda_task.h"
#include "replicationthrottle.h"
#include "tasks.h"
#include "tests/mock/mock_global_task.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucketdeletiontask.h"
#include "warmup.h"

#include <platform/dirutils.h>
#include <chrono>
#include <thread>

#include <string_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

void KVBucketTest::SetUp() {
    // Paranoia - kill any existing files in case they are left over
    // from a previous run.
    try {
        cb::io::rmrf(test_dbname);
    } catch (std::system_error& e) {
        if (e.code() != std::error_code(ENOENT, std::system_category())) {
            throw e;
        }
    }

    initialise(config_string);

    if (completeWarmup && engine->getKVBucket()->getWarmup()) {
        engine->getKVBucket()->getWarmup()->setComplete();
        engine->getKVBucket()->getWarmup()->processCreateVBucketsComplete();
    }
}

void KVBucketTest::initialise(std::string config) {
    // Add dbname to config string.
    if (config.size() > 0) {
        config += ";";
    }
    config += "dbname=" + std::string(test_dbname);

    engine.reset(new SynchronousEPEngine(config));
    ObjectRegistry::onSwitchThread(engine.get());

    engine->setKVBucket(
            engine->public_makeMockBucket(engine->getConfiguration()));
    store = engine->getKVBucket();

    store->chkTask = std::make_shared<ClosedUnrefCheckpointRemoverTask>(
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
    destroy();
    // Shutdown the ExecutorPool singleton (initialized when we create
    // an EPBucket object). Must happen after engine
    // has been destroyed (to allow the tasks the engine has
    // registered a chance to be unregistered).
    ExecutorPool::shutdown();
}

void KVBucketTest::destroy() {
    destroy_mock_cookie(cookie);
    destroy_mock_event_callbacks();
    engine->getDcpConnMap().manageConnections();
    ObjectRegistry::onSwitchThread(nullptr);
    engine.reset();
}

void KVBucketTest::reinitialise(std::string config) {
    destroy();
    initialise(config);
}

Item KVBucketTest::store_item(uint16_t vbid,
                              const DocKey& key,
                              const std::string& value,
                              uint32_t exptime,
                              const std::vector<cb::engine_errc>& expected,
                              protocol_binary_datatype_t datatype) {
    auto item = make_item(vbid, key, value, exptime, datatype);
    auto returnCode = store->set(item, cookie);
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
        auto err = store->set(item, cookie);
        if (ENGINE_SUCCESS != err) {
            return ::testing::AssertionFailure()
                   << "Failed to store " << keyii.data() << " error:" << err;
        }
    }
    return ::testing::AssertionSuccess();
}

void KVBucketTest::flush_vbucket_to_disk(uint16_t vbid, int expected) {
    size_t actualFlushed = 0;
    const auto time_limit = std::chrono::seconds(10);
    const auto deadline = std::chrono::steady_clock::now() + time_limit;

    // Need to retry as warmup may not have completed, or if the flush is
    // in multiple parts.
    bool flush_successful = false;
    bool moreAvailable;
    do {
        size_t count;
        std::tie(moreAvailable, count) =
                dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
        actualFlushed += count;
        if (!moreAvailable) {
            flush_successful = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    } while ((std::chrono::steady_clock::now() < deadline) && moreAvailable);

    ASSERT_TRUE(flush_successful)
            << "Hit timeout (" << time_limit.count()
            << " seconds) waiting for "
               "warmup to complete while flushing VBucket.";

    ASSERT_EQ(expected, actualFlushed)
            << "Unexpected items (" << actualFlushed
            << ") in flush_vbucket_to_disk(" << vbid << ", " << expected << ")";
}

void KVBucketTest::flushVBucketToDiskIfPersistent(uint16_t vbid, int expected) {
    if (engine->getConfiguration().getBucketType() == "persistent") {
        flush_vbucket_to_disk(vbid, expected);
    }
}

void KVBucketTest::delete_item(uint16_t vbid, const DocKey& key) {
    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    EXPECT_EQ(ENGINE_SUCCESS,
              store->deleteItem(key,
                                cas,
                                vbid,
                                cookie,
                                /*itemMeta*/ nullptr,
                                mutation_descr));
}

void KVBucketTest::evict_key(uint16_t vbid, const DocKey& key) {
    const char* msg;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              store->evictKey(key, vbid, &msg));
    EXPECT_STREQ("Ejected.", msg);
}

GetValue KVBucketTest::getInternal(const DocKey& key,
                                   uint16_t vbucket,
                                   const void* cookie,
                                   vbucket_state_t allowedState,
                                   get_options_t options) {
    return store->getInternal(key, vbucket, cookie, allowedState, options);
}

void KVBucketTest::scheduleItemPager() {
    ExecutorPool::get()->schedule(store->itemPagerTask);
}

void KVBucketTest::initializeExpiryPager() {
    store->initializeExpiryPager(engine->getConfiguration());
}

bool KVBucketTest::isItemFreqDecayerTaskSnoozed() const {
    return store->isItemFreqDecayerTaskSnoozed();
}

void KVBucketTest::runBGFetcherTask() {
    MockGlobalTask mockTask(engine->getTaskable(),
                            TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);
}

/**
 * Create a del_with_meta packet with the key/body (body can be empty)
 */
std::vector<char> KVBucketTest::buildWithMetaPacket(
        protocol_binary_command opcode,
        protocol_binary_datatype_t datatype,
        uint16_t vbucket,
        uint32_t opaque,
        uint64_t cas,
        ItemMetaData metaData,
        const std::string& key,
        const std::string& body,
        const std::vector<char>& emd,
        int options) {
    EXPECT_EQ(sizeof(protocol_binary_request_set_with_meta),
              sizeof(protocol_binary_request_delete_with_meta));

    size_t size = sizeof(protocol_binary_request_set_with_meta);
    // body at least the meta
    size_t extlen = (sizeof(uint32_t) * 2) + (sizeof(uint64_t) * 2);
    size_t bodylen = extlen;
    if (options) {
        size += sizeof(uint32_t);
        bodylen += sizeof(uint32_t);
        extlen += sizeof(uint32_t);
    }
    if (!emd.empty()) {
        EXPECT_TRUE(emd.size() < std::numeric_limits<uint16_t>::max());
        size += sizeof(uint16_t) + emd.size();
        bodylen += sizeof(uint16_t) + emd.size();
        extlen += sizeof(uint16_t);
    }
    size += body.size();
    bodylen += body.size();
    size += key.size();
    bodylen += key.size();

    protocol_binary_request_set_with_meta header;
    header.message.header.request.magic = PROTOCOL_BINARY_REQ;
    header.message.header.request.opcode = opcode;
    header.message.header.request.keylen = htons(key.size());
    header.message.header.request.extlen = uint8_t(extlen);
    header.message.header.request.datatype = datatype;
    header.message.header.request.vbucket = htons(vbucket);
    header.message.header.request.bodylen = htonl(bodylen);
    header.message.header.request.opaque = opaque;
    header.message.header.request.cas = htonll(cas);
    header.message.body.flags = metaData.flags;
    header.message.body.expiration = htonl(metaData.exptime);
    header.message.body.seqno = htonll(metaData.revSeqno);
    header.message.body.cas = htonll(metaData.cas);

    std::vector<char> packet;
    packet.reserve(size);
    packet.insert(packet.end(),
                  reinterpret_cast<char*>(&header),
                  reinterpret_cast<char*>(&header) +
                          sizeof(protocol_binary_request_set_with_meta));

    if (options) {
        options = htonl(options);
        std::copy_n(reinterpret_cast<char*>(&options),
                    sizeof(uint32_t),
                    std::back_inserter(packet));
    }

    if (!emd.empty()) {
        uint16_t emdSize = htons(emd.size());
        std::copy_n(reinterpret_cast<char*>(&emdSize),
                    sizeof(uint16_t),
                    std::back_inserter(packet));
    }

    std::copy_n(key.c_str(), key.size(), std::back_inserter(packet));
    std::copy_n(body.c_str(), body.size(), std::back_inserter(packet));
    packet.insert(packet.end(), emd.begin(), emd.end());
    return packet;
}

bool KVBucketTest::addResponse(const void* k,
                               uint16_t keylen,
                               const void* ext,
                               uint8_t extlen,
                               const void* body,
                               uint32_t bodylen,
                               uint8_t datatype,
                               uint16_t status,
                               uint64_t pcas,
                               const void* cookie) {
    addResponseStatus = protocol_binary_response_status(status);
    return true;
}

protocol_binary_response_status KVBucketTest::getAddResponseStatus(
        protocol_binary_response_status newval) {
    protocol_binary_response_status rv = addResponseStatus;
    addResponseStatus = newval;
    return rv;
}

protocol_binary_response_status KVBucketTest::addResponseStatus =
        PROTOCOL_BINARY_RESPONSE_SUCCESS;


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

// Test CAS set against a deleted item
TEST_P(KVBucketParamTest, SetCASDeleted) {
    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "value");

    // Store item
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie));

    // Delete item
    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    EXPECT_EQ(ENGINE_SUCCESS,
              store->deleteItem(key,
                                cas,
                                vbid,
                                cookie,
                                /*itemMeta*/ nullptr,
                                mutation_descr));

    if (engine->getConfiguration().getBucketType() == "persistent") {
        // Trigger a flush to disk.
        flush_vbucket_to_disk(vbid);
    }

    // check we have the cas
    ASSERT_NE(0, cas);

    auto item2 = make_item(vbid, key, "value2");
    item2.setCas(cas);

    // Store item
    if (engine->getConfiguration().getItemEvictionPolicy() ==
               "full_eviction") {
        EXPECT_EQ(ENGINE_EWOULDBLOCK, store->set(item2, cookie));

        // Manually run the bgfetch task.
        MockGlobalTask mockTask(engine->getTaskable(),
                                TaskId::MultiBGFetcherTask);
        store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);
    }

    EXPECT_EQ(ENGINE_KEY_ENOENT, store->set(item2, cookie));
}

/**
 * Regression test for MB-25398 - Test CAS set (deleted value) against a
 * deleted, non-resident key.
 */
TEST_P(KVBucketParamTest, MB_25398_SetCASDeletedItem) {
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value");

    flushVBucketToDiskIfPersistent(vbid);

    // delete it, retaining a value.
    auto item = make_item(vbid, key, "deletedvalue");
    item.setDeleted();
    const auto inCAS = item.getCas();
    ASSERT_EQ(ENGINE_SUCCESS, store->set(item, cookie));
    ASSERT_NE(inCAS, item.getCas());

    // Flush, ensuring that the persistence callback runs and item is removed
    // from the HashTable.
    flushVBucketToDiskIfPersistent(vbid);

    // Create a different deleted value (with an incorrect CAS).
    auto item2 = make_item(vbid, key, "deletedvalue2");
    item2.setDeleted();
    item2.setCas(item.getCas() + 1);

    if (engine->getConfiguration().getBucketType() == "persistent") {
        // Deleted item won't be resident (after a flush), so expect to need to
        // bgfetch.
        EXPECT_EQ(ENGINE_EWOULDBLOCK, store->set(item2, cookie));

        runBGFetcherTask();
    }

    // Try with incorrect CAS.
    EXPECT_EQ(ENGINE_KEY_EEXISTS, store->set(item2, cookie));

    // Try again, this time with correct CAS.
    item2.setCas(item.getCas());
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item2, cookie));
}

/**
 * Negative variant of the regression test for MB-25398 - Test that a CAS set
 * (deleted value) to a non-existent item fails.
 */
TEST_P(KVBucketParamTest, MB_25398_SetCASDeletedItemNegative) {
    auto key = makeStoredDocKey("key");

    flushVBucketToDiskIfPersistent(vbid, 0);

    // Attempt to mutate a non-existent key (with a specific, incorrect CAS)
    auto item2 = make_item(vbid, key, "deletedvalue");
    item2.setDeleted();
    item2.setCas(1234);

    if (engine->getConfiguration().getBucketType() == "persistent") {
        // Deleted item won't be resident (after a flush), so expect to need to
        // bgfetch.
        EXPECT_EQ(ENGINE_EWOULDBLOCK, store->set(item2, cookie));
        runBGFetcherTask();
    }

    // Try with a specific CAS.
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->set(item2, cookie));

    // Try with no CAS (wildcard) - should be possible to store.
    item2.setCas(0);
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item2, cookie));
}

// Add tests //////////////////////////////////////////////////////////////////

// Test successful add
TEST_P(KVBucketParamTest, Add) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item, cookie));
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
                                 {vbucket_state_active},
                                 CheckConflicts::Yes,
                                 /*allowExisting*/ false));
}

// Test setWithMeta with a conflict with an existing item.
TEST_P(KVBucketParamTest, SetWithMeta_Conflicted) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie));

    uint64_t seqno;
    // Attempt to set with the same rev Seqno - should get EEXISTS.
    EXPECT_EQ(ENGINE_KEY_EEXISTS,
              store->setWithMeta(item,
                                 item.getCas(),
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::Yes,
                                 /*allowExisting*/ true));
}

// Test setWithMeta replacing existing item
TEST_P(KVBucketParamTest, SetWithMeta_Replace) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie));

    // Increase revSeqno so conflict resolution doesn't fail.
    item.setRevSeqno(item.getRevSeqno() + 1);
    uint64_t seqno;
    // Should get EEXISTS if we don't force (and use wrong CAS).
    EXPECT_EQ(ENGINE_KEY_EEXISTS,
              store->setWithMeta(item,
                                 item.getCas() + 1,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::Yes,
                                 /*allowExisting*/ true));

    // Should succeed with correct CAS, and different RevSeqno.
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setWithMeta(item,
                                 item.getCas(),
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::Yes,
                                 /*allowExisting*/ true));
}

/**
 * 1. setWithMeta to store an item with an expiry value
 * 2. Call get after expiry to ensure that item is deleted
 * 3. setWithMeta to store an item with lesser rev seqno
 *    than what is stored in hash table
 * 4. (3) should result in an EWOULDBLOCK and a temporary
 *    deleted item in hash table
 * 5. setWithMeta after BG Fetch should result in EEXISTS
 * 6. Temporary item should be deleted from the hash table
 */
TEST_P(KVBucketParamTest, MB_28078_SetWithMeta_tempDeleted) {
    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "value");
    item.setExpTime(1);
    item.setCas();
    uint64_t seqno;
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setWithMeta(item,
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::No,
                                 /*allowExisting*/ true));

    TimeTraveller docBrown(20);
    get_options_t options =
            static_cast<get_options_t>(QUEUE_BG_FETCH | GET_DELETED_VALUE);

    auto doGet = [&]() { return store->get(key, vbid, nullptr, options); };
    GetValue result = doGet();

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto doSetWithMeta = [&]() {
        return store->setWithMeta(item,
                                  item.getCas(),
                                  &seqno,
                                  cookie,
                                  {vbucket_state_active},
                                  CheckConflicts::Yes,
                                  /*allowExisting*/ true);
    };

    if (engine->getConfiguration().getBucketType() == "persistent") {
        ASSERT_EQ(ENGINE_EWOULDBLOCK, doSetWithMeta());
    }

    auto* shard = store->getVBucket(vbid)->getShard();
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    if (engine->getConfiguration().getBucketType() == "persistent") {
        shard->getBgFetcher()->run(&mockTask);
        ASSERT_EQ(ENGINE_KEY_EEXISTS, doSetWithMeta());
    }

    EXPECT_EQ(0, store->getVBucket(vbid)->getNumItems());
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
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
                                 {vbucket_state_active,
                                  vbucket_state_replica,
                                  vbucket_state_pending},
                                 CheckConflicts::No,
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
                      key, vbid, cookie, itemMeta1, deleted, datatype));

    uint64_t cas = 0;
    ItemMetaData itemMeta2;
    mutation_descr_t mutation_descr;
    EXPECT_EQ(ENGINE_KEY_ENOENT,
              store->deleteItem(
                      key, cas, vbid, cookie, &itemMeta2, mutation_descr));

    // Should be getting the same CAS from the failed delete as getMetaData
    EXPECT_EQ(itemMeta1.cas, itemMeta2.cas);
}

/**
 *  Test that the first item updates the hlcSeqno, but not the second
 */
TEST_P(KVBucketParamTest, test_hlcEpochSeqno) {
    auto vb = store->getVBucket(vbid);

    // A persistent bucket will store something then set the hlc_epoch
    // An ephemeral bucket always has an epoch
    int64_t initialEpoch =
            engine->getConfiguration().getBucketType() == "persistent"
                    ? HlcCasSeqnoUninitialised
                    : 0;

    EXPECT_EQ(initialEpoch, vb->getHLCEpochSeqno());
    auto item = make_item(vbid, makeStoredDocKey("key1"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item, cookie));
    if (engine->getConfiguration().getBucketType() == "persistent") {
        // Trigger a flush to disk.
        flush_vbucket_to_disk(vbid);
    }

    auto seqno = vb->getHLCEpochSeqno();
    EXPECT_NE(HlcCasSeqnoUninitialised, seqno);

    auto item2 = make_item(vbid, makeStoredDocKey("key2"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item2, cookie));
    if (engine->getConfiguration().getBucketType() == "persistent") {
        // Trigger a flush to disk.
        flush_vbucket_to_disk(vbid);
    }

    // hlc seqno doesn't change was more items are stored
    EXPECT_EQ(seqno, vb->getHLCEpochSeqno());
}

TEST_F(KVBucketTest, DataRaceInDoWorkerStat) {
    /* MB-23529: TSAN intermittently reports a data race.
     * This race appears to be caused by GGC's buggy string COW as seen
     * multiple times, e.g., MB-23454.
     * doWorkerStat calls getLog/getSlowLog to get a vector of TaskLogEntrys,
     * which have been copied out of the tasklog ringbuffer of a given
     * ExecutorThread. These copies logically have copies of the original's
     * `std::string name`.
     * As the ringbuffer overwrites older entries, the deletion of the old
     * entry's `std::string name` races with doWorkerStats reading the COW'd
     * name of its copy.
     * */
    EpEngineTaskable& taskable = engine->getTaskable();
    ExecutorPool* pool = ExecutorPool::get();

    // Task which does nothing
    ExTask task = std::make_shared<LambdaTask>(
            taskable, TaskId::DcpConsumerTask, 0, true, [&]() -> bool {
                return true; // reschedule (immediately)
            });

    pool->schedule(task);

    // nop callback to serve as add_stat
    auto dummy_cb = [](const char* key,
                       const uint16_t klen,
                       const char* val,
                       const uint32_t vlen,
                       gsl::not_null<const void*> cookie) {};

    for (uint64_t i = 0; i < 10; ++i) {
        pool->doWorkerStat(engine.get(),
                           // The callback don't use the cookie at all, but
                           // the API requires it to be set.. use the pool
                           // as the cookie
                           static_cast<const void*>(pool),
                           dummy_cb);
    }

    pool->cancel(task->getId());
}

void KVBucketTest::storeAndDeleteItem(uint16_t vbid,
                                      const DocKey& key,
                                      std::string value) {
    Item item = store_item(vbid,
                           key,
                           value,
                           0,
                           {cb::engine_errc::success},
                           PROTOCOL_BINARY_RAW_BYTES);

    delete_item(vbid, key);
    flushVBucketToDiskIfPersistent(vbid, 1);
}

ENGINE_ERROR_CODE KVBucketTest::getMeta(uint16_t vbid,
                                        const DocKey key,
                                        const void* cookie,
                                        ItemMetaData& itemMeta,
                                        uint32_t& deleted,
                                        uint8_t& datatype) {
    auto doGetMetaData = [&]() {
        return store->getMetaData(
                key, vbid, cookie, itemMeta, deleted, datatype);
    };

    auto engineResult = doGetMetaData();
    auto* shard = store->getVBucket(vbid)->getShard();
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    if (engine->getConfiguration().getBucketType() == "persistent") {
        EXPECT_EQ(ENGINE_EWOULDBLOCK, engineResult);
        // Manually run the bgfetch task, and re-attempt getMetaData
        shard->getBgFetcher()->run(&mockTask);

        engineResult = doGetMetaData();
    }

    return engineResult;
}

TEST_P(KVBucketParamTest, lockKeyTempDeletedTest) {
    //This test is to check if the lockKey function will
    //remove temporary deleted items from memory
    auto key = makeStoredDocKey("key");
    storeAndDeleteItem(vbid, key, std::string("value"));

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto engineResult = getMeta(vbid, key, cookie, itemMeta, deleted, datatype);

    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(ENGINE_SUCCESS, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketType() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for getLocked
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    GetValue gv = store->getLocked(key, vbid, ep_current_time(), 10, cookie);
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

TEST_P(KVBucketParamTest, unlockKeyTempDeletedTest) {
    //This test is to check if the unlockKey function will
    //remove temporary deleted items from memory
    auto key = makeStoredDocKey("key");
    std::string value("value");

    Item itm = store_item(vbid,
                          key,
                          value,
                          0,
                          {cb::engine_errc::success},
                          PROTOCOL_BINARY_RAW_BYTES);

    GetValue gv = store->getAndUpdateTtl(key, vbid, cookie, ep_real_time());
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());

    gv = store->getLocked(key, vbid, ep_current_time(), 10, cookie);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());

    itm.setCas(gv.item->getCas());
    store->deleteExpiredItem(itm, ep_real_time() + 10, ExpireBy::Pager);

    flushVBucketToDiskIfPersistent(vbid, 1);

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto engineResult = getMeta(vbid, key, cookie, itemMeta, deleted, datatype);

    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(ENGINE_SUCCESS, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketType() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for unlockKey
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->unlockKey(key, vbid, 0,
                                 ep_current_time()));
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

TEST_P(KVBucketParamTest, replaceTempDeletedTest) {
    //This test is to check if the replace function will
    //remove temporary deleted items from memory
    auto key = makeStoredDocKey("key");
    storeAndDeleteItem(vbid, key, std::string("value"));

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto engineResult = getMeta(vbid, key, cookie, itemMeta, deleted, datatype);
    ASSERT_EQ(ENGINE_SUCCESS, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketType() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for replace
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    auto replace_item = make_item(vbid, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->replace(replace_item, cookie));
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

TEST_P(KVBucketParamTest, statsVKeyTempDeletedTest) {
    //This test is to check if the statsVKey function will
    //remove temporary deleted items from memory
    auto key = makeStoredDocKey("key");
    storeAndDeleteItem(vbid, key, std::string("value"));

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto engineResult = getMeta(vbid, key, cookie, itemMeta, deleted, datatype);

    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(ENGINE_SUCCESS, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    ENGINE_ERROR_CODE expRetCode = ENGINE_ENOTSUP;
    if (engine->getConfiguration().getBucketType() == "persistent") {
        expTempItems = 1;
        expRetCode = ENGINE_KEY_ENOENT;
    }

    //Check that the temp item is removed for statsVKey
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    EXPECT_EQ(expRetCode, store->statsVKey(key, vbid, cookie));
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

TEST_P(KVBucketParamTest, getAndUpdateTtlTempDeletedItemTest) {
    //This test is to check if the getAndUpdateTtl function will
    //remove temporary deleted items from memory
    auto key = makeStoredDocKey("key");
    storeAndDeleteItem(vbid, key, std::string("value"));

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto engineResult = getMeta(vbid, key, cookie, itemMeta, deleted, datatype);
    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(ENGINE_SUCCESS, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketType() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for getAndUpdateTtl
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    GetValue gv = store->getAndUpdateTtl(makeStoredDocKey("key"), vbid,
                                         cookie, time(NULL));
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

TEST_P(KVBucketParamTest, validateKeyTempDeletedItemTest) {
    //This test is to check if the getAndUpdateTtl function will
    //remove temporary deleted items from memory
    auto key = makeStoredDocKey("key");
    storeAndDeleteItem(vbid, key, std::string("value"));

    ItemMetaData itemMeta;
    uint32_t deleted;
    uint8_t datatype;
    auto engineResult = getMeta(vbid, key, cookie, itemMeta, deleted, datatype);

    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(ENGINE_SUCCESS, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketType() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for validateKey
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    std::unique_ptr<Item> diskItem;
    std::string result = store->validateKey(key, vbid, *diskItem);
    EXPECT_STREQ("item_deleted", result.c_str());
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

// Test demonstrates MB-25948 with a subtle difference. In the MB the issue
// says delete(key1), but in this test we use expiry. That is because using
// ep-engine deleteItem doesn't do the system-xattr pruning (that's part of
// memcached). So we use expiry which will use the pre_expiry hook to prune
// the xattrs.
TEST_P(KVBucketParamTest, MB_25948) {

    // 1. Store key1 with an xattr value
    auto key = makeStoredDocKey("key");

    std::string value = createXattrValue("body");

    Item item = store_item(0,
                           key,
                           value,
                           1,
                           {cb::engine_errc::success},
                           PROTOCOL_BINARY_DATATYPE_XATTR);

    TimeTraveller docBrown(20);

    // 2. Force expiry of the item and flush the delete
    get_options_t options =
            static_cast<get_options_t>(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    auto doGet = [&]() { return store->get(key, vbid, nullptr, options); };
    GetValue result = doGet();

    flushVBucketToDiskIfPersistent(vbid, 1);

    // 3. GetMeta for key1, retrieving the tombstone
    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto doGetMetaData = [&]() {
        return store->getMetaData(
                key, vbid, cookie, itemMeta, deleted, datatype);
    };

    auto engineResult = doGetMetaData();

    auto* shard = store->getVBucket(vbid)->getShard();
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    if (engine->getConfiguration().getBucketType() == "persistent") {
        EXPECT_EQ(ENGINE_EWOULDBLOCK, engineResult);
        // Manually run the bgfetch task, and re-attempt getMetaData
        shard->getBgFetcher()->run(&mockTask);

        engineResult = doGetMetaData();
    }
    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(ENGINE_SUCCESS, engineResult);
    ASSERT_TRUE(deleted);
    ASSERT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, datatype);
    ASSERT_EQ(item.getFlags(), itemMeta.flags);
    // CAS and revSeqno not checked as changed when the document was expired.

    // 4. Now get deleted value - we want to retrieve the _sync field.
    result = doGet();

    // Manually run the bgfetch task and retry the get()
    if (engine->getConfiguration().getBucketType() == "persistent") {
        ASSERT_EQ(ENGINE_EWOULDBLOCK, result.getStatus());
        shard->getBgFetcher()->run(&mockTask);
        result = doGet();
    }
    ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());

    cb::xattr::Blob blob({const_cast<char*>(result.item->getData()),
                          result.item->getNBytes()},
                         false);

    // user and meta gone, _sync remains.
    EXPECT_EQ(0, blob.get("user").size());
    EXPECT_EQ(0, blob.get("meta").size());
    ASSERT_NE(0, blob.get("_sync").size());
    EXPECT_STREQ("{\"cas\":\"0xdeadbeefcafefeed\"}",
                 reinterpret_cast<char*>(blob.get("_sync").data()));
}

/**
 * Test performs the following operations
 * 1. Store an item
 * 2. Delete an item and make sure it is removed from memory
 * 3. Store the item again
 * 4. Evict the item from memory to ensure that meta data
 *    will be retrieved from disk
 * 5. Check that the revision seq no. retrieved from disk
 *    is equal to 3 (the number of updates on that item)
 */
TEST_P(KVBucketParamTest, MB_27162) {
     auto key = makeStoredDocKey("key");
     std::string value("value");

     Item item = store_item(vbid, key, value, 0, {cb::engine_errc::success},
                            PROTOCOL_BINARY_RAW_BYTES);

     delete_item(vbid, key);

     flushVBucketToDiskIfPersistent(vbid, 1);

     store_item(vbid, key, value, 0, {cb::engine_errc::success},
                PROTOCOL_BINARY_RAW_BYTES);

     flushVBucketToDiskIfPersistent(vbid, 1);

     if (GetParam() != "bucket_type=ephemeral") {
         evict_key(vbid, key);
     }

     ItemMetaData itemMeta;
     uint32_t deleted = 0;
     uint8_t datatype = 0;
     auto doGetMetaData = [&]() {
        return store->getMetaData(
                key, vbid, cookie, itemMeta, deleted, datatype);
     };

     auto engineResult = doGetMetaData();

     auto* shard = store->getVBucket(vbid)->getShard();
     MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
     if (engine->getConfiguration().getBucketType() == "persistent" &&
         GetParam() == "item_eviction_policy=full_eviction") {
         ASSERT_EQ(ENGINE_EWOULDBLOCK, engineResult);
         // Manually run the bgfetch task, and re-attempt getMetaData
         shard->getBgFetcher()->run(&mockTask);

         engineResult = doGetMetaData();
     }
     // Verify that GetMeta succeeded; and metadata is correct.
     ASSERT_EQ(ENGINE_SUCCESS, engineResult);
     EXPECT_EQ(3, itemMeta.revSeqno);
}

/***
 * Test class to expose the behaviour needed to create an ItemAccessVisitor
 */
class MockAccessScanner : public AccessScanner {
public:
    MockAccessScanner(KVBucket& _store,
                      Configuration& conf,
                      EPStats& st,
                      double sleeptime = 0,
                      bool useStartTime = false,
                      bool completeBeforeShutdown = false)
        : AccessScanner(_store,
                        conf,
                        st,
                        sleeptime,
                        useStartTime,
                        completeBeforeShutdown) {
    }

    void public_createAndScheduleTask(const size_t shard) {
        return createAndScheduleTask(shard);
    }
};

/***
 * Test to make sure the Access Scanner doesn't throw an exception with a log
 * location specified in the config which doesn't exist.
 */
TEST_P(KVBucketParamTest, AccessScannerInvalidLogLocation) {
    /* Manually edit the configuration to change the location of the
     * access log to be somewhere that doesn't exist */
    engine->getConfiguration().setAlogPath("/path/to/somewhere");
    ASSERT_EQ(engine->getConfiguration().getAlogPath(), "/path/to/somewhere");
    ASSERT_FALSE(cb::io::isDirectory(engine->getConfiguration().getAlogPath()));

    /* Create the Access Scanner task with our modified configuration
     * In this case, the 1000 refers to the sleep time for the job, but it never
     * gets used as part of the test case. */
    auto as = std::make_unique<MockAccessScanner>(*(engine->getKVBucket()),
                                                  engine->getConfiguration(),
                                                  engine->getEpStats(),
                                                  1000);

    /* Make sure this doesn't throw an exception when tyring to run the task*/
    EXPECT_NO_THROW(as->public_createAndScheduleTask(0))
            << "Access Scanner threw unexpected "
               "exception where log location does "
               "not exist";
}

class StoreIfTest : public KVBucketTest {
public:
    void SetUp() override {
        config_string += "warmup=false";
        KVBucketTest::SetUp();
        // Have all the objects, activate vBucket zero so we can store data.
        store->setVBucketState(vbid, vbucket_state_active, false);
    }
};

/**
 * Test the basic store_if (via engine) - a forced fail predicate will allow
 * add, but fail set/replace with predicate_failed
 */
TEST_F(StoreIfTest, store_if_basic) {
    cb::StoreIfPredicate pred = [](const boost::optional<item_info>& existing,
                                   cb::vbucket_info vb) -> cb::StoreIfStatus {
        return cb::StoreIfStatus::Fail;
    };
    auto item = make_item(vbid, {"key", DocNamespace::DefaultCollection}, "value", 0, 0);
    auto rv = engine->store_if(cookie, item, 0, OPERATION_ADD, pred);
    EXPECT_EQ(cb::engine_errc::success, rv.status);
    rv = engine->store_if(cookie, item, 0, OPERATION_REPLACE, pred);
    EXPECT_EQ(cb::engine_errc::predicate_failed, rv.status);
    rv = engine->store_if(cookie, item, 0, OPERATION_SET, pred);
    EXPECT_EQ(cb::engine_errc::predicate_failed, rv.status);
}

class ExpiryLimitTest : public KVBucketTest {
public:
    void SetUp() override {
        config_string += "max_ttl=86400";
        KVBucketTest::SetUp();
        // Have all the objects, activate vBucket zero so we can store data.
        store->setVBucketState(vbid, vbucket_state_active, false);
    }
};

// Test that item allocate with a limit stops 0 expiry
TEST_F(ExpiryLimitTest, itemAllocate) {
    item* itm;
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->itemAllocate(&itm,
                                   {"key", DocNamespace::DefaultCollection},
                                   5,
                                   0,
                                   0,
                                   0 /*expiry*/,
                                   0,
                                   vbid));

    Item* i = reinterpret_cast<Item*>(itm);
    auto info = engine->getItemInfo(*i);
    EXPECT_NE(0, info.exptime);

    engine->itemRelease(itm);
}

// Test that GAT with a limit stops 0 expiry
TEST_F(ExpiryLimitTest, gat) {
    // This will actually skip the initial expiry limiting code as this function
    // doesn't use itemAllocate
    Item item = store_item(
            vbid, {"key", DocNamespace::DefaultCollection}, "value", 0);

    // Now touch with 0
    auto rval = engine->get_and_touch(
            cookie, {"key", DocNamespace::DefaultCollection}, vbid, 0);

    EXPECT_EQ(cb::engine_errc::success, rval.first);

    Item* i = reinterpret_cast<Item*>(rval.second.get());
    auto info = engine->getItemInfo(*i);
    EXPECT_NE(0, info.exptime);
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
