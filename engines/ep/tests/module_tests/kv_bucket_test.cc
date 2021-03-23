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
#include "checkpoint_manager.h"
#include "checkpoint_remover.h"
#include "couch-kvstore/couch-kvstore-config.h"
#include "couch-kvstore/couch-kvstore.h"
#include "dcp/dcpconnmap.h"
#include "dcp/flow-control-manager.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ep_vb.h"
#include "evp_store_single_threaded_test.h"
#include "failover-table.h"
#include "fakes/fake_executorpool.h"
#include "flusher.h"
#include "globaltask.h"
#include "kv_bucket.h"
#include "lambda_task.h"
#include "replicationthrottle.h"
#include "tasks.h"
#include "tests/mock/mock_global_task.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucketdeletiontask.h"
#include "warmup.h"

#ifdef EP_USE_MAGMA
#include "../mock/mock_magma_kvstore.h"
#endif

#include <gmock/gmock-generated-matchers.h>
#include <mcbp/protocol/framebuilder.h>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <string_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

#include <chrono>
#include <thread>

KVBucketTest::KVBucketTest() : test_dbname(dbnameFromCurrentGTestInfo()) {
}

void KVBucketTest::SetUp() {
    // Paranoia - kill any existing files in case they are left over
    // from a previous run.
    try {
        cb::io::rmrf(test_dbname);
    } catch (const std::system_error& e) {
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
    if (!config.empty()) {
        config += ";";
    }
    config += "dbname=" + test_dbname;

    // Need to initialize ep_real_time and friends.
    initialize_time_functions(get_mock_server_api()->core);

    if (config.find("backend=magma") != std::string::npos) {
        config += ";" + magmaConfig;
    }
    engine = SynchronousEPEngine::build(config);

    store = engine->getKVBucket();

    store->chkTask = std::make_shared<ClosedUnrefCheckpointRemoverTask>(
            engine.get(),
            engine->getEpStats(),
            engine->getConfiguration().getChkRemoverStime());

    cookie = create_mock_cookie(engine.get());
}

void KVBucketTest::TearDown() {
    destroy();
    // Shutdown the ExecutorPool singleton (initialized when we create
    // an EPBucket object). Must happen after engine
    // has been destroyed (to allow the tasks the engine has
    // registered a chance to be unregistered).
    ExecutorPool::shutdown();
    // Cleanup any files we created.
    try {
        cb::io::rmrf(test_dbname);
    } catch (const std::system_error& e) {
        // ignore - test cases may destroy data dir to force a test condition
    }
}

void KVBucketTest::destroy(bool force) {
    destroy_mock_cookie(cookie);
    engine->getDcpConnMap().manageConnections();
    engine->destroyInner(force);
    engine.reset();
}

void KVBucketTest::reinitialise(std::string config, bool force) {
    destroy(force);
    initialise(config);
}

Item KVBucketTest::store_item(Vbid vbid,
                              const DocKey& key,
                              const std::string& value,
                              uint32_t exptime,
                              const std::vector<cb::engine_errc>& expected,
                              protocol_binary_datatype_t datatype,
                              std::optional<cb::durability::Requirements> reqs,
                              bool deleted) {
    auto item = make_item(vbid, key, value, exptime, datatype);
    if (reqs) {
        item.setPendingSyncWrite(*reqs);
    }
    if (deleted) {
        item.setDeleted(DeleteSource::Explicit);
    }
    auto returnCode = store->set(item, cookie);
    // Doing the EXPECT this way as it is a less noisy when many operations fail
    auto expectedCount = std::count(
            expected.begin(), expected.end(), cb::engine_errc(returnCode));
    EXPECT_NE(0, expectedCount)
            << "unexpected error:" << cb::to_string(returnCode)
            << " for key:" << key.to_string();
    return item;
}

Item KVBucketTest::store_deleted_item(
        Vbid vbid,
        const DocKey& key,
        const std::string& value,
        uint32_t exptime,
        const std::vector<cb::engine_errc>& expected,
        protocol_binary_datatype_t datatype,
        std::optional<cb::durability::Requirements> reqs) {
    return store_item(
            vbid, key, value, exptime, expected, datatype, reqs, true);
}

::testing::AssertionResult KVBucketTest::store_items(
        int nitems,
        Vbid vbid,
        const DocKey& key,
        const std::string& value,
        uint32_t exptime,
        protocol_binary_datatype_t datatype) {
    for (int ii = 0; ii < nitems; ii++) {
        auto keyii = makeStoredDocKey(
                std::string(reinterpret_cast<const char*>(
                                    key.makeDocKeyWithoutCollectionID().data()),
                            key.makeDocKeyWithoutCollectionID().size()) +
                        std::to_string(ii),
                key.getCollectionID());
        auto item = make_item(vbid, keyii, value, exptime, datatype);
        auto err = store->set(item, cookie);
        if (cb::engine_errc::success != err) {
            return ::testing::AssertionFailure()
                   << "Failed to store " << keyii.data() << " error:" << err;
        }
    }
    return ::testing::AssertionSuccess();
}

void KVBucketTest::flush_vbucket_to_disk(Vbid vbid, size_t expected) {
    size_t actualFlushed = 0;
    const auto time_limit = std::chrono::seconds(10);
    const auto deadline = std::chrono::steady_clock::now() + time_limit;

    // Need to retry as warmup may not have completed, or if the flush is
    // in multiple parts.
    bool flush_successful = false;
    using MoreAvailable = EPBucket::MoreAvailable;
    MoreAvailable moreAvailable;
    do {
        const auto res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
        moreAvailable = res.moreAvailable;
        actualFlushed += res.numFlushed;
        if (moreAvailable == MoreAvailable::No) {
            flush_successful = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    } while ((std::chrono::steady_clock::now() < deadline) &&
             moreAvailable == MoreAvailable::Yes);

    ASSERT_TRUE(flush_successful)
            << "Hit timeout (" << time_limit.count()
            << " seconds) waiting for "
               "warmup to complete while flushing VBucket.";

    ASSERT_EQ(expected, actualFlushed)
            << "Unexpected items (" << actualFlushed
            << ") in flush_vbucket_to_disk(" << vbid << ", " << expected << ")";
}

void KVBucketTest::flushVBucketToDiskIfPersistent(Vbid vbid, int expected) {
    if (engine->getConfiguration().getBucketType() == "persistent") {
        flush_vbucket_to_disk(vbid, expected);
    }
}

void KVBucketTest::removeCheckpoint(VBucket& vb, int numItems) {
    /* Create new checkpoint so that we can remove the current checkpoint
       and force a backfill in the DCP stream */
    auto& ckpt_mgr = *vb.checkpointManager;
    ckpt_mgr.createNewCheckpoint();

    // flush to move the persistence cursor
    flushVBucketToDiskIfPersistent(vb.getId(), 0);

    bool new_ckpt_created = false;
    int itemsRemoved = 0;

    while (true) {
        auto removed =
                ckpt_mgr.removeClosedUnrefCheckpoints(vb, new_ckpt_created);
        itemsRemoved += removed;

        if (itemsRemoved >= numItems || !removed) {
            break;
        }
    }

    EXPECT_EQ(numItems, itemsRemoved);
}

void KVBucketTest::flushAndRemoveCheckpoints(Vbid vbid) {
    auto& vb = *store->getVBucket(vbid);
    auto& ckpt_mgr = *vb.checkpointManager;
    ckpt_mgr.createNewCheckpoint();

    dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);

    bool new_ckpt_created = false;
    ckpt_mgr.removeClosedUnrefCheckpoints(vb, new_ckpt_created);
}

void KVBucketTest::delete_item(Vbid vbid, const DocKey& key) {
    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    EXPECT_EQ(cb::engine_errc::success,
              store->deleteItem(key,
                                cas,
                                vbid,
                                cookie,
                                {},
                                /*itemMeta*/ nullptr,
                                mutation_descr));
}

void KVBucketTest::evict_key(Vbid vbid, const DocKey& key) {
    const char* msg;
    EXPECT_EQ(cb::mcbp::Status::Success, store->evictKey(key, vbid, &msg));
    EXPECT_STREQ("Ejected.", msg);
}

GetValue KVBucketTest::getInternal(const DocKey& key,
                                   Vbid vbucket,
                                   const void* cookie,
                                   const ForGetReplicaOp getReplicaItem,
                                   get_options_t options) {
    return store->getInternal(key, vbucket, cookie, getReplicaItem, options);
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
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto* epVb = dynamic_cast<EPVBucket*>(vb.get());
    epVb->getBgFetcher().run(&mockTask);
}

/**
 * Create a del_with_meta packet with the key/body (body can be empty)
 */
std::vector<char> KVBucketTest::buildWithMetaPacket(
        cb::mcbp::ClientOpcode opcode,
        protocol_binary_datatype_t datatype,
        Vbid vbucket,
        uint32_t opaque,
        uint64_t cas,
        ItemMetaData metaData,
        const std::string& key,
        const std::string& body,
        const std::vector<char>& emd,
        int options) {
    EXPECT_EQ(sizeof(cb::mcbp::request::SetWithMetaPayload),
              sizeof(cb::mcbp::request::DelWithMetaPayload));

    // When using the engine interface directly by calling unknown_command
    // the packet validators have already been called (and verified the
    // content of the framing extras). None of the current engine functions
    // currently tries to inspect the framing extras, so lets's just
    // inject a blob to move the offsets around and verify that the
    // current unit tests still pass.
    std::vector<uint8_t> frame_extras(10);
    std::vector<uint8_t> extras_backing(
            sizeof(cb::mcbp::request::SetWithMetaPayload));
    auto* extdata = reinterpret_cast<cb::mcbp::request::SetWithMetaPayload*>(
            extras_backing.data());
    extdata->setFlagsInNetworkByteOrder(metaData.flags);
    extdata->setExpiration(gsl::narrow<uint32_t>(metaData.exptime));
    extdata->setSeqno(metaData.revSeqno);
    extdata->setCas(metaData.cas);
    cb::byte_buffer extras{extras_backing.data(), extras_backing.size()};

    if (options) {
        options = htonl(options);
        std::copy_n(reinterpret_cast<uint8_t*>(&options),
                    sizeof(uint32_t),
                    std::back_inserter(extras_backing));
        extras = {extras_backing.data(), extras_backing.size()};
    }

    if (!emd.empty()) {
        EXPECT_TRUE(emd.size() < std::numeric_limits<uint16_t>::max());
        uint16_t emdSize = htons(emd.size());
        std::copy_n(reinterpret_cast<uint8_t*>(&emdSize),
                    sizeof(uint16_t),
                    std::back_inserter(extras_backing));
        extras = {extras_backing.data(), extras_backing.size()};
    }

    std::vector<char> packet(sizeof(cb::mcbp::Request) + frame_extras.size() +
                             extras.size() + key.size() + body.size() +
                             emd.size());
    cb::mcbp::RequestBuilder builder(
            {reinterpret_cast<uint8_t*>(packet.data()), packet.size()});

    builder.setMagic(cb::mcbp::Magic::AltClientRequest);
    builder.setOpcode(opcode);
    builder.setDatatype(cb::mcbp::Datatype(datatype));
    builder.setVBucket(vbucket);
    builder.setOpaque(opaque);
    builder.setCas(cas);
    builder.setFramingExtras({frame_extras.data(), frame_extras.size()});
    builder.setExtras(extras);
    builder.setKey({reinterpret_cast<const uint8_t*>(key.data()), key.size()});

    if (emd.empty()) {
        builder.setValue(
                {reinterpret_cast<const uint8_t*>(body.data()), body.size()});
    } else {
        std::vector<uint8_t> buffer;
        std::copy(body.begin(), body.end(), std::back_inserter(buffer));
        std::copy(emd.begin(), emd.end(), std::back_inserter(buffer));
        builder.setValue({buffer.data(), buffer.size()});
    }

    return packet;
}

bool KVBucketTest::addResponse(std::string_view key,
                               std::string_view extras,
                               std::string_view body,
                               uint8_t datatype,
                               cb::mcbp::Status status,
                               uint64_t pcas,
                               const void* cookie) {
    addResponseStatus = status;
    return true;
}

cb::mcbp::Status KVBucketTest::getAddResponseStatus(cb::mcbp::Status newval) {
    auto rv = addResponseStatus;
    addResponseStatus = newval;
    return rv;
}

cb::mcbp::Status KVBucketTest::addResponseStatus = cb::mcbp::Status::Success;

void KVBucketTest::setRandomFunction(std::function<long()>& randFunction) {
    store->getRandom = randFunction;
}

Collections::Manager& KVBucketTest::getCollectionsManager() {
    return *store->collectionsManager.get();
}

/**
 * Replace the rw KVStore with one that uses the given ops. This function
 * will test the config to be sure the KVBucket is persistsent/couchstore
 */
void KVBucketTest::replaceCouchKVStore(FileOpsInterface& ops) {
    ASSERT_EQ(engine->getConfiguration().getBucketType(), "persistent");
    ASSERT_EQ(engine->getConfiguration().getBackend(), "couchdb");

    const auto& config = store->getRWUnderlying(vbid)->getConfig();
    auto rw = std::make_unique<CouchKVStore>(
            dynamic_cast<const CouchKVStoreConfig&>(config), ops);

    const auto shardId = store->getShardId(vbid);
    auto rwro = store->takeRWRO(shardId);

    store->setRWRO(shardId, std::move(rw), std::move(rwro.ro));
}

void KVBucketTest::replaceMagmaKVStore(MagmaKVStoreConfig& config) {
    EXPECT_EQ(engine->getConfiguration().getBucketType(), "persistent");
    EXPECT_EQ(engine->getConfiguration().getBackend(), "magma");
#ifdef EP_USE_MAGMA
    auto rwro = store->takeRWRO(0);
    auto rw = std::make_unique<MockMagmaKVStore>(config);
    store->setRWRO(0, std::move(rw), std::move(rwro.ro));
#endif
}

unique_request_ptr KVBucketTest::createObserveRequest(
        const std::vector<std::string>& keys) {
    // EPE::observe documents the value format as:
    //  Each entry is built up by:
    //  2 bytes vb id
    //  2 bytes key length
    //  n bytes key

    // create big enough string for all entries
    const size_t valueSize =
            (sizeof(Vbid) + sizeof(uint16_t)) * keys.size() +
            std::accumulate(
                    keys.begin(), keys.end(), 0, [](auto sum, auto key) {
                        return sum + key.size();
                    });
    std::string valueStr(valueSize, '\0');
    auto* value = &valueStr[0];

    for (const auto& key : keys) {
        Vbid entryVbid = vbid.hton();
        uint16_t keylen = htons(key.size());

        std::memcpy(value, &entryVbid, sizeof(Vbid));
        value += sizeof(Vbid);
        std::memcpy(value, &keylen, sizeof(uint16_t));
        value += sizeof(uint16_t);
        std::memcpy(value, key.data(), key.size());
        value += key.size();
    }

    return createPacket(cb::mcbp::ClientOpcode::Observe,
                        vbid,
                        {/* cas */},
                        {/* extras */},
                        {/* key */},
                        valueStr);
}

class KVBucketParamTest : public STParameterizedBucketTest {
    void SetUp() override {
        STParameterizedBucketTest::SetUp();
        // Have all the objects, activate vBucket zero so we can store data.
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    }
};

#ifdef EP_USE_MAGMA
// Test to verify stats aggregation across shards is working.
TEST_P(KVBucketParamTest, GetKVStoreStats) {
    if (!isMagma()) {
        GTEST_SKIP_("magma only");
    }
    auto vbid1 = vbid;
    auto vbid2 = vbid;
    vbid2++;
    store->setVBucketState(vbid2, vbucket_state_active);

    auto key1 = makeStoredDocKey("key1");
    store_item(vbid1, key1, "value");
    flush_vbucket_to_disk(vbid1, 1);

    auto key2 = makeStoredDocKey("key2");
    store_item(vbid2, key2, "value");
    flush_vbucket_to_disk(vbid2, 1);

    size_t nSetsVbid1 = 0;
    size_t nSetsVbid2 = 0;
    size_t nSetsAll = 0;
    constexpr auto nSetsStatName = "magma_NSets";
    constexpr auto fooStatName = "foo";
    constexpr std::array<std::string_view, 2> keys = {
            {nSetsStatName, fooStatName}};

    auto stats = store->getKVStoreStats(keys, KVBucketIface::KVSOption::RW);
    store->getRWUnderlying(vbid1)->getStat(nSetsStatName, nSetsVbid1);
    store->getRWUnderlying(vbid2)->getStat(nSetsStatName, nSetsVbid2);
    store->getKVStoreStat(
            nSetsStatName, nSetsAll, KVBucketIface::KVSOption::RW);

    EXPECT_EQ(nSetsVbid1, 1);
    EXPECT_EQ(nSetsVbid2, 1);
    EXPECT_NE(stats.find(nSetsStatName), stats.end());
    EXPECT_EQ(stats[nSetsStatName], 2);
    EXPECT_EQ(nSetsAll, 2);
    EXPECT_EQ(stats.find(fooStatName), stats.end());
}
#endif

// getKeyStats tests //////////////////////////////////////////////////////////

// Check that keystats on resident items works correctly.
TEST_P(KVBucketParamTest, GetKeyStatsResident) {
    key_stats kstats;

    // Should start with key not existing.
    auto getKeyStats = [&]() -> cb::engine_errc {
        return store->getKeyStats(makeStoredDocKey("key"),
                                  Vbid(0),
                                  cookie,
                                  kstats,
                                  WantsDeleted::No);
    };

    auto rv = getKeyStats();
    if (needsBGFetch(rv)) {
        runBGFetcherTask();
        rv = getKeyStats();
    }
    EXPECT_EQ(cb::engine_errc::no_such_key, rv);

    store_item(Vbid(0), makeStoredDocKey("key"), "value");
    EXPECT_EQ(cb::engine_errc::success, getKeyStats())
            << "Expected to get key stats on existing item";
    EXPECT_EQ(vbucket_state_active, kstats.vb_state);
    EXPECT_FALSE(kstats.logically_deleted);
}

// Create then delete an item, checking we get key-stats reporting the item
// as deleted.
TEST_P(KVBucketParamTest, GetKeyStatsDeleted) {
    auto& kvbucket = *engine->getKVBucket();
    key_stats kstats;

    store_item(Vbid(0), makeStoredDocKey("key"), "value");
    delete_item(vbid, makeStoredDocKey("key"));

    // Should get ENOENT if we don't ask for deleted items.
    EXPECT_EQ(cb::engine_errc::no_such_key,
              kvbucket.getKeyStats(makeStoredDocKey("key"),
                                   Vbid(0),
                                   cookie,
                                   kstats,
                                   WantsDeleted::No));

    // Should get success (and item flagged as deleted) if we ask for deleted
    // items.
    EXPECT_EQ(cb::engine_errc::success,
              kvbucket.getKeyStats(makeStoredDocKey("key"),
                                   Vbid(0),
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

    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              kvbucket.getKeyStats(makeStoredDocKey("key"),
                                   Vbid(1),
                                   cookie,
                                   kstats,
                                   WantsDeleted::No));
}

// Replace tests //////////////////////////////////////////////////////////////

// Test replace against a non-existent key.
TEST_P(KVBucketParamTest, ReplaceENOENT) {
    // Should start with key not existing (and hence cannot replace).
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    auto rv = store->replace(item, cookie);
    if (needsBGFetch(rv)) {
        runBGFetcherTask();
        rv = store->replace(item, cookie);
    }
    EXPECT_EQ(cb::engine_errc::no_such_key, rv);
}

// Create then delete an item, checking replace reports ENOENT.
TEST_P(KVBucketParamTest, ReplaceDeleted) {
    store_item(vbid, makeStoredDocKey("key"), "value");
    delete_item(vbid, makeStoredDocKey("key"));

    // Replace should fail.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(cb::engine_errc::no_such_key, store->replace(item, cookie));
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(KVBucketParamTest, ReplaceNMVB) {
    auto item =
            make_item(Vbid(vbid.get() + 1), makeStoredDocKey("key"), "value2");
    EXPECT_EQ(cb::engine_errc::not_my_vbucket, store->replace(item, cookie));
}

// Check pending vbucket returns EWOULDBLOCK.
TEST_P(KVBucketParamTest, ReplacePendingVB) {
    store->setVBucketState(vbid, vbucket_state_pending);
    auto item = make_item(vbid, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(cb::engine_errc::would_block, store->replace(item, cookie));
}

// Set tests //////////////////////////////////////////////////////////////////

// Test CAS set against a non-existent key
TEST_P(KVBucketParamTest, SetCASNonExistent) {
    // Create an item with a non-zero CAS.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    item.setCas();
    ASSERT_NE(0, item.getCas());

    // Should get ENOENT as we should immediately know (either from metadata
    // being resident, or by bloomfilter) that key doesn't exist. Might need to
    // bg fetch for magma which implements their own bloom filters
    auto rv = store->set(item, cookie);
    if (needsBGFetch(rv)) {
        runBGFetcherTask();
        rv = store->set(item, cookie);
    }
    EXPECT_EQ(cb::engine_errc::no_such_key, rv);
}

// Test CAS set against a deleted item
TEST_P(KVBucketParamTest, SetCASDeleted) {
    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "value");

    // Store item
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));

    // Delete item
    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    EXPECT_EQ(cb::engine_errc::success,
              store->deleteItem(key,
                                cas,
                                vbid,
                                cookie,
                                {},
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
        EXPECT_EQ(cb::engine_errc::would_block, store->set(item2, cookie));
        runBGFetcherTask();
    }

    EXPECT_EQ(cb::engine_errc::no_such_key, store->set(item2, cookie));
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
    ASSERT_EQ(cb::engine_errc::success, store->set(item, cookie));
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
        EXPECT_EQ(cb::engine_errc::would_block, store->set(item2, cookie));

        runBGFetcherTask();
    }

    // Try with incorrect CAS.
    EXPECT_EQ(cb::engine_errc::key_already_exists, store->set(item2, cookie));

    // Try again, this time with correct CAS.
    item2.setCas(item.getCas());
    EXPECT_EQ(cb::engine_errc::success, store->set(item2, cookie));
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
        EXPECT_EQ(cb::engine_errc::would_block, store->set(item2, cookie));
        runBGFetcherTask();
    }

    // Try with a specific CAS.
    EXPECT_EQ(cb::engine_errc::no_such_key, store->set(item2, cookie));

    // Try with no CAS (wildcard) - should be possible to store.
    item2.setCas(0);
    EXPECT_EQ(cb::engine_errc::success, store->set(item2, cookie));
}

// Add tests //////////////////////////////////////////////////////////////////

// Test successful add
TEST_P(KVBucketParamTest, Add) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    auto rv = store->add(item, cookie);
    if (needsBGFetch(rv)) {
        runBGFetcherTask();
        rv = store->add(item, cookie);
    }
    EXPECT_EQ(cb::engine_errc::success, rv);
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(KVBucketParamTest, AddNMVB) {
    auto item =
            make_item(Vbid(vbid.get() + 1), makeStoredDocKey("key"), "value2");
    EXPECT_EQ(cb::engine_errc::not_my_vbucket, store->add(item, cookie));
}

// SetWithMeta tests //////////////////////////////////////////////////////////

// Test basic setWithMeta
TEST_P(KVBucketParamTest, SetWithMeta) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    item.setCas();
    uint64_t seqno;
    auto setWithMeta = [&]() -> cb::engine_errc {
        return store->setWithMeta(item,
                                  0,
                                  &seqno,
                                  cookie,
                                  {vbucket_state_active},
                                  CheckConflicts::Yes,
                                  /*allowExisting*/ false);
    };

    auto rv = setWithMeta();
    if (isMagma()) {
        // Magma lacks bloom filters so needs to bg fetch
        auto vb = store->getVBucket(vbid);
        EXPECT_TRUE(vb->hasPendingBGFetchItems());
        runBGFetcherTask();
        rv = setWithMeta();
    }
    EXPECT_EQ(cb::engine_errc::success, rv);
}

// Test setWithMeta with a conflict with an existing item.
TEST_P(KVBucketParamTest, SetWithMeta_Conflicted) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));

    uint64_t seqno;
    // Attempt to set with the same rev Seqno - should get EEXISTS.
    EXPECT_EQ(cb::engine_errc::key_already_exists,
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
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));

    // Increase revSeqno so conflict resolution doesn't fail.
    item.setRevSeqno(item.getRevSeqno() + 1);
    uint64_t seqno;
    // Should get EEXISTS if we don't force (and use wrong CAS).
    EXPECT_EQ(cb::engine_errc::key_already_exists,
              store->setWithMeta(item,
                                 item.getCas() + 1,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::Yes,
                                 /*allowExisting*/ true));

    // Should succeed with correct CAS, and different RevSeqno.
    EXPECT_EQ(cb::engine_errc::success,
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
    EXPECT_EQ(cb::engine_errc::success,
              store->setWithMeta(item,
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::No,
                                 /*allowExisting*/ true));

    TimeTraveller docBrown(20);
    auto options =
            static_cast<get_options_t>(QUEUE_BG_FETCH | GET_DELETED_VALUE);

    auto doGet = [&]() { return store->get(key, vbid, cookie, options); };
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
        ASSERT_EQ(cb::engine_errc::would_block, doSetWithMeta());
    }

    if (engine->getConfiguration().getBucketType() == "persistent") {
        runBGFetcherTask();
        ASSERT_EQ(cb::engine_errc::key_already_exists, doSetWithMeta());
    }

    EXPECT_EQ(0, store->getVBucket(vbid)->getNumItems());
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

// Test forced setWithMeta
TEST_P(KVBucketParamTest, SetWithMeta_Forced) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    item.setCas();
    uint64_t seqno;
    EXPECT_EQ(cb::engine_errc::success,
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
    store_item(Vbid(0), key, "value", 1);
    TimeTraveller docBrown(20);

    uint32_t deleted = false;
    ItemMetaData itemMeta1;
    auto datatype = PROTOCOL_BINARY_RAW_BYTES;
    EXPECT_EQ(cb::engine_errc::success,
              store->getMetaData(
                      key, vbid, cookie, itemMeta1, deleted, datatype));

    uint64_t cas = 0;
    ItemMetaData itemMeta2;
    mutation_descr_t mutation_descr;
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->deleteItem(
                      key, cas, vbid, cookie, {}, &itemMeta2, mutation_descr));

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
    auto rv = store->add(item, cookie);
    if (needsBGFetch(rv)) {
        EXPECT_TRUE(vb->hasPendingBGFetchItems());
        runBGFetcherTask();
        rv = store->add(item, cookie);
    }
    EXPECT_EQ(cb::engine_errc::success, rv);

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto seqno = vb->getHLCEpochSeqno();
    EXPECT_NE(HlcCasSeqnoUninitialised, seqno);

    auto item2 = make_item(vbid, makeStoredDocKey("key2"), "value");
    rv = store->add(item2, cookie);
    if (needsBGFetch(rv)) {
        EXPECT_TRUE(vb->hasPendingBGFetchItems());
        runBGFetcherTask();
        rv = store->add(item2, cookie);
    }
    EXPECT_EQ(cb::engine_errc::success, rv);

    flushVBucketToDiskIfPersistent(vbid, 1);

    // hlc seqno doesn't change was more items are stored
    EXPECT_EQ(seqno, vb->getHLCEpochSeqno());
}

TEST_F(KVBucketTest, DataRaceInDoWorkerStat) {
    if (engine->getConfiguration().getExecutorPoolBackend() == "folly") {
        // doWorkerStat() as required by this test below not yet implemented
        // for FollyExecutorPool.
        GTEST_SKIP();
    }

    /* MB-23529: TSAN intermittently reports a data race.
     * This race appears to be caused by GGC's buggy string COW as seen
     * multiple times, e.g., MB-23454.
     * doWorkerStat calls getLog/getSlowLog to get a vector of TaskLogEntrys,
     * which have been copied out of the tasklog ringbuffer of a given
     * CB3ExecutorThread. These copies logically have copies of the original's
     * `std::string name`.
     * As the ringbuffer overwrites older entries, the deletion of the old
     * entry's `std::string name` races with doWorkerStats reading the COW'd
     * name of its copy.
     * */
    EpEngineTaskable& taskable = engine->getTaskable();
    ExecutorPool* pool = ExecutorPool::get();

    // Task which does nothing
    ExTask task = std::make_shared<LambdaTask>(
            taskable,
            TaskId::DcpConsumerTask,
            0,
            true,
            [&](LambdaTask&) -> bool {
                return true; // reschedule (immediately)
            });

    pool->schedule(task);

    // nop callback to serve as add_stat
    auto dummy_cb = [](std::string_view key,
                       std::string_view value,
                       gsl::not_null<const void*> cookie) {};

    for (uint64_t i = 0; i < 10; ++i) {
        pool->doWorkerStat(engine->getTaskable(),
                           // The callback don't use the cookie at all, but
                           // the API requires it to be set.. use the pool
                           // as the cookie
                           static_cast<const void*>(pool),
                           dummy_cb);
    }

    pool->cancel(task->getId());
}

void KVBucketTest::storeAndDeleteItem(Vbid vbid,
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

cb::engine_errc KVBucketTest::getMeta(Vbid vbid,
                                      const DocKey key,
                                      const void* cookie,
                                      ItemMetaData& itemMeta,
                                      uint32_t& deleted,
                                      uint8_t& datatype,
                                      bool retryOnEWouldBlock) {
    auto doGetMetaData = [&]() {
        return store->getMetaData(
                key, vbid, cookie, itemMeta, deleted, datatype);
    };

    auto engineResult = doGetMetaData();
    if (engine->getConfiguration().getBucketType() == "persistent" &&
        retryOnEWouldBlock) {
        EXPECT_EQ(cb::engine_errc::would_block, engineResult);
        // Manually run the bgfetch task, and re-attempt getMetaData
        runBGFetcherTask();

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
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketType() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for getLocked
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    GetValue gv = store->getLocked(key, vbid, ep_current_time(), 10, cookie);
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());
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
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());

    gv = store->getLocked(key, vbid, ep_current_time(), 10, cookie);
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());

    itm.setCas(gv.item->getCas());
    store->deleteExpiredItem(itm, ep_real_time() + 10, ExpireBy::Pager);

    flushVBucketToDiskIfPersistent(vbid, 1);

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto engineResult = getMeta(vbid, key, cookie, itemMeta, deleted, datatype);

    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketType() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for unlockKey
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->unlockKey(key, vbid, 0, ep_current_time(), cookie));
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

// Test that getLocked correctly returns ESyncWriteInProgress if targetted at
// a key which has a prepared SyncWrite in progress.
TEST_P(KVBucketParamTest, GetLockedWithPreparedSyncWrite) {
    // Setup - need a valid topology to accept SyncWrites - but don't want them
    // to auto-commit so create a topology with 2 nodes.
    auto meta =
            nlohmann::json{{"topology", nlohmann::json::array({{"a", "b"}})}};
    ASSERT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, &meta));

    // Store both a committed and prepared SV.
    auto key = makeStoredDocKey("key");
    ASSERT_EQ(cb::engine_errc::success,
              store->set(*makeCommittedItem(key, "value1"), cookie));
    ASSERT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*makePendingItem(key, "value2"), cookie));

    // Test
    auto gv = store->getLocked(key, vbid, ep_current_time(), 10, cookie);
    EXPECT_EQ(cb::engine_errc::sync_write_in_progress, gv.getStatus());
}

// Test that unlock correctly returns ESyncWriteInProgress if targetted at
// a key which has a prepared SyncWrite in progress.
TEST_P(KVBucketParamTest, UnlockWithPreparedSyncWrite) {
    // Setup - need a valid topology to accept SyncWrites - but don't want them
    // to auto-commit so create a topology with 2 nodes.
    auto meta =
            nlohmann::json{{"topology", nlohmann::json::array({{"a", "b"}})}};
    ASSERT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, &meta));

    // Store both a committed and prepared SV.
    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "value1");
    ASSERT_EQ(cb::engine_errc::success, store->set(*committed, cookie));
    ASSERT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*makePendingItem(key, "value2"), cookie));

    // Test
    EXPECT_EQ(
            cb::engine_errc::sync_write_in_progress,
            store->unlockKey(
                    key, vbid, committed->getCas(), ep_current_time(), cookie));
}

// Test that GAT correctly returns ESyncWriteInProgress if targetted at
// a key which has a prepared SyncWrite in progress.
TEST_P(KVBucketParamTest, GetAndUpdateTtlWithPreparedSyncWrite) {
    // Setup - need a valid topology to accept SyncWrites - but don't want them
    // to auto-commit so create a topology with 2 nodes.
    auto meta =
            nlohmann::json{{"topology", nlohmann::json::array({{"a", "b"}})}};
    ASSERT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, &meta));

    // Store both a committed and prepared SV.
    auto key = makeStoredDocKey("key");
    ASSERT_EQ(cb::engine_errc::success,
              store->set(*makeCommittedItem(key, "value1"), cookie));
    ASSERT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*makePendingItem(key, "value2"), cookie));

    // Test
    auto gv = store->getAndUpdateTtl(key, vbid, cookie, 10);
    EXPECT_EQ(cb::engine_errc::sync_write_in_progress, gv.getStatus());
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
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketType() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for replace
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    auto replace_item = make_item(vbid, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->replace(replace_item, cookie));
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
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    cb::engine_errc expRetCode = cb::engine_errc::not_supported;
    if (engine->getConfiguration().getBucketType() == "persistent") {
        expTempItems = 1;
        expRetCode = cb::engine_errc::no_such_key;
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
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketType() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for getAndUpdateTtl
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    GetValue gv = store->getAndUpdateTtl(makeStoredDocKey("key"), vbid,
                                         cookie, time(NULL));
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());
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
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketType() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for validateKey
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());

    // dummy item; don't expect to need it for deleted case.
    auto dummy = make_item(vbid, key, {});
    std::string result = store->validateKey(key, vbid, dummy);
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

    Item item = store_item(Vbid(0),
                           key,
                           value,
                           1,
                           {cb::engine_errc::success},
                           PROTOCOL_BINARY_DATATYPE_XATTR);

    TimeTraveller docBrown(20);

    // 2. Force expiry of the item and flush the delete
    auto options =
            static_cast<get_options_t>(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    auto doGet = [&]() { return store->get(key, vbid, cookie, options); };
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

    if (engine->getConfiguration().getBucketType() == "persistent") {
        EXPECT_EQ(cb::engine_errc::would_block, engineResult);
        // Manually run the bgfetch task, and re-attempt getMetaData
        runBGFetcherTask();

        engineResult = doGetMetaData();
    }
    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);
    ASSERT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, datatype);
    ASSERT_EQ(item.getFlags(), itemMeta.flags);
    // CAS and revSeqno not checked as changed when the document was expired.

    // 4. Now get deleted value - we want to retrieve the _sync field.
    result = doGet();

    // Manually run the bgfetch task and retry the get()
    if (engine->getConfiguration().getBucketType() == "persistent") {
        ASSERT_EQ(cb::engine_errc::would_block, result.getStatus());
        runBGFetcherTask();
        result = doGet();
    }
    ASSERT_EQ(cb::engine_errc::success, result.getStatus());

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

     if (isPersistent()) {
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

     if (isPersistent() && isFullEviction()) {
         ASSERT_EQ(cb::engine_errc::would_block, engineResult);
         // Manually run the bgfetch task, and re-attempt getMetaData
         runBGFetcherTask();

         engineResult = doGetMetaData();
     }
     // Verify that GetMeta succeeded; and metadata is correct.
     ASSERT_EQ(cb::engine_errc::success, engineResult);
     EXPECT_EQ(3, itemMeta.revSeqno);
}

TEST_P(KVBucketParamTest, numberOfVBucketsInState) {
    EXPECT_EQ(1, store->getNumOfVBucketsInState(vbucket_state_active));
    EXPECT_EQ(0, store->getNumOfVBucketsInState(vbucket_state_replica));
}

/**
 * Test to verify if the vbucket opsGet stat is incremented when
 * the vbucket is in pending state in the case of a get.
 * Test in the case of a getReplica it does not increase the opsGet
 * stat but instead increases the the not my vbucket stat.
 */
TEST_P(KVBucketParamTest, testGetPendingOpsStat) {
   auto key = makeStoredDocKey("key");
   store_item(vbid, key, "value");

   store->setVBucketState(vbid, vbucket_state_pending);

   auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

   auto doGet = [&]() { return store->get(key, vbid, cookie, options); };
   GetValue result = doGet();
   ASSERT_EQ(cb::engine_errc::would_block, result.getStatus());
   EXPECT_EQ(1, store->getVBucket(vbid)->opsGet);

   auto doGetReplica = [&]() { return store->getReplica(key, vbid, cookie, options); };
   result = doGetReplica();
   ASSERT_EQ(cb::engine_errc::not_my_vbucket, result.getStatus());
   EXPECT_EQ(1, store->getVBucket(vbid)->opsGet);
   EXPECT_EQ(1, engine->getEpStats().numNotMyVBuckets);
}

// Test that GetReplica against an expired item correctly returns ENOENT.
// Regression test for MB-38498.
TEST_P(KVBucketParamTest, ReplicaExpiredItem) {
    // Create a document with TTL=10s, then advance clock by 20s so it is
    // past it expiration.
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value", ep_real_time() + 10);
    // Flush so item is clean and can be evicted.
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Evict the item (to check bgfetch logic)
    if (!ephemeral()) {
        const char* msg = nullptr;
        ASSERT_EQ(cb::mcbp::Status::Success, store->evictKey(key, vbid, &msg))
                << msg;
    }

    TimeTraveller hgWells(20);

    // Change to replica so we can test getReplica()
    store->setVBucketState(vbid, vbucket_state_replica);

    // Same default options as getReplicaCmd
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    // Test: Attempt to read expired item.
    if (engine->getConfiguration().getItemEvictionPolicy() == "full_eviction") {
        auto result = store->getReplica(key, vbid, nullptr, options);
        EXPECT_EQ(cb::engine_errc::would_block, result.getStatus());
        runBGFetcherTask();
    }

    auto result = store->getReplica(key, vbid, nullptr, options);
    EXPECT_EQ(cb::engine_errc::no_such_key, result.getStatus());
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

// Check that getRandomKey works correctly when given a random value of zero
TEST_P(KVBucketParamTest, MB31495_GetRandomKey) {
    std::function<long()> returnZero = []() { return 0; };
    setRandomFunction(returnZero);

    // Try with am empty hash table
    auto gv = store->getRandomKey(CollectionID::Default, cookie);
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

    Item item = store_item(
            vbid, {"key", DocKeyEncodesCollectionId::No}, "value", 0);
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Try with a non-empty hash table
    gv = store->getRandomKey(CollectionID::Default, cookie);
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
}

// MB-33702: Test that SetVBucket state creates a new failover table entry when
// transitioning from non-active to active.
TEST_P(KVBucketParamTest, FailoverEntryAddedNonActiveToActive) {
    // Setup - set vBucket to a non-active state.
    store->setVBucketState(vbid, vbucket_state_replica);
    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(1, vb->failovers->getNumEntries());

    // Test
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active));
    EXPECT_EQ(2, vb->failovers->getNumEntries());
}

// MB-33702: Test that SetVBucket state doesn't create a new failover table
// entry when set to active when already active - this can happen if the
// replication topology is changed (but state stays as active).
TEST_P(KVBucketParamTest, FailoverEntryNotAddedActiveToActive) {
    // Setup - Should start in active state.
    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(vbucket_state_active, vb->getState());
    ASSERT_EQ(1, vb->failovers->getNumEntries());

    // Test - with a topology specified, we shouldn't get a new failover entry.
    auto meta =
            nlohmann::json{{"topology", nlohmann::json::array({{"a", "b"}})}};
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, &meta));
    EXPECT_EQ(1, vb->failovers->getNumEntries());
}

// Test that expiring a compressed xattr doesn't trigger any errors
TEST_P(KVBucketParamTest, MB_34346) {
    // Create an XTTR value with only a large system xattr, and compress the lot
    // Note the large xattr should be highly compressible to make it easier to
    // trigger the MB.
    cb::xattr::Blob blob;
    blob.set("_sync",
             R"({"fffff":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})");
    auto xattr = blob.finalize();
    cb::compression::Buffer output;
    cb::compression::deflate(cb::compression::Algorithm::Snappy,
                             {xattr.data(), xattr.size()},
                             output);
    EXPECT_LT(output.size(), xattr.size())
            << "Expected the compressed buffer to be smaller than the input";

    auto key = makeStoredDocKey("key_1");
    store_item(
            vbid,
            key,
            {output.data(), output.size()},
            ep_abs_time(ep_current_time() + 10),
            {cb::engine_errc::success},
            PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_SNAPPY);

    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(1, engine->getVBucket(vbid)->getNumItems())
            << "Should have 1 item after calling store()";

    TimeTraveller docBrown(15);

    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
    GetValue gv2 = store->get(key, vbid, cookie, options);
    EXPECT_TRUE(gv2.item->isDeleted());
    // Check that the datatype does not include SNAPPY
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, gv2.item->getDataType());
    // Check the returned blob is what we initially set
    cb::xattr::Blob returnedBlob(
            {const_cast<char*>(gv2.item->getData()), gv2.item->getNBytes()},
            false);
    EXPECT_STREQ(
            "{\"fffff\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}",
            reinterpret_cast<char*>(returnedBlob.get("_sync").data()));

    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(0, engine->getVBucket(vbid)->getNumItems())
            << "Should still have 0 items after time-travelling/expiry";
}

// Test that calling getPerVBucketDiskStats when a vBucket file hasn't yet been
// flushed to disk doesn't throw an exception.
// Regression test for MB-35560.
TEST_P(KVBucketParamTest, VBucketDiskStatsENOENT) {
    // Shouldn't see any stats calls if the vBucket doesn't exist.
    auto mockStatFn = [](std::string_view key,
                         std::string_view value,
                         gsl::not_null<const void*> cookie) { ADD_FAILURE(); };

    auto expected = (isPersistent()) ? cb::engine_errc::success
                                     : cb::engine_errc::no_such_key;
    EXPECT_EQ(expected, store->getPerVBucketDiskStats({}, mockStatFn));
}

TEST_P(KVBucketParamTest, VbucketStateCounts) {
    // confirm the vbMap correctly changes the number of vbuckets in a given
    // state when vbuckets change state
    auto vbA = Vbid(0);
    auto vbB = Vbid(1);

    auto expectVbCounts = [this](uint16_t active, uint16_t replica) {
        auto message = "Expected " + std::to_string(active) + " active and " +
                       std::to_string(replica) + " replica vbs";
        EXPECT_EQ(active, store->getNumOfVBucketsInState(vbucket_state_active))
                << message;
        EXPECT_EQ(replica,
                  store->getNumOfVBucketsInState(vbucket_state_replica))
                << message;
    };
    store->setVBucketState(vbA, vbucket_state_active);
    expectVbCounts(1, 0);
    store->setVBucketState(vbB, vbucket_state_active);
    expectVbCounts(2, 0);
    store->setVBucketState(vbA, vbucket_state_replica);
    expectVbCounts(1, 1);
    store->setVBucketState(vbB, vbucket_state_replica);
    expectVbCounts(0, 2);
}

class StoreIfTest : public KVBucketTest {
public:
    void SetUp() override {
        config_string += "warmup=false";
        KVBucketTest::SetUp();
        // Have all the objects, activate vBucket zero so we can store data.
        store->setVBucketState(vbid, vbucket_state_active);
    }
};

/**
 * Test the basic store_if (via engine) - a forced fail predicate will allow
 * add, but fail set/replace with predicate_failed
 */
TEST_F(StoreIfTest, store_if_basic) {
    cb::StoreIfPredicate pred = [](const std::optional<item_info>& existing,
                                   cb::vbucket_info vb) -> cb::StoreIfStatus {
        return cb::StoreIfStatus::Fail;
    };
    auto item = make_item(
            vbid, {"key", DocKeyEncodesCollectionId::No}, "value", 0, 0);
    auto rv = engine->storeIfInner(
            cookie, item, 0, StoreSemantics::Add, pred, false);
    EXPECT_EQ(cb::engine_errc::success, rv.status);
    rv = engine->storeIfInner(
            cookie, item, 0, StoreSemantics::Replace, pred, false);
    EXPECT_EQ(cb::engine_errc::predicate_failed, rv.status);
    rv = engine->storeIfInner(
            cookie, item, 0, StoreSemantics::Set, pred, false);
    EXPECT_EQ(cb::engine_errc::predicate_failed, rv.status);
}

class RelativeExpiryLimitTest : public KVBucketTest {
public:
    void SetUp() override {
        config_string += "max_ttl=2592000";
        KVBucketTest::SetUp();
        // Have all the objects, activate vBucket zero so we can store data.
        store->setVBucketState(vbid, vbucket_state_active);
    }
};

// Test add/set/replace gets an enforced expiry
TEST_F(RelativeExpiryLimitTest, add_set_replace) {
    auto item1 = make_item(vbid, makeStoredDocKey("key1"), "value");
    auto item2 = make_item(vbid, makeStoredDocKey("key2"), "value");
    auto item3 = make_item(vbid, makeStoredDocKey("key2"), "value");

    ASSERT_EQ(0, item1.getExptime());
    ASSERT_EQ(0, item2.getExptime());

    // add a key and set a key
    EXPECT_EQ(cb::engine_errc::success, store->add(item1, cookie));
    EXPECT_EQ(cb::engine_errc::success, store->set(item2, cookie));

    std::vector<cb::EngineErrorItemPair> results;

    auto f = [](const item_info&) { return true; };
    results.push_back(engine->getIfInner(cookie, item1.getKey(), vbid, f));
    results.push_back(engine->getIfInner(cookie, item2.getKey(), vbid, f));

    // finally replace key2
    EXPECT_EQ(cb::engine_errc::success, store->replace(item3, cookie));
    results.push_back(engine->getIfInner(cookie, item2.getKey(), vbid, f));

    for (const auto& rval : results) {
        ASSERT_EQ(cb::engine_errc::success, rval.first);
        Item* i = reinterpret_cast<Item*>(rval.second.get());
        auto info = engine->getItemInfo(*i);
        EXPECT_NE(0, info.exptime);
    }
}

// Test that GAT with a limit stops 0 expiry
TEST_F(RelativeExpiryLimitTest, gat) {
    // This will actually skip the initial expiry limiting code as this function
    // doesn't use itemAllocate
    Item item = store_item(
            vbid, {"key", DocKeyEncodesCollectionId::No}, "value", 0);

    // Now touch with 0
    auto rval = engine->getAndTouchInner(
            cookie, {"key", DocKeyEncodesCollectionId::No}, vbid, 0);

    ASSERT_EQ(cb::engine_errc::success, rval.first);

    Item* i = reinterpret_cast<Item*>(rval.second.get());
    auto info = engine->getItemInfo(*i);
    EXPECT_NE(0, info.exptime);
}

class AbsoluteExpiryLimitTest : public KVBucketTest {
public:
    void SetUp() override {
        config_string += "max_ttl=2592001";
        KVBucketTest::SetUp();
        // Have all the objects, activate vBucket zero so we can store data.
        store->setVBucketState(vbid, vbucket_state_active);
    }
};

TEST_F(AbsoluteExpiryLimitTest, MB_37643) {
    // Go forwards by 30 days + 1000 seconds, we want uptime > max_ttl
    const int uptime = (60 * 60 * 24 * 30) + 1000;
    TimeTraveller biff(uptime);

    auto item = store_item(Vbid(0), makeStoredDocKey("key"), "value");

    // We expect that the expiry time is in the future. The future here being
    // current_time + our time shift.

    // We expect that the expiry is at least now+uptime+max_ttl
    EXPECT_GT(item.getExptime(), ep_abs_time(ep_current_time()));
}

// Test cases which run for EP (Full and Value eviction) and Ephemeral
INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         KVBucketParamTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
