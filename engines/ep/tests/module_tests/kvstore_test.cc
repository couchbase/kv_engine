/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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
#include "kvstore_test.h"

#include <platform/dirutils.h>

#include "bucket_logger.h"
#include "collections/vbucket_manifest.h"
#include "couch-kvstore/couch-kvstore-config.h"
#include "couch-kvstore/couch-kvstore.h"
#include "item.h"
#include "kvstore.h"
#include "kvstore_config.h"
#include "tests/mock/mock_couch_kvstore.h"
#include "vb_commit.h"
#ifdef EP_USE_ROCKSDB
#include "rocksdb-kvstore/rocksdb-kvstore_config.h"
#endif
#include "collections/collection_persisted_stats.h"
#ifdef EP_USE_MAGMA
#include "../mock/mock_magma_kvstore.h"
#include "magma-kvstore/magma-kvstore.h"
#include "magma-kvstore/magma-kvstore_config.h"
#include "magma-kvstore/magma-kvstore_iorequest.h"
#endif
#include "programs/engine_testapp/mock_server.h"
#include "src/internal.h"
#include "src/rollback_result.h"
#include "test_helpers.h"
#include "tests/module_tests/test_helpers.h"
#include "tests/test_fileops.h"
#include "thread_gate.h"
#include "tools/couchfile_upgrade/input_couchfile.h"
#include "tools/couchfile_upgrade/output_couchfile.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"
#include "workload.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <kvstore.h>
#include <thread>
#include <unordered_map>
#include <utility>

#include <vector>

using namespace std::string_literals;

class KVStoreTestCacheCallback : public StatusCallback<CacheLookup> {
public:
    KVStoreTestCacheCallback(int64_t s, int64_t e, Vbid vbid)
        : start(s), end(e), vb(vbid) {
    }

    void callback(CacheLookup& lookup) override {
        EXPECT_EQ(vb, lookup.getVBucketId());
        EXPECT_LE(start, lookup.getBySeqno());
        EXPECT_LE(lookup.getBySeqno(), end);
    }

private:
    int64_t start;
    int64_t end;
    Vbid vb;
};

class GetCallback : public StatusCallback<GetValue> {
public:
    GetCallback(ENGINE_ERROR_CODE _expectedErrorCode = ENGINE_SUCCESS) :
        expectCompressed(false),
        expectedErrorCode(_expectedErrorCode) { }

    GetCallback(bool expect_compressed,
                ENGINE_ERROR_CODE _expectedErrorCode = ENGINE_SUCCESS) :
        expectCompressed(expect_compressed),
        expectedErrorCode(_expectedErrorCode) { }

    void callback(GetValue& result) override {
        EXPECT_EQ(expectedErrorCode, result.getStatus());
        if (result.getStatus() == ENGINE_SUCCESS) {
            if (expectCompressed) {
                EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_SNAPPY,
                          result.item->getDataType());
                result.item->decompressValue();
            }

            EXPECT_EQ(0,
                      strncmp("value",
                              result.item->getData(),
                              result.item->getNBytes()));
        }
    }

private:
    bool expectCompressed;
    ENGINE_ERROR_CODE expectedErrorCode;
};

struct WriteCallback {
    void operator()(TransactionContext, KVStore::FlushStateMutation) {
    }
};

struct DeleteCallback {
public:
    void operator()(TransactionContext&, KVStore::FlushStateDeletion) {
    }
};

void checkGetValue(GetValue& result,
                   ENGINE_ERROR_CODE expectedErrorCode = ENGINE_SUCCESS,
                   bool expectCompressed = false) {
    EXPECT_EQ(expectedErrorCode, result.getStatus());
    if (result.getStatus() == ENGINE_SUCCESS) {
        if (expectCompressed) {
            EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_SNAPPY,
                      result.item->getDataType());
            result.item->decompressValue();
        }

        EXPECT_EQ(0,
                  strncmp("value",
                          result.item->getData(),
                          result.item->getNBytes()));
    }
}

class ExpiryCallback : public Callback<Item&, time_t&> {
public:
    ExpiryCallback() {}
    void callback(Item&, time_t&) override {
    }
};

/**
 * Utility template for generating callbacks for various
 * KVStore functions from a lambda/std::function
 */
template <typename... RV>
class CustomCallback : public StatusCallback<RV...> {
public:
    CustomCallback(std::function<void(RV...)> _cb)
        : cb(_cb) {}
    CustomCallback()
        : cb([](RV... val){}) {}

    void callback(RV&... result) override {
        cb(std::forward<RV>(result)...);
        processed++;
    }

    uint32_t getProcessedCount() {
        return processed;
    }

protected:
    std::function<void(RV...)> cb;

private:
    uint32_t processed = 0;
};

/**
 * Callback that can be given a lambda to use, specifically
 * for the Rollback callback
 */
class CustomRBCallback : public RollbackCB {
public:
    CustomRBCallback(std::function<void(GetValue)> _cb) : cb(std::move(_cb)) {
    }
    CustomRBCallback()
        : cb([](GetValue val){}) {}

    void callback(GetValue& result) override {
        cb(std::move(result));
    }

protected:
    std::function<void(GetValue)> cb;
};

// Initializes a KVStore
static void initialize_kv_store(KVStore* kvstore, Vbid vbid = Vbid(0)) {
    // simulate the setVbState by incrementing the rev
    kvstore->prepareToCreate(vbid);
    vbucket_state state;
    state.transition.state = vbucket_state_active;
    // simulate the setVbState by incrementing the rev
    kvstore->prepareToCreate(vbid);
    kvstore->snapshotVBucket(vbid, state);
}

// Creates and initializes a KVStore with the given config
static std::unique_ptr<KVStore> setup_kv_store(KVStoreConfig& config,
                                               std::vector<Vbid> vbids = {
                                                       Vbid(0)}) {
    auto kvstore = KVStoreFactory::create(config);
    for (auto vbid : vbids) {
        initialize_kv_store(kvstore.rw.get(), vbid);
    }
    return std::move(kvstore.rw);
}

/* Test callback for stats handling.
 * 'cookie' is a std::unordered_map<std::string, std::string) which stats
 * are accumulated in.
 */
static void add_stat_callback(std::string_view key,
                              std::string_view value,
                              gsl::not_null<const void*> cookie) {
    auto* map = reinterpret_cast<std::map<std::string, std::string>*>(
            const_cast<void*>(cookie.get()));
    ASSERT_NE(nullptr, map);
    map->insert(std::make_pair(std::string(key.data(), key.size()),
                               std::string(value.data(), value.size())));
}

KVStoreTest::KVStoreTest()
    : data_dir(dbnameFromCurrentGTestInfo()), flush(manifest) {
}

void KVStoreTest::SetUp() {
    if (cb::io::isDirectory(data_dir)) {
        try {
            cb::io::rmrf(data_dir);
        } catch (std::system_error& e) {
            throw e;
        }
    }
}

void KVStoreTest::TearDown() {
    cb::io::rmrf(data_dir);
}

/// Test fixture for tests which run only on Couchstore.
class CouchKVStoreTest : public KVStoreTest {
public:
    CouchKVStoreTest() : KVStoreTest() {
    }
};

class KVStoreParamTestSkipRocks : public KVStoreParamTest {
public:
    KVStoreParamTestSkipRocks() : KVStoreParamTest() {
    }
};

// Rocks doesn't support returning compressed values.
TEST_P(KVStoreParamTestSkipRocks, CompressedTest) {
    kvstore->begin(std::make_unique<TransactionContext>(vbid));

    for (int i = 1; i <= 5; i++) {
        std::string key("key" + std::to_string(i));
        auto qi = makeCommittedItem(makeStoredDocKey(key), "value");
        qi->setBySeqno(5);
        kvstore->set(qi);
    }
    // Ensure a valid vbstate is committed
    flush.proposedVBState.lastSnapEnd = 5;
    kvstore->commit(flush);

    auto scanCtx = kvstore->initBySeqnoScanContext(
            std::make_unique<GetCallback>(true /*expectcompressed*/),
            std::make_unique<KVStoreTestCacheCallback>(1, 5, Vbid(0)),
            Vbid(0),
            1,
            DocumentFilter::ALL_ITEMS,
            ValueFilter::VALUES_COMPRESSED,
            SnapshotSource::Head);

    ASSERT_TRUE(scanCtx);
    EXPECT_EQ(scan_success, kvstore->scan(*scanCtx));
}

// Verify the stats returned from operations are accurate.
TEST_F(CouchKVStoreTest, StatsTest) {
    CouchKVStoreConfig config(1024, 4, data_dir, "couchdb", 0);
    auto kvstore = setup_kv_store(config);

    // Perform a transaction with a single mutation (set) in it.
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    const std::string key{"key"};
    const std::string value{"value"};
    kvstore->set(makeCommittedItem(makeStoredDocKey(key), value));

    EXPECT_TRUE(kvstore->commit(flush));

    // Check statistics are correct.
    std::map<std::string, std::string> stats;
    kvstore->addStats(add_stat_callback, &stats, "");
    EXPECT_EQ("1", stats["rw_0:io_num_write"]);
    const size_t io_write_bytes = stoul(stats["rw_0:io_document_write_bytes"]);
    // 1 (for the namespace)
    EXPECT_EQ(1 + key.size() + value.size() +
                      MetaData::getMetaDataSize(MetaData::Version::V1),
              io_write_bytes);

    // Hard to determine exactly how many bytes should have been written, but
    // expect non-zero, and least as many as the actual documents.
    const size_t io_total_write_bytes = stoul(stats["rw_0:io_total_write_bytes"]);
    EXPECT_GT(io_total_write_bytes, 0);
    EXPECT_GE(io_total_write_bytes, io_write_bytes);
}

// Verify the compaction stats returned from operations are accurate.
TEST_F(CouchKVStoreTest, CompactStatsTest) {
    CouchKVStoreConfig config(1, 4, data_dir, "couchdb", 0);
    auto kvstore = setup_kv_store(config);

    // Perform a transaction with a single mutation (set) in it.
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    const std::string key{"key"};
    const std::string value{"value"};
    kvstore->set(makeCommittedItem(makeStoredDocKey(key), value));

    EXPECT_TRUE(kvstore->commit(flush));

    CompactionConfig compactionConfig;
    compactionConfig.purge_before_seq = 0;
    compactionConfig.purge_before_ts = 0;
    compactionConfig.drop_deletes = 0;
    compactionConfig.db_file_id = Vbid(0);
    auto cctx = std::make_shared<compaction_ctx>(compactionConfig, 0);

    EXPECT_TRUE(kvstore->compactDB(cctx));
    // Check statistics are correct.
    std::map<std::string, std::string> stats;
    kvstore->addStats(add_stat_callback, &stats, "");
    EXPECT_EQ("1", stats["rw_0:io_num_write"]);
    const size_t io_write_bytes = stoul(stats["rw_0:io_document_write_bytes"]);

    // Hard to determine exactly how many bytes should have been written, but
    // expect non-zero, and at least twice as many as the actual documents for
    // the total and once as many for compaction alone.
    const size_t io_total_write_bytes = stoul(stats["rw_0:io_total_write_bytes"]);
    const size_t io_compaction_write_bytes = stoul(stats["rw_0:io_compaction_write_bytes"]);
    EXPECT_GT(io_total_write_bytes, 0);
    EXPECT_GT(io_compaction_write_bytes, 0);
    EXPECT_GT(io_total_write_bytes, io_compaction_write_bytes);
    EXPECT_GE(io_total_write_bytes, io_write_bytes * 2);
    EXPECT_GE(io_compaction_write_bytes, io_write_bytes);
}

// Regression test for MB-17517 - ensure that if a couchstore file has a max
// CAS of -1, it is detected and reset to zero when file is loaded.
TEST_F(CouchKVStoreTest, MB_17517MaxCasOfMinus1) {
    CouchKVStoreConfig config(1024, 4, data_dir, "couchdb", 0);
    auto kvstore = KVStoreFactory::create(config);
    ASSERT_NE(nullptr, kvstore.rw);

    // Activate vBucket.
    vbucket_state state;
    state.transition.state = vbucket_state_active;
    state.maxCas = -1;
    EXPECT_TRUE(kvstore.rw->snapshotVBucket(Vbid(0), state));
    EXPECT_EQ(~0ull, kvstore.rw->listPersistedVbuckets()[0]->maxCas);

    // Close the file, then re-open.
    kvstore = KVStoreFactory::create(config);
    EXPECT_NE(nullptr, kvstore.rw);

    // Check that our max CAS was repaired on startup.
    EXPECT_EQ(0u, kvstore.rw->listPersistedVbuckets()[0]->maxCas);
}

// Regression test for MB-19430 - ensure that an attempt to get the
// item count from a file which doesn't exist yet propagates the
// error so the caller can detect (and retry as necessary).
TEST_F(CouchKVStoreTest, MB_18580_ENOENT) {
    CouchKVStoreConfig config(1024, 4, data_dir, "couchdb", 0);
    // Create a read-only kvstore (which disables item count caching), then
    // attempt to get the count from a non-existent vbucket.
    auto kvstore = KVStoreFactory::create(config);
    ASSERT_NE(nullptr, kvstore.ro);

    // Expect to get a system_error (ENOENT)
    EXPECT_THROW(kvstore.ro->getDbFileInfo(Vbid(0)), std::system_error);
}

class CollectionsOfflineUpgradeCallback : public StatusCallback<CacheLookup> {
public:
    CollectionsOfflineUpgradeCallback(CollectionID cid) : expectedCid(cid) {
    }

    void callback(CacheLookup& lookup) override {
        EXPECT_EQ(expectedCid, lookup.getKey().getDocKey().getCollectionID());
        callbacks++;
    }

    int callbacks = 0;
    CollectionID expectedCid;
};

class CollectionsOfflineGetCallback : public StatusCallback<GetValue> {
public:
    CollectionsOfflineGetCallback(std::pair<int, int> deletedRange)
        : deletedRange(std::move(deletedRange)) {
    }

    void callback(GetValue& result) override {
        EXPECT_EQ(ENGINE_SUCCESS, result.getStatus());

        if (result.item->isDeleted()) {
            DocKey dk = result.item->getKey();
            EXPECT_EQ(500, dk.getCollectionID());
            auto noCollection = dk.makeDocKeyWithoutCollectionID();
            EXPECT_EQ(2, noCollection.size());
            std::string str(reinterpret_cast<const char*>(noCollection.data()),
                            noCollection.size());
            auto index = std::stoi(str);
            EXPECT_GE(index, deletedRange.first);
            EXPECT_LE(index, deletedRange.second);

            if (index & 1) {
                // The odd deleted docs have no body to validate
                return;
            } else {
                EXPECT_TRUE(result.item->getDataType() &
                            PROTOCOL_BINARY_DATATYPE_XATTR);
            }
        }
        EXPECT_TRUE(PROTOCOL_BINARY_DATATYPE_SNAPPY &
                    result.item->getDataType());
        result.item->decompressValue();

        EXPECT_EQ(0,
                  strncmp("valuable",
                          result.item->getData(),
                          result.item->getNBytes()));
    }

private:
    std::pair<int, int> deletedRange;
};

// Test the InputCouchFile/OutputCouchFile objects (in a simple test) to
// check they do what we expect, that is create a new couchfile with all keys
// moved into a specified collection.
TEST_F(CouchKVStoreTest, CollectionsOfflineUpgrade) {
    CouchKVStoreConfig config1(1024, 4, data_dir, "couchdb", 0);

    CouchKVStoreConfig config2(1024, 4, data_dir, "couchdb", 0);

    // Test setup, create a new file
    auto kvstore = setup_kv_store(config1);
    kvstore->begin(std::make_unique<TransactionContext>(vbid));

    // The key count is large enough to ensure the count uses more than 1 byte
    // of leb storage so we validate that leb encode/decode works on this path
    const int keys = 129;
    const int deletedKeys = 14;

    for (int i = 0; i < keys; i++) {
        auto key = std::to_string(i);
        // create Item and use a raw key, but say it has a cid encoded so that
        // the constructor doesn't push this key into the default collection.
        // If we don't do this, the source file won't be representative of real
        // source files when the upgrade is deployed
        std::unique_ptr<Item> item = std::make_unique<Item>(
                DocKey(key, DocKeyEncodesCollectionId::Yes),
                0,
                0,
                "valuable",
                8,
                PROTOCOL_BINARY_RAW_BYTES,
                0,
                i + 1);
        kvstore->set(queued_item(std::move(item)));
    }

    kvstore->commit(flush);

    kvstore->begin(std::make_unique<TransactionContext>(vbid));

    // Delete some keys. With and without a value (like xattr)
    for (int i = 18, j = 1; i < 18 + deletedKeys; ++i, ++j) {
        std::unique_ptr<Item> item;
        auto key = std::to_string(i);
        if (i & 1) {
            item.reset(Item::makeDeletedItem(
                    DeleteSource::Explicit,
                    DocKey(key, DocKeyEncodesCollectionId::Yes),
                    0,
                    0,
                    nullptr,
                    0));
        } else {
            // Note: this is not really xattr, just checking the datatype is
            // preserved on upgrade
            item.reset(Item::makeDeletedItem(
                    DeleteSource::Explicit,
                    DocKey(key, DocKeyEncodesCollectionId::Yes),
                    0,
                    0,
                    "valuable",
                    8,
                    PROTOCOL_BINARY_DATATYPE_XATTR));
        }
        item->setBySeqno(keys + j);
        kvstore->del(queued_item(std::move(item)));
    }
    kvstore->commit(flush);

    rewriteCouchstoreVBState(Vbid(0), data_dir, 2, false /*no namespaces*/);

    // Use the upgrade tool's objects to run an upgrade
    // setup_kv_store will have progressed the rev to .2
    Collections::InputCouchFile input({}, data_dir + "/0.couch.2");
    CollectionID cid = 500;
    Collections::OutputCouchFile output({},
                                        data_dir + "/0.couch.3",
                                        cid /*collection-id*/,
                                        1024 * 1024 /*buffersize*/);
    input.upgrade(output);
    output.writeUpgradeComplete(input);
    output.commit();

    auto kvstore2 = KVStoreFactory::create(config2);
    auto scanCtx = kvstore2.rw->initBySeqnoScanContext(
            std::make_unique<CollectionsOfflineGetCallback>(
                    std::pair<int, int>{18, 18 + deletedKeys}),
            std::make_unique<CollectionsOfflineUpgradeCallback>(cid),
            Vbid(0),
            1,
            DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS,
            ValueFilter::VALUES_COMPRESSED,
            SnapshotSource::Head);

    ASSERT_TRUE(scanCtx);
    EXPECT_EQ(scan_success, kvstore2.rw->scan(*scanCtx));

    const auto& cl = static_cast<const CollectionsOfflineUpgradeCallback&>(
            scanCtx->getCacheCallback());
    EXPECT_EQ(keys, cl.callbacks);

    // Check item count
    auto kvstoreContext = kvstore2.rw->makeFileHandle(Vbid(0));
    auto stats = kvstore2.rw->getCollectionStats(*kvstoreContext, cid);
    ASSERT_TRUE(stats);
    EXPECT_EQ(keys - deletedKeys, stats->itemCount);
    EXPECT_EQ(keys + deletedKeys, stats->highSeqno);
}

/**
 * The CouchKVStoreErrorInjectionTest cases utilise GoogleMock to inject
 * errors into couchstore as if they come from the filesystem in order
 * to observe how CouchKVStore handles the error and logs it.
 *
 * The GoogleMock framework allows expectations to be set on how an object
 * will be called and how it will respond. Generally we will set a Couchstore
 * FileOps instance to return an error code on the 'nth' call as follows:
 *
 *      EXPECT_CALL(ops, open(_, _, _, _)).Times(AnyNumber());
 *      EXPECT_CALL(ops, open(_, _, _, _))
 *          .WillOnce(Return(COUCHSTORE_ERROR_OPEN_FILE)).RetiresOnSaturation();
 *      EXPECT_CALL(ops, open(_, _, _, _)).Times(n).RetiresOnSaturation();
 *
 * We will additionally set an expectation on the LoggerMock regarding how it
 * will be called by CouchKVStore. In this instance we have set an expectation
 * that the logger will be called with a logging level greater than or equal
 * to info, and the log message will contain the error string that corresponds
 * to `COUCHSTORE_ERROR_OPEN_FILE`.
 *
 *      EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
 *      EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
 *                               VCE(COUCHSTORE_ERROR_OPEN_FILE))
 *      ).Times(1).RetiresOnSaturation();
 */

using namespace testing;

/**
 * The MockBucket Logger is used to verify that the logger is called with
 * certain parameters / messages.
 *
 * The MockBucketLogger calls the log method as normal, and intercepts the
 * _sink_it call by overriding it to determine the correctness of the logging
 */
class MockBucketLogger : public BucketLogger {
public:
    MockBucketLogger(std::string name) : BucketLogger(name) {
        // Set the log level of the BucketLogger to trace to ensure messages
        // make it through to the sink it method. Does not alter the logging
        // level of the underlying spdlogger so we will not see console
        // output during the test.
        set_level(spdlog::level::level_enum::trace);
        ON_CALL(*this, mlog(_, _))
                .WillByDefault(Invoke([](spdlog::level::level_enum sev,
                                         const std::string& msg) {}));
    }

    // Mock a method taking a logging level and formatted message to test log
    // outputs.
    MOCK_CONST_METHOD2(mlog,
                       void(spdlog::level::level_enum severity,
                            const std::string& message));

protected:
    // Override the sink_it_ method to redirect to the mocked method
    // Must call the mlog method to check the message details as they are
    // bundled in the log_msg object. Beware, msg.raw is not null terminated.
    // In these test cases however we just search for a substring within the log
    // message so this is okay.
    void sink_it_(spdlog::details::log_msg& msg) override {
        mlog(msg.level, msg.raw.data());
    }
};

/**
 * VCE: Verify Couchstore Error
 *
 * This is a GoogleMock matcher which will match against a string
 * which has the corresponding message for the passed couchstore
 * error code in it. e.g.
 *
 *     VCE(COUCHSTORE_ERROR_WRITE)
 *
 * will match against a string which contains 'error writing to file'.
 */
MATCHER_P(VCE, value, "is string of %(value)") {
    return arg.find(couchstore_strerror(value)) != std::string::npos;
}

/**
 * CouchKVStoreErrorInjectionTest is used for tests which verify
 * log messages from error injection in couchstore.
 */
class CouchKVStoreErrorInjectionTest : public ::testing::Test {
public:
    CouchKVStoreErrorInjectionTest()
        : data_dir("CouchKVStoreErrorInjectionTest.db"),
          ops(create_default_file_ops()),
          logger("couchKVStoreTest"),
          config(1024, 4, data_dir, "couchdb", 0),
          flush(manifest) {
        config.setLogger(logger);
        config.setBuffered(false);
        try {
            cb::io::rmrf(data_dir.c_str());
        } catch (std::system_error& e) {
            if (e.code() != std::error_code(ENOENT, std::system_category())) {
                throw e;
            }
        }
        kvstore.reset(new CouchKVStore(
                dynamic_cast<CouchKVStoreConfig&>(config), ops));
        initialize_kv_store(kvstore.get());
    }
    ~CouchKVStoreErrorInjectionTest() override {
        cb::io::rmrf(data_dir.c_str());
    }

protected:
    void generate_items(size_t count) {
        for(unsigned i(0); i < count; i++) {
            std::string key("key" + std::to_string(i));
            auto qi = makeCommittedItem(makeStoredDocKey(key), "value");
            qi->setBySeqno(i + 1);
            items.push_back(qi);
        }
    }

    void populate_items(size_t count) {
        generate_items(count);
        kvstore->begin(std::make_unique<TransactionContext>(vbid));
        for(const auto& item: items) {
            kvstore->set(item);
        }
        // Ensure a valid vbstate is committed
        flush.proposedVBState.lastSnapEnd = items.back()->getBySeqno();
        kvstore->commit(flush);
    }

    vb_bgfetch_queue_t make_bgfetch_queue() {
        vb_bgfetch_queue_t itms;
        for(const auto& item: items) {
            vb_bgfetch_item_ctx_t ctx;
            ctx.isMetaOnly = GetMetaOnly::No;
            itms[DiskDocKey{*item}] = std::move(ctx);
        }
        return itms;
    }


    const std::string data_dir;

    ::testing::NiceMock<MockOps> ops;
    ::testing::NiceMock<MockBucketLogger> logger;

    CouchKVStoreConfig config;
    std::unique_ptr<CouchKVStore> kvstore;
    std::vector<queued_item> items;
    Collections::VB::Manifest manifest;
    VB::Commit flush;
    Vbid vbid = Vbid(0);
};

/**
 * Injects error during CouchKVStore::openDB_retry/couchstore_open_db_ex
 */
TEST_F(CouchKVStoreErrorInjectionTest, openDB_retry_open_db_ex) {
    generate_items(1);

    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    kvstore->set(items.front());
    {
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::info),
                         VCE(COUCHSTORE_ERROR_OPEN_FILE)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, open(_, _, _, _)).Times(AnyNumber());
        EXPECT_CALL(ops, open(_, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_OPEN_FILE)).RetiresOnSaturation();

        kvstore->commit(flush);
    }
}

/**
 * Injects error during CouchKVStore::openDB/couchstore_open_db_ex
 */
TEST_F(CouchKVStoreErrorInjectionTest, openDB_open_db_ex) {
    generate_items(1);

    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    kvstore->set(items.front());
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_OPEN_FILE)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, open(_, _, _, _))
            .WillRepeatedly(Return(COUCHSTORE_ERROR_OPEN_FILE)).RetiresOnSaturation();

        kvstore->commit(flush);
    }
}

/**
 * Injects error during CouchKVStore::commit/couchstore_save_documents
 */
TEST_F(CouchKVStoreErrorInjectionTest, commit_save_documents) {
    generate_items(1);

    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    kvstore->set(items.front());
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_WRITE)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pwrite(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_WRITE)).RetiresOnSaturation();

        kvstore->commit(flush);
    }

}

/**
 * Injects error during CouchKVStore::commit/couchstore_save_local_document
 */
TEST_F(CouchKVStoreErrorInjectionTest, commit_save_local_document) {
    generate_items(1);

    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    kvstore->set(items.front());
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_WRITE)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pwrite(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_WRITE)).RetiresOnSaturation();
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).Times(5).RetiresOnSaturation();

        kvstore->commit(flush);
    }

}

/**
 * Injects error during CouchKVStore::commit/couchstore_commit
 */
TEST_F(CouchKVStoreErrorInjectionTest, commit_commit) {
    generate_items(1);

    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    kvstore->set(items.front());
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_WRITE)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pwrite(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_WRITE)).RetiresOnSaturation();
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).Times(5).RetiresOnSaturation();

        kvstore->commit(flush);
    }
}

/**
 * Injects error during CouchKVStore::get/couchstore_docinfo_by_id
 */
TEST_F(CouchKVStoreErrorInjectionTest, get_docinfo_by_id) {
    populate_items(1);
    GetValue gv;
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_READ)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(3).RetiresOnSaturation();
        gv = kvstore->get(DiskDocKey{*items.front()}, Vbid(0));
    }
    EXPECT_EQ(ENGINE_TMPFAIL, gv.getStatus());
}

/**
 * Injects error during CouchKVStore::get/couchstore_open_doc_with_docinfo
 */
TEST_F(CouchKVStoreErrorInjectionTest, get_open_doc_with_docinfo) {
    populate_items(1);
    GetValue gv;
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_READ)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(5).RetiresOnSaturation();
        gv = kvstore->get(DiskDocKey{*items.front()}, Vbid(0));
    }
    EXPECT_EQ(ENGINE_TMPFAIL, gv.getStatus());
}

/**
 * Injects error during CouchKVStore::getMulti/couchstore_docinfos_by_id
 */
TEST_F(CouchKVStoreErrorInjectionTest, getMulti_docinfos_by_id) {
    populate_items(1);
    vb_bgfetch_queue_t itms(make_bgfetch_queue());
    {

        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_READ)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(3).RetiresOnSaturation();
        kvstore->getMulti(Vbid(0), itms);
    }
    EXPECT_EQ(ENGINE_TMPFAIL, itms[DiskDocKey{*items.at(0)}].value.getStatus());
}


/**
 * Injects error during CouchKVStore::getMulti/couchstore_open_doc_with_docinfo
 */
TEST_F(CouchKVStoreErrorInjectionTest, getMulti_open_doc_with_docinfo) {
    populate_items(1);
    vb_bgfetch_queue_t itms(make_bgfetch_queue());
    {
        /* Check preconditions */
        ASSERT_EQ(0, kvstore->getKVStoreStat().numGetFailure);

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(5).RetiresOnSaturation();
        kvstore->getMulti(Vbid(0), itms);

        EXPECT_EQ(1, kvstore->getKVStoreStat().numGetFailure);
    }
    EXPECT_EQ(ENGINE_TMPFAIL, itms[DiskDocKey{*items.at(0)}].value.getStatus());
}

/**
 * Injects error during CouchKVStore::compactDB/couchstore_compact_db_ex
 */
TEST_F(CouchKVStoreErrorInjectionTest, compactDB_compact_db_ex) {
    populate_items(1);

    CompactionConfig config;
    config.purge_before_seq = 0;
    config.purge_before_ts = 0;
    config.drop_deletes = 0;
    config.db_file_id = Vbid(0);
    auto cctx = std::make_shared<compaction_ctx>(config, 0);

    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_OPEN_FILE)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, open(_, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_OPEN_FILE)).RetiresOnSaturation();
        EXPECT_CALL(ops, open(_, _, _, _)).Times(1).RetiresOnSaturation();
        kvstore->compactDB(cctx);
    }
}

/**
 * Injects error during CouchKVStore::reset/couchstore_commit
 */
TEST_F(CouchKVStoreErrorInjectionTest, reset_commit) {
    populate_items(1);
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_READ)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, sync(_, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();

        kvstore->reset(Vbid(0));
    }
}

/**
 * Injects error during
 * CouchKVStore::initBySeqnoScanContext/couchstore_changes_count
 */
TEST_F(CouchKVStoreErrorInjectionTest, initBySeqnoScanContext_changes_count) {
    populate_items(1);
    {
        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(3).RetiresOnSaturation();

        auto scanCtx = kvstore->initBySeqnoScanContext(
                std::make_unique<CustomCallback<GetValue>>(),
                std::make_unique<CustomCallback<CacheLookup>>(),
                Vbid(0),
                0,
                DocumentFilter::ALL_ITEMS,
                ValueFilter::VALUES_DECOMPRESSED,
                SnapshotSource::Head);
        EXPECT_FALSE(scanCtx)
                << "kvstore->initBySeqnoScanContext(cb, cl, 0, 0, "
                   "DocumentFilter::ALL_ITEMS, "
                   "ValueFilter::VALUES_DECOMPRESSED); should "
                   "have returned NULL";
    }
}

/**
 * Injects error during CouchKVStore::scan/couchstore_changes_since
 */
TEST_F(CouchKVStoreErrorInjectionTest, scan_changes_since) {
    populate_items(1);
    auto scan_context = kvstore->initBySeqnoScanContext(
            std::make_unique<CustomCallback<GetValue>>(),
            std::make_unique<CustomCallback<CacheLookup>>(),
            Vbid(0),
            0,
            DocumentFilter::ALL_ITEMS,
            ValueFilter::VALUES_DECOMPRESSED,
            SnapshotSource::Head);
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_READ)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();

        kvstore->scan(*scan_context);
    }
}

/**
 * Injects error during CouchKVStore::recordDbDump/couchstore_open_doc_with_docinfo
 */
TEST_F(CouchKVStoreErrorInjectionTest, recordDbDump_open_doc_with_docinfo) {
    populate_items(1);
    auto scan_context = kvstore->initBySeqnoScanContext(
            std::make_unique<CustomCallback<GetValue>>(),
            std::make_unique<CustomCallback<CacheLookup>>(),
            Vbid(0),
            0,
            DocumentFilter::ALL_ITEMS,
            ValueFilter::VALUES_DECOMPRESSED,
            SnapshotSource::Head);
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_READ)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(2).RetiresOnSaturation();

        kvstore->scan(*scan_context);
    }
}

/**
 * Injects error during CouchKVStore::rollback/couchstore_changes_count/1
 */
TEST_F(CouchKVStoreErrorInjectionTest, rollback_changes_count1) {
    generate_items(6);

    for(const auto item: items) {
        kvstore->begin(std::make_unique<TransactionContext>(vbid));
        kvstore->set(item);
        kvstore->commit(flush);
    }

    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_READ)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(3).RetiresOnSaturation();

        kvstore->rollback(Vbid(0), 5, std::make_unique<CustomRBCallback>());
    }
}

/**
 * Injects error during CouchKVStore::rollback/couchstore_rewind_header
 */
TEST_F(CouchKVStoreErrorInjectionTest, rollback_rewind_header) {
    generate_items(6);

    for(const auto item: items) {
        kvstore->begin(std::make_unique<TransactionContext>(vbid));
        kvstore->set(item);
        kvstore->commit(flush);
    }

    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_DB_NO_LONGER_VALID)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            /* Doing an ALLOC_FAIL as Couchstore will just
             * keep rolling back otherwise */
            .WillOnce(Return(COUCHSTORE_ERROR_ALLOC_FAIL)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(9).RetiresOnSaturation();

        kvstore->rollback(Vbid(0), 5, std::make_unique<CustomRBCallback>());
    }
}

/**
 * Injects error during CouchKVStore::rollback/couchstore_changes_count/2
 */
TEST_F(CouchKVStoreErrorInjectionTest, rollback_changes_count2) {
    generate_items(6);

    for(const auto item: items) {
        kvstore->begin(std::make_unique<TransactionContext>(vbid));
        kvstore->set(item);
        kvstore->commit(flush);
    }

    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_READ)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(11).RetiresOnSaturation();

        kvstore->rollback(Vbid(0), 5, std::make_unique<CustomRBCallback>());
    }
}

/**
 * Injects error during CouchKVStore::readVBState/couchstore_open_local_document
 */
TEST_F(CouchKVStoreErrorInjectionTest, readVBState_open_local_document) {
    generate_items(6);

    for(const auto item: items) {
        kvstore->begin(std::make_unique<TransactionContext>(vbid));
        // Commit a valid vbstate
        flush.proposedVBState.lastSnapEnd = item->getBySeqno();
        kvstore->set(item);
        kvstore->commit(flush);
    }

    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_READ)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        // Called twice, once when we read the vbstate from disk in
        // initBySeqnoScanContext, and again when we read the vbstate as part of
        // rollback.
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .Times(2)
                .WillRepeatedly(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(20).RetiresOnSaturation();

        EXPECT_EQ(
                false,
                kvstore->rollback(
                               Vbid(0), 5, std::make_unique<CustomRBCallback>())
                        .success);
    }
}

/**
 * Injects error during CouchKVStore::getAllKeys/couchstore_all_docs
 */
TEST_F(CouchKVStoreErrorInjectionTest, getAllKeys_all_docs) {
    populate_items(1);

    auto adcb(std::make_shared<CustomCallback<const DiskDocKey&>>());
    auto start = makeDiskDocKey("");
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_READ)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(3).RetiresOnSaturation();

        kvstore->getAllKeys(Vbid(0), start, 1, adcb);
    }
}

/**
 * Injects error during CouchKVStore::closeDB/couchstore_close_file
 */
TEST_F(CouchKVStoreErrorInjectionTest, closeDB_close_file) {
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_FILE_CLOSE)))
                .Times(1)
                .RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, close(_, _)).Times(AnyNumber());
        EXPECT_CALL(ops, close(_, _))
                .WillOnce(DoAll(IgnoreResult(Invoke(ops.get_wrapped(),
                                                    &FileOpsInterface::close)),
                                Return(COUCHSTORE_ERROR_FILE_CLOSE)))
                .RetiresOnSaturation();

        populate_items(1);
    }
}

/**
 * Injects error during CouchKVStore::saveDocs/couchstore_docinfos_by_id
 */
TEST_F(CouchKVStoreErrorInjectionTest, savedocs_doc_infos_by_id) {
    // Insert some items into the B-Tree
    generate_items(6);

    for (const auto item : items) {
        kvstore->begin(std::make_unique<TransactionContext>(vbid));
        kvstore->set(item);
        kvstore->commit(flush);
    }

    {
        generate_items(1);

        kvstore->begin(std::make_unique<TransactionContext>(vbid));
        kvstore->set(items.front());
        {
            /* Establish Logger expectation */
            EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
            EXPECT_CALL(logger,
                        mlog(Ge(spdlog::level::level_enum::warn),
                             VCE(COUCHSTORE_ERROR_READ)))
                    .Times(1)
                    .RetiresOnSaturation();

            /* Establish FileOps expectation */
            EXPECT_CALL(ops, pread(_, _, _, _, _))
                    .WillOnce(Return(COUCHSTORE_ERROR_READ))
                    .RetiresOnSaturation();
            EXPECT_CALL(ops, pread(_, _, _, _, _))
                    .Times(4)
                    .RetiresOnSaturation();

            kvstore->commit(flush);
        }
    }
}

/**
 * Verify the failed compaction statistic is accurate.
 */
TEST_F(CouchKVStoreErrorInjectionTest, CompactFailedStatsTest) {
    populate_items(1);

    CompactionConfig config;
    auto cctx = std::make_shared<compaction_ctx>(config, 0);

    {
        /* Establish FileOps expectation */
        EXPECT_CALL(ops, open(_, _, _, _)).WillOnce(
                Return(COUCHSTORE_ERROR_OPEN_FILE)).RetiresOnSaturation();
        EXPECT_CALL(ops, open(_, _, _, _)).Times(1).RetiresOnSaturation();
        kvstore->compactDB(cctx);
    }

    // Check the fail compaction statistic is correct.
    std::map<std::string, std::string> stats;
    kvstore->addStats(add_stat_callback, &stats, "");

    EXPECT_EQ("1", stats["rw_0:failure_compaction"]);
}

/**
 * Injects corruption (invalid header length) during
 * CouchKVStore::readVBState/couchstore_open_local_document
 */
TEST_F(CouchKVStoreErrorInjectionTest, corruption_get_open_doc_with_docinfo) {
    // Create a couchstore file with an item in it.
    populate_items(1);

    // Attempt to read the item.
    GetValue gv;
    {
        // Should see a sequence of preads - the penultimate one is a read
        // of the value's chunk length. For that we corrupt it so to check
        // that checksum fail is detected and reported correctly.
        {
            // ProTip: These values should be stable; but if they are not (and
            // test starts to fail after unrelated changes) then run with
            // "--gmock_verbose=info" to show a trace of what parameters pread
            // is being called with.
            using ::testing::Sequence;
            InSequence s;
            // 1 byte - detect block type
            EXPECT_CALL(ops, pread(_, _, _, 1, _));
            // 8 bytes - file header
            EXPECT_CALL(ops, pread(_, _, _, 8, _));
            // <variable> - byId tree root
            EXPECT_CALL(ops, pread(_, _, _, _, _));
            // 8 bytes - header
            EXPECT_CALL(ops, pread(_, _, _, 8, _));
            // <variable - seqno tree root
            EXPECT_CALL(ops, pread(_, _, _, _, _));

            // chunk header - we want to corrupt the length (1st 32bit word)
            // so the checksum fails.
            EXPECT_CALL(ops, pread(_, _, _, 8, _))
                    .WillOnce(Invoke([this](couchstore_error_info_t* errinfo,
                                            couch_file_handle handle,
                                            void* buf,
                                            size_t nbytes,
                                            cs_off_t offset) -> ssize_t {
                        // First perform the real pread():
                        auto res = ops.get_wrapped()->pread(
                                errinfo, handle, buf, nbytes, offset);
                        // Now check and modify the return value.
                        auto* length_ptr = reinterpret_cast<uint32_t*>(buf);
                        EXPECT_EQ(0x80000007, htonl(*length_ptr))
                                << "Unexpected chunk.length for value chunk";

                        // assumptions pass; now make length too small so CRC32
                        // should mismatch.
                        *length_ptr = ntohl(0x80000006);
                        return res;
                    }));
            // Final read of the value's data (should be size 6 given we
            // changed the chunk.length above).
            EXPECT_CALL(ops, pread(_, _, _, 6, _));
        }

        // As a result, expect to see a CHECKSUM_FAIL log message
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_CHECKSUM_FAIL)))
                .Times(1)
                .RetiresOnSaturation();

        // Trigger the get().
        gv = kvstore->get(DiskDocKey{*items.front()}, Vbid(0));
    }
    EXPECT_EQ(ENGINE_TMPFAIL, gv.getStatus());
}

//
// Explicitly test couchstore (not valid for other KVStores)
// Intended to ensure we can read and write couchstore files and
// parse metadata we store in them.
//
class CouchstoreTest : public ::testing::Test {
public:
    CouchstoreTest()
        : data_dir("CouchstoreTest.db"),
          vbid(0),
          config(1024, 4, data_dir, "couchdb", 0),
          flush(manifest) {
        config.setBuffered(false);
        try {
            cb::io::rmrf(data_dir.c_str());
        } catch (std::system_error& e) {
            if (e.code() != std::error_code(ENOENT, std::system_category())) {
                throw e;
            }
        }
        kvstore.reset(new MockCouchKVStore(config));
        std::string failoverLog("");
        // simulate a setVBState - increment the rev and then persist the
        // state
        kvstore->prepareToCreateImpl(vbid);
        vbucket_state state;
        state.transition.state = vbucket_state_active;
        // simulate a setVBState - increment the dbFile revision
        kvstore->prepareToCreateImpl(vbid);
        kvstore->snapshotVBucket(vbid, state);
    }

    ~CouchstoreTest() override {
        cb::io::rmrf(data_dir.c_str());
    }

protected:
    std::string data_dir;
    std::unique_ptr<MockCouchKVStore> kvstore;
    Vbid vbid;
    CouchKVStoreConfig config;
    Collections::VB::Manifest manifest;
    VB::Commit flush;
};

template<class T>
class MockedGetCallback : public Callback<T> {
    public:
        MockedGetCallback() {}

        void callback(GetValue& value) override {
            status(value.getStatus());
            if (value.getStatus() == ENGINE_SUCCESS) {
                EXPECT_CALL(*this, value("value"));
                cas(value.item->getCas());
                expTime(value.item->getExptime());
                flags(value.item->getFlags());
                datatype(protocol_binary_datatype_t(value.item->getDataType()));
                this->value(std::string(value.item->getData(),
                                        value.item->getNBytes()));
                savedValue = std::move(value);
            }
        }

        Item* getValue() {
            return savedValue.item.get();
        }

        /*
         * Define a number of mock methods that will be invoked by the
         * callback method. Functions can then setup expectations of the
         * value of each method e.g. expect cas to be -1
         */
        MOCK_METHOD1_T(status, void(ENGINE_ERROR_CODE));
        MOCK_METHOD1_T(cas, void(uint64_t));
        MOCK_METHOD1_T(expTime, void(uint32_t));
        MOCK_METHOD1_T(flags, void(uint32_t));
        MOCK_METHOD1_T(datatype, void(protocol_binary_datatype_t));
        MOCK_METHOD1_T(value, void(std::string));
    private:
        GetValue savedValue;
};

/*
 * The overall aim of these tests is to create an Item, write it to disk
 * then read it back from disk and look at various fields which are
 * built from the couchstore rev_meta feature.
 *
 * Validation of the Item read from disk is performed by the GetCallback.
 * A number of validators can be called upon which compare the disk Item
 * against an expected Item.
 *
 * The MockCouchKVStore exposes some of the internals of the class so we
 * can inject custom metadata by using ::setAndReturnRequest instead of ::set
 *
 */
TEST_F(CouchstoreTest, noMeta) {
    StoredDocKey key = makeStoredDocKey("key");
    auto item = makeCommittedItem(key, "value");
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    auto* request = kvstore->setAndReturnRequest(item);

    // Now directly mess with the metadata of the value which will be written
    MockCouchRequest::MetaData meta;
    request->writeMetaData(meta, 0); // no meta!

    kvstore->commit(flush);

    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    checkGetValue(gv, ENGINE_TMPFAIL);
}

TEST_F(CouchstoreTest, shortMeta) {
    StoredDocKey key = makeStoredDocKey("key");
    auto item = makeCommittedItem(key, "value");
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    auto* request = kvstore->setAndReturnRequest(item);

    // Now directly mess with the metadata of the value which will be written
    MockCouchRequest::MetaData meta;
    request->writeMetaData(meta, 4); // not enough meta!
    kvstore->commit(flush);

    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    checkGetValue(gv, ENGINE_TMPFAIL);
}

TEST_F(CouchstoreTest, testV0MetaThings) {
    StoredDocKey key = makeStoredDocKey("key");
    // Baseline test, just writes meta things and reads them
    // via standard interfaces
    // Ensure CAS, exptime and flags are set to something.
    queued_item item(std::make_unique<Item>(key,
                                            0x01020304 /*flags*/,
                                            0xaa00bb11 /*expiry*/,
                                            "value",
                                            5,
                                            PROTOCOL_BINARY_RAW_BYTES,
                                            0xf00fcafe11225566ull));

    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    kvstore->set(item);
    kvstore->commit(flush);

    MockedGetCallback<GetValue> gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(0xf00fcafe11225566ull));
    EXPECT_CALL(gc, expTime(0xaa00bb11));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(PROTOCOL_BINARY_RAW_BYTES));
    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    gc.callback(gv);
}

TEST_F(CouchstoreTest, testV1MetaThings) {
    // Baseline test, just writes meta things and reads them
    // via standard interfaces
    // Ensure CAS, exptime and flags are set to something.
    auto datatype = PROTOCOL_BINARY_DATATYPE_JSON; //lies, but non-zero
    StoredDocKey key = makeStoredDocKey("key");
    queued_item item(std::make_unique<Item>(key,
                                            0x01020304 /*flags*/,
                                            0xaa00bb11, /*expiry*/
                                            "value",
                                            5,
                                            datatype,
                                            0xf00fcafe11225566ull));
    EXPECT_NE(0, datatype); // make sure we writing non-zero
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    kvstore->set(item);
    kvstore->commit(flush);

    MockedGetCallback<GetValue> gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(0xf00fcafe11225566ull));
    EXPECT_CALL(gc, expTime(0xaa00bb11));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(PROTOCOL_BINARY_DATATYPE_JSON));

    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    gc.callback(gv);
}

TEST_F(CouchstoreTest, fuzzV1) {
    StoredDocKey key = makeStoredDocKey("key");
    auto item = makeCommittedItem(key, "value");
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    auto* request = kvstore->setAndReturnRequest(item);

    // Now directly mess with the metadata of the value which will be written
    MockCouchRequest::MetaData meta;
    meta.cas = 0xf00fcafe11225566ull;
    meta.expiry = 0xaa00bb11;
    meta.flags = 0x01020304;
    meta.ext1 = 2;
    meta.ext2 = 33;
    request->writeMetaData(meta, MockCouchRequest::MetaData::sizeofV1);
    kvstore->commit(flush);
    MockedGetCallback<GetValue> gc;
    uint8_t expectedDataType = 33;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(protocol_binary_datatype_t(expectedDataType)));
    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    gc.callback(gv);
}

TEST_F(CouchstoreTest, testV2WriteRead) {
    // Ensure CAS, exptime and flags are set to something.
    auto datatype = PROTOCOL_BINARY_DATATYPE_JSON; //lies, but non-zero
    StoredDocKey key = makeStoredDocKey("key");
    queued_item item(std::make_unique<Item>(key,
                                            0x01020304 /*flags*/,
                                            0xaa00bb11, /*expiry*/
                                            "value",
                                            5,
                                            datatype,
                                            0xf00fcafe11225566ull));

    EXPECT_NE(0, datatype); // make sure we writing non-zero values

    // Write an item with forced (valid) V2 meta
    // In 4.6 we removed the extra conflict resolution byte, so be sure we
    // operate correctly if a document has V2 meta.
    MockCouchRequest::MetaData meta;
    meta.cas = 0xf00fcafe11225566ull;
    meta.expiry = 0xaa00bb11;
    meta.flags = 0x01020304;
    meta.ext1 = FLEX_META_CODE;
    meta.ext2 = datatype;
    meta.legacyDeleted = 0x01;

    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    auto* request = kvstore->setAndReturnRequest(item);

    // Force the meta to be V2 (19 bytes)
    request->writeMetaData(meta, MockCouchRequest::MetaData::sizeofV2);

    // Commit it
    kvstore->commit(flush);

    // Read back successful, the extra byte will of been dropped.
    MockedGetCallback<GetValue> gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(protocol_binary_datatype_t(meta.ext2)));
    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    gc.callback(gv);
}

class CouchKVStoreMetaData : public ::testing::Test {
};

TEST_F(CouchKVStoreMetaData, basic) {
    // Lock down the size assumptions.
    EXPECT_EQ(16, MetaData::getMetaDataSize(MetaData::Version::V0));
    EXPECT_EQ(16 + 2, MetaData::getMetaDataSize(MetaData::Version::V1));
    EXPECT_EQ(16 + 2 + 1, MetaData::getMetaDataSize(MetaData::Version::V2));
    EXPECT_EQ(16 + 2 + 7, MetaData::getMetaDataSize(MetaData::Version::V3));
}

TEST_F(CouchKVStoreMetaData, overlay) {
    std::vector<char> data(16);
    sized_buf meta;
    meta.buf = data.data();
    meta.size = data.size();
    auto metadata = MetaDataFactory::createMetaData(meta);
    EXPECT_EQ(MetaData::Version::V0, metadata->getVersionInitialisedFrom());

    data.resize(16 + 2);
    meta.buf = data.data();
    meta.size = data.size();
    metadata = MetaDataFactory::createMetaData(meta);
    EXPECT_EQ(MetaData::Version::V1, metadata->getVersionInitialisedFrom());

    // Even with a 19 byte (v2) meta, the expectation is we become V1
    data.resize(16 + 2 + 1);
    meta.buf = data.data();
    meta.size = data.size();
    metadata = MetaDataFactory::createMetaData(meta);
    EXPECT_EQ(MetaData::Version::V1, metadata->getVersionInitialisedFrom());

    // Increase to size of V3; should create V3.
    data.resize(16 + 2 + 7);
    meta.buf = data.data();
    meta.size = data.size();
    metadata = MetaDataFactory::createMetaData(meta);
    EXPECT_EQ(MetaData::Version::V3, metadata->getVersionInitialisedFrom());

    // Buffers too large and small
    data.resize(MetaData::getMetaDataSize(MetaData::Version::V3) + 1);
    meta.buf = data.data();
    meta.size = data.size();
    EXPECT_THROW(MetaDataFactory::createMetaData(meta), std::logic_error);

    data.resize(MetaData::getMetaDataSize(MetaData::Version::V0) - 1);
    meta.buf = data.data();
    meta.size = data.size();
    EXPECT_THROW(MetaDataFactory::createMetaData(meta), std::logic_error);
}

TEST_F(CouchKVStoreMetaData, overlayExpands1) {
    std::vector<char> data(16);
    sized_buf meta;
    sized_buf out;
    meta.buf = data.data();
    meta.size = data.size();

    // V0 in yet V1 "moved out"
    auto metadata = MetaDataFactory::createMetaData(meta);
    EXPECT_EQ(MetaData::Version::V0, metadata->getVersionInitialisedFrom());
    out.size = MetaData::getMetaDataSize(MetaData::Version::V1);
    out.buf = new char[out.size];
    metadata->copyToBuf(out);
    EXPECT_EQ(out.size, MetaData::getMetaDataSize(MetaData::Version::V1));

    // We created a copy of the metadata so we must cleanup
    delete [] out.buf;
}

TEST_F(CouchKVStoreMetaData, overlayExpands2) {
    std::vector<char> data(16 + 2);
    sized_buf meta;
    sized_buf out;
    meta.buf = data.data();
    meta.size = data.size();

    // V1 in V1 "moved out"
    auto metadata = MetaDataFactory::createMetaData(meta);
    EXPECT_EQ(MetaData::Version::V1, metadata->getVersionInitialisedFrom());
    out.size = MetaData::getMetaDataSize(MetaData::Version::V1);
    out.buf = new char[out.size];
    metadata->copyToBuf(out);
    EXPECT_EQ(out.size, MetaData::getMetaDataSize(MetaData::Version::V1));

    // We created a copy of the metadata so we must cleanup
    delete [] out.buf;
}

TEST_F(CouchKVStoreMetaData, overlayExpands3) {
    std::vector<char> data(16 + 2 + 7);
    sized_buf meta;
    sized_buf out;
    meta.buf = data.data();
    meta.size = data.size();

    // V1 in V1 "moved out"
    auto metadata = MetaDataFactory::createMetaData(meta);
    EXPECT_EQ(MetaData::Version::V3, metadata->getVersionInitialisedFrom());
    out.size = MetaData::getMetaDataSize(MetaData::Version::V3);
    out.buf = new char[out.size];
    metadata->copyToBuf(out);
    EXPECT_EQ(out.size, MetaData::getMetaDataSize(MetaData::Version::V3));

    // We created a copy of the metadata so we must cleanup
    delete[] out.buf;
}

TEST_F(CouchKVStoreMetaData, writeToOverlay) {
    std::vector<char> data(16);
    sized_buf meta;
    sized_buf out;
    meta.buf = data.data();
    meta.size = data.size();

    // Test that we can initialise from V0 but still set
    // all fields of all versions
    auto metadata = MetaDataFactory::createMetaData(meta);
    EXPECT_EQ(MetaData::Version::V0, metadata->getVersionInitialisedFrom());

    uint64_t cas = 0xf00f00ull;
    uint32_t exp = 0xcafe1234;
    uint32_t flags = 0xc0115511;
    DeleteSource deleteSource = DeleteSource::Explicit;
    metadata->setCas(cas);
    metadata->setExptime(exp);
    metadata->setFlags(flags);
    metadata->setDeleteSource(deleteSource);
    metadata->setDataType(PROTOCOL_BINARY_DATATYPE_JSON);
    constexpr auto level = cb::durability::Level::Majority;
    metadata->setDurabilityOp(queue_op::pending_sync_write);
    metadata->setPrepareProperties(level, /*isSyncDelete*/ false);

    // Check they all read back
    EXPECT_EQ(cas, metadata->getCas());
    EXPECT_EQ(exp, metadata->getExptime());
    EXPECT_EQ(flags, metadata->getFlags());
    EXPECT_EQ(FLEX_META_CODE, metadata->getFlexCode());
    EXPECT_EQ(deleteSource, metadata->getDeleteSource());
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, metadata->getDataType());
    EXPECT_EQ(level, metadata->getDurabilityLevel());
    EXPECT_EQ(queue_op::pending_sync_write, metadata->getDurabilityOp());

    metadata->setDurabilityOp(queue_op::commit_sync_write);
    metadata->setCompletedProperties(1234);
    EXPECT_EQ(queue_op::commit_sync_write, metadata->getDurabilityOp());
    EXPECT_EQ(1234, metadata->getPrepareSeqno());

    // Now we move the metadata out, this will give back a V1 structure
    out.size = MetaData::getMetaDataSize(MetaData::Version::V1);
    out.buf = new char[out.size];
    metadata->copyToBuf(out);
    metadata = MetaDataFactory::createMetaData(out);
    EXPECT_EQ(MetaData::Version::V1, metadata->getVersionInitialisedFrom()); // Is it V1?

    // All the written fields should be the same
    // Check they all read back
    EXPECT_EQ(cas, metadata->getCas());
    EXPECT_EQ(exp, metadata->getExptime());
    EXPECT_EQ(flags, metadata->getFlags());
    EXPECT_EQ(FLEX_META_CODE, metadata->getFlexCode());
    EXPECT_EQ(deleteSource, metadata->getDeleteSource());
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, metadata->getDataType());
    EXPECT_EQ(out.size, MetaData::getMetaDataSize(MetaData::Version::V1));

    // Now expand to V3; check fields are read / written correctly.

    delete[] out.buf;
    out.size = MetaData::getMetaDataSize(MetaData::Version::V3);
    out.buf = new char[out.size];
    metadata->copyToBuf(out);
    metadata = MetaDataFactory::createMetaData(out);
    EXPECT_EQ(MetaData::Version::V3,
              metadata->getVersionInitialisedFrom()); // Is it V1?

    // We moved the metadata so we must cleanup
    delete [] out.buf;
}

//
// Test that assignment operates as expected (we use this in edit_docinfo_hook)
//
TEST_F(CouchKVStoreMetaData, assignment) {
    std::vector<char> data(16);
    sized_buf meta;
    meta.buf = data.data();
    meta.size = data.size();
    auto metadata = MetaDataFactory::createMetaData(meta);
    uint64_t cas = 0xf00f00ull;
    uint32_t exp = 0xcafe1234;
    uint32_t flags = 0xc0115511;
    DeleteSource deleteSource = DeleteSource::TTL;
    metadata->setCas(cas);
    metadata->setExptime(exp);
    metadata->setFlags(flags);
    metadata->setDeleteSource(deleteSource);
    metadata->setDataType( PROTOCOL_BINARY_DATATYPE_JSON);

    // Create a second metadata to write into
    auto copy = MetaDataFactory::createMetaData();

    // Copy overlaid into managed
    *copy = *metadata;

    // Test that the copy doesn't write to metadata
    copy->setExptime(100);
    EXPECT_EQ(exp, metadata->getExptime());

    EXPECT_EQ(cas, copy->getCas());
    EXPECT_EQ(100, copy->getExptime());
    EXPECT_EQ(flags, copy->getFlags());
    EXPECT_EQ(FLEX_META_CODE, copy->getFlexCode());
    EXPECT_EQ(deleteSource, copy->getDeleteSource());
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, copy->getDataType());

    // And a final assignment
    auto copy2 = MetaDataFactory::createMetaData();
    *copy2 = *copy;

    // test that copy2 doesn't update copy
    copy2->setCas(99);
    EXPECT_NE(99, copy->getCas());

    // Yet copy2 did
    EXPECT_EQ(99, copy2->getCas());
    EXPECT_EQ(100, copy2->getExptime());
    EXPECT_EQ(flags, copy2->getFlags());
    EXPECT_EQ(FLEX_META_CODE, copy2->getFlexCode());
    EXPECT_EQ(deleteSource, copy2->getDeleteSource());
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, copy2->getDataType());
}

class PersistenceCallbacks {
public:
    virtual ~PersistenceCallbacks() {
    }

    // Actual operator() methods which will be called by the storage layer.
    // GMock cannot mock these directly, so instead provide named 'callback'
    // methods which these functions call.
    void operator()(TransactionContext& txCtx,
                    KVStore::FlushStateMutation state) {
        callback(txCtx, state);
    }
    void operator()(TransactionContext& txCtx,
                    KVStore::FlushStateDeletion state) {
        callback(txCtx, state);
    }

    // SET callback.
    virtual void callback(TransactionContext&,
                          KVStore::FlushStateMutation&) = 0;

    // DEL callback.
    // @param value number of items that the underlying storage has deleted
    virtual void callback(TransactionContext& txCtx,
                          KVStore::FlushStateDeletion&) = 0;
};

class MockPersistenceCallbacks : public PersistenceCallbacks {
public:
    MOCK_METHOD2(callback,
                 void(TransactionContext&, KVStore::FlushStateMutation&));
    MOCK_METHOD2(callback,
                 void(TransactionContext&, KVStore::FlushStateDeletion&));
};

class MockTransactionContext : public TransactionContext {
public:
    MockTransactionContext(Vbid vb) : TransactionContext(vb) {
    }

    MOCK_METHOD2(setCallback,
                 void(const queued_item&, KVStore::FlushStateMutation));
    MOCK_METHOD2(deleteCallback,
                 void(const queued_item&, KVStore::FlushStateDeletion));
};

void KVStoreParamTest::SetUp() {
    KVStoreTest::SetUp();
    Configuration config;
    // `GetParam` returns the string parameter representing the KVStore
    // implementation.
    auto configStr = "dbname="s + data_dir + ";backend="s + GetParam();
    config.parseConfiguration(configStr.c_str(), get_mock_server_api());
    WorkLoadPolicy workload(config.getMaxNumWorkers(),
                            config.getMaxNumShards());

    if (config.getBackend() == "couchdb") {
        kvstoreConfig = std::make_unique<CouchKVStoreConfig>(
                config, workload.getNumShards(), 0 /*shardId*/);
    }
#ifdef EP_USE_ROCKSDB
    else if (config.getBackend() == "rocksdb") {
        kvstoreConfig = std::make_unique<RocksDBKVStoreConfig>(
                config, workload.getNumShards(), 0 /*shardId*/);
    }
#endif
#ifdef EP_USE_MAGMA
    else if (config.getBackend() == "magma") {
        kvstoreConfig = std::make_unique<MagmaKVStoreConfig>(
                config, workload.getNumShards(), 0 /*shardId*/);
    }
#endif
    kvstore = setup_kv_store(*kvstoreConfig);
}

void KVStoreParamTest::TearDown() {
    // Under RocksDB, removing the database folder (which is equivalent to
    // calling rocksdb::DestroyDB()) for a live DB is an undefined
    // behaviour. So, close the DB before destroying it.
    kvstore.reset();
    KVStoreTest::TearDown();
}

class KVStoreParamTestSkipMagma : public KVStoreParamTest {
public:
    KVStoreParamTestSkipMagma() : KVStoreParamTest() {
    }
};

// Test basic set / get of a document
TEST_P(KVStoreParamTest, BasicTest) {
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->set(qi);

    EXPECT_TRUE(kvstore->commit(flush));

    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    checkGetValue(gv);
}

TEST_P(KVStoreParamTest, TestPersistenceCallbacksForSet) {
    // Grab a pointer to our MockTransactionContext so that we can establish
    // expectations on it throughout the test. We consume our unique_ptr to it
    // in KVStore::begin but our raw pointer will remain.
    std::unique_ptr<TransactionContext> tc =
            std::make_unique<MockTransactionContext>(Vbid(0));
    auto mutationStatus = KVStore::FlushStateMutation::Insert;
    auto* mockTC = dynamic_cast<MockTransactionContext*>(tc.get());

    kvstore->begin(std::move(tc));

    // Expect that the SET callback will not be called just after `set`
    EXPECT_CALL(*mockTC, setCallback(_, mutationStatus)).Times(0);

    auto key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->set(qi);

    // Expect that the SET callback will be called once after `commit`
    EXPECT_CALL(*mockTC, setCallback(_, mutationStatus)).Times(1);

    EXPECT_TRUE(kvstore->commit(flush));
}

// This test does not work under RocksDB because we assume that every
// deletion is to an item that does not exist
TEST_P(KVStoreParamTestSkipRocks, TestPersistenceCallbacksForDel) {
    // Store an item
    auto key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    kvstore->set(qi);
    kvstore->commit(flush);

    // Grab a pointer to our MockTransactionContext so that we can establish
    // expectations on it throughout the test. We consume our unique_ptr to it
    // in KVStore::begin but our raw pointer will remain.
    std::unique_ptr<TransactionContext> tc =
            std::make_unique<MockTransactionContext>(Vbid(0));
    auto* mockTC = dynamic_cast<MockTransactionContext*>(tc.get());

    kvstore->begin(std::move(tc));
    // Expect that the DEL callback will not be called just after `del`
    auto status = KVStore::FlushStateDeletion::Delete;
    EXPECT_CALL(*mockTC, deleteCallback(_, status)).Times(0);

    qi->setDeleted();
    qi->setBySeqno(2);
    kvstore->del(qi);

    // Expect that the DEL callback will be called once after `commit`
    EXPECT_CALL(*mockTC, deleteCallback(_, status)).Times(1);

    EXPECT_TRUE(kvstore->commit(flush));
}

TEST_P(KVStoreParamTest, TestDataStoredInTheRightVBucket) {
    std::string value = "value";
    std::vector<Vbid> vbids = {Vbid(0), Vbid(1)};
    uint64_t seqno = 1000;

    // For this test we need to initialize both VBucket 0 and VBucket 1.
    // In the case of RocksDB we need to release the DB
    // already opened in 'kvstore'
    if (kvstoreConfig->getBackend() == "rocksdb") {
        kvstore.reset();
        // For magma, we need to delete the vbucket since
        // we are going to reuse it.
    } else if (kvstoreConfig->getBackend() == "magma") {
        auto kvsRev = kvstore->prepareToDelete(Vbid(0));
        kvstore->delVBucket(Vbid(0), kvsRev);
    }

    kvstore = setup_kv_store(*kvstoreConfig, vbids);

    // Store an item into each VBucket
    for (auto vbid : vbids) {
        kvstore->begin(std::make_unique<TransactionContext>(vbid));
        auto key = makeStoredDocKey("key-" + std::to_string(vbid.get()));
        auto qi = makeCommittedItem(key, value);
        qi->setBySeqno(seqno++);
        kvstore->set(qi);
        kvstore->commit(flush);
    }

    // Check that each item has been stored in the right VBucket
    for (auto vbid : vbids) {
        GetValue gv = kvstore->get(
                makeDiskDocKey("key-" + std::to_string(vbid.get())), vbid);
        checkGetValue(gv);
    }

    // Check that an item is not found in a different VBucket
    GetValue gv = kvstore->get(makeDiskDocKey("key-0"), Vbid(1));
    checkGetValue(gv, ENGINE_KEY_ENOENT);
    gv = kvstore->get(makeDiskDocKey("key-1"), Vbid(0));
    checkGetValue(gv, ENGINE_KEY_ENOENT);
}

// Verify thread-safeness for 'delVBucket' concurrent operations.
// Expect ThreadSanitizer to pick this.
// Rocks has race condition issues
TEST_P(KVStoreParamTestSkipRocks, DelVBucketConcurrentOperationsTest) {
    // MB-37159: Skipping for magma as this has been seen to timeout
    if (kvstoreConfig->getBackend() == "magma") {
        return;
    }

    std::atomic<bool> stop{false};
    bool okToDelete{false};
    uint32_t deletes{0};
    uint32_t minNumDeletes = 25;
    std::mutex delMutex;
    std::condition_variable delWait;

    ThreadGate tg(3);

    auto set = [&] {
        int64_t seqno = 1;
        while (!stop.load()) {
            kvstore->begin(std::make_unique<TransactionContext>(vbid));
            auto qi = makeCommittedItem(makeStoredDocKey("key"), "value");
            qi->setBySeqno(seqno++);
            kvstore->set(qi);
            auto ok = kvstore->commit(flush);

            // Everytime we get a successful commit, that
            // means we have a vbucket we can drop.
            if (ok) {
                std::lock_guard<std::mutex> lock(delMutex);
                okToDelete = true;
                delWait.notify_one();
            }
        }
    };

    auto delVBucket = [&] {
        tg.threadUp();
        while (!stop.load()) {
            {
                std::unique_lock<std::mutex> lock(delMutex);
                delWait.wait(lock, [&okToDelete] { return okToDelete; });
                okToDelete = false;
            }
            kvstore->delVBucket(Vbid(0), 0);
            if (deletes++ > minNumDeletes) {
                stop = true;
            }
        }
    };

    auto get = [&] {
        tg.threadUp();
        auto key = makeDiskDocKey("key");
        while (!stop.load()) {
            kvstore->get(key, Vbid(0));
        }
    };

    auto initScan = [&] {
        tg.threadUp();
        while (!stop.load()) {
            auto scanCtx = kvstore->initBySeqnoScanContext(
                    std::make_unique<GetCallback>(true),
                    std::make_unique<KVStoreTestCacheCallback>(1, 5, Vbid(0)),
                    Vbid(0),
                    1,
                    DocumentFilter::ALL_ITEMS,
                    ValueFilter::VALUES_COMPRESSED,
                    SnapshotSource::Head);
        }
    };

    std::vector<std::thread> workers;

    auto tid = std::thread(set);
    workers.push_back(std::move(tid));
    tid = std::thread(delVBucket);
    workers.push_back(std::move(tid));
    tid = std::thread(get);
    workers.push_back(std::move(tid));
    tid = std::thread(initScan);
    workers.push_back(std::move(tid));

    for (auto& tid : workers) {
        tid.join();
    }
    EXPECT_LT(minNumDeletes, deletes);
}

// MB-27963 identified that compaction and scan are racing with respect to
// the current view of the fileMap causing scan to fail.
TEST_P(KVStoreParamTest, CompactAndScan) {
    for (int i = 1; i < 10; i++) {
        kvstore->begin(std::make_unique<TransactionContext>(vbid));
        auto key = makeStoredDocKey(std::string(i, 'k'));
        auto qi = makeCommittedItem(key, "value");
        qi->setBySeqno(i);
        kvstore->set(qi);
        // Ensure a valid vbstate is committed
        flush.proposedVBState.lastSnapEnd = i;
        kvstore->commit(flush);
    }

    ThreadGate tg(3);

    auto initScan = [this, &tg] {
        tg.threadUp();
        for (int i = 0; i < 10; i++) {
            auto cb = std::make_unique<GetCallback>(true /*expectcompressed*/);
            auto cl = std::make_unique<KVStoreTestCacheCallback>(1, 5, Vbid(0));
            auto scanCtx = kvstore->initBySeqnoScanContext(
                    std::make_unique<GetCallback>(true /*expectcompressed*/),
                    std::make_unique<KVStoreTestCacheCallback>(1, 5, Vbid(0)),
                    Vbid(0),
                    1,
                    DocumentFilter::ALL_ITEMS,
                    ValueFilter::VALUES_COMPRESSED,
                    SnapshotSource::Head);
            if (!scanCtx) {
                FAIL() << "initBySeqnoScanContext returned nullptr";
                return;
            }
        }
    };
    auto compact = [this, &tg] {
        tg.threadUp();
        CompactionConfig config;
        config.purge_before_seq = 0;
        config.purge_before_ts = 0;

        config.drop_deletes = 0;
        config.db_file_id = Vbid(0);
        auto cctx = std::make_shared<compaction_ctx>(config, 0);
        for (int i = 0; i < 10; i++) {
            EXPECT_TRUE(kvstore->compactDB(cctx));
        }
    };

    std::thread t1(compact);
    std::thread t2(initScan);
    std::thread t3(initScan);
    t1.join();
    t2.join();
    t3.join();
}

TEST_P(KVStoreParamTest, HighSeqnoCorrectlyStoredForCommitBatch) {
    auto key = makeStoredDocKey("key");
    std::string value = "value";
    Vbid vbid = Vbid(0);

    // Upsert an item 10 times in a single transaction (we want to test that
    // the VBucket state is updated with the highest seqno found in a commit
    // batch)
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    for (int i = 1; i <= 10; i++) {
        auto qi = makeCommittedItem(key, value);
        qi->setBySeqno(i);
        kvstore->set(qi);
    }
    // Ensure a valid vbstate is committed
    flush.proposedVBState.lastSnapEnd = 10;
    kvstore->commit(flush);
    // KVStore::commit does not update in-memory vbstate, so manually do it
    kvstore->setVBucketState(vbid, flush.proposedVBState);

    GetValue gv = kvstore->get(DiskDocKey{key}, vbid);
    checkGetValue(gv);
    EXPECT_EQ(kvstore->getVBucketState(vbid)->highSeqno, 10);
}

// Test the getRange() function
TEST_P(KVStoreParamTest, GetRangeBasic) {
    // Setup: store 5 keys, a, b, c, d, e (with matching values)
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    int64_t seqno = 1;
    for (char k = 'a'; k < 'f'; k++) {
        auto item = makeCommittedItem(makeStoredDocKey({k}),
                                      "value_"s + std::string{k});
        item->setBySeqno(seqno++);
        kvstore->set(item);
    }
    kvstore->commit(flush);

    // Test: Ask for keys in the range [b,d]. Should return b & c.
    std::vector<GetValue> results;
    kvstore->getRange(
            Vbid{0},
            makeDiskDocKey("b"),
            makeDiskDocKey("d"),
            [&results](GetValue&& cb) { results.push_back(std::move(cb)); });
    ASSERT_EQ(2, results.size());
    EXPECT_EQ("b"s, results.at(0).item->getKey().c_str());
    EXPECT_EQ("value_b"s, results.at(0).item->getValue()->to_s());
    EXPECT_EQ("c"s, results.at(1).item->getKey().c_str());
    EXPECT_EQ("value_c"s, results.at(1).item->getValue()->to_s());
}

// Test the getRange() function skips deleted items.
TEST_P(KVStoreParamTest, GetRangeDeleted) {
    // Setup: 1) store 8 keys, a, b, c, d, e, f, g (with matching values)
    //        2) delete 3 of them (b, d, f)
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    int64_t seqno = 1;
    for (char k = 'a'; k < 'h'; k++) {
        auto item = makeCommittedItem(makeStoredDocKey({k}),
                                      "value_"s + std::string{k});
        item->setBySeqno(seqno++);
        kvstore->set(item);
    }
    kvstore->commit(flush);

    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    for (char k = 'b'; k < 'g'; k += 2) {
        auto item = makeCommittedItem(makeStoredDocKey({k}),
                                      "value_"s + std::string{k});
        item->setDeleted(DeleteSource::Explicit);
        item->setBySeqno(seqno++);
        kvstore->del(item);
    }
    kvstore->commit(flush);

    // Test: Ask for keys in the range [b,f]. Should return c and e.
    std::vector<GetValue> results;
    kvstore->getRange(
            Vbid{0},
            makeDiskDocKey("b"),
            makeDiskDocKey("f"),
            [&results](GetValue&& cb) { results.push_back(std::move(cb)); });
    ASSERT_EQ(2, results.size());
    EXPECT_EQ("c"s, results.at(0).item->getKey().c_str());
    EXPECT_EQ("value_c"s, results.at(0).item->getValue()->to_s());
    EXPECT_EQ("e"s, results.at(1).item->getKey().c_str());
    EXPECT_EQ("value_e"s, results.at(1).item->getValue()->to_s());
}

TEST_P(KVStoreParamTest, Durability_PersistPrepare) {
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makePendingItem(key, "value");
    qi->setBySeqno(1);

    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    kvstore->set(qi);
    kvstore->commit(flush);

    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    DiskDocKey prefixedKey(key, true /*prepare*/);
    gv = kvstore->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isPending());
    EXPECT_FALSE(gv.item->isDeleted());
}

TEST_P(KVStoreParamTest, Durability_PersistAbort) {
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makePendingItem(key, "value");
    qi->setAbortSyncWrite();
    qi->setDeleted();
    qi->setPrepareSeqno(999);
    qi->setBySeqno(1);

    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    kvstore->del(qi);
    kvstore->commit(flush);

    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    // Note: Aborts are in the DurabilityPrepare namespace.
    DiskDocKey prefixedKey(key, true /*pending*/);
    gv = kvstore->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isAbort());
    EXPECT_TRUE(gv.item->isDeleted());
    EXPECT_EQ(999, gv.item->getPrepareSeqno());
}

TEST_P(KVStoreParamTest, OptimizeWrites) {
    std::vector<queued_item> items;
    std::vector<StoredDocKey> keys;
    keys.resize(3);

    for (int i = 2; i >= 0; i--) {
        std::string key("foo" + std::to_string(i));
        keys[i] = makeStoredDocKey(key);
        items.push_back(makeCommittedItem(keys[i], "value"));
    }

    // sort the items
    kvstore->optimizeWrites(items);

    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(0, keys[i].compare(items[i]->getKey()));
    }
}

TEST_P(KVStoreParamTestSkipRocks, GetAllKeysSanity) {
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    int keys = 20;
    for (int i = 0; i < keys; i++) {
        std::string key("key" + std::to_string(i));
        auto qi = makeCommittedItem(makeStoredDocKey(key), "value");
        qi->setBySeqno(5);
        kvstore->set(qi);
    }

    kvstore->commit(flush);
    auto cb(std::make_shared<CustomCallback<const DiskDocKey&>>());
    DiskDocKey start(nullptr, 0);
    kvstore->getAllKeys(Vbid(0), start, 20, cb);
    EXPECT_EQ(keys, int(cb->getProcessedCount()));
}

class ReuseSnapshotCallback : public StatusCallback<GetValue> {
public:
    ReuseSnapshotCallback(uint64_t startSeqno,
                          uint64_t endSeqno,
                          uint64_t enomemSeqno)
        : startSeqno(startSeqno),
          endSeqno(endSeqno),
          enomemSeqno(enomemSeqno){};

    void callback(GetValue& result) override {
        EXPECT_LE(startSeqno, result.item->getBySeqno());
        EXPECT_LE(result.item->getBySeqno(), endSeqno);
        if (!didEnomem && result.item->getBySeqno() == enomemSeqno) {
            setStatus(ENGINE_ENOMEM);
            didEnomem = true;
            return;
        }
        nItems++;
        setStatus(ENGINE_SUCCESS);
        return;
    }

    uint32_t nItems{0};

private:
    int64_t startSeqno{0};
    int64_t endSeqno{0};
    int64_t enomemSeqno{0};
    bool didEnomem{false};
};

// Simulate an ENOMEM error during scan and show that continuing
// the scan stays on the same snapshot.
TEST_P(KVStoreParamTest, reuseSeqIterator) {
    uint64_t seqno = 1;

    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    for (int j = 0; j < 2; j++) {
        auto key = makeStoredDocKey("key" + std::to_string(j));
        auto qi = makeCommittedItem(key, "value");
        qi->setBySeqno(seqno++);
        kvstore->set(qi);
    }
    // Need a valid snap end for couchstore
    flush.proposedVBState.lastSnapEnd = seqno - 1;

    kvstore->commit(flush);

    auto cb = std::make_unique<ReuseSnapshotCallback>(1, 2, 2);
    auto cl = std::make_unique<KVStoreTestCacheCallback>(1, 2, vbid);
    auto callback = cb.get();
    auto scanCtx =
            kvstore->initBySeqnoScanContext(std::move(cb),
                                            std::move(cl),
                                            vbid,
                                            1,
                                            DocumentFilter::ALL_ITEMS,
                                            ValueFilter::VALUES_COMPRESSED,
                                            SnapshotSource::Head);

    ASSERT_NE(nullptr, scanCtx);
    kvstore->scan(*scanCtx);
    ASSERT_EQ(callback->nItems, 1);

    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    for (int j = 0; j < 2; j++) {
        auto key = makeStoredDocKey("key" + std::to_string(j));
        auto qi = makeCommittedItem(key, "value");
        qi->setBySeqno(seqno++);
        kvstore->set(qi);
    }
    kvstore->commit(flush);

    CompactionConfig compactionConfig;
    compactionConfig.purge_before_seq = 0;
    compactionConfig.purge_before_ts = 0;
    compactionConfig.drop_deletes = 0;
    compactionConfig.db_file_id = vbid;
    auto cctx = std::make_shared<compaction_ctx>(compactionConfig, 0);

    EXPECT_TRUE(kvstore->compactDB(cctx));

    kvstore->scan(*scanCtx);

    // We are picking up a scan which was prematurely stopped in a simulated
    // ENOMEM error. Since we've done a compaction, we have removed all the
    // remaining keys that would have been returned so this test should
    // be verifying that we haven't lost the original snapshot of the scan.
    EXPECT_EQ(callback->nItems, 2);
}

static std::string kvstoreTestParams[] = {
#ifdef EP_USE_MAGMA
        "magma",
#endif
#ifdef EP_USE_ROCKSDB
        "rocksdb",
#endif
        "couchdb"};

INSTANTIATE_TEST_SUITE_P(KVStoreParam,
                         KVStoreParamTest,
                         ::testing::ValuesIn(kvstoreTestParams),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

static std::string kvstoreTestParamsSkipMagma[] = {
#ifdef EP_USE_ROCKSDB
        "rocksdb",
#endif
        "couchdb"};

INSTANTIATE_TEST_SUITE_P(KVStoreParam,
                         KVStoreParamTestSkipMagma,
                         ::testing::ValuesIn(kvstoreTestParamsSkipMagma),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

static std::string kvstoreTestParamsSkipRocks[] = {
#ifdef EP_USE_MAGMA
        "magma",
#endif
        "couchdb"};

INSTANTIATE_TEST_SUITE_P(KVStoreParam,
                         KVStoreParamTestSkipRocks,
                         ::testing::ValuesIn(kvstoreTestParamsSkipRocks),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

#ifdef EP_USE_ROCKSDB
// Test fixture for tests which run only on RocksDB.
class RocksDBKVStoreTest : public KVStoreTest {
protected:
    void SetUp() override {
        KVStoreTest::SetUp();
        Configuration config;
        config.parseConfiguration(
                ("dbname="s + data_dir + ";backend=rocksdb").c_str(),
                get_mock_server_api());
        WorkLoadPolicy workload(config.getMaxNumWorkers(),
                                config.getMaxNumShards());

        kvstoreConfig = std::make_unique<RocksDBKVStoreConfig>(
                config, workload.getNumShards(), 0 /*shardId*/);
        kvstore = setup_kv_store(*kvstoreConfig);
    }

    void TearDown() override {
        // Under RocksDB, removing the database folder (which is equivalent to
        // calling rocksdb::DestroyDB()) for a live DB is an undefined
        // behaviour. So, close the DB before destroying it.
        kvstore.reset();
        KVStoreTest::TearDown();
    }

    std::unique_ptr<KVStoreConfig> kvstoreConfig;
    std::unique_ptr<KVStore> kvstore;
};

// Verify that RocksDB internal stats are returned
TEST_F(RocksDBKVStoreTest, StatsTest) {
    size_t value;

    // Memory Usage
    EXPECT_TRUE(kvstore->getStat("kMemTableTotal", value));
    EXPECT_TRUE(kvstore->getStat("kMemTableUnFlushed", value));
    EXPECT_TRUE(kvstore->getStat("kTableReadersTotal", value));
    EXPECT_TRUE(kvstore->getStat("kCacheTotal", value));

    // MemTable Size per CF
    EXPECT_TRUE(kvstore->getStat("default_kSizeAllMemTables", value));
    EXPECT_TRUE(kvstore->getStat("seqno_kSizeAllMemTables", value));

    // Block Cache
    Configuration config;

    // Note: we need to switch-on DB Statistics
    auto configStr = ("dbname="s + data_dir +
                      ";backend=rocksdb;rocksdb_stats_level=kAll");
    config.parseConfiguration(configStr.c_str(), get_mock_server_api());
    WorkLoadPolicy workload(config.getMaxNumWorkers(),
                            config.getMaxNumShards());

    kvstoreConfig = std::make_unique<RocksDBKVStoreConfig>(
            config, workload.getNumShards(), 0 /*shardId*/);
    // Close the opened DB instance
    kvstore.reset();
    // Re-open with the new configuration
    kvstore = setup_kv_store(*kvstoreConfig);
    EXPECT_TRUE(kvstore->getStat("rocksdb.block.cache.hit", value));
    EXPECT_TRUE(kvstore->getStat("rocksdb.block.cache.miss", value));
    EXPECT_TRUE(kvstore->getStat("rocksdb.block.cache.data.hit", value));
    EXPECT_TRUE(kvstore->getStat("rocksdb.block.cache.data.miss", value));
    EXPECT_TRUE(kvstore->getStat("rocksdb.block.cache.index.hit", value));
    EXPECT_TRUE(kvstore->getStat("rocksdb.block.cache.index.miss", value));
    EXPECT_TRUE(kvstore->getStat("rocksdb.block.cache.filter.hit", value));
    EXPECT_TRUE(kvstore->getStat("rocksdb.block.cache.filter.miss", value));

    // Disk Usage per-CF
    EXPECT_TRUE(kvstore->getStat("default_kTotalSstFilesSize", value));
    EXPECT_TRUE(kvstore->getStat("seqno_kTotalSstFilesSize", value));

    // Scan stats
    EXPECT_TRUE(kvstore->getStat("scan_totalSeqnoHits", value));
    EXPECT_TRUE(kvstore->getStat("scan_oldSeqnoHits", value));
}

// Verify that a wrong value of 'rocksdb_statistics_option' is caught
TEST_F(RocksDBKVStoreTest, StatisticsOptionWrongValueTest) {
    Configuration config;
    const auto baseConfig = "dbname="s + data_dir + ";backend=rocksdb";

    // Test wrong value
    config.parseConfiguration(
            (baseConfig + ";rocksdb_stats_level=wrong_value").c_str(),
            get_mock_server_api());
    WorkLoadPolicy workload(config.getMaxNumWorkers(),
                            config.getMaxNumShards());
    kvstoreConfig = std::make_unique<RocksDBKVStoreConfig>(
            config, workload.getNumShards(), 0 /*shardId*/);

    // Close the opened DB instance
    kvstore.reset();
    // Re-open with the new configuration
    EXPECT_THROW(kvstore = setup_kv_store(*kvstoreConfig),
                 std::invalid_argument);

    // Test one right value
    config.parseConfiguration(
            (baseConfig + ";rocksdb_stats_level=kAll").c_str(),
            get_mock_server_api());
    kvstoreConfig = std::make_unique<RocksDBKVStoreConfig>(
            config, workload.getNumShards(), 0 /*shardId*/);
    // Close the opened DB instance
    kvstore.reset();
    // Re-open with the new configuration
    kvstore = setup_kv_store(*kvstoreConfig);
}
#endif

#ifdef EP_USE_MAGMA

// Test fixture for tests which run only on Magma.
class MagmaKVStoreTest : public KVStoreTest {
protected:
    void SetUp() override {
        KVStoreTest::SetUp();

        auto configStr =
                "dbname="s + data_dir + ";"s + "backend=magma;" + magmaConfig;
        Configuration config;
        config.parseConfiguration(configStr.c_str(), get_mock_server_api());
        WorkLoadPolicy workload(config.getMaxNumWorkers(),
                                config.getMaxNumShards());
        kvstoreConfig = std::make_unique<MagmaKVStoreConfig>(
                config, workload.getNumShards(), 0 /*shardId*/);
        kvstore = std::make_unique<MockMagmaKVStore>(*kvstoreConfig);
    }

    void TearDown() override {
        kvstore.reset();
        KVStoreTest::TearDown();
    }

    std::unique_ptr<MagmaKVStoreConfig> kvstoreConfig;
    std::unique_ptr<MockMagmaKVStore> kvstore;
};

TEST_F(MagmaKVStoreTest, Rollback) {
    uint64_t seqno = 1;

    for (int i = 0; i < 2; i++) {
        kvstore->begin(std::make_unique<TransactionContext>(vbid));
        for (int j = 0; j < 5; j++) {
            auto key = makeStoredDocKey("key" + std::to_string(seqno));
            auto qi = makeCommittedItem(key, "value");
            qi->setBySeqno(seqno++);
            kvstore->set(qi);
        }
        kvstore->commit(flush);
    }

    auto rv = kvstore->get(makeDiskDocKey("key5"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), ENGINE_SUCCESS);
    rv = kvstore->get(makeDiskDocKey("key6"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), ENGINE_SUCCESS);

    auto rollbackResult =
            kvstore->rollback(Vbid(0), 5, std::make_unique<CustomRBCallback>());
    ASSERT_TRUE(rollbackResult.success);

    rv = kvstore->get(makeDiskDocKey("key1"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), ENGINE_SUCCESS);
    rv = kvstore->get(makeDiskDocKey("key5"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), ENGINE_SUCCESS);
    rv = kvstore->get(makeDiskDocKey("key6"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), ENGINE_KEY_ENOENT);
    rv = kvstore->get(makeDiskDocKey("key10"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), ENGINE_KEY_ENOENT);

    auto vbs = kvstore->getVBucketState(Vbid(0));
    ASSERT_EQ(uint64_t(5), vbs->highSeqno);
    ASSERT_EQ(size_t(5), kvstore->getItemCount(Vbid(0)));
}

TEST_F(MagmaKVStoreTest, RollbackNoValidCommitPoint) {
    uint64_t seqno = 1;

    auto cfg = reinterpret_cast<MagmaKVStoreConfig*>(kvstoreConfig.get());
    auto maxCommitPoints = cfg->getMagmaMaxCommitPoints();

    for (int i = 0; i < int(maxCommitPoints) + 1; i++) {
      kvstore->begin(std::make_unique<TransactionContext>(vbid));
      for (int j = 0; j < 5; j++) {
        auto key = makeStoredDocKey("key" + std::to_string(seqno));
        auto qi = makeCommittedItem(key, "value");
        qi->setBySeqno(seqno++);
        kvstore->set(qi);
      }
      kvstore->commit(flush);
    }

    auto rollbackResult =
            kvstore->rollback(Vbid(0), 5, std::make_unique<CustomRBCallback>());
    ASSERT_FALSE(rollbackResult.success);
}

TEST_F(MagmaKVStoreTest, prepareToCreate) {
    vbucket_state state;
    state.transition.state = vbucket_state_active;
    kvstore->snapshotVBucket(vbid, state);
    auto kvsRev = kvstore->prepareToDelete(Vbid(0));
    ASSERT_EQ(1, int(kvsRev));
    EXPECT_NO_THROW(kvstore->prepareToCreate(Vbid(0)));
}

TEST_F(MagmaKVStoreTest, getStat) {
    size_t val;
    ASSERT_FALSE(kvstore->getStat("foobar", val));
    ASSERT_TRUE(kvstore->getStat("memory_quota", val));
}

TEST_F(MagmaKVStoreTest, setMaxDataSize) {
    uint64_t seqno{1};

    // Magma's memory quota is recalculated on each commit batch.
    kvstore->begin(std::make_unique<TransactionContext>(Vbid{0}));
    auto qi = makeCommittedItem(makeStoredDocKey("key"), "value");
    qi->setBySeqno(seqno++);
    kvstore->set(qi);
    kvstore->commit(flush);

    size_t memQuota;
    ASSERT_TRUE(kvstore->getStat("memory_quota", memQuota));
    size_t writeCacheQuota;
    ASSERT_TRUE(kvstore->getStat("write_cache_quota", writeCacheQuota));

    kvstore->setMaxDataSize(memQuota / 10);

    // Magma's memory quota is recalculated on each commit batch.
    kvstore->begin(std::make_unique<TransactionContext>(Vbid{0}));
    qi->setBySeqno(seqno++);
    kvstore->set(qi);
    kvstore->commit(flush);

    size_t memQuotaAfter;
    ASSERT_TRUE(kvstore->getStat("memory_quota", memQuotaAfter));
    ASSERT_LT(memQuotaAfter, memQuota);

    size_t writeCacheQuotaAfter;
    ASSERT_TRUE(kvstore->getStat("write_cache_quota", writeCacheQuotaAfter));
    ASSERT_LT(writeCacheQuotaAfter, writeCacheQuota);
}

TEST_F(MagmaKVStoreTest, badSetRequest) {
    // Grab a pointer to our MockTransactionContext so that we can establish
    // expectations on it throughout the test. We consume our unique_ptr to it
    // in KVStore::begin but our raw pointer will remain.
    std::unique_ptr<TransactionContext> tc =
            std::make_unique<MockTransactionContext>(Vbid(0));
    auto* mockTC = dynamic_cast<MockTransactionContext*>(tc.get());

    kvstore->begin(std::move(tc));
    auto key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->set(qi);

    kvstore->saveDocsErrorInjector = [](VB::Commit& cmt,
                                        kvstats_ctx& ctx) -> int {
        return magma::Status::IOError;
    };

    EXPECT_CALL(*mockTC, setCallback(_, KVStore::FlushStateMutation::Failed))
            .Times(1);
    EXPECT_FALSE(kvstore->commit(flush));
}

TEST_F(MagmaKVStoreTest, badDelRequest) {
    // Grab a pointer to our MockTransactionContext so that we can establish
    // expectations on it throughout the test. We consume our unique_ptr to it
    // in KVStore::begin but our raw pointer will remain.
    std::unique_ptr<TransactionContext> tc =
            std::make_unique<MockTransactionContext>(Vbid(0));
    auto* mockTC = dynamic_cast<MockTransactionContext*>(tc.get());

    kvstore->begin(std::move(tc));
    auto key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    qi->setDeleted(DeleteSource::Explicit);
    kvstore->set(qi);

    kvstore->saveDocsErrorInjector = [](VB::Commit& cmt,
                                        kvstats_ctx& ctx) -> int {
        return magma::Status::IOError;
    };

    EXPECT_CALL(*mockTC, deleteCallback(_, KVStore::FlushStateDeletion::Failed))
            .Times(1);
    EXPECT_FALSE(kvstore->commit(flush));
}
#endif
