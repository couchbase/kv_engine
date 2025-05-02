/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

// Note: This *must* be included first to avoid issues on Windows with ambiguous
// symbols for close() et al.
#include <folly/portability/GTest.h>

#include "bucket_logger.h"
#include "collections/collection_persisted_stats.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "encryption_key_provider.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "kvstore/couch-kvstore/couch-kvstore-config.h"
#include "kvstore/couch-kvstore/couch-kvstore.h"
#include "kvstore/kvstore_transaction_context.h"
#include "kvstore_test.h"
#include "rollback_result.h"
#include "src/internal.h"
#include "test_helpers.h"
#include "tests/mock/mock_bucket_logger.h"
#include "tests/mock/mock_couch_kvstore.h"
#include "tests/test_fileops.h"
#include "tools/couchfile_upgrade/input_couchfile.h"
#include "tools/couchfile_upgrade/output_couchfile.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_test.h"

#include <folly/portability/GMock.h>
#include <platform/dirutils.h>

#include <programs/engine_testapp/mock_cookie.h>
#include <fstream>
#include <memory>

/// Test fixture for tests which run only on Couchstore.
class CouchKVStoreTest : public KVStoreTest {
public:
    CouchKVStoreTest() : KVStoreTest() {
    }

    /**
     * Run an offline upgrade
     * @param writeAsMadHatter Make the data file appear as if it's 6.5
     * @param outputCid Upgrade and move the keys to this collection
     * @param keys how many keys to write
     * @param deleteKeyRange a range of keys that will be deleted
     */
    void collectionsOfflineUpgrade(bool writeAsMadHatter,
                                   CollectionID outputCid,
                                   int keys,
                                   std::pair<int, int> deletedKeyRange);

    void runCompaction(KVStoreIface& kvstore, CompactionConfig config) {
        std::mutex mutex;
        std::unique_lock<std::mutex> lock(mutex);
        auto vb = TestEPVBucketFactory::makeVBucket(vbid);
        auto ctx = std::make_shared<CompactionContext>(vb, config, 0);
        EXPECT_EQ(CompactDBStatus::Success, kvstore.compactDB(lock, ctx));
    }
};

// Verify the stats returned from operations are accurate.
TEST_F(CouchKVStoreTest, StatsTest) {
    CouchKVStoreConfig config(1024, 4, data_dir, "couchdb", 0);
    auto kvstore = setup_kv_store(config);

    // Perform a transaction with a single mutation (set) in it.
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    const std::string key{"key"};
    const std::string value{"value"};
    kvstore->set(*ctx, makeCommittedItem(makeStoredDocKey(key), value));

    EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));

    // Check statistics are correct.
    std::map<std::string, std::string> stats;
    auto add_stat_callback = [&stats](std::string_view key,
                                      std::string_view value,
                                      CookieIface&) {
        stats.insert(std::make_pair(std::string(key.data(), key.size()),
                                    std::string(value.data(), value.size())));
    };
    auto* cookie = create_mock_cookie();
    kvstore->addStats(add_stat_callback, *cookie);
    destroy_mock_cookie(cookie);
    EXPECT_EQ("1", stats["rw_0:io_num_write"]);
    const size_t io_write_bytes = stoul(stats["rw_0:io_document_write_bytes"]);
    // 1 (for the namespace)
    EXPECT_EQ(1 + key.size() + value.size() +
                      MetaData::getMetaDataSize(MetaData::Version::V1),
              io_write_bytes);

    // Hard to determine exactly how many bytes should have been written, but
    // expect non-zero, and least as many as the actual documents.
    const size_t io_total_write_bytes =
            stoul(stats["rw_0:io_total_write_bytes"]);
    EXPECT_GT(io_total_write_bytes, 0);
    EXPECT_GE(io_total_write_bytes, io_write_bytes);
}

// Verify the compaction stats returned from operations are accurate.
TEST_F(CouchKVStoreTest, CompactStatsTest) {
    CouchKVStoreConfig config(4, 4, data_dir, "couchdb", 0);
    auto kvstore = setup_kv_store(config);

    // Perform a transaction with a single mutation (set) in it.
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    const std::string key{"key"};
    const std::string value{"value"};
    kvstore->set(*ctx, makeCommittedItem(makeStoredDocKey(key), value));
    flush.proposedVBState.transition.state = vbucket_state_active;
    EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));

    CompactionConfig compactionConfig;
    compactionConfig.purge_before_seq = 0;
    compactionConfig.purge_before_ts = 0;
    compactionConfig.drop_deletes = false;
    auto vb = TestEPVBucketFactory::makeVBucket(vbid);
    auto cctx = std::make_shared<CompactionContext>(vb, compactionConfig, 0);
    {
        auto vbLock = getVbLock();
        EXPECT_EQ(CompactDBStatus::Success, kvstore->compactDB(vbLock, cctx));
    }
    // Check statistics are correct.
    std::map<std::string, std::string> stats;
    auto add_stat_callback = [&stats](std::string_view key,
                                      std::string_view value,
                                      CookieIface&) {
        stats.insert(std::make_pair(std::string(key.data(), key.size()),
                                    std::string(value.data(), value.size())));
    };
    auto* cookie = create_mock_cookie();
    kvstore->addStats(add_stat_callback, *cookie);
    destroy_mock_cookie(cookie);
    EXPECT_EQ("1", stats["rw_0:io_num_write"]);
    const size_t io_write_bytes = stoul(stats["rw_0:io_document_write_bytes"]);

    // Hard to determine exactly how many bytes should have been written, but
    // expect non-zero, and at least twice as many as the actual documents for
    // the total and once as many for compaction alone.
    const size_t io_total_write_bytes =
            stoul(stats["rw_0:io_total_write_bytes"]);
    const size_t io_compaction_write_bytes =
            stoul(stats["rw_0:io_compaction_write_bytes"]);
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
    auto kvstore = KVStoreFactory::create(config, {}, {});
    ASSERT_NE(nullptr, kvstore);

    // Activate vBucket.
    Collections::VB::Manifest m{std::make_shared<Collections::Manager>()};
    VB::Commit meta(m);
    meta.proposedVBState.transition.state = vbucket_state_active;
    meta.proposedVBState.maxCas = -1;
    EXPECT_TRUE(kvstore->snapshotVBucket(Vbid(0), meta));
    EXPECT_EQ(~0ull, kvstore->listPersistedVbuckets()[0]->maxCas);

    // Close the file, then re-open.
    kvstore = KVStoreFactory::create(config, {}, {});
    EXPECT_NE(nullptr, kvstore);

    // Check that our max CAS was repaired on startup.
    EXPECT_EQ(0u, kvstore->listPersistedVbuckets()[0]->maxCas);
}

// Regression test for MB-19430 - ensure that an attempt to get the
// item count from a file which doesn't exist yet propagates the
// error so the caller can detect (and retry as necessary).
TEST_F(CouchKVStoreTest, MB_18580_ENOENT) {
    CouchKVStoreConfig config(1024, 4, data_dir, "couchdb", 0);
    auto kvstore = KVStoreFactory::create(config, {}, {});
    ASSERT_NE(nullptr, kvstore);

    // Expect to get a system_error (ENOENT)
    EXPECT_THROW(kvstore->getDbFileInfo(Vbid(0)), std::system_error);
}

class CollectionsOfflineUpgradeCallback : public StatusCallback<CacheLookup> {
public:
    explicit CollectionsOfflineUpgradeCallback(CollectionID cid)
        : expectedCid(cid) {
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
    explicit CollectionsOfflineGetCallback(CollectionID expectedId,
                                           std::pair<int, int> deletedRange)
        : expectedId(expectedId), deletedRange(std::move(deletedRange)) {
    }

    void callback(GetValue& result) override {
        EXPECT_EQ(cb::engine_errc::success, result.getStatus());

        if (result.item->isDeleted()) {
            DocKeyView dk = result.item->getKey();
            EXPECT_EQ(expectedId, dk.getCollectionID());
            auto noCollection = dk.makeDocKeyWithoutCollectionID();
            // create a string from the logical-key, i.e. +1 and skip the
            // collection-ID
            std::string str(
                    reinterpret_cast<const char*>(noCollection.data() + 1),
                    noCollection.size());
            auto index = std::stoi(str);
            EXPECT_GE(index, deletedRange.first);
            EXPECT_LE(index, deletedRange.second);

            if (index & 1) {
                // The odd deleted docs have no body to validate
                return;
            }
            EXPECT_TRUE(result.item->getDataType() &
                        PROTOCOL_BINARY_DATATYPE_XATTR);
        }
        EXPECT_TRUE(PROTOCOL_BINARY_DATATYPE_SNAPPY &
                    result.item->getDataType());
        result.item->decompressValue();

        std::string_view value{result.item->getData(),
                               result.item->getNBytes()};

        std::string expectedValue = "valuable";
        if (result.item->getDataType() & PROTOCOL_BINARY_DATATYPE_XATTR) {
            expectedValue = createXattrValue("value");
        }
        EXPECT_EQ(expectedValue, value);
    }

private:
    CollectionID expectedId;
    std::pair<int, int> deletedRange;
};

// Test the InputCouchFile/OutputCouchFile objects (in a simple test) to
// check they do what we expect, that is create a new couchfile with all keys
// moved into a specified collection.
void CouchKVStoreTest::collectionsOfflineUpgrade(
        bool writeAsMadHatter,
        CollectionID outputCid,
        int keys,
        std::pair<int, int> deletedRange) {
    ASSERT_LE(deletedRange.first, deletedRange.second);
    ASSERT_LE(deletedRange.second, keys);
    CouchKVStoreConfig config1(1024, 4, data_dir, "couchdb", 0);
    CouchKVStoreConfig config2(1024, 4, data_dir, "couchdb", 0);

    // Test setup, create a new file
    auto kvstore = setup_kv_store(config1);
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());

    for (int i = 0; i < keys; i++) {
        // key needs to look like it's in the default collection so we can flush
        // it
        auto key = makeStoredDocKey(std::to_string(i));
        // create Item and use the raw key, but say it has a cid encoded so that
        // the constructor doesn't push this key into the default collection.
        std::unique_ptr<Item> item = std::make_unique<Item>(
                key, 0, 0, "valuable", 8, PROTOCOL_BINARY_RAW_BYTES, 0, i + 1);
        kvstore->set(*ctx, queued_item(std::move(item)));
    }

    kvstore->commit(std::move(ctx), flush);

    EXPECT_FALSE(ctx);
    EXPECT_FALSE(ctx.get());
    ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());

    // Now delete keys (if requested). Alternate between value and no-value
    // (like xattr)
    for (int i = deletedRange.first, seqno = keys + 1; i < deletedRange.second;
         ++i, ++seqno) {
        std::unique_ptr<Item> item;
        auto key = makeStoredDocKey(std::to_string(i));
        if (i & 1) {
            item.reset(Item::makeDeletedItem(
                    DeleteSource::Explicit, key, 0, 0, nullptr, 0));
        } else {
            const auto body = createXattrValue("value");
            item.reset(Item::makeDeletedItem(DeleteSource::Explicit,
                                             key,
                                             0,
                                             0,
                                             body.data(),
                                             body.size(),
                                             PROTOCOL_BINARY_DATATYPE_XATTR));
        }
        item->setBySeqno(seqno);
        kvstore->del(*ctx, queued_item(std::move(item)));
    }
    kvstore->commit(std::move(ctx), flush);

    rewriteCouchstoreVBState(Vbid(0), data_dir, 2, false /*no namespaces*/);

    // Use the upgrade tool's objects to run an upgrade
    // setup_kv_store will have progressed the rev to .2
    Collections::InputCouchFile input({}, data_dir + "/0.couch.2");
    CollectionID cid = outputCid;
    Collections::OutputCouchFile output({},
                                        data_dir + "/0.couch.3",
                                        cid /*collection-id*/,
                                        1024 * 1024 /*buffersize*/);
    input.upgrade(output);
    if (writeAsMadHatter) {
        output.writeUpgradeCompleteMadHatter(input);
    } else {
        output.writeUpgradeComplete(input);
    }
    output.commit();

    auto kvstore2 = KVStoreFactory::create(config2, {}, {});
    auto scanCtx = kvstore2->initBySeqnoScanContext(
            std::make_unique<CollectionsOfflineGetCallback>(cid, deletedRange),
            std::make_unique<CollectionsOfflineUpgradeCallback>(cid),
            Vbid(0),
            1,
            DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS,
            ValueFilter::VALUES_COMPRESSED,
            SnapshotSource::Head);

    ASSERT_TRUE(scanCtx);
    EXPECT_EQ(ScanStatus::Success, kvstore2->scan(*scanCtx));

    const auto& cl = static_cast<const CollectionsOfflineUpgradeCallback&>(
            scanCtx->getCacheCallback());
    EXPECT_EQ(keys, cl.callbacks);

    // Check item count
    auto [status, stats] = kvstore2->getCollectionStats(Vbid(0), cid);
    EXPECT_EQ(KVStore::GetCollectionStatsStatus::Success, status);
    auto deletedCount = deletedRange.second - deletedRange.first;
    EXPECT_EQ(keys - deletedCount, stats.itemCount);
    EXPECT_EQ(keys + deletedCount, stats.highSeqno);
    EXPECT_NE(0, stats.diskSize);

    auto kvstoreContext = kvstore2->makeFileHandle(Vbid(0));
    auto uid = kvstore2->getCollectionsManifestUid(*kvstoreContext);
    EXPECT_TRUE(uid.has_value());
    EXPECT_EQ(0, uid.value());

    // Test that the datafile can be compacted.
    // MB-45917
    CompactionConfig config{0, 0, true /*purge all tombstones*/, false, {}};
    runCompaction(*kvstore2, config);
}

TEST_F(CouchKVStoreTest, CollectionsOfflineUpgrade) {
    // The key count is large enough to ensure the count uses more than 1 byte
    // of leb storage so we validate that leb encode/decode works on this path
    collectionsOfflineUpgrade(false, CollectionID{500}, 129, {14, 14 + 18});
}

TEST_F(CouchKVStoreTest, CollectionsOfflineUpgradeMadHatter) {
    // The key count is large enough to ensure the count uses more than 1 byte
    // of leb storage so we validate that leb encode/decode works on this path
    collectionsOfflineUpgrade(true, CollectionID{500}, 129, {14, 14 + 18});
}

TEST_F(CouchKVStoreTest, CollectionsOfflineUpgradeMadHatter_MB_45917) {
    // Delete all the keys, this would trigger MB-45917.
    // Here we upgrade as if from mad-hatter, that means there is no diskSize
    // stat in the collections local doc, which then leads to an underflow as
    // the wrong file disk size was used in calculating the collection's disk
    // size. The target collection could be anything, but do a to=default so
    // it more mimics what happens. Finally write 129 keys and delete them all.
    collectionsOfflineUpgrade(true, CollectionID::Default, 129, {0, 129});
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
class CouchKVStoreErrorInjectionTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<bool> {
public:
    CouchKVStoreErrorInjectionTest()
        : data_dir("CouchKVStoreErrorInjectionTest.db"),
          ops(create_default_file_ops()),
          logger("couchKVStoreTest"),
          config(1024, 4, data_dir, "couchdb", 0),
          flush(manifest) {
        config.setLogger(logger);
        config.setBuffered(false);
        std::filesystem::remove_all(data_dir);
        // Data directory creation is normally done by the engine
        // initialization; we're not running a full engine here so we have to
        // create the directory manually.
        try {
            std::filesystem::create_directories(data_dir);
        } catch (const std::system_error& error) {
            throw std::runtime_error(
                    fmt::format("Failed to create data directory [{}]:{}",
                                data_dir,
                                error.code().message()));
        }
        if (isEncrypted()) {
            keyStore = nlohmann::json::parse(R"({
    "keys": [
        {
            "id": "MyActiveKey",
            "cipher": "AES-256-GCM",
            "key": "cXOdH9oGE834Y2rWA+FSdXXi5CN3mLJ+Z+C0VpWbOdA="
        },
        {
            "id": "MyOtherKey",
            "cipher": "AES-256-GCM",
            "key": "hDYX36zHrP0/eApT7Gf3g2sQ9L5gubHSDeLQxg4v4kM="
        }
    ],
    "active": "MyActiveKey"
})");
            encryptionKeyProvider.setKeys(keyStore);
        }
        kvstore = std::make_unique<CouchKVStore>(
                config, ops, &encryptionKeyProvider);
        initialize_kv_store(kvstore.get());
        defaultCreateItemCallback = kvstore->getDefaultCreateItemCallback();
    }

    ~CouchKVStoreErrorInjectionTest() override {
        std::filesystem::remove_all(data_dir);
    }

    EPStats globalStats;
    VBucketPtr makeVBucket() {
        Configuration conf;
        return std::make_unique<EPVBucket>(
                vbid,
                vbucket_state_active,
                globalStats,
                *std::make_unique<CheckpointConfig>(conf),
                /*kvshard*/ nullptr,
                /*lastSeqno*/ 1000,
                /*lastSnapStart*/ 1000,
                /*lastSnapEnd*/ 1000,
                /*table*/ nullptr,
                std::make_shared<DummyCB>(),
                [](Vbid) {},
                NoopSyncWriteCompleteCb,
                NoopSyncWriteTimeoutFactory,
                NoopSeqnoAckCb,
                conf,
                EvictionPolicy::Value,
                std::make_unique<Collections::VB::Manifest>(
                        std::make_shared<Collections::Manager>()));
    }

protected:
    void generate_items(size_t count) {
        for (unsigned i(0); i < count; i++) {
            std::string key("key" + std::to_string(i));
            auto qi = makeCommittedItem(makeStoredDocKey(key), "value");
            qi->setBySeqno(i + 1);
            items.push_back(qi);
        }
    }

    void populate_items(size_t count) {
        generate_items(count);
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        for (const auto& item : items) {
            kvstore->set(*ctx, item);
        }
        // Ensure a valid vbstate is committed
        flush.proposedVBState.lastSnapEnd = items.back()->getBySeqno();
        kvstore->commit(std::move(ctx), flush);
    }

    vb_bgfetch_queue_t make_bgfetch_queue() {
        vb_bgfetch_queue_t itms;
        for (const auto& item : items) {
            vb_bgfetch_item_ctx_t ctx;
            ctx.addBgFetch(std::make_unique<FrontEndBGFetchItem>(
                    nullptr, ValueFilter::VALUES_DECOMPRESSED, 0));
            itms[DiskDocKey{*item}] = std::move(ctx);
        }
        return itms;
    }

    static bool isEncrypted() {
        return GetParam();
    }

    /**
     * Number of read calls per metadata header
     */
    static int numMetadataHeaderReads() {
        // Unencrypted files don't have a metadata header
        return isEncrypted() ? 3 : 0;
    }

    /**
     * Number of write calls per data chunk
     */
    static int numDataChunkWrites() {
        return isEncrypted() ? 1 : 2;
    }

    /**
     * Size of data chunk header
     */
    static size_t dataChunkHeaderSize() {
        return isEncrypted() ? 4 : 8;
    }

    /**
     * Test that CouchKVStore::compactDB performs as expected when it errors
     * during an open
     */
    void testCompactDBCompactDBEx();

    const std::string data_dir;

    ::testing::NiceMock<MockOps> ops;
    ::testing::NiceMock<MockBucketLogger> logger;

    CouchKVStoreConfig config;
    KVStoreIface::CreateItemCB defaultCreateItemCallback;
    cb::crypto::KeyStore keyStore;
    EncryptionKeyProvider encryptionKeyProvider;
    std::unique_ptr<CouchKVStore> kvstore;
    std::vector<queued_item> items;
    Collections::VB::Manifest manifest{
            std::make_shared<Collections::Manager>()};
    VB::Commit flush;
    Vbid vbid = Vbid(0);
};

/**
 * Injects error during CouchKVStore::writeVBucketState/couchstore_commit
 */
TEST_P(CouchKVStoreErrorInjectionTest, initializeWithHeaderButNoVBState) {
    vbid = Vbid(10);

    // Make sure the vBucket does not exist before this test
    ASSERT_FALSE(kvstore->getCachedVBucketState(vbid));
    ASSERT_THROW(kvstore->getPersistedVBucketState(vbid), std::logic_error);
    ASSERT_EQ(0, kvstore->getKVStoreStat().numVbSetFailure);

    EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
    EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
    EXPECT_CALL(logger,
                mlog(Ge(spdlog::level::level_enum::warn),
                     VCE(COUCHSTORE_ERROR_WRITE)))
            .Times(1)
            .RetiresOnSaturation();

    // Inject a failure in the setVBState phase
    using namespace testing;
    EXPECT_CALL(ops, sync(_, _))
            .Times(3)
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // boot pre-commit
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // boot commit
            .WillOnce(Return(COUCHSTORE_ERROR_WRITE)); // setVBS pre-commit

    // Set something in the vbucket_state to differentiate it from the
    // default constructed one. It doesn't matter what we set.
    Collections::VB::Manifest m{std::make_shared<Collections::Manager>()};
    VB::Commit meta(m);
    meta.proposedVBState.maxVisibleSeqno = 10;

    // Must fail
    EXPECT_FALSE(kvstore->snapshotVBucket(vbid, meta));

    // vbucket_state is still default as readVBState returns a default value
    // instead of a non-success status or exception...
    vbucket_state defaultState;
    auto diskState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::NotFound, diskState.status);
    ASSERT_EQ(defaultState, diskState.state);
    EXPECT_EQ(1, kvstore->getKVStoreStat().numVbSetFailure);

    // Recreate the kvstore and the state should equal the default constructed
    // state (and not throw an exception)
    kvstore =
            std::make_unique<CouchKVStore>(config, ops, &encryptionKeyProvider);

    EXPECT_FALSE(kvstore->getCachedVBucketState(vbid));

    diskState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::NotFound, diskState.status);
    EXPECT_EQ(defaultState, diskState.state);
}

/**
 * Injects error during CouchKVStore::openDB_retry/couchstore_open_db_ex
 */
TEST_P(CouchKVStoreErrorInjectionTest, openDB_retry_open_db_ex) {
    generate_items(1);

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    kvstore->set(*ctx, items.front());
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
                .WillOnce(Return(COUCHSTORE_ERROR_OPEN_FILE))
                .RetiresOnSaturation();

        EXPECT_FALSE(kvstore->commit(std::move(ctx), flush));
    }
}

/**
 * Injects error during CouchKVStore::openDB/couchstore_open_db_ex
 */
TEST_P(CouchKVStoreErrorInjectionTest, openDB_open_db_ex) {
    generate_items(1);

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    kvstore->set(*ctx, items.front());
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
                .WillRepeatedly(Return(COUCHSTORE_ERROR_OPEN_FILE))
                .RetiresOnSaturation();

        EXPECT_FALSE(kvstore->commit(std::move(ctx), flush));
    }
}

/**
 * Injects error during CouchKVStore::commit/couchstore_save_documents/write_doc
 */
TEST_P(CouchKVStoreErrorInjectionTest, commit_save_documents_write_doc) {
    generate_items(1);

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    kvstore->set(*ctx, items.front());
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
                .WillOnce(Return(COUCHSTORE_ERROR_WRITE))
                .RetiresOnSaturation();

        EXPECT_FALSE(kvstore->commit(std::move(ctx), flush));
    }
}

/**
 * Injects error during CouchKVStore::commit/couchstore_save_documents/flush_mr
 */
TEST_P(CouchKVStoreErrorInjectionTest, commit_save_documents_flush_mr) {
    generate_items(1);

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    kvstore->set(*ctx, items.front());
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
                .WillOnce(Return(COUCHSTORE_ERROR_WRITE))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pwrite(_, _, _, _, _))
                .Times(2 * numDataChunkWrites())
                .RetiresOnSaturation();

        EXPECT_FALSE(kvstore->commit(std::move(ctx), flush));
    }
}

/**
 * Injects error during CouchKVStore::commit/updateLocalDocuments
 */
TEST_P(CouchKVStoreErrorInjectionTest, commit_updateLocalDocuments) {
    generate_items(1);
    // Establish Logger expectation
    EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
    EXPECT_CALL(logger,
                mlog(Ge(spdlog::level::level_enum::warn),
                     VCE(COUCHSTORE_ERROR_WRITE)))
            .RetiresOnSaturation();
    {
        // Establish FileOps expectation
        InSequence s;
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).Times(3 * numDataChunkWrites());
        EXPECT_CALL(ops, pwrite(_, _, _, _, _))
                .WillOnce(Return(COUCHSTORE_ERROR_WRITE));
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).Times(AnyNumber());
    }
    {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->set(*ctx, items.front());
        EXPECT_FALSE(kvstore->commit(std::move(ctx), flush));
    }
    {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->set(*ctx, items.front());
        EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));
    }
}

/**
 * Injects error during CouchKVStore::commit/couchstore_commit
 */
TEST_P(CouchKVStoreErrorInjectionTest, commit_commit) {
    generate_items(1);

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    kvstore->set(*ctx, items.front());
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
                .WillOnce(Return(COUCHSTORE_ERROR_WRITE))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pwrite(_, _, _, _, _))
                .Times(4 * numDataChunkWrites())
                .RetiresOnSaturation();

        EXPECT_FALSE(kvstore->commit(std::move(ctx), flush));
    }
}

/**
 * Injects error during CouchKVStore::get/couchstore_docinfo_by_id
 */
TEST_P(CouchKVStoreErrorInjectionTest, get_docinfo_by_id) {
    populate_items(1);
    GetValue gv;
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .Times(3 + numMetadataHeaderReads())
                .RetiresOnSaturation();
        gv = kvstore->get(DiskDocKey{*items.front()}, Vbid(0));
    }
    EXPECT_EQ(cb::engine_errc::temporary_failure, gv.getStatus());

    /* Check we incremented the read failures */
    size_t getFailures = 0;
    kvstore->getStat("failure_get", getFailures);
    EXPECT_EQ(1, getFailures);
}

/**
 * Injects error during CouchKVStore::get/couchstore_open_doc_with_docinfo
 */
TEST_P(CouchKVStoreErrorInjectionTest, get_open_doc_with_docinfo) {
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
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .Times(5 + numMetadataHeaderReads())
                .RetiresOnSaturation();
        gv = kvstore->get(DiskDocKey{*items.front()}, Vbid(0));
    }
    EXPECT_EQ(cb::engine_errc::temporary_failure, gv.getStatus());
}

/**
 * Injects error during CouchKVStore::getMulti/FileOpsInterface::open
 */
TEST_P(CouchKVStoreErrorInjectionTest, getMulti_open_file) {
    populate_items(2);
    vb_bgfetch_queue_t itms(make_bgfetch_queue());

    /* Check preconditions */
    ASSERT_EQ(0, kvstore->getKVStoreStat().numGetFailure);

    /* Establish FileOps expectation */
    EXPECT_CALL(ops, open(_, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_OPEN_FILE))
            .RetiresOnSaturation();
    kvstore->getMulti(Vbid(0), itms, defaultCreateItemCallback);

    EXPECT_EQ(2, kvstore->getKVStoreStat().numGetFailure);

    EXPECT_EQ(cb::engine_errc::temporary_failure,
              itms[DiskDocKey{*items.at(0)}].value.getStatus());
    EXPECT_EQ(cb::engine_errc::temporary_failure,
              itms[DiskDocKey{*items.at(1)}].value.getStatus());
}

/**
 * Injects error during CouchKVStore::getMulti/couchstore_docinfos_by_id
 */
TEST_P(CouchKVStoreErrorInjectionTest, getMulti_docinfos_by_id) {
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
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .Times(3 + numMetadataHeaderReads())
                .RetiresOnSaturation();
        kvstore->getMulti(Vbid(0), itms, defaultCreateItemCallback);
    }
    EXPECT_EQ(cb::engine_errc::temporary_failure,
              itms[DiskDocKey{*items.at(0)}].value.getStatus());

    /* Check we incremented the read failures */
    size_t getFailures = 0;
    kvstore->getStat("failure_get", getFailures);
    EXPECT_EQ(1, getFailures);
}

/**
 * Injects error during CouchKVStore::getMulti/couchstore_open_doc_with_docinfo
 */
TEST_P(CouchKVStoreErrorInjectionTest, getMulti_open_doc_with_docinfo) {
    populate_items(1);
    vb_bgfetch_queue_t itms(make_bgfetch_queue());
    {
        /* Check preconditions */
        ASSERT_EQ(0, kvstore->getKVStoreStat().numGetFailure);

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .Times(5 + numMetadataHeaderReads())
                .RetiresOnSaturation();
        kvstore->getMulti(Vbid(0), itms, defaultCreateItemCallback);

        EXPECT_EQ(1, kvstore->getKVStoreStat().numGetFailure);
    }
    EXPECT_EQ(cb::engine_errc::temporary_failure,
              itms[DiskDocKey{*items.at(0)}].value.getStatus());
}

void CouchKVStoreErrorInjectionTest::testCompactDBCompactDBEx() {
    populate_items(1);

    CompactionConfig config;
    config.purge_before_seq = 0;
    config.purge_before_ts = 0;
    config.drop_deletes = false;
    auto vb = makeVBucket();
    auto cctx = std::make_shared<CompactionContext>(vb, config, 0);

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
                .WillOnce(Return(COUCHSTORE_ERROR_OPEN_FILE))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, open(_, _, _, _)).Times(1).RetiresOnSaturation();
        std::mutex mutex;
        std::unique_lock<std::mutex> lock(mutex);
        kvstore->compactDB(lock, cctx);
    }
}

TEST_P(CouchKVStoreErrorInjectionTest, compactDB_compact_db_ex) {
    testCompactDBCompactDBEx();
}

TEST_P(CouchKVStoreErrorInjectionTest, compactDB_EncryptionKeys) {
    if (!isEncrypted()) {
        GTEST_SKIP();
    }
    populate_items(4);
    auto vb = makeVBucket();
    std::mutex mutex;
    CompactionConfig config;
    config.purge_before_seq = 0;
    config.purge_before_ts = 0;
    config.drop_deletes = false;
    auto getKeyIds = [this]() {
        return std::get<std::unordered_set<std::string>>(
                kvstore->getEncryptionKeyIds());
    };
    {
        auto keys = getKeyIds();
        ASSERT_FALSE(keys.empty());
        EXPECT_EQ("MyActiveKey", *keys.begin());
        nlohmann::json json = keyStore;
        json["active"] = "MyOtherKey";
        keyStore = json;
        encryptionKeyProvider.setKeys(keyStore);
    }
    {
        auto cctx = std::make_shared<CompactionContext>(vb, config, 0);
        std::unique_lock lock{mutex};
        kvstore->compactDB(lock, cctx);
    }
    {
        auto keys = getKeyIds();
        ASSERT_FALSE(keys.empty());
        EXPECT_EQ("MyOtherKey", *keys.begin());
        nlohmann::json json = keyStore;
        json["active"] = "MyActiveKey";
        keyStore = json;
        encryptionKeyProvider.setKeys(keyStore);
    }
    {
        auto cctx = std::make_shared<CompactionContext>(vb, config, 0);
        std::unique_lock lock{mutex};
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_READ)))
                .RetiresOnSaturation();
        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .Times(4 + numMetadataHeaderReads())
                .RetiresOnSaturation();
        kvstore->compactDB(lock, cctx);
    }
    {
        auto keys = getKeyIds();
        ASSERT_FALSE(keys.empty());
        EXPECT_EQ("MyOtherKey", *keys.begin());
    }
}

/**
 * Injects error during
 * CouchKVStore::initBySeqnoScanContext/couchstore_changes_count
 */
TEST_P(CouchKVStoreErrorInjectionTest, initBySeqnoScanContext_changes_count) {
    populate_items(1);
    {
        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .Times(3 + numMetadataHeaderReads())
                .RetiresOnSaturation();

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
TEST_P(CouchKVStoreErrorInjectionTest, scan_changes_since) {
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
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();

        kvstore->scan(*scan_context);
    }
}

/**
 * Injects error during
 * CouchKVStore::recordDbDump/couchstore_open_doc_with_docinfo
 */
TEST_P(CouchKVStoreErrorInjectionTest, recordDbDump_open_doc_with_docinfo) {
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
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        // DB already open so no metadata reads
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(2).RetiresOnSaturation();

        kvstore->scan(*scan_context);
    }
}

/**
 * Injects error during CouchKVStore::rollback/couchstore_changes_count/1
 */
TEST_P(CouchKVStoreErrorInjectionTest, rollback_changes_count1) {
    generate_items(6);

    for (const auto& item : items) {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->set(*ctx, item);
        kvstore->commit(std::move(ctx), flush);
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
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .Times(3 + numMetadataHeaderReads())
                .RetiresOnSaturation();

        kvstore->rollback(Vbid(0), 5, std::make_unique<CustomRBCallback>());
    }
}

/**
 * Injects error during CouchKVStore::rollback/couchstore_rewind_header
 */
TEST_P(CouchKVStoreErrorInjectionTest, rollback_rewind_header) {
    generate_items(6);

    for (const auto& item : items) {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->set(*ctx, item);
        kvstore->commit(std::move(ctx), flush);
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
                .WillOnce(Return(COUCHSTORE_ERROR_ALLOC_FAIL))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .Times(9 + 2 * numMetadataHeaderReads())
                .RetiresOnSaturation();

        kvstore->rollback(Vbid(0), 5, std::make_unique<CustomRBCallback>());
    }
}

/**
 * Injects error during CouchKVStore::rollback/couchstore_changes_count/2
 */
TEST_P(CouchKVStoreErrorInjectionTest, rollback_changes_count2) {
    generate_items(6);

    for (const auto& item : items) {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->set(*ctx, item);
        kvstore->commit(std::move(ctx), flush);
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
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .Times(11 + 2 * numMetadataHeaderReads())
                .RetiresOnSaturation();

        kvstore->rollback(Vbid(0), 5, std::make_unique<CustomRBCallback>());
    }
}

/**
 * Injects error during CouchKVStore::readVBState/couchstore_open_local_document
 */
TEST_P(CouchKVStoreErrorInjectionTest, readVBState_open_local_document) {
    generate_items(6);

    for (const auto& item : items) {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        // Commit a valid vbstate
        flush.proposedVBState.lastSnapEnd = item->getBySeqno();
        kvstore->set(*ctx, item);
        kvstore->commit(std::move(ctx), flush);
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
        // Called once, when we read the vbstate in initBySeqnoScanContext.
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .Times(20 + 3 * numMetadataHeaderReads())
                .RetiresOnSaturation();

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
TEST_P(CouchKVStoreErrorInjectionTest, getAllKeys_all_docs) {
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
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .Times(3 + numMetadataHeaderReads())
                .RetiresOnSaturation();

        kvstore->getAllKeys(Vbid(0), start, 1, adcb);
    }
}

/**
 * Injects error during CouchKVStore::closeDB/couchstore_close_file
 */
TEST_P(CouchKVStoreErrorInjectionTest, closeDB_close_file) {
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
TEST_P(CouchKVStoreErrorInjectionTest, savedocs_doc_infos_by_id) {
    // Insert some items into the B-Tree
    generate_items(6);

    for (const auto& item : items) {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->set(*ctx, item);
        kvstore->commit(std::move(ctx), flush);
    }

    {
        std::string key("key7");
        auto qi = makeCommittedItem(makeStoredDocKey(key), "value");
        qi->setBySeqno(7);
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->set(*ctx, qi);
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
                    .Times(6 + numMetadataHeaderReads())
                    .RetiresOnSaturation();

            kvstore->commit(std::move(ctx), flush);
        }
    }
}

/**
 * Injects corruption (invalid header length) during
 * CouchKVStore::readVBState/couchstore_open_local_document
 */
TEST_P(CouchKVStoreErrorInjectionTest, corruption_get_open_doc_with_docinfo) {
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
            if (isEncrypted()) {
                // Metadata header
                // 1 byte - detect block type
                EXPECT_CALL(ops, pread(_, _, _, 1, _));
                // 8 bytes - header
                EXPECT_CALL(ops, pread(_, _, _, 8, _));
                // <variable> - content
                EXPECT_CALL(ops, pread(_, _, _, _, _));
            }
            // 4/8 bytes - header
            EXPECT_CALL(ops, pread(_, _, _, dataChunkHeaderSize(), _));
            // <variable - seqno tree root
            EXPECT_CALL(ops, pread(_, _, _, _, _));

            // chunk header - we want to corrupt the length (1st 32bit word)
            // so the checksum fails.
            EXPECT_CALL(ops, pread(_, _, _, dataChunkHeaderSize(), _))
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
                        uint32_t expectedLength = 7;
                        if (isEncrypted()) {
                            // Encrypted chunks have a 16 byte MAC tag.
                            expectedLength += 16;
                        } else {
                            // Unencrypted chunks have the top bit set.
                            expectedLength |= 0x80000000;
                        }
                        EXPECT_EQ(expectedLength, ntohl(*length_ptr))
                                << "Unexpected chunk.length for value chunk";

                        // assumptions pass; now make length too small so CRC32
                        // should mismatch.
                        *length_ptr = htonl(expectedLength - 1);
                        return res;
                    }));
            // Final read of the value's data (should be size 6 given we
            // changed the chunk.length above).
            EXPECT_CALL(ops, pread(_, _, _, (isEncrypted() ? 6 + 16 : 6), _));
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
    EXPECT_EQ(cb::engine_errc::temporary_failure, gv.getStatus());
}

TEST_P(CouchKVStoreErrorInjectionTest, mountVBucket) {
    populate_items(1);
    const auto dbPath = std::filesystem::path(data_dir);
    const auto copyPath = dbPath / "0.couch.9";
    std::filesystem::copy(dbPath / "0.couch.2", copyPath);
    const std::vector<std::string> copyPaths{{copyPath.string()}};
    const size_t expectedDeks = isEncrypted() ? 1 : 0;
    {
        auto rev = kvstore->prepareToDelete(vbid);
        kvstore->delVBucket(vbid, std::move(rev));
        const auto [status, deks] = kvstore->mountVBucket(
                vbid, VBucketSnapshotSource::Local, copyPaths);
        EXPECT_EQ(cb::engine_errc::success, status);
        EXPECT_EQ(expectedDeks, deks.size());
    }
    {
        const auto [status, deks] = kvstore->mountVBucket(
                vbid, VBucketSnapshotSource::Local, copyPaths);
        EXPECT_EQ(cb::engine_errc::key_already_exists, status);
        EXPECT_EQ(0, deks.size());
        auto rev = kvstore->prepareToDelete(vbid);
        kvstore->delVBucket(vbid, std::move(rev));
    }
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_READ)))
                .RetiresOnSaturation();
        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(AnyNumber());
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).RetiresOnSaturation();
        const auto [status, deks] = kvstore->mountVBucket(
                vbid, VBucketSnapshotSource::Local, copyPaths);
        EXPECT_EQ(cb::engine_errc::failed, status);
        EXPECT_EQ(0, deks.size());
    }
    {
        const auto [status, deks] = kvstore->mountVBucket(
                vbid, VBucketSnapshotSource::Local, copyPaths);
        EXPECT_EQ(cb::engine_errc::success, status);
        EXPECT_EQ(expectedDeks, deks.size());
    }
}

TEST_P(CouchKVStoreErrorInjectionTest, loadVBucketSnapshot) {
    populate_items(1);
    const auto dbPath = std::filesystem::path(data_dir);
    const auto copyPath = dbPath / "0.couch.9";
    std::filesystem::copy(dbPath / "0.couch.2", copyPath);
    const std::vector<std::string> copyPaths{{copyPath.string()}};
    const size_t expectedDeks = isEncrypted() ? 1 : 0;
    auto getKeyIds = [this]() {
        return std::get<std::unordered_set<std::string>>(
                kvstore->getEncryptionKeyIds());
    };
    {
        auto rev = kvstore->prepareToDelete(vbid);
        kvstore->delVBucket(vbid, std::move(rev));
        EXPECT_TRUE(getKeyIds().empty());
        const auto [status, deks] = kvstore->mountVBucket(
                vbid, VBucketSnapshotSource::Local, copyPaths);
        EXPECT_EQ(cb::engine_errc::success, status);
        EXPECT_EQ(expectedDeks, deks.size());
        if (!deks.empty()) {
            EXPECT_EQ("MyActiveKey", deks.front());
        }
        EXPECT_TRUE(getKeyIds().empty());
    }
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger,
                    mlog(Ge(spdlog::level::level_enum::warn),
                         VCE(COUCHSTORE_ERROR_READ)))
                .RetiresOnSaturation();
        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(AnyNumber());
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .WillOnce(Return(COUCHSTORE_ERROR_READ))
                .RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _))
                .Times(4 + numMetadataHeaderReads())
                .RetiresOnSaturation();
        auto result =
                kvstore->loadVBucketSnapshot(vbid, vbucket_state_replica, {});
        EXPECT_EQ(KVStoreIface::ReadVBStateStatus::Error, result.status);
        EXPECT_TRUE(getKeyIds().empty());
    }
    {
        auto result =
                kvstore->loadVBucketSnapshot(vbid, vbucket_state_replica, {});
        EXPECT_EQ(KVStoreIface::ReadVBStateStatus::Success, result.status);
        auto keys = getKeyIds();
        ASSERT_EQ(1, keys.size());
        if (isEncrypted()) {
            EXPECT_EQ("MyActiveKey", *keys.begin());
        } else {
            EXPECT_EQ("unencrypted", *keys.begin());
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
        CouchKVStoreErrorInjectionTest,
        CouchKVStoreErrorInjectionTest,
        ::testing::Bool(),
        [](const ::testing::TestParamInfo<bool>& testInfo) -> std::string {
            return testInfo.param ? "Encrypted" : "Unencrypted";
        });

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
        std::filesystem::remove_all(data_dir);
        // Data directory creation is normally done by the engine
        // initialization; we're not running a full engine here so we have to
        // create the directory manually.
        try {
            std::filesystem::create_directories(data_dir);
        } catch (const std::system_error& error) {
            throw std::runtime_error(
                    fmt::format("Failed to create data directory [{}]:{}",
                                data_dir,
                                error.code().message()));
        }
        kvstore = std::make_unique<MockCouchKVStore>(config);
        std::string failoverLog;
        // simulate a setVBState - increment the rev and then persist the
        // state
        kvstore->prepareToCreateImpl(vbid);
        Collections::VB::Manifest m{std::make_shared<Collections::Manager>()};
        VB::Commit meta(m);
        meta.proposedVBState.transition.state = vbucket_state_active;
        // simulate a setVBState - increment the dbFile revision
        kvstore->prepareToCreateImpl(vbid);
        kvstore->snapshotVBucket(vbid, meta);

        // Flush defaults the vBucket state to deleted and any test reading
        // the state from disk needs it to be active so set to active here
        flush.proposedVBState.transition.state = vbucket_state_active;
    }

    void persistPrepare(std::string key, std::string value, uint64_t seqno) {
        auto storedDocKey = makeStoredDocKey(key);
        auto qi = makePendingItem(storedDocKey, value);
        qi->setBySeqno(seqno);

        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->del(*ctx, qi);
        VB::Commit commit(manifest);
        auto diskState = kvstore->getPersistedVBucketState(vbid);
        ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, diskState.status);
        commit.proposedVBState = diskState.state;
        kvstore->commit(std::move(ctx), commit);
    }

    void persistAbort(std::string key, std::string value, uint64_t seqno) {
        auto storedDocKey = makeStoredDocKey(key);
        auto qi = makePendingItem(storedDocKey, value);
        qi->setBySeqno(seqno);
        qi->setAbortSyncWrite();

        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->del(*ctx, qi);
        VB::Commit commit(manifest);
        auto diskState = kvstore->getPersistedVBucketState(vbid);
        ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, diskState.status);
        commit.proposedVBState = diskState.state;
        kvstore->commit(std::move(ctx), commit);
    }

    ~CouchstoreTest() override {
        std::filesystem::remove_all(data_dir);
    }

    void flushItem(queued_item item) {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->set(*ctx, item);

        // The collections flush data doesn't get reset after a commit (it
        // normally would be reset by the flusher) so instead we just need to
        // use a new Flush/Commit object each time
        VB::Commit commit(manifest);
        auto diskState = kvstore->getPersistedVBucketState(vbid);
        ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, diskState.status);
        commit.proposedVBState = diskState.state;
        kvstore->commit(std::move(ctx), commit);
    }

    void runCompaction() {
        std::mutex mutex;
        std::unique_lock<std::mutex> lock(mutex);
        CompactionConfig compactConfig;
        auto vb = TestEPVBucketFactory::makeVBucket(vbid);
        auto ctx = std::make_shared<CompactionContext>(vb, compactConfig, 0);

        // We have some tests in this test suite that check that collection
        // stats are updated. The manfiest stats are normally updated via the
        // completionCallback which lives in EPBucket. Set the callback here
        // @TODO move the tests that rely on this to a different test suite and
        // remove
        ctx->completionCallback = [this](CompactionContext& ctx) {
            for (const auto& [cid, droppedPrepareBytes] :
                 ctx.stats.collectionSizeUpdates) {
                manifest.lock(cid).setDiskSize(droppedPrepareBytes);
            }
        };
        kvstore->compactDB(lock, ctx);
    }

protected:
    std::string data_dir;
    std::unique_ptr<MockCouchKVStore> kvstore;
    Vbid vbid;
    CouchKVStoreConfig config;
    Collections::VB::Manifest manifest{
            std::make_shared<Collections::Manager>()};
    VB::Commit flush;
};

template <class T>
class MockedGetCallback : public Callback<T> {
public:
    MockedGetCallback() = default;

    void callback(GetValue& value) override {
        status(value.getStatus());
        if (value.getStatus() == cb::engine_errc::success) {
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
    MOCK_METHOD1_T(status, void(cb::engine_errc));
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
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    auto* request = kvstore->setAndReturnRequest(*ctx, item);

    // Now directly mess with the metadata of the value which will be written
    MockCouchRequest::MetaData meta;
    request->writeMetaData(meta, 0); // no meta!

    kvstore->commit(std::move(ctx), flush);

    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    checkGetValue(gv, cb::engine_errc::temporary_failure);
}

TEST_F(CouchstoreTest, shortMeta) {
    StoredDocKey key = makeStoredDocKey("key");
    auto item = makeCommittedItem(key, "value");
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    auto* request = kvstore->setAndReturnRequest(*ctx, item);

    // Now directly mess with the metadata of the value which will be written
    MockCouchRequest::MetaData meta;
    request->writeMetaData(meta, 4); // not enough meta!
    kvstore->commit(std::move(ctx), flush);

    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    checkGetValue(gv, cb::engine_errc::temporary_failure);
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

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    kvstore->set(*ctx, item);
    kvstore->commit(std::move(ctx), flush);

    MockedGetCallback<GetValue> gc;
    EXPECT_CALL(gc, status(cb::engine_errc::success));
    EXPECT_CALL(gc, cas(0xf00fcafe11225566ull));
    EXPECT_CALL(gc, expTime(0xaa00bb11));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(PROTOCOL_BINARY_RAW_BYTES));
    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    gc.callback(gv);
}

// Prior to MB-48033 (Neo), CommittedSyncWrites were stored on-disk as
// queue_op::commit_sync_write and V3 metadata.
// However this was optimized in MB-48033 to store ad queue_op::mutation
// and V1 metadata (as V1 is smaller, and when reading from disk we
// must assume any Mutation was potentially a SyncWrite in terms of
// durability sequencing.
// Verify that the older V3+commit_sync_write format as written by older
// versions can still be read correctly.
TEST_F(CouchstoreTest, MB50286_ReadV3CommitSyncWrite) {
    // Setup to write a Committed item, but modify its metadata back to the
    // pre-Neo format prior to writing to disk.
    StoredDocKey key = makeStoredDocKey("key");
    auto item = makeCommittedItem(key, "value");
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    auto* request = kvstore->setAndReturnRequest(*ctx, item);

    // Replace metadata with pre-Neo format of V3+commit_sync_write.
    // We do this by manually creating a V3 Metadata blob (we cannot use the
    // ::MetaData class as it prevents us from setting operation to
    // commit_sync_write now, given that is no longer valid).
    ASSERT_EQ(MetaData::getMetaDataSize(MetaData::Version::V1),
              request->getDbDocInfo()->rev_meta.size)
            << "Expected current version of CouchKVStore to write V1 metadata "
               "for CommitSyncWrite";

    MockCouchRequest::MetaData meta;
    meta.metaV2V3.v3.operation = 1; // MetaDataV3::Operation::Commit
    request->writeMetaData(meta,
                           MetaData::getMetaDataSize(MetaData::Version::V3));

    // Finally write the pre-Neo style item to Couchstore.
    ASSERT_TRUE(kvstore->commit(std::move(ctx), flush));

    MockedGetCallback<GetValue> gc;
    EXPECT_CALL(gc, status(cb::engine_errc::success));
    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    gc.callback(gv);
    EXPECT_EQ(queue_op::mutation, gc.getValue()->getOperation());
}

TEST_F(CouchstoreTest, testV1MetaThings) {
    // Baseline test, just writes meta things and reads them
    // via standard interfaces
    // Ensure CAS, exptime and flags are set to something.
    auto datatype = PROTOCOL_BINARY_DATATYPE_JSON; // lies, but non-zero
    StoredDocKey key = makeStoredDocKey("key");
    queued_item item(std::make_unique<Item>(key,
                                            0x01020304 /*flags*/,
                                            0xaa00bb11, /*expiry*/
                                            "value",
                                            5,
                                            datatype,
                                            0xf00fcafe11225566ull));
    EXPECT_NE(0, datatype); // make sure we writing non-zero
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    kvstore->set(*ctx, item);
    kvstore->commit(std::move(ctx), flush);

    MockedGetCallback<GetValue> gc;
    EXPECT_CALL(gc, status(cb::engine_errc::success));
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
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    auto* request = kvstore->setAndReturnRequest(*ctx, item);

    // Now directly mess with the metadata of the value which will be written
    MockCouchRequest::MetaData meta;
    meta.cas = 0xf00fcafe11225566ull;
    meta.expiry = 0xaa00bb11;
    meta.flags = 0x01020304;
    meta.ext1 = 2;
    meta.ext2 = 33;
    request->writeMetaData(meta, MockCouchRequest::MetaData::sizeofV1);
    kvstore->commit(std::move(ctx), flush);
    MockedGetCallback<GetValue> gc;
    uint8_t expectedDataType = 33;
    EXPECT_CALL(gc, status(cb::engine_errc::success));
    EXPECT_CALL(gc, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(protocol_binary_datatype_t(expectedDataType)));
    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    gc.callback(gv);
}

TEST_F(CouchstoreTest, testV2WriteRead) {
    // Ensure CAS, exptime and flags are set to something.
    auto datatype = PROTOCOL_BINARY_DATATYPE_JSON; // lies, but non-zero
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
    meta.metaV2V3.v2.conflictResMode = 0x01;

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    auto* request = kvstore->setAndReturnRequest(*ctx, item);

    // Force the meta to be V2 (19 bytes)
    request->writeMetaData(meta, MockCouchRequest::MetaData::sizeofV2);

    // Commit it
    kvstore->commit(std::move(ctx), flush);

    // Read back successful, the extra byte will of been dropped.
    MockedGetCallback<GetValue> gc;
    EXPECT_CALL(gc, status(cb::engine_errc::success));
    EXPECT_CALL(gc, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(protocol_binary_datatype_t(meta.ext2)));
    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    gc.callback(gv);
}

// Verify that if the precommit hook fails (we didn't update the
// _local/vbstate) the compaction would fail (so that during restart
// we wouldn't potentially get a database header without the _local/document
TEST_F(CouchstoreTest, MB40415_regression_test) {
    CompactionConfig config;
    auto vb = TestEPVBucketFactory::makeVBucket(vbid);
    auto ctx = std::make_shared<CompactionContext>(vb, config, 0);

    // Verify that if we would "fail" the precommit hook for some reason
    // the entire compaction would fail...
    kvstore->setMb40415RegressionHook(true);
    std::mutex mutex;
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_EQ(CompactDBStatus::Aborted, kvstore->compactDB(lock, ctx));
}

class CouchKVStoreMetaData : public ::testing::Test {};

TEST_F(CouchKVStoreMetaData, basic) {
    // Lock down the size assumptions.
    EXPECT_EQ(16, MetaData::getMetaDataSize(MetaData::Version::V0));
    EXPECT_EQ(16 + 2, MetaData::getMetaDataSize(MetaData::Version::V1));
    EXPECT_EQ(16 + 2 + 1, MetaData::getMetaDataSize(MetaData::Version::V2));
    EXPECT_EQ(16 + 2 + 7, MetaData::getMetaDataSize(MetaData::Version::V3));
}

TEST_F(CouchKVStoreMetaData, overlay) {
    // V0 (16 bytes) is no longer supported.
    std::vector<char> data(16);
    sized_buf meta;
    meta.buf = data.data();
    meta.size = data.size();
    EXPECT_THROW(MetaDataFactory::createMetaData(meta), std::invalid_argument);

    data.resize(16 + 2);
    meta.buf = data.data();
    meta.size = data.size();
    auto metadata = MetaDataFactory::createMetaData(meta);
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
    delete[] out.buf;
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
    std::vector<char> data(16 + 2);
    sized_buf meta;
    sized_buf out;
    meta.buf = data.data();
    meta.size = data.size();

    // Test that we can initialise from V1 but still set
    // all fields of all versions
    auto metadata = MetaDataFactory::createMetaData(meta);
    EXPECT_EQ(MetaData::Version::V1, metadata->getVersionInitialisedFrom());

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

    metadata->setDurabilityOp(queue_op::abort_sync_write);
    metadata->setCompletedProperties(1234);
    EXPECT_EQ(queue_op::abort_sync_write, metadata->getDurabilityOp());
    EXPECT_EQ(1234, metadata->getPrepareSeqno());

    // Now we move the metadata out, this will give back a V1 structure
    out.size = MetaData::getMetaDataSize(MetaData::Version::V1);
    out.buf = new char[out.size];
    metadata->copyToBuf(out);
    metadata = MetaDataFactory::createMetaData(out);
    EXPECT_EQ(MetaData::Version::V1,
              metadata->getVersionInitialisedFrom()); // Is it V1?

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
    delete[] out.buf;
}

//
// Test that assignment operates as expected (we use this in edit_docinfo_hook)
//
TEST_F(CouchKVStoreMetaData, assignment) {
    std::vector<char> data(16 + 2);
    sized_buf meta;
    meta.buf = data.data();
    meta.size = data.size();
    auto metadata = MetaDataFactory::createMetaData(meta);
    ASSERT_EQ(MetaData::Version::V1, metadata->getVersionInitialisedFrom());
    uint64_t cas = 0xf00f00ull;
    uint32_t exp = 0xcafe1234;
    uint32_t flags = 0xc0115511;
    DeleteSource deleteSource = DeleteSource::TTL;
    metadata->setCas(cas);
    metadata->setExptime(exp);
    metadata->setFlags(flags);
    metadata->setDeleteSource(deleteSource);
    metadata->setDataType(PROTOCOL_BINARY_DATATYPE_JSON);

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

// Test the protected method works as expected for a variety of inputs. Inside
// the class this will be given files that match *.couch.*
TEST_F(CouchstoreTest, getVbucketRevisions) {
    std::vector<std::string> filenames = {""};
    auto map = kvstore->public_getVbucketRevisions(filenames);
    EXPECT_TRUE(map.empty());

    filenames = {"junk"};
    map = kvstore->public_getVbucketRevisions(filenames);
    EXPECT_TRUE(map.empty());

    filenames = {"x.couch.y"};
    map = kvstore->public_getVbucketRevisions(filenames);
    EXPECT_TRUE(map.empty());

    filenames = {"/dir/4.couch.16", "/dir/4.couch.16"};
    map = kvstore->public_getVbucketRevisions(filenames);
    EXPECT_EQ(1, map.size());
    EXPECT_THAT(map, UnorderedElementsAre(Key(Vbid(4))));
    EXPECT_EQ(1, map[Vbid(4)].size());
    EXPECT_THAT(map[Vbid(4)], UnorderedElementsAre(16));

    filenames = {
            "/dir/4.couch.0",
            "/dir/4.couch.2",
            "/dir/4.couch.3",
            "/dir/4..couch..4", // will be ignored
            "/dir/.4.couch.4.", // will be ignored
            "/dir/4.couch.4.", // will be ignored
            "/dir/.4couch.4", // will be ignored
            "/dir/100.couch.100",
            "/dir/100.couch.101",
            "/dir/100.couch.102",
            "/dir/1.couch.1" // will be ignored - different shard
    };
    map = kvstore->public_getVbucketRevisions(filenames);
    EXPECT_EQ(2, map.size());
    EXPECT_THAT(map, UnorderedElementsAre(Key(Vbid(4)), Key(Vbid(100))));

    EXPECT_EQ(3, map[Vbid(4)].size());
    EXPECT_EQ(3, map[Vbid(100)].size());

    EXPECT_THAT(map[Vbid(4)], UnorderedElementsAre(0, 2, 3));
    EXPECT_THAT(map[Vbid(100)], UnorderedElementsAre(100, 101, 102));

    // acceptable limits
    filenames = {"/dir/65532.couch.18446744073709551615"};
    map = kvstore->public_getVbucketRevisions(filenames);
    EXPECT_EQ(1, map.size());
    EXPECT_EQ(1, map.count(Vbid(65532)));
    EXPECT_EQ(1, map[Vbid(65532)].size());
    EXPECT_EQ(1, map[Vbid(65532)].count(std::numeric_limits<uint64_t>::max()));

    // unacceptable limits, Vbid allows for 2^16 values and we use stoul for
    // conversion of the id, these inputs exceed two different checks
    filenames = {"/dir/65536.couch.0", "/dir/8589934591.couch.0"};
    // this throw comes from our own check that the id is in range
    map = kvstore->public_getVbucketRevisions(filenames);
    EXPECT_TRUE(map.empty());
}

// Add stale files to data directory and create a RW store which will clean
// up.
TEST_F(CouchstoreTest, CouchKVStore_construct_and_cleanup) {
    struct CouchstoreFile {
        uint16_t id;
        uint64_t revision;
    };
    // We'll create these as minimal couchstore files (they all need vbstate)
    std::vector<CouchstoreFile> filenames = {
            {4, 0}, // 4.couch.0
            {4, 2}, // 4.couch.2
            {4, 3}, // 4.couch.3
            {100, 100}, // 100.couch.100
            {100, 101}, // 100.couch.101
            {100, 102} // 100.couch.102
    };

    // And we create some other files which should be ignored or removed, these
    // can be empty files.
    // Note: 6.couch.3.compact will not removed as we have no 6.couch.*
    //       5.couch.1.compact will not removed as kvstore only looks for rev:3
    std::vector<std::string> otherFilenames = {
            data_dir + cb::io::DirectorySeparator + "4.couch.3.compact",
            data_dir + cb::io::DirectorySeparator + "8.couch.3.compact",
            data_dir + cb::io::DirectorySeparator + "4.couch.1.compact",
            data_dir + cb::io::DirectorySeparator + "junk",
            data_dir + cb::io::DirectorySeparator + "master.couch.0",
            data_dir + cb::io::DirectorySeparator + "stats.json",
            data_dir + cb::io::DirectorySeparator + "stats.json.old"};

    // Finally two sets of files for EXPECT after creating CouchKVStore
    std::vector<std::string> expectedFilenames = {
            data_dir + cb::io::DirectorySeparator + "4.couch.3",
            data_dir + cb::io::DirectorySeparator + "8.couch.3.compact",
            data_dir + cb::io::DirectorySeparator + "4.couch.1.compact",
            data_dir + cb::io::DirectorySeparator + "100.couch.102",
            data_dir + cb::io::DirectorySeparator + "junk",
            data_dir + cb::io::DirectorySeparator + "master.couch.0",
            data_dir + cb::io::DirectorySeparator + "stats.json",
            data_dir + cb::io::DirectorySeparator + "stats.json.old"};

    std::vector<std::string> removedFilenames = {
            data_dir + cb::io::DirectorySeparator + "4.couch.0",
            data_dir + cb::io::DirectorySeparator + "4.couch.2",
            data_dir + cb::io::DirectorySeparator + "100.couch.100",
            data_dir + cb::io::DirectorySeparator + "100.couch.101",
            data_dir + cb::io::DirectorySeparator + "4.couch.3.compact"};

    auto createFiles = [&filenames, &otherFilenames, this]() {
        for (const auto& filename : filenames) {
            rewriteCouchstoreVBState(
                    Vbid(filename.id), data_dir, filename.revision);
        }
        for (const auto& filename : otherFilenames) {
            std::ofstream output(filename);
            output.close();
            EXPECT_TRUE(cb::io::isFile(filename));
        }
    };
    createFiles();

    // new instance, construction will clean up stale files
    kvstore = std::make_unique<MockCouchKVStore>(config);

    // 1) Check db revisions are the most recent
    EXPECT_EQ(3, kvstore->public_getDbRevision(Vbid(4)));
    EXPECT_EQ(102, kvstore->public_getDbRevision(Vbid(100)));

    // 2) Check clean-up removed some files and left the others
    for (const auto& filename : expectedFilenames) {
        EXPECT_TRUE(cb::io::isFile(filename));
    }
    for (const auto& filename : removedFilenames) {
        EXPECT_FALSE(cb::io::isFile(filename))
                << "File should not exist filename:" << filename;
    }
}

TEST_F(CouchstoreTest, ConcurrentCompactionAndFlushing) {
    int64_t seqno = 1;
    for (int ii = 0; ii < 5; ++ii) {
        StoredDocKey key = makeStoredDocKey("key-" + std::to_string(ii));
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->set(
                *ctx,
                queued_item{std::make_unique<Item>(key,
                                                   0,
                                                   0,
                                                   "value",
                                                   5,
                                                   PROTOCOL_BINARY_RAW_BYTES,
                                                   uint64_t(ii),
                                                   seqno++)});
        kvstore->commit(std::move(ctx), flush);
    }
    ASSERT_EQ(5, kvstore->getItemCount(Vbid{0}));

    int ii = 0;
    kvstore->setConcurrentCompactionPostLockHook([&ii, &seqno, this](
                                                         const std::string&) {
        StoredDocKey key = makeStoredDocKey("concurrent-" + std::to_string(ii));
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->set(
                *ctx,
                queued_item{std::make_unique<Item>(key,
                                                   0,
                                                   0,
                                                   "concurrent",
                                                   10,
                                                   PROTOCOL_BINARY_RAW_BYTES,
                                                   uint64_t(ii + 5),
                                                   seqno++)});
        kvstore->commit(std::move(ctx), flush);
        ++ii;
    });

    std::mutex mutex;
    std::unique_lock<std::mutex> lock(mutex);
    CompactionConfig config;
    auto vb = TestEPVBucketFactory::makeVBucket(vbid);
    auto ctx = std::make_shared<CompactionContext>(vb, config, 0);
    kvstore->compactDB(lock, ctx);
    ASSERT_GT(ii, 1) << "There should at least be two callbacks";
    ASSERT_LT(ii, 12) << "There should be up to 10 catch up without holding "
                         "the lock, and one with the lock";
    EXPECT_EQ(5 + ii, kvstore->getItemCount(Vbid{0}));
}

// This test writes during compaction in a way that means we frequently hit
// the couchstore 4096 block size. Couchstore issues a 1 byte write in that
// case to insert a leading byte in the page. If that happens during the
// compaction 'catch-up' copying, KV-engine doesn't know that the physical_size
// of a document now differs from what is recorded in the VB::Manifest
TEST_F(CouchstoreTest, MB_39946_diskSize_could_underflow) {
    int64_t seqno = 1;
    const int items = 2;
    std::string value(2047, 'b');
    auto doWrite = [&seqno, &value, items, this](const std::string&) {
        for (int ii = 0; ii < items; ++ii) {
            StoredDocKey key = makeStoredDocKey("key-" + std::to_string(ii));
            auto ctx = kvstore->begin(vbid,
                                      std::make_unique<PersistenceCallback>());
            queued_item qi;
            qi = makeCommittedItem(key, value);

            qi->setBySeqno(seqno++);
            // Lies, lies and damned lies - just don't read it back
            // doing this means the value above is stored as is - critically
            // the length stored is what we define.
            qi->setDataType(PROTOCOL_BINARY_DATATYPE_SNAPPY);
            kvstore->set(*ctx, qi);
            VB::Commit flush(manifest);
            kvstore->commit(std::move(ctx), flush);
        }
    };

    doWrite("");
    auto diskSize1 = manifest.lock()
                             .getStatsForFlush(CollectionID::Default, seqno)
                             .diskSize;
    EXPECT_NE(0, diskSize1);
    kvstore->setConcurrentCompactionPostLockHook(doWrite);

    std::mutex mutex;
    std::unique_lock<std::mutex> lock(mutex);
    CompactionConfig config;
    auto vb = TestEPVBucketFactory::makeVBucket(vbid);
    auto ctx = std::make_shared<CompactionContext>(vb, config, 0);
    kvstore->compactDB(lock, ctx);

    kvstore->setConcurrentCompactionPostLockHook([](const std::string&) {});

    // Delete all keys, should result in collection stats being 0
    for (int ii = 0; ii < items; ++ii) {
        VB::Commit flush(manifest);
        StoredDocKey key = makeStoredDocKey("key-" + std::to_string(ii));
        auto qi = makeCommittedItem(key, {});
        qi->setBySeqno(seqno++);
        qi->setDeleted();
        qi->replaceValue({});
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->del(*ctx, qi);
        kvstore->commit(std::move(ctx), flush); // Would throw for underflow
    }
    auto stats = manifest.lock().getStatsForFlush(CollectionID::Default, seqno);
    EXPECT_EQ(0, stats.itemCount);
    // diskSize doesn't get to zero because we still have the items key/meta
    // stored (tombstones). It should though be > 0 and < diskSize1
    EXPECT_GT(stats.diskSize, 0);
    EXPECT_LT(stats.diskSize, diskSize1);
}

/// MB-43121: Make sure that we abort compaction if someone tries to delete
///           the same vbucket while it is running.
TEST_F(CouchstoreTest, MB43121) {
    StoredDocKey key = makeStoredDocKey("mykey");
    auto txnCtx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    kvstore->set(
            *txnCtx,
            queued_item{std::make_unique<Item>(
                    key, 0, 0, "value", 5, PROTOCOL_BINARY_RAW_BYTES, 0, 1)});
    kvstore->commit(std::move(txnCtx), flush);

    std::mutex mutex;
    std::unique_lock<std::mutex> lock(mutex);
    std::string filename;
    bool aborted = false;
    kvstore->setConcurrentCompactionPostLockHook(
            [&aborted, &lock, &filename, this](const std::string& fname) {
                ASSERT_TRUE(lock.owns_lock())
                        << "Unit test callback should be called "
                           "when the compactor holds the lock";
                ASSERT_TRUE(cb::io::isFile(fname))
                        << "The compaction file " << fname << " should exist!";
                kvstore->abortCompactionIfRunning(lock, vbid);
                aborted = true;
                filename = fname;
            });

    CompactionConfig config;
    auto vb = TestEPVBucketFactory::makeVBucket(vbid);
    auto ctx = std::make_shared<CompactionContext>(vb, config, 0);
    ASSERT_EQ(CompactDBStatus::Aborted, kvstore->compactDB(lock, ctx))
            << "Compaciton should fail";
    ASSERT_TRUE(aborted) << "Callback not called";
    ASSERT_FALSE(filename.empty()) << "A filename should be set";

    // The filenames look like: somepath/<vbid>.couch.<rev>, and when
    // compaction starts it will compact the input file into a new file
    // adding with the same name with a .compact suffix. Once compaction
    // is _complete_ it bumps the revision number and renames the file to
    // the new filename.
    // To verify that the compaction was indeed aborted we should check
    // that we've only got a single vbucket file for vb0, and that it has
    // the same revision number it had before compaction:
    filename.resize(filename.find(".compact"));
    ASSERT_TRUE(cb::io::isFile(filename));
    // Strip off the revison number and verify that the only file we have
    // for vbucket 0 is the expected one (and no temporary files etc)
    filename.resize(filename.rfind('.'));
    auto files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "Multiple files exists";
}

TEST_F(CouchstoreTest, ConcurrentCompactionAndFlushingPrepareToAbort) {
    // 1) Set prepare first
    auto docKey = makeStoredDocKey("key");
    auto item = makePendingItem(docKey, "value");
    flushItem(item);

    // And verify that we count it towards the on disk prepares stat
    auto vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    EXPECT_EQ(1, vbstate.state.onDiskPrepares);
    EXPECT_LT(0, vbstate.state.getOnDiskPrepareBytes());

    bool seenPrepare = false;
    kvstore->setConcurrentCompactionPostLockHook(
            [&seenPrepare, &docKey, this](auto& key) {
                if (seenPrepare) {
                    return;
                }
                seenPrepare = true;

                // 3) Set the prepare to an abort
                flushItem(makeAbortedItem(docKey, "differentValue"));
            });

    // 2) Do the compaction
    runCompaction();

    // And verify that we decrement the on disk prepare count
    vbstate = kvstore->getPersistedVBucketState(vbid);
    vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    EXPECT_EQ(0, vbstate.state.onDiskPrepares);
    EXPECT_EQ(0, vbstate.state.getOnDiskPrepareBytes());

    // Should also check the cached count
    auto cachedVBState = kvstore->getCachedVBucketState(vbid);
    EXPECT_EQ(0, cachedVBState->onDiskPrepares);
    EXPECT_EQ(0, cachedVBState->getOnDiskPrepareBytes());
}

TEST_F(CouchstoreTest, ConcurrentCompactionAndFlushingAbortToPrepare) {
    // Setup - Flush the item as a prepare so that we can check that we add the
    // correct amount when we turn the abort to a prepare (i.e. the new size
    // rather than the original
    auto docKey = makeStoredDocKey("key");
    flushItem(makePendingItem(docKey, "value"));

    auto vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    auto abortSize = vbstate.state.getOnDiskPrepareBytes();
    EXPECT_NE(0, abortSize);

    // 1) Set abort first
    flushItem(makeAbortedItem(docKey, "value"));

    // And verify that we don't count it towards the prepare count
    vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    EXPECT_EQ(0, vbstate.state.onDiskPrepares);
    EXPECT_EQ(0, vbstate.state.getOnDiskPrepareBytes());

    bool seenPrepare = false;
    kvstore->setConcurrentCompactionPostLockHook(
            [&seenPrepare, &docKey, this](auto& key) {
                if (seenPrepare) {
                    return;
                }
                seenPrepare = true;

                // 3) Change the abort to a prepare
                flushItem(makePendingItem(docKey, "differentValue"));
            });

    // 2) Do the compaction
    runCompaction();

    // And verify that we increment the on disk prepare count
    vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    EXPECT_EQ(1, vbstate.state.onDiskPrepares);
    EXPECT_LT(0, vbstate.state.getOnDiskPrepareBytes());
    EXPECT_LT(abortSize, vbstate.state.getOnDiskPrepareBytes());

    // Should also check the cached count
    auto cachedVBState = kvstore->getCachedVBucketState(vbid);
    EXPECT_EQ(1, cachedVBState->onDiskPrepares);
    EXPECT_EQ(vbstate.state.getOnDiskPrepareBytes(),
              cachedVBState->getOnDiskPrepareBytes());
}

TEST_F(CouchstoreTest, ConcurrentCompactionAndFlushingPrepareToPrepare) {
    // 1) Set prepare first
    auto docKey = makeStoredDocKey("key");
    flushItem(makePendingItem(docKey, "value"));

    // And verify that we increment the on disk prepare count
    auto vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    EXPECT_EQ(1, vbstate.state.onDiskPrepares);
    auto prepareSize = vbstate.state.getOnDiskPrepareBytes();
    EXPECT_LT(0, prepareSize);

    {
        Collections::Summary summary;
        manifest.lock().updateSummary(summary);
        EXPECT_EQ(prepareSize, summary[CollectionID::Default].diskSize);
    }

    bool seenPrepare = false;
    kvstore->setConcurrentCompactionPostLockHook(
            [&seenPrepare, &docKey, this](auto& key) {
                if (seenPrepare) {
                    return;
                }
                seenPrepare = true;

                // 3) Update the prepare
                flushItem(makePendingItem(docKey, "differentValue"));
            });

    // 2) Do the compaction
    runCompaction();

    // And verify that we don't change the prepare count
    vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    EXPECT_EQ(1, vbstate.state.onDiskPrepares);
    EXPECT_LT(0, vbstate.state.getOnDiskPrepareBytes());
    // Prepare size should increase
    EXPECT_LT(prepareSize, vbstate.state.getOnDiskPrepareBytes());

    {
        Collections::Summary summary;
        manifest.lock().updateSummary(summary);
        EXPECT_EQ(vbstate.state.getOnDiskPrepareBytes(),
                  summary[CollectionID::Default].diskSize);
    }

    // Should also check the cached count
    auto cachedVBState = kvstore->getCachedVBucketState(vbid);
    EXPECT_EQ(1, cachedVBState->onDiskPrepares);
    EXPECT_LT(0, cachedVBState->getOnDiskPrepareBytes());
}

TEST_F(CouchstoreTest, ConcurrentCompactionAndFlushingPreparePurgeToPrepare) {
    // 1) Set prepare first
    StoredDocKey docKey = makeStoredDocKey("prepare");
    auto prepare = makePendingItem(docKey, "value");
    prepare->setBySeqno(1);
    flushItem(prepare);

    // 2) Persist a dummy item and bump the completed seqno to make the
    //    prepare from step 1 eligible for purging
    auto dummy = makeCommittedItem(makeStoredDocKey("dummy"), "dummy");
    dummy->setBySeqno(2);

    // And set the PCS so that the compactor tries to drop the prepare at 1
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    kvstore->set(*ctx, dummy);
    auto vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    flush.proposedVBState = vbstate.state;
    flush.proposedVBState.persistedCompletedSeqno = 1;
    kvstore->commit(std::move(ctx), flush);

    // Verify our stats before the rest of the test
    vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    EXPECT_EQ(1, vbstate.state.onDiskPrepares);
    auto prepareSize = vbstate.state.getOnDiskPrepareBytes();
    EXPECT_LT(0, prepareSize);

    uint64_t dummySize = 0;
    {
        Collections::Summary summary;
        manifest.lock().updateSummary(summary);
        dummySize = summary[CollectionID::Default].diskSize - prepareSize;
        auto expectedPrepareSize =
                summary[CollectionID::Default].diskSize - dummySize;
        EXPECT_EQ(expectedPrepareSize, prepareSize);
    }

    bool seenPrepare = false;
    kvstore->setConcurrentCompactionPostLockHook(
            [&seenPrepare, &docKey, this](auto& compactionKey) {
                if (seenPrepare) {
                    return;
                }
                seenPrepare = true;

                // 3) Flush a new value to the prepare, we should have a large
                // prepare
                //    size post compaction
                flushItem(makePendingItem(docKey, "differentValue"));

                auto state = kvstore->getPersistedVBucketState(vbid);
                ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success,
                          state.status);
                EXPECT_LT(0, state.state.getOnDiskPrepareBytes());
            });

    // 2) Do the compaction
    runCompaction();

    // And verify that we don't change the prepare count
    vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    EXPECT_EQ(1, vbstate.state.onDiskPrepares);
    EXPECT_LT(0, vbstate.state.getOnDiskPrepareBytes());
    // Prepare size should increase
    EXPECT_LT(prepareSize, vbstate.state.getOnDiskPrepareBytes());

    {
        Collections::Summary summary;
        manifest.lock().updateSummary(summary);
        auto expected = summary[CollectionID::Default].diskSize - dummySize;
        EXPECT_EQ(expected, vbstate.state.getOnDiskPrepareBytes());
    }

    // Should also check the cached count
    auto cachedVBState = kvstore->getCachedVBucketState(vbid);
    EXPECT_EQ(1, cachedVBState->onDiskPrepares);
    EXPECT_LT(0, cachedVBState->getOnDiskPrepareBytes());
}

TEST_F(CouchstoreTest, ConcurrentCompactionAndFlushingPrepareCompleteToAbort) {
    // 1) Set prepare first
    StoredDocKey docKey = makeStoredDocKey("prepare");
    auto prepare = makePendingItem(docKey, "value");
    prepare->setBySeqno(1);
    flushItem(prepare);

    // 2) Persist a dummy item and bump the completed seqno to make the
    //    prepare from step 1 eligible for purging
    auto dummy = makeCommittedItem(makeStoredDocKey("dummy"), "dummy");
    dummy->setBySeqno(2);
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    kvstore->set(*ctx, dummy);

    // And set the PCS so that the compactor tries to drop the prepare at 1
    auto vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    flush.proposedVBState = vbstate.state;
    flush.proposedVBState.persistedCompletedSeqno = 1;
    kvstore->commit(std::move(ctx), flush);

    // Verify our stats before the rest of the test
    vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    EXPECT_EQ(1, vbstate.state.onDiskPrepares);
    auto prepareSize = vbstate.state.getOnDiskPrepareBytes();

    EXPECT_LT(0, prepareSize);

    uint64_t dummySize = 0;
    {
        Collections::Summary summary;
        manifest.lock().updateSummary(summary);
        dummySize = summary[CollectionID::Default].diskSize - prepareSize;
        auto expectedPrepareSize =
                summary[CollectionID::Default].diskSize - dummySize;
        EXPECT_EQ(expectedPrepareSize, prepareSize);
    }

    bool seenPrepare = false;
    kvstore->setConcurrentCompactionPostLockHook(
            [&seenPrepare, &docKey, this](auto& compactionKey) {
                if (seenPrepare) {
                    return;
                }
                seenPrepare = true;

                // 3) Flush a new value to the prepare, we should have a large
                // prepare
                //    size post compaction
                flushItem(makeAbortedItem(docKey, "differentValue"));

                auto state = kvstore->getPersistedVBucketState(vbid);
                ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success,
                          state.status);
                EXPECT_EQ(0, state.state.getOnDiskPrepareBytes());
            });

    // 2) Do the compaction
    auto preCompactionDiskSize =
            manifest.lock(CollectionID::Default).getDiskSize();
    runCompaction();

    // And verify that we don't change the prepare count
    vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    EXPECT_EQ(0, vbstate.state.onDiskPrepares);
    EXPECT_EQ(0, vbstate.state.getOnDiskPrepareBytes());

    // Check collection stats, the abort should be accounted for, disk increased
    EXPECT_GT(manifest.lock(CollectionID::Default).getDiskSize(),
              preCompactionDiskSize);

    // Just for a full sanity check, use this manually calculated size and
    // compare. The final data files stores two keys. 1 committed and 1 aborted.
    // These sizes are taken from couch_dbdump
    // dummy: 15 for value, 6 for key and 18 for meta
    auto expectedSz = 15 + 6 + 18;
    // abort: 24 for value, 9 for key and 25 for meta
    expectedSz += 24 + 9 + 25;
    EXPECT_EQ(expectedSz, manifest.lock(CollectionID::Default).getDiskSize());

    // Should also check the cached count
    auto cachedVBState = kvstore->getCachedVBucketState(vbid);
    EXPECT_EQ(0, cachedVBState->onDiskPrepares);
    EXPECT_EQ(0, cachedVBState->getOnDiskPrepareBytes());
}

TEST_F(CouchstoreTest, ConcurrentCompactionAndFlushingAbortToAbort) {
    // 1) Set abort first
    auto docKey = makeStoredDocKey("key");
    flushItem(makeAbortedItem(docKey, "value"));

    // And verify that we dont' increment the on disk prepare count
    auto vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    EXPECT_EQ(0, vbstate.state.onDiskPrepares);
    EXPECT_EQ(0, vbstate.state.getOnDiskPrepareBytes());

    {
        Collections::Summary summary;
        manifest.lock().updateSummary(summary);
        EXPECT_EQ(0, vbstate.state.getOnDiskPrepareBytes());
        EXPECT_NE(0, summary[CollectionID::Default].diskSize);
    }

    bool seenPrepare = false;
    kvstore->setConcurrentCompactionPostLockHook(
            [&seenPrepare, &docKey, this](auto& key) {
                if (seenPrepare) {
                    return;
                }
                seenPrepare = true;

                // 3) Update the abort
                flushItem(makeAbortedItem(docKey, "differentValue"));
            });

    // 2) Do the compaction
    runCompaction();

    // And verify that we don't change the prepare count
    vbstate = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, vbstate.status);
    EXPECT_EQ(0, vbstate.state.onDiskPrepares);
    EXPECT_EQ(0, vbstate.state.getOnDiskPrepareBytes());

    {
        Collections::Summary summary;
        manifest.lock().updateSummary(summary);
        EXPECT_EQ(0, vbstate.state.getOnDiskPrepareBytes());
        EXPECT_NE(0, summary[CollectionID::Default].diskSize);
    }

    // Should also check the cached count
    auto cachedVBState = kvstore->getCachedVBucketState(vbid);
    EXPECT_EQ(0, cachedVBState->onDiskPrepares);
    EXPECT_EQ(0, cachedVBState->getOnDiskPrepareBytes());
}

TEST_F(CouchstoreTest, PersistPrepareStats) {
    persistPrepare("key", "value", 1);

    auto persistedVBState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success,
              persistedVBState.status);
    EXPECT_EQ(1, persistedVBState.state.onDiskPrepares);
    EXPECT_LT(0, persistedVBState.state.getOnDiskPrepareBytes());

    auto dbFileInfo = kvstore->getDbFileInfo(vbid);
    EXPECT_LT(0, dbFileInfo.prepareBytes);

    {
        Collections::Summary summary;
        manifest.lock().updateSummary(summary);
        EXPECT_EQ(persistedVBState.state.getOnDiskPrepareBytes(),
                  summary[CollectionID::Default].diskSize);
    }
}

TEST_F(CouchstoreTest, PersistAbortStats) {
    persistAbort("key", "value", 1);

    auto persistedVBState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success,
              persistedVBState.status);
    EXPECT_EQ(0, persistedVBState.state.onDiskPrepares);
    EXPECT_EQ(0, persistedVBState.state.getOnDiskPrepareBytes());

    auto dbFileInfo = kvstore->getDbFileInfo(vbid);
    EXPECT_EQ(0, dbFileInfo.prepareBytes);

    {
        Collections::Summary summary;
        manifest.lock().updateSummary(summary);
        EXPECT_EQ(0, persistedVBState.state.getOnDiskPrepareBytes());
        // Collection disk usage accounts the abort
        EXPECT_NE(0, summary[CollectionID::Default].diskSize);
    }
}

TEST_F(CouchstoreTest, PersistPreparePrepareStats) {
    persistPrepare("key", "value", 1);
    persistPrepare("key", "longervalue", 2);

    auto persistedVBState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success,
              persistedVBState.status);
    EXPECT_EQ(1, persistedVBState.state.onDiskPrepares);
    EXPECT_LT(0, persistedVBState.state.getOnDiskPrepareBytes());

    auto dbFileInfo = kvstore->getDbFileInfo(vbid);
    EXPECT_LT(0, dbFileInfo.prepareBytes);

    {
        Collections::Summary summary;
        manifest.lock().updateSummary(summary);
        EXPECT_EQ(persistedVBState.state.getOnDiskPrepareBytes(),
                  summary[CollectionID::Default].diskSize);
    }
}

TEST_F(CouchstoreTest, PersistPrepareAbortStats) {
    persistPrepare("key", "value", 1);
    persistAbort("key", "differentvalue", 2);

    auto persistedVBState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success,
              persistedVBState.status);
    EXPECT_EQ(0, persistedVBState.state.onDiskPrepares);
    EXPECT_EQ(0, persistedVBState.state.getOnDiskPrepareBytes());

    auto dbFileInfo = kvstore->getDbFileInfo(vbid);
    EXPECT_EQ(0, dbFileInfo.prepareBytes);

    {
        Collections::Summary summary;
        manifest.lock().updateSummary(summary);
        EXPECT_EQ(0, persistedVBState.state.getOnDiskPrepareBytes());
        EXPECT_NE(0, summary[CollectionID::Default].diskSize);
    }
}

TEST_F(CouchstoreTest, PersistAbortPrepareStats) {
    persistAbort("key", "value", 1);
    persistPrepare("key", "differentvalue", 2);

    auto persistedVBState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success,
              persistedVBState.status);
    EXPECT_EQ(1, persistedVBState.state.onDiskPrepares);
    EXPECT_LT(0, persistedVBState.state.getOnDiskPrepareBytes());

    auto dbFileInfo = kvstore->getDbFileInfo(vbid);
    EXPECT_LT(0, dbFileInfo.prepareBytes);

    {
        Collections::Summary summary;
        manifest.lock().updateSummary(summary);
        EXPECT_EQ(persistedVBState.state.getOnDiskPrepareBytes(),
                  summary[CollectionID::Default].diskSize);
    }
}

TEST_F(CouchstoreTest, PersistAbortAbortStats) {
    persistAbort("key", "value", 1);
    persistAbort("key", "differentvalue", 2);

    auto persistedVBState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success,
              persistedVBState.status);
    EXPECT_EQ(0, persistedVBState.state.onDiskPrepares);
    EXPECT_EQ(0, persistedVBState.state.getOnDiskPrepareBytes());

    auto dbFileInfo = kvstore->getDbFileInfo(vbid);
    EXPECT_EQ(0, dbFileInfo.prepareBytes);

    {
        Collections::Summary summary;
        manifest.lock().updateSummary(summary);
        // prepare bytes is 0
        EXPECT_EQ(0, persistedVBState.state.getOnDiskPrepareBytes());
        // collection size though accounts for aborts
        EXPECT_NE(0, summary[CollectionID::Default].diskSize);
    }
}

TEST_F(CouchstoreTest, EnsureCompactionThrowsIfdroppedKeyCbIsntSepcified) {
    std::mutex mutex;
    std::unique_lock<std::mutex> lock(mutex);
    CompactionConfig config;
    auto vb = TestEPVBucketFactory::makeVBucket(vbid);
    auto ctx = std::make_shared<CompactionContext>(vb, config, 0);
    ctx->droppedKeyCb = nullptr;
    EXPECT_THROW(kvstore->compactDB(lock, ctx), std::invalid_argument);
}

TEST_F(CouchstoreTest, DeleteMountVBucket) {
    {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->commit(std::move(ctx), flush);
    }
    const auto dbPath = std::filesystem::path(data_dir);
    const auto copyPath = dbPath / "0.couch.9";
    std::filesystem::copy(dbPath / "0.couch.2", copyPath);
    const std::vector<std::string> copyPaths{{copyPath.string()}};
    auto rev = kvstore->prepareToDelete(vbid);
    {
        const auto [status, deks] = kvstore->mountVBucket(
                vbid, VBucketSnapshotSource::Local, copyPaths);
        EXPECT_EQ(cb::engine_errc::success, status);
    }
    kvstore->delVBucket(vbid, std::move(rev));
    {
        const auto [status, deks] = kvstore->mountVBucket(
                vbid, VBucketSnapshotSource::Local, copyPaths);
        EXPECT_EQ(cb::engine_errc::key_already_exists, status);
    }
}

TEST(CouchKVStoreStatic, collectionStatsNames) {
    EXPECT_EQ(
            "|0x0|",
            CouchKVStore::getCollectionStatsLocalDocId(CollectionID::Default));
    EXPECT_EQ("|0x63|",
              CouchKVStore::getCollectionStatsLocalDocId(CollectionID{99}));
    EXPECT_EQ("|0xffffffff|",
              CouchKVStore::getCollectionStatsLocalDocId(
                      CollectionID{uint32_t(~0)}));

    EXPECT_EQ(CollectionID::Default,
              CouchKVStore::getCollectionIdFromStatsDocId("|0x0|"));
    EXPECT_EQ(99, CouchKVStore::getCollectionIdFromStatsDocId("|0x63|"));
    EXPECT_EQ(~0, CouchKVStore::getCollectionIdFromStatsDocId("|0xffffffff|"));
    EXPECT_EQ(4294967280,
              CouchKVStore::getCollectionIdFromStatsDocId("|0xfffffff0|"));

    // Error detection is based on
    // length < 5
    // invalid format |0x
    // failure of the string conversion
    EXPECT_THROW(CouchKVStore::getCollectionIdFromStatsDocId(""),
                 std::logic_error);
    EXPECT_THROW(CouchKVStore::getCollectionIdFromStatsDocId("0x0|"),
                 std::logic_error);
    EXPECT_THROW(CouchKVStore::getCollectionIdFromStatsDocId("|what|"),
                 std::logic_error);
    EXPECT_THROW(CouchKVStore::getCollectionIdFromStatsDocId("|0xwhat|"),
                 std::logic_error);
}
