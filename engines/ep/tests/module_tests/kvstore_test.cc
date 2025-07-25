/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "kvstore_test.h"

#include "bucket_logger.h"
#include "collections/collection_persisted_stats.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "item.h"
#include "kvstore/kvstore.h"
#include "kvstore/kvstore_config.h"
#include "programs/engine_testapp/mock_server.h"
#include "test_helpers.h"
#include "thread_gate.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"
#include "vbucket_test.h"
#include <executor/workload.h>
#include <folly/portability/GTest.h>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <filesystem>
#include <fstream>
#include <thread>
#include <utility>
#include <vector>

using namespace std::string_literals;
using namespace testing;

std::string_view format_as(KVStoreTestEncryption encryption) {
    return (encryption == KVStoreTestEncryption::Unencrypted) ? "Unencrypted"
                                                              : "Encrypted";
}

std::string to_string(KVStoreTestEncryption encryption) {
    return std::string(format_as(encryption));
}

std::string_view format_as(KVStoreTestBuffering buffering) {
    return (buffering == KVStoreTestBuffering::Unbuffered) ? "Unbuffered"
                                                           : "Buffered";
}

std::string to_string(KVStoreTestBuffering buffering) {
    return std::string(format_as(buffering));
}

// Value to use when testing Snappy compression.
static const std::string COMPRESSIBLE_VALUE = "xxyyzzxxyyzzxxyyzzxxyyzz";

std::shared_ptr<VBucket> TestEPVBucketFactory::makeVBucket(Vbid vbid) {
    Configuration config;
    static auto globalStats = std::make_unique<EPStats>();
    static auto checkpointConfig = std::make_unique<CheckpointConfig>(config);
    return std::make_shared<EPVBucket>(
            vbid,
            vbucket_state_active,
            *globalStats,
            *checkpointConfig,
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
            config,
            EvictionPolicy::Value,
            std::make_unique<Collections::VB::Manifest>(
                    std::make_shared<Collections::Manager>()));
}

void KVStoreTestCacheCallback::callback(CacheLookup& lookup) {
    EXPECT_EQ(vb, lookup.getVBucketId());
    EXPECT_LE(start, lookup.getBySeqno());
    EXPECT_LE(lookup.getBySeqno(), end);
}

void GetCallback::callback(GetValue& result) {
    EXPECT_EQ(expectedErrorCode, result.getStatus());
    if (result.getStatus() == cb::engine_errc::success) {
        if (expectCompressed) {
            EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_SNAPPY,
                      result.item->getDataType());
            result.item->decompressValue();
        }

        EXPECT_EQ(COMPRESSIBLE_VALUE,
                  result.item->getValue()->to_string_view());
    }
}

struct WriteCallback {
    void operator()(TransactionContext, FlushStateMutation) {
    }
};

struct DeleteCallback {
    void operator()(TransactionContext&, FlushStateDeletion) {
    }
};

void checkGetValue(GetValue& result,
                   cb::engine_errc expectedErrorCode,
                   bool expectCompressed) {
    EXPECT_EQ(expectedErrorCode, result.getStatus());
    if (result.getStatus() == cb::engine_errc::success) {
        if (expectCompressed) {
            EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_SNAPPY,
                      result.item->getDataType());
            EXPECT_TRUE(result.item->decompressValue());
        }

        EXPECT_EQ("value", result.item->getValue()->to_string_view());
    }
}

void initialize_kv_store(KVStoreIface* kvstore, Vbid vbid) {
    // simulate the setVbState by incrementing the rev
    kvstore->prepareToCreate(vbid);
    Collections::VB::Manifest m{std::make_shared<Collections::Manager>()};
    VB::Commit meta(m);
    meta.proposedVBState.transition.state = vbucket_state_active;
    // simulate the setVbState by incrementing the rev
    kvstore->prepareToCreate(vbid);
    kvstore->snapshotVBucket(vbid, meta);
}

std::unique_ptr<KVStoreIface> setup_kv_store(KVStoreConfig& config,
                                             std::vector<Vbid> vbids) {
    auto kvstore = KVStoreFactory::create(config, {}, {});

    for (auto vbid : vbids) {
        initialize_kv_store(kvstore.get(), vbid);
    }
    return kvstore;
}

KVStoreTest::KVStoreTest()
    : data_dir(getProcessUniqueDatabaseName()), flush(manifest) {
}

void KVStoreTest::SetUp() {
    std::filesystem::remove_all(data_dir);

    // Data directory creation is normally done by the engine initialization;
    // we're not running a full engine here so we have to create the directory
    // manually.
    try {
        std::filesystem::create_directories(data_dir);
    } catch (const std::system_error& error) {
        throw std::runtime_error(
                fmt::format("Failed to create data directory [{}]:{}",
                            data_dir,
                            error.code().message()));
    }
}

void KVStoreTest::TearDown() {
    std::filesystem::remove_all(data_dir);
}

TEST_P(KVStoreParamTest, CompressedTest) {
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());

    for (int i = 1; i <= 5; i++) {
        std::string key("key" + std::to_string(i));
        auto qi = makeCommittedItem(makeStoredDocKey(key), COMPRESSIBLE_VALUE);
        qi->setBySeqno(i);
        kvstore->set(*ctx, qi);
    }
    // Ensure a valid vbstate is committed
    flush.proposedVBState.lastSnapEnd = 5;
    kvstore->commit(std::move(ctx), flush);

    auto scanCtx = kvstore->initBySeqnoScanContext(
            std::make_unique<GetCallback>(true /*expectcompressed*/),
            std::make_unique<KVStoreTestCacheCallback>(1, 5, Vbid(0)),
            Vbid(0),
            1,
            DocumentFilter::ALL_ITEMS,
            ValueFilter::VALUES_COMPRESSED,
            SnapshotSource::Head);

    ASSERT_TRUE(scanCtx);
    EXPECT_EQ(ScanStatus::Success, kvstore->scan(*scanCtx));
}

MATCHER(IsDatatypeSnappy,
        negation ? "datatype isn't Snappy" : "datatype is Snappy") {
    return cb::mcbp::datatype::is_snappy(arg.getDataType());
}

/// For the item Item object, check that if the value is compressed, it can be
/// decompressed successfully
MATCHER(IsValueValid,
        "has a valid value (if Snappy can be decompressed successfully)") {
    if (cb::mcbp::datatype::is_snappy(arg.getDataType())) {
        // Take a copy and attempt to decompress it to check compression.
        auto itemCopy = arg;
        return itemCopy.decompressValue();
    }
    return true;
}

// Check that when deleted docs with no value are fetched from disk, they
// do not have snappy bit set (zero length should not be compressed).
TEST_P(KVStoreParamTest, ZeroSizeValueNotCompressed) {
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());

    auto qi = makeDeletedItem(makeStoredDocKey("key"));
    qi->setBySeqno(1);
    kvstore->del(*ctx, qi);

    // Ensure a valid vbstate is committed
    flush.proposedVBState.lastSnapEnd = 1;
    kvstore->commit(std::move(ctx), flush);

    auto mockGetCb = std::make_unique<MockGetValueCallback>();
    EXPECT_CALL(
            *mockGetCb,
            callback(AllOf(
                    Property(&GetValue::getStatus, cb::engine_errc::success),
                    Field(&GetValue::item, Pointee(Not(IsDatatypeSnappy()))),
                    Field(&GetValue::item, Pointee(IsValueValid())))));

    auto scanCtx = kvstore->initBySeqnoScanContext(
            std::move(mockGetCb),
            std::make_unique<KVStoreTestCacheCallback>(1, 1, Vbid(0)),
            Vbid(0),
            1,
            DocumentFilter::ALL_ITEMS,
            ValueFilter::VALUES_COMPRESSED,
            SnapshotSource::Head);

    ASSERT_TRUE(scanCtx);
    EXPECT_EQ(ScanStatus::Success, kvstore->scan(*scanCtx));
}

class PersistenceCallbacks {
public:
    virtual ~PersistenceCallbacks() = default;

    // Actual operator() methods which will be called by the storage layer.
    // GMock cannot mock these directly, so instead provide named 'callback'
    // methods which these functions call.
    void operator()(TransactionContext& txCtx, FlushStateMutation state) {
        callback(txCtx, state);
    }
    void operator()(TransactionContext& txCtx, FlushStateDeletion state) {
        callback(txCtx, state);
    }

    // SET callback.
    virtual void callback(TransactionContext&, FlushStateMutation&) = 0;

    // DEL callback.
    // @param value number of items that the underlying storage has deleted
    virtual void callback(TransactionContext& txCtx, FlushStateDeletion&) = 0;
};

void KVStoreBackend::setup(const std::string& dataDir,
                           const std::string& backend) {
    // `GetParam` returns the string parameter representing the KVStore
    // implementation.
    auto configStr = "dbname="s + dataDir + ";";
    configStr += generateBackendConfig("persistent_" + backend) + ";";

    if (backend == "magma") {
        configStr += magmaConfig + ';';
    }

    // Run with a fixed (and lower than default) number of shards and vBuckets -
    // we rarely need all 1024 vBuckets or shards, and running with smaller
    // numbers makes tests faster / consume less memory etc.
    configStr += "max_vbuckets=16;max_num_shards=2";

    config.parseConfiguration(configStr);
    WorkLoadPolicy workload(gsl::narrow_cast<int>(config.getMaxNumWorkers()),
                            gsl::narrow_cast<int>(config.getMaxNumShards()));

    kvstoreConfig = KVStoreConfig::createKVStoreConfig(
            config,
            config.getBackendString(),
            gsl::narrow_cast<uint16_t>(workload.getNumShards()),
            0 /*shardId*/);
    kvstore = setup_kv_store(*kvstoreConfig);
}

void KVStoreBackend::teardown() {
    kvstore.reset();
}

void KVStoreParamTest::SetUp() {
    KVStoreTest::SetUp();
    KVStoreBackend::setup(data_dir, GetParam());
    testCallback = kvstore->getDefaultCreateItemCallback();
}

void KVStoreParamTest::TearDown() {
    KVStoreBackend::teardown();
    KVStoreTest::TearDown();
}

bool KVStoreParamTest::supportsFetchingAsSnappy() const {
    return isCouchstore() || isMagma();
}

bool KVStoreParamTest::isSnappyCompressedAtPersistence() const {
    return isCouchstore() ||
           (isMagma() && config.isMagmaPerDocumentCompressionEnabled());
}

// Test basic set / get of a document
TEST_P(KVStoreParamTest, BasicTest) {
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->set(*ctx, qi);

    EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));

    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    checkGetValue(gv, cb::engine_errc::success);
}

// Test different modes of get()
TEST_P(KVStoreParamTest, GetModes) {
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->set(*ctx, qi);

    EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));

    auto gv = kvstore->get(
            DiskDocKey{key}, Vbid(0), ValueFilter::VALUES_COMPRESSED);
    // Magma and couchstore compresses documents individually, hence will return
    // compressed documents when requested.
    const auto expectCompressed = supportsFetchingAsSnappy();
    checkGetValue(gv, cb::engine_errc::success, expectCompressed);

    gv = kvstore->get(DiskDocKey{key}, Vbid(0), ValueFilter::KEYS_ONLY);
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
    EXPECT_EQ(key.to_string(), gv.item->getKey().to_string());
    EXPECT_EQ(1, gv.item->getBySeqno());
    EXPECT_FALSE(gv.item->getValue());
}

// A doc not found should not equal a get failure for a get call (used for
// failover by ns_server).
TEST_P(KVStoreParamTest, GetMissNumGetFailure) {
    GetValue gv = kvstore->get(DiskDocKey{makeStoredDocKey("key")}, Vbid(0));
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

    auto stats = kvstore->getKVStoreStat();
    EXPECT_EQ(0, stats.numGetFailure);
    EXPECT_EQ(0, kvstore->getKVStoreStat().io_bg_fetch_docs_read);
    EXPECT_EQ(0, kvstore->getKVStoreStat().io_bgfetch_doc_bytes);
}

// A doc not found doesn't result in a get failure for a getMulti (bgfetch)
TEST_P(KVStoreParamTest, GetMultiMissNumGetFailure) {
    vb_bgfetch_queue_t q;
    vb_bgfetch_item_ctx_t ctx;
    auto diskDocKey = makeDiskDocKey("key");
    q[diskDocKey] = std::move(ctx);

    kvstore->getMulti(vbid, q, testCallback);

    for (auto& fetched : q) {
        EXPECT_EQ(cb::engine_errc::no_such_key,
                  fetched.second.value.getStatus());
    }

    auto stats = kvstore->getKVStoreStat();
    EXPECT_EQ(0, stats.numGetFailure);
    EXPECT_EQ(0, kvstore->getKVStoreStat().io_bg_fetch_docs_read);
    EXPECT_EQ(0, kvstore->getKVStoreStat().io_bgfetch_doc_bytes);
}

TEST_P(KVStoreParamTest, GetRangeMissNumGetFailure) {
    std::vector<GetValue> results;
    kvstore->getRange(
            Vbid{0},
            makeDiskDocKey("a"),
            makeDiskDocKey("b"),
            ValueFilter::KEYS_ONLY,
            [&results](GetValue&& cb) { results.push_back(std::move(cb)); });

    for (auto& fetched : results) {
        EXPECT_EQ(cb::engine_errc::no_such_key, fetched.getStatus());
    }

    // It wouldn't make sense to report get failures if we don't return anything
    // as who knows what should exist in a range.
    auto stats = kvstore->getKVStoreStat();
    EXPECT_EQ(0, stats.numGetFailure);
}

TEST_P(KVStoreParamTest, SaveDocsHisto) {
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->set(*ctx, qi);

    StoredDocKey key1 = makeStoredDocKey("key1");
    auto qi1 = makeCommittedItem(key1, "value");
    qi1->setBySeqno(2);
    kvstore->set(*ctx, qi1);

    EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));

    auto& stats = kvstore->getKVStoreStat();

    EXPECT_EQ(1, stats.saveDocsHisto.getValueCount());
    EXPECT_EQ(1, stats.commitHisto.getValueCount());
}

TEST_P(KVStoreParamTest, BatchSizeHisto) {
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->set(*ctx, qi);

    StoredDocKey key1 = makeStoredDocKey("key1");
    auto qi1 = makeCommittedItem(key1, "value");
    qi1->setBySeqno(2);
    kvstore->set(*ctx, qi1);

    EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));

    auto& stats = kvstore->getKVStoreStat();

    EXPECT_EQ(1, stats.batchSize.getValueCount());
    EXPECT_EQ(2, stats.batchSize.getMaxValue());
}

TEST_P(KVStoreParamTest, DocsCommittedStat) {
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->set(*ctx, qi);

    EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));

    auto& stats = kvstore->getKVStoreStat();
    EXPECT_EQ(1, stats.docsCommitted);
}

void KVStoreParamTest::testBgFetchDocsReadGet(bool deleted) {
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);

    if (deleted) {
        qi->setDeleted();
    }

    kvstore->set(*ctx, qi);

    EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));

    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    checkGetValue(gv);
    EXPECT_EQ(1, kvstore->getKVStoreStat().io_bg_fetch_docs_read);
    EXPECT_NE(0, kvstore->getKVStoreStat().io_bgfetch_doc_bytes);
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGet) {
    CB_SCOPED_TRACE("");
    testBgFetchDocsReadGet(false /*deleted*/);
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGetDeleted) {
    CB_SCOPED_TRACE("");
    testBgFetchDocsReadGet(true /*deleted*/);
}

void KVStoreParamTest::testBgFetchDocsReadGetMulti(bool deleted,
                                                   ValueFilter filter) {
    auto testDoc = storeDocument(deleted);

    vb_bgfetch_queue_t q;
    vb_bgfetch_item_ctx_t ctx;
    ctx.addBgFetch(std::make_unique<FrontEndBGFetchItem>(nullptr, filter, 0));
    auto diskDocKey = makeDiskDocKey("key");
    q[diskDocKey] = std::move(ctx);
    kvstore->getMulti(vbid, q, testCallback);

    for (auto& fetched : q) {
        checkBGFetchResult(filter, *testDoc, fetched.second);
    }

    EXPECT_EQ(1, kvstore->getKVStoreStat().io_bg_fetch_docs_read);
    EXPECT_NE(0, kvstore->getKVStoreStat().io_bgfetch_doc_bytes);
}

void KVStoreParamTest::checkBGFetchResult(
        const ValueFilter& filter,
        const Item& testDoc,
        const vb_bgfetch_item_ctx_t& fetched) const {
    EXPECT_EQ(cb::engine_errc::success, fetched.value.getStatus());
    const auto& fetchedItem = fetched.value.item;
    const auto& fetchedBlob = fetchedItem->getValue();
    switch (filter) {
    case ValueFilter::KEYS_ONLY:
        EXPECT_FALSE(fetchedBlob);
        break;
    case ValueFilter::VALUES_COMPRESSED:
        if (supportsFetchingAsSnappy()) {
            EXPECT_TRUE(
                    cb::mcbp::datatype::is_snappy(fetchedItem->getDataType()));
            EXPECT_GT(testDoc.getValue()->valueSize(),
                      fetchedBlob->valueSize());
            break;
        }
        [[fallthrough]];
    case ValueFilter::VALUES_DECOMPRESSED:
        EXPECT_FALSE(cb::mcbp::datatype::is_snappy(fetchedItem->getDataType()));
        EXPECT_EQ(*testDoc.getValue(), *fetchedBlob);
        break;
    }
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGetMulti) {
    testBgFetchDocsReadGetMulti(false /*deleted*/,
                                ValueFilter::VALUES_DECOMPRESSED);
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGetMultiDeleted) {
    testBgFetchDocsReadGetMulti(true /*deleted*/,
                                ValueFilter::VALUES_DECOMPRESSED);
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGetMultiCompressed) {
    testBgFetchDocsReadGetMulti(false /*deleted*/,
                                ValueFilter::VALUES_COMPRESSED);
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGetMultiDeletedCompressed) {
    testBgFetchDocsReadGetMulti(true /*deleted*/,
                                ValueFilter::VALUES_COMPRESSED);
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGetMultiMetaOnly) {
    testBgFetchDocsReadGetMulti(false /*deleted*/, ValueFilter::KEYS_ONLY);
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGetMultiDeletedMetaOnly) {
    testBgFetchDocsReadGetMulti(true /*deleted*/, ValueFilter::KEYS_ONLY);
}

void KVStoreParamTest::testBgFetchValueFilter(ValueFilter requestMode1,
                                              ValueFilter requestMode2,
                                              ValueFilter fetchedMode) {
    // Setup - store a document.
    auto testDoc = storeDocument();

    // Setup bgfetch context for the key, based on the two ValueFilters
    vb_bgfetch_queue_t q;
    vb_bgfetch_item_ctx_t ctx;
    ctx.addBgFetch(
            std::make_unique<FrontEndBGFetchItem>(nullptr, requestMode1, 0));
    ctx.addBgFetch(
            std::make_unique<FrontEndBGFetchItem>(nullptr, requestMode2, 0));
    EXPECT_EQ(fetchedMode, ctx.getValueFilter());

    // Test: Peform bgfetch, check returned value is of correct type.
    auto diskDocKey = makeDiskDocKey("key");
    q[diskDocKey] = std::move(ctx);
    kvstore->getMulti(vbid, q, testCallback);

    for (auto& fetched : q) {
        checkBGFetchResult(fetchedMode, *testDoc, fetched.second);
    }
}

TEST_P(KVStoreParamTest, BgFetchValueFilterKeyOnlyKeyOnly) {
    testBgFetchValueFilter(ValueFilter::KEYS_ONLY,
                           ValueFilter::KEYS_ONLY,
                           ValueFilter::KEYS_ONLY);
}

TEST_P(KVStoreParamTest, BgFetchValueFilterKeyOnlyValuesCompressed) {
    testBgFetchValueFilter(ValueFilter::KEYS_ONLY,
                           ValueFilter::VALUES_COMPRESSED,
                           ValueFilter::VALUES_COMPRESSED);
}

TEST_P(KVStoreParamTest, BgFetchValueFilterKeyOnlyValuesDecompressed) {
    testBgFetchValueFilter(ValueFilter::KEYS_ONLY,
                           ValueFilter::VALUES_DECOMPRESSED,
                           ValueFilter::VALUES_DECOMPRESSED);
}

TEST_P(KVStoreParamTest, BgFetchValueFilterValuesCompressedValuesCompressed) {
    testBgFetchValueFilter(ValueFilter::VALUES_COMPRESSED,
                           ValueFilter::VALUES_COMPRESSED,
                           ValueFilter::VALUES_COMPRESSED);
}

TEST_P(KVStoreParamTest, BgFetchValueFilterValuesCompressedValuesDecompressed) {
    testBgFetchValueFilter(ValueFilter::VALUES_COMPRESSED,
                           ValueFilter::VALUES_DECOMPRESSED,
                           ValueFilter::VALUES_DECOMPRESSED);
}

TEST_P(KVStoreParamTest,
       BgFetchValueFilterValuesDecompressedValuesDecompressed) {
    testBgFetchValueFilter(ValueFilter::VALUES_DECOMPRESSED,
                           ValueFilter::VALUES_DECOMPRESSED,
                           ValueFilter::VALUES_DECOMPRESSED);
}

queued_item KVStoreParamTest::storeDocument(bool deleted) {
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "valuevaluevaluevaluevalue");
    qi->setBySeqno(1);

    if (deleted) {
        qi->setDeleted();
    }

    kvstore->set(*ctx, qi);

    EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));
    return qi;
}

TEST_P(KVStoreParamTest, TestPersistenceCallbacksForSet) {
    // Nexus not supported as we do some funky stuff with the
    // PersistenceCallbacks
    if (isNexus()) {
        GTEST_SKIP();
    }

    auto tc = kvstore->begin(Vbid(0),
                             std::make_unique<MockPersistenceCallback>());
    auto mutationStatus = FlushStateMutation::Insert;
    auto& mockPersistenceCallback =
            dynamic_cast<MockPersistenceCallback&>(*tc->cb);

    // Expect that the SET callback will not be called just after `set`
    EXPECT_CALL(mockPersistenceCallback, setCallback(_, mutationStatus))
            .Times(0);

    auto key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->set(*tc, qi);

    // Expect that the SET callback will be called once after `commit`
    EXPECT_CALL(mockPersistenceCallback, setCallback(_, mutationStatus))
            .Times(1);

    EXPECT_TRUE(kvstore->commit(std::move(tc), flush));
}

TEST_P(KVStoreParamTest, TestPersistenceCallbacksForDel) {
    // Nexus not supported as we do some funky stuff with the
    // PersistenceCallbacks
    if (isNexus()) {
        GTEST_SKIP();
    }

    // Store an item
    auto key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    kvstore->set(*ctx, qi);
    kvstore->commit(std::move(ctx), flush);

    auto tc = kvstore->begin(Vbid(0),
                             std::make_unique<MockPersistenceCallback>());
    auto& mockPersistenceCallback =
            dynamic_cast<MockPersistenceCallback&>(*tc->cb);

    // Expect that the DEL callback will not be called just after `del`
    auto status = FlushStateDeletion::Delete;
    EXPECT_CALL(mockPersistenceCallback, deleteCallback(_, status)).Times(0);

    qi->setDeleted();
    qi->setBySeqno(2);
    kvstore->del(*tc, qi);

    // Expect that the DEL callback will be called once after `commit`
    EXPECT_CALL(mockPersistenceCallback, deleteCallback(_, status)).Times(1);

    EXPECT_TRUE(kvstore->commit(std::move(tc), flush));
}

TEST_P(KVStoreParamTest, TestDataStoredInTheRightVBucket) {
    std::string value = "value";
    std::vector<Vbid> vbids = {Vbid(0), Vbid(1)};
    uint64_t seqno = 1000;

    kvstore.reset();
    kvstore = setup_kv_store(*kvstoreConfig, vbids);

    // Check our loaded vb stat
    EXPECT_EQ(1, kvstore->getKVStoreStat().numLoadedVb);

    // Store an item into each VBucket
    for (auto vbid : vbids) {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        auto key = makeStoredDocKey("key-" + std::to_string(vbid.get()));
        auto qi = makeCommittedItem(key, value);
        qi->setBySeqno(seqno++);
        kvstore->set(*ctx, qi);
        kvstore->commit(std::move(ctx), flush);
    }

    // Check that each item has been stored in the right VBucket
    for (auto vbid : vbids) {
        GetValue gv = kvstore->get(
                makeDiskDocKey("key-" + std::to_string(vbid.get())), vbid);
        checkGetValue(gv);
    }

    // Check that an item is not found in a different VBucket
    GetValue gv = kvstore->get(makeDiskDocKey("key-0"), Vbid(1));
    checkGetValue(gv, cb::engine_errc::no_such_key);
    gv = kvstore->get(makeDiskDocKey("key-1"), Vbid(0));
    checkGetValue(gv, cb::engine_errc::no_such_key);
}

// Test that listPersistedVbuckets() returns the correct purge seqno after
// restart.
// Regression test for MB-51328; where only the first vBucket was
// reported correctly, due to incorrect mapping from vbucket id to cacheSlot.
TEST_P(KVStoreParamTest, ListPersistedVBucketsPurgeSeqnoAfterRestart) {
    ASSERT_GT(kvstore->getConfig().getMaxShards(), 1)
            << "Require at least 2 shards to verify multi-shard "
               "listPersistentVBuckets behaviour.";
    ASSERT_GT(kvstore->getConfig().getMaxVBuckets(), 1)
            << "Require at least 2 vBuckets to verify multi-shard "
               "listPersistentVBuckets behaviour.";

    // Setup - activate all vBuckets owned by this shard; give each one a unique
    // purge seqno and then verify that each one gets its own purge_seqno after
    // restart.
    // To set a unique purge seqno, perform two deletes (cannot purge the high
    // seqno, so need at least two docs to purge anything).
    // Seqnos of first delete is '1,000,000+vbucketId` (cannot have a zero seqno
    // so must apply some offset). This ensures we can check each vBucket has a
    // it's own unique purgeSeqno after compaction.
    const auto seqnoOffset = 1'000'000;
    for (auto v = kvstoreConfig->getShardId();
         v < kvstoreConfig->getMaxVBuckets();
         v += kvstoreConfig->getMaxShards()) {
        Vbid vbucketId{v};
        auto ctx = kvstore->begin(vbucketId,
                                  std::make_unique<PersistenceCallback>());

        for (int doc = 0; doc < 2; doc++) {
            std::string key("key" + std::to_string(doc));
            auto qi = makeDeletedItem(makeStoredDocKey(key));
            qi->setVBucketId(vbucketId);
            qi->setBySeqno(seqnoOffset + v + doc);
            kvstore->del(*ctx, qi);
        }
        flush.proposedVBState.lastSnapEnd = seqnoOffset + v + 2;
        ASSERT_TRUE(kvstore->commit(std::move(ctx), flush));

        // Compact, to advance purgeSeqno.
        auto vb = TestEPVBucketFactory::makeVBucket(vbucketId);
        CompactionConfig compCfg;
        // Purge all deletes in each vBucket.
        compCfg.purge_before_seq = std::numeric_limits<uint64_t>::max();
        compCfg.drop_deletes = true;
        auto cctx = std::make_shared<CompactionContext>(vb, compCfg, 0);
        {
            auto lock = getVbLock();
            ASSERT_EQ(CompactDBStatus::Success, kvstore->compactDB(lock, cctx));
        }

        // Sanity check that purge seqno has correctly advanced.
        auto preWarmupState = *kvstore->getCachedVBucketState(vbucketId);
        ASSERT_EQ(v + seqnoOffset, preWarmupState.purgeSeqno);
    }

    // Recreate kvstore, so cached vbState is reloaded from disk.
    kvstore->deinitialize();
    kvstore = KVStoreFactory::create(*kvstoreConfig, {}, {});

    // Test: Verify that purgeSeqno is the same as previous.
    auto postWarmupState = kvstore->listPersistedVbuckets();
    for (auto v = kvstoreConfig->getShardId();
         v < kvstoreConfig->getMaxVBuckets();
         v += kvstoreConfig->getMaxShards()) {
        Vbid vbucketId{v};
        const auto slot = v / kvstoreConfig->getMaxShards();
        ASSERT_TRUE(postWarmupState[slot]);
        EXPECT_EQ(seqnoOffset + v, postWarmupState[slot]->purgeSeqno)
                << "After reload from disk, purgeSeqno should be identical for "
                << vbucketId;
    }
}

/// Verify that deleting a vBucket while a Scan is open is handled correctly.
TEST_P(KVStoreParamTest, DelVBucketWhileScanning) {
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());

    // Store some documents.
    for (int i = 1; i <= 5; i++) {
        std::string key("key" + std::to_string(i));
        auto qi = makeCommittedItem(makeStoredDocKey(key), COMPRESSIBLE_VALUE);
        qi->setBySeqno(i);
        kvstore->set(*ctx, qi);
    }
    // Ensure a valid vbstate is committed
    flush.proposedVBState.lastSnapEnd = 5;
    ASSERT_TRUE(kvstore->commit(std::move(ctx), flush));

    // Setup the mock GetValue callback. We want to perform the scan in two
    // parts, to allow us to delete the vBucket while the scan is in progress.
    // To do that, setup the callback to yield the second item which will cause
    // the first scan to yield (and return).
    auto mockGetCB = std::make_unique<MockGetValueCallback>();
    {
        ::testing::InSequence s;
        EXPECT_CALL(*mockGetCB,
                    callback(ResultOf(
                            [](GetValue& gv) { return gv.item->getKey(); },
                            Eq(makeStoredDocKey("key1")))))
                .WillOnce([mock = mockGetCB.get()](GetValue&) {
                    mock->setStatus(cb::engine_errc::success);
                });
        EXPECT_CALL(*mockGetCB, callback(_))
                .WillOnce(
                        [mock = mockGetCB.get()](GetValue&) { mock->yield(); });
        EXPECT_CALL(*mockGetCB, callback(_))
                .WillRepeatedly([mock = mockGetCB.get()](GetValue&) {
                    mock->setStatus(cb::engine_errc::success);
                });
    }

    // Initialise a scan
    auto scanCtx = kvstore->initBySeqnoScanContext(
            std::move(mockGetCB),
            std::make_unique<KVStoreTestCacheCallback>(1, 5, Vbid(0)),
            Vbid(0),
            1,
            DocumentFilter::ALL_ITEMS,
            ValueFilter::VALUES_COMPRESSED,
            SnapshotSource::Head);
    ASSERT_TRUE(scanCtx);

    // Begin the scan, which should pause after first item, before the second
    // (so we know the underlying KVStore has definitely started iterating on
    // the disk structures).
    EXPECT_EQ(ScanStatus::Yield, kvstore->scan(*scanCtx));

    // Delete the vBucket
    kvstore->delVBucket(vbid, kvstore->prepareToDelete(vbid));

    // Test - attempt to scan again. Permitted to either complete all
    // items (couchstore) or fail now vBucket has been deleted.
    kvstore->scan(*scanCtx);
}

TEST_P(KVStoreParamTest, InvalidSnapshotDetected) {
    // Store item without setting the snapshot constraints
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    auto qi = makeCommittedItem(makeStoredDocKey("key"), "value");
    qi->setBySeqno(1);
    kvstore->set(*ctx, qi);
    EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));

    EXPECT_THROW(kvstore->getPersistedVBucketState(vbid), std::exception);
}

TEST_P(KVStoreParamTest, InvalidSnapshotDetectedAtScan) {
    // Store item without setting the snapshot constraints
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    auto qi = makeCommittedItem(makeStoredDocKey("key"), "value");
    qi->setBySeqno(1);
    kvstore->set(*ctx, qi);
    EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));

    auto cb = std::make_unique<StrictMock<MockGetValueCallback>>();
    auto cl = std::make_unique<KVStoreTestCacheCallback>(0, 0, Vbid(0));
    auto scanCtx =
            kvstore->initBySeqnoScanContext(std::move(cb),
                                            std::move(cl),
                                            vbid,
                                            1,
                                            DocumentFilter::NO_DELETES,
                                            ValueFilter::VALUES_COMPRESSED,
                                            SnapshotSource::Head);
    EXPECT_EQ(nullptr, scanCtx);
}

// Verify thread-safeness for 'delVBucket' concurrent operations.
// Expect ThreadSanitizer to pick this.
TEST_P(KVStoreParamTest, DelVBucketConcurrentOperationsTest) {
    if (isNexus()) {
        // Test doesn't poke the typical deletion path w.r.t revisions so does
        // not work for Nexus which returns a custom revision type
        GTEST_SKIP();
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
            auto ctx = kvstore->begin(vbid,
                                      std::make_unique<PersistenceCallback>());
            auto qi = makeCommittedItem(makeStoredDocKey("key"), "value");
            flush.proposedVBState.lastSnapStart = seqno;
            flush.proposedVBState.lastSnapEnd = seqno;
            qi->setBySeqno(seqno++);
            kvstore->set(*ctx, qi);
            auto ok = kvstore->commit(std::move(ctx), flush);

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
            kvstore->delVBucket(Vbid(0), std::make_unique<KVStoreRevision>(0));
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

    for (auto& t : workers) {
        t.join();
    }
    EXPECT_LT(minNumDeletes, deletes);
}

// MB-27963 identified that compaction and scan are racing with respect to
// the current view of the fileMap causing scan to fail.
TEST_P(KVStoreParamTest, CompactAndScan) {
    for (int i = 1; i < 10; i++) {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        auto key = makeStoredDocKey(std::string(i, 'k'));
        auto qi = makeCommittedItem(key, "value");
        qi->setBySeqno(i);
        kvstore->set(*ctx, qi);
        // Ensure a valid vbstate is committed
        flush.proposedVBState.lastSnapEnd = i;
        kvstore->commit(std::move(ctx), flush);
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

        config.drop_deletes = false;
        auto vb = TestEPVBucketFactory::makeVBucket(vbid);
        auto cctx = std::make_shared<CompactionContext>(vb, config, 0);
        for (int i = 0; i < 10; i++) {
            auto lock = getVbLock();
            EXPECT_EQ(CompactDBStatus::Success, kvstore->compactDB(lock, cctx));
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
    std::string value = "value";
    Vbid vbid = Vbid(0);

    // Upsert 10 items in a single transaction (we want to test that the VBucket
    // state is updated with the highest seqno found in a commit batch)
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    for (int i = 1; i <= 10; i++) {
        auto qi = makeCommittedItem(makeStoredDocKey("key" + std::to_string(i)),
                                    value);
        qi->setBySeqno(i);
        kvstore->set(*ctx, qi);
    }
    // Ensure a valid vbstate is committed
    flush.proposedVBState.lastSnapEnd = 10;
    kvstore->commit(std::move(ctx), flush);

    EXPECT_EQ(kvstore->getCachedVBucketState(vbid)->highSeqno, 10);
}

void KVStoreParamTest::testGetRange(ValueFilter filter) {
    // Setup: store 5 keys, a, b, c, d, e (with matching values)
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    int64_t seqno = 1;
    for (char k = 'a'; k < 'f'; k++) {
        auto item = makeCommittedItem(makeStoredDocKey({k}),
                                      "value_"s + std::string{k});
        item->setBySeqno(seqno++);
        kvstore->set(*ctx, item);
    }
    kvstore->commit(std::move(ctx), flush);

    // Test: Ask for keys in the range [b,d]. Should return b & c.
    std::vector<GetValue> results;
    kvstore->getRange(
            Vbid{0},
            makeDiskDocKey("b"),
            makeDiskDocKey("d"),
            filter,
            [&results](GetValue&& cb) { results.push_back(std::move(cb)); });
    ASSERT_EQ(2, results.size());

    auto checkItem = [this, filter](Item& item,
                                    DocKeyView expectedKey,
                                    std::string_view expectedValue) {
        const auto expectedDatatype = filter == ValueFilter::VALUES_COMPRESSED
                                              ? PROTOCOL_BINARY_DATATYPE_SNAPPY
                                              : PROTOCOL_BINARY_RAW_BYTES;
        EXPECT_EQ(expectedKey, item.getKey());
        if (filter == ValueFilter::KEYS_ONLY) {
            EXPECT_FALSE(item.getValue());
            // magma may return snappy datatype if that is the datatype
            // of the document on disk, even if KEYS_ONLY is requested.
        } else {
            EXPECT_EQ(expectedDatatype, item.getDataType());
            item.decompressValue();
            EXPECT_EQ(expectedValue, item.getValue()->to_string_view());
        }
    };

    checkItem(*results.at(0).item, makeStoredDocKey("b"), "value_b");
    checkItem(*results.at(1).item, makeStoredDocKey("c"), "value_c");
}

// Test the getRange() function
TEST_P(KVStoreParamTest, GetRangeBasic) {
    testGetRange(ValueFilter::VALUES_DECOMPRESSED);
}

// Test the getRange() function support for returning Snappy-compressed
// documents.
TEST_P(KVStoreParamTest, GetRangeCompressed) {
    if (!supportsFetchingAsSnappy()) {
        GTEST_SKIP();
    }
    testGetRange(ValueFilter::VALUES_COMPRESSED);
}

// Test the getRange() function support for returning only keys.
TEST_P(KVStoreParamTest, GetRangeKeys) {
    testGetRange(ValueFilter::KEYS_ONLY);
}

// Test the getRange() function skips deleted items.
TEST_P(KVStoreParamTest, GetRangeDeleted) {
    // Setup: 1) store 8 keys, a, b, c, d, e, f, g (with matching values)
    //        2) delete 3 of them (b, d, f)
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    int64_t seqno = 1;
    for (char k = 'a'; k < 'h'; k++) {
        auto item = makeCommittedItem(makeStoredDocKey({k}),
                                      "value_"s + std::string{k});
        item->setBySeqno(seqno++);
        kvstore->set(*ctx, item);
    }
    kvstore->commit(std::move(ctx), flush);

    ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    for (char k = 'b'; k < 'g'; k += 2) {
        auto item = makeCommittedItem(makeStoredDocKey({k}),
                                      "value_"s + std::string{k});
        item->setDeleted(DeleteSource::Explicit);
        item->setBySeqno(seqno++);
        kvstore->del(*ctx, item);
    }
    kvstore->commit(std::move(ctx), flush);

    // Test: Ask for keys in the range [b,f]. Should return c and e.
    std::vector<GetValue> results;
    kvstore->getRange(
            Vbid{0},
            makeDiskDocKey("b"),
            makeDiskDocKey("f"),
            ValueFilter::VALUES_DECOMPRESSED,
            [&results](GetValue&& cb) { results.push_back(std::move(cb)); });
    ASSERT_EQ(2, results.size());
    EXPECT_EQ(makeStoredDocKey("c"), results.at(0).item->getKey());
    EXPECT_EQ("value_c"s, results.at(0).item->getValue()->to_string_view());
    EXPECT_EQ(makeStoredDocKey("e"), results.at(1).item->getKey());
    EXPECT_EQ("value_e"s, results.at(1).item->getValue()->to_string_view());
}

TEST_P(KVStoreParamTest, Durability_PersistPrepare) {
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makePendingItem(key, "value");
    qi->setBySeqno(1);

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    kvstore->set(*ctx, qi);
    kvstore->commit(std::move(ctx), flush);

    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

    DiskDocKey prefixedKey(key, true /*prepare*/);
    gv = kvstore->get(prefixedKey, Vbid(0));
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
    EXPECT_TRUE(gv.item->isPending());
    EXPECT_FALSE(gv.item->isDeleted());
}

TEST_P(KVStoreParamTest, Durability_PersistAbort) {
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makePendingItem(key, "value");
    qi->setAbortSyncWrite();
    qi->setPrepareSeqno(999);
    qi->setBySeqno(1);

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    kvstore->del(*ctx, qi);
    kvstore->commit(std::move(ctx), flush);

    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

    // Note: Aborts are in the DurabilityPrepare namespace.
    DiskDocKey prefixedKey(key, true /*pending*/);
    gv = kvstore->get(prefixedKey, Vbid(0));
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
    EXPECT_TRUE(gv.item->isAbort());
    EXPECT_TRUE(gv.item->isDeleted());
    EXPECT_EQ(999, gv.item->getPrepareSeqno());
}

TEST_P(KVStoreParamTest, PrepareForDeduplication) {
    std::vector<queued_item> items;
    std::vector<StoredDocKey> keys;
    keys.resize(3);

    for (int i = 2; i >= 0; i--) {
        std::string key("foo" + std::to_string(i));
        keys[i] = makeStoredDocKey(key);
        items.push_back(makeCommittedItem(keys[i], "value"));
    }

    // sort the items
    kvstore->prepareForDeduplication(items);

    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(0, keys[i].compare(items[i]->getKey()));
    }
}

// Verify basic functionality of getItemCount.
TEST_P(KVStoreParamTest, GetItemCount) {
    if (isNexus()) {
        // Doesn't work for nexus as NexusKVStore::getItemCount() needs to read
        // the vbstate to adjust for prepare namespace items and we don't write
        // one in this test.
        GTEST_SKIP();
    }

    ASSERT_EQ(0, kvstore->getItemCount(vbid));

    // Setup: store 3 keys, a, b, c
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    int64_t seqno = 1;
    for (char k = 'a'; k <= 'c'; k++) {
        auto item = makeCommittedItem(makeStoredDocKey({k}), "value");
        item->setBySeqno(seqno++);
        kvstore->set(*ctx, item);
    }
    kvstore->commit(std::move(ctx), flush);

    EXPECT_EQ(3, kvstore->getItemCount(vbid));
}

// Verify the negative behavour of getItemCount - if the given vbucket doens't
// exist then getItemCount should throw std::system_error.
TEST_P(KVStoreParamTest, GetItemCountInvalidVBucket) {
    EXPECT_THROW(kvstore->getItemCount(Vbid{12}), std::system_error);
}

TEST_P(KVStoreParamTest, DeletedItemsForNoDeletesScanMovesLastReadSeqno) {
    uint64_t seqno = 1;

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    auto key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(seqno++);
    qi->setDeleted(DeleteSource::Explicit);
    kvstore->del(*ctx, qi);

    // Need a valid snap end
    flush.proposedVBState.lastSnapEnd = seqno - 1;

    kvstore->commit(std::move(ctx), flush);

    // Strick mock, we don't expect any callback
    auto cb = std::make_unique<StrictMock<MockGetValueCallback>>();
    auto cl = std::make_unique<KVStoreTestCacheCallback>(0, 0, Vbid(0));
    auto scanCtx =
            kvstore->initBySeqnoScanContext(std::move(cb),
                                            std::move(cl),
                                            vbid,
                                            1,
                                            DocumentFilter::NO_DELETES,
                                            ValueFilter::VALUES_COMPRESSED,
                                            SnapshotSource::Head);

    ASSERT_NE(nullptr, scanCtx);
    kvstore->scan(*scanCtx);

    EXPECT_EQ(1, scanCtx->lastReadSeqno);
}

TEST_P(KVStoreParamTest, IdScanResumesFromNextItemAfterPause) {
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    for (size_t i = 1; i <= 3; ++i) {
        auto qi = makeCommittedItem(makeStoredDocKey("key" + std::to_string(i)),
                                    "value");
        qi->setBySeqno(i);
        kvstore->set(*ctx, qi);
    }
    // Need a valid snap end
    flush.proposedVBState.lastSnapEnd = 3;
    kvstore->commit(std::move(ctx), flush);
    ASSERT_EQ(3, kvstore->getLastPersistedSeqno(vbid));

    // Cache callback - just proceed to disk callback
    class CacheCallback : public StatusCallback<CacheLookup> {
        void callback(CacheLookup& lookup) override {
            setStatus(cb::engine_errc::success);
        }
    };
    // Disk callback - Simulate backfill pause/resume
    class DiskCallback : public StatusCallback<GetValue> {
    public:
        DiskCallback() {
            setStatus(cb::engine_errc::invalid_arguments);
        }

        void callback(GetValue& v) override {
            keys.push_back(v.item->getKey());
            if (numCalls == 1) {
                yield();
            } else {
                // call 0 + extra calls
                lastBackfilledKey = DiskDocKey(v.item->getKey());
                setStatus(cb::engine_errc::success);
            }
            ++numCalls;
            // Note: In Trinity a backfill yield after processing the item
            // in ActiveStream::backfillReceived, so we increase the counter
            // here for both temporary_failure/success
            ++numBackfilled;
        }

        size_t numCalls = 0;
        DiskDocKey lastBackfilledKey{nullptr, 0};
        std::vector<StoredDocKey> keys;
        size_t numBackfilled = 0;
    };

    const auto sKey1 = makeStoredDocKey("key1");
    const auto sKey2 = makeStoredDocKey("key2");
    const auto sKey3 = makeStoredDocKey("key3");

    const auto key1 = DiskDocKey(sKey1);
    const auto key2 = DiskDocKey(sKey2);
    const auto key3 = DiskDocKey(sKey3);
    auto cb = std::make_unique<DiskCallback>();
    auto* callback = cb.get();
    auto scanCtx =
            kvstore->initByIdScanContext(std::move(cb),
                                         std::make_unique<CacheCallback>(),
                                         vbid,
                                         {{key1, key3}}, // range
                                         DocumentFilter::ALL_ITEMS,
                                         ValueFilter::VALUES_COMPRESSED);
    ASSERT_TRUE(scanCtx);
    ASSERT_EQ(cb::engine_errc::invalid_arguments, callback->getStatus());
    ASSERT_EQ(0, callback->lastBackfilledKey.size());
    ASSERT_EQ(0, callback->numBackfilled);
    EXPECT_EQ(0, scanCtx->keysScanned);
    auto position1 = scanCtx->getPosition();
    // key1 backfilled -> key2 backfilled -> no mem -> paused -> resume point is
    // key3
    kvstore->scan(*scanCtx);
    EXPECT_TRUE(callback->shouldYield());
    EXPECT_EQ(key1, callback->lastBackfilledKey);
    EXPECT_EQ(2, callback->numBackfilled);
    EXPECT_EQ(2, scanCtx->keysScanned);
    auto position2 = scanCtx->getPosition();
    EXPECT_NE(position1, position2);
    EXPECT_EQ(2, callback->keys.size());
    // Before the fix for MB-57106 the resume point is wrongly set to key1, so
    // backfill sends key1 again at resume
    auto expectedResumePoint = key2;
    expectedResumePoint.append(0);
    EXPECT_EQ(expectedResumePoint, scanCtx->resumeFromKey);
    // Resumed to completion
    kvstore->scan(*scanCtx);
    EXPECT_EQ(cb::engine_errc::success, callback->getStatus());
    EXPECT_EQ(key3, callback->lastBackfilledKey);
    EXPECT_EQ(3, callback->numBackfilled);
    EXPECT_EQ(3, scanCtx->keysScanned);
    EXPECT_NE(position2, scanCtx->getPosition());
    ASSERT_EQ(3, callback->keys.size());
    EXPECT_EQ(sKey1, callback->keys.at(0));
    EXPECT_EQ(sKey2, callback->keys.at(1));
    EXPECT_EQ(sKey3, callback->keys.at(2));
}

TEST_P(KVStoreParamTest, GetAllKeysSanity) {
    using namespace std::string_view_literals;
    using namespace testing;

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    int keys = 20;
    for (int i = 1; i <= keys; i++) {
        std::string key("key" + std::to_string(i));
        auto qi = makeCommittedItem(makeStoredDocKey(key), "value");
        qi->setBySeqno(i);
        kvstore->set(*ctx, qi);
    }

    kvstore->commit(std::move(ctx), flush);
    auto cb(std::make_shared<CustomCallback<const DiskDocKey&>>());
    DiskDocKey start(nullptr, 0);
    kvstore->getAllKeys(Vbid(0), start, 20, cb);
    EXPECT_EQ(keys, int(cb->getProcessedCount()));

    NiceMock<MockFunction<void(
            std::string_view, std::string_view, CookieIface&)>>
            addStatCb;

    auto cbFunc = addStatCb.AsStdFunction();

    EXPECT_CALL(addStatCb, Call(_, _, _)).Times(AnyNumber());
    EXPECT_CALL(addStatCb, Call(HasSubstr("rw_0:getAllKeys"sv), _, _))
            .Times(AtLeast(1));

    auto* cookie = create_mock_cookie();
    kvstore->addTimingStats(cbFunc, *cookie);
    destroy_mock_cookie(cookie);
}

TEST_P(KVStoreParamTest, GetCollectionStatsNoStats) {
    auto kvHandle = kvstore->makeFileHandle(vbid);
    EXPECT_TRUE(kvHandle);
    auto [status, stats] = kvstore->getCollectionStats(vbid, CollectionID(99));
    EXPECT_EQ(KVStore::GetCollectionStatsStatus::NotFound, status);
    EXPECT_EQ(0, stats.itemCount);
    EXPECT_EQ(0, stats.highSeqno);
    EXPECT_EQ(0, stats.diskSize);
}

TEST_P(KVStoreParamTest, GetCollectionManifest) {
    auto kvHandle = kvstore->makeFileHandle(vbid);
    EXPECT_TRUE(kvHandle);
    auto uid = kvstore->getCollectionsManifestUid(*kvHandle);
    EXPECT_TRUE(uid.has_value());
    EXPECT_EQ(0, uid.value());
}

TEST_P(KVStoreParamTest, GetCollectionStats) {
    CollectionID cid;
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    int64_t seqno = 1;
    auto item = makeCommittedItem(makeStoredDocKey("mykey", cid), "value");
    item->setBySeqno(seqno++);
    kvstore->set(*ctx, item);
    kvstore->commit(std::move(ctx), flush);

    auto kvHandle = kvstore->makeFileHandle(vbid);
    EXPECT_TRUE(kvHandle);
    auto [status, stats] = kvstore->getCollectionStats(vbid, cid);
    EXPECT_EQ(KVStore::GetCollectionStatsStatus::Success, status);
    EXPECT_EQ(1, stats.itemCount);
    EXPECT_EQ(1, stats.highSeqno);
    EXPECT_LT(0, stats.diskSize);
}

TEST_P(KVStoreParamTest, GetPersistedVBucketState) {
    auto kvHandle = kvstore->makeFileHandle(vbid);
    EXPECT_TRUE(kvHandle);
    EXPECT_NO_THROW(kvstore->getPersistedVBucketState(*kvHandle, vbid));
}

void KVStoreParamTest::corruptCouchKVStoreDataFile() {
    ASSERT_EQ("couchdb", GetParam())
            << "This method should only be used for couchdb";
    namespace fs = std::filesystem;
    fs::path dataDir(fs::current_path() / kvstore->getConfig().getDBName());
    fs::path dataFile;
    for (const auto& file :
         std::filesystem::recursive_directory_iterator(dataDir)) {
        if (file.path().has_filename() &&
            file.path().filename() == "stats.json") {
            continue;
        }
        dataFile = file;
    }
    // manually write nothing to the file as resizing it to 0 using boost
    // fails on windows.
    std::ofstream osf{dataFile};
    if (osf.is_open()) {
        osf << "";
        osf.close();
    } else {
        FAIL();
    }
    ASSERT_TRUE(fs::is_regular_file(dataFile));
}

TEST_P(KVStoreParamTest, GetCollectionStatsFailed) {
    /* Magma gets its collection stats from in memory so any corruption of
     data files between KVStore::makeFileHandle() and
     KVStore::getCollectionStats() won't cause the call to fail */
    if (GetParam() == "magma" || isNexus()) {
        return;
    }

    CollectionID cid;
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    int64_t seqno = 1;
    auto item = makeCommittedItem(makeStoredDocKey("mykey", cid), "value");
    item->setBySeqno(seqno++);
    kvstore->set(*ctx, item);
    kvstore->commit(std::move(ctx), flush);

    auto kvHandle = kvstore->makeFileHandle(vbid);
    EXPECT_TRUE(kvHandle);

    // Corrupt couchdb file under
    corruptCouchKVStoreDataFile();

    auto [status, stats] = kvstore->getCollectionStats(vbid, cid);
    EXPECT_EQ(KVStore::GetCollectionStatsStatus::Failed, status);
    // check values for sanity and to use the variable
    EXPECT_EQ(0, stats.itemCount);
    EXPECT_EQ(0, stats.highSeqno);
    EXPECT_EQ(0, stats.diskSize);
}

TEST_P(KVStoreParamTest, SyncDeletePrepareOverwriteCorrectFlushState) {
    if (isNexus()) {
        // Nexus doesn't support the MockPersistenceCallback
        GTEST_SKIP();
    }

    auto key = makeStoredDocKey("key");
    {
        auto tc = kvstore->begin(Vbid(0),
                                 std::make_unique<MockPersistenceCallback>());
        auto& mockPersistenceCallback =
                dynamic_cast<MockPersistenceCallback&>(*tc->cb);
        auto qi = makePendingItem(key, "value");
        qi->setBySeqno(1);
        qi->setDeleted(DeleteSource::Explicit);
        kvstore->set(*tc, qi);

        EXPECT_CALL(mockPersistenceCallback,
                    setCallback(_, FlushStateMutation::Insert))
                .Times(1);
        EXPECT_TRUE(kvstore->commit(std::move(tc), flush));
    }

    auto tc = kvstore->begin(Vbid(0),
                             std::make_unique<MockPersistenceCallback>());
    auto& mockPersistenceCallback =
            dynamic_cast<MockPersistenceCallback&>(*tc->cb);

    auto qi = makePendingItem(key, "value");
    qi->setBySeqno(2);
    qi->setDeleted(DeleteSource::Explicit);
    kvstore->set(*tc, qi);

    EXPECT_CALL(mockPersistenceCallback,
                setCallback(_, FlushStateMutation::Update))
            .Times(1);
    EXPECT_TRUE(kvstore->commit(std::move(tc), flush));
}

TEST_P(KVStoreParamTest, SyncDeletePrepareNotPurgedByTimestamp) {
    auto key = makeStoredDocKey("key");
    auto tc = kvstore->begin(Vbid(0), std::make_unique<PersistenceCallback>());

    auto qi = makePendingItem(key, "value");
    qi->setBySeqno(1);
    qi->setDeleted(DeleteSource::Explicit);
    qi->setExpTime(1);
    kvstore->set(*tc, qi);

    // Highest seqno is not eligible for purging so write a dummy item to make
    // the SyncDelete prepare eligible
    auto dummyItem = makeCommittedItem(makeStoredDocKey("dummy"), "dummy");
    dummyItem->setBySeqno(2);
    kvstore->set(*tc, dummyItem);

    flush.proposedVBState.transition.state = vbucket_state_active;
    EXPECT_TRUE(kvstore->commit(std::move(tc), flush));

    CompactionConfig compactionConfig;
    compactionConfig.purge_before_seq = 0;
    compactionConfig.purge_before_ts = 2;
    compactionConfig.drop_deletes = false;
    auto vb = TestEPVBucketFactory::makeVBucket(vbid);
    auto cctx = std::make_shared<CompactionContext>(vb, compactionConfig, 0);
    {
        auto lock = getVbLock();
        EXPECT_EQ(CompactDBStatus::Success, kvstore->compactDB(lock, cctx));
    }

    EXPECT_EQ(0, cctx->stats.tombstonesPurged);
}

class ReuseSnapshotCallback : public StatusCallback<GetValue> {
public:
    ReuseSnapshotCallback(uint64_t startSeqno,
                          uint64_t endSeqno,
                          uint64_t enomemSeqno)
        : startSeqno(startSeqno), endSeqno(endSeqno), enomemSeqno(enomemSeqno) {
    }

    void callback(GetValue& result) override {
        EXPECT_LE(startSeqno, result.item->getBySeqno());
        EXPECT_LE(result.item->getBySeqno(), endSeqno);
        nItems++;
        if (!didEnomem && result.item->getBySeqno() == enomemSeqno) {
            yield();
            didEnomem = true;
            return;
        }
        setStatus(cb::engine_errc::success);
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

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    for (int j = 0; j < 2; j++) {
        auto key = makeStoredDocKey("key" + std::to_string(j));
        auto qi = makeCommittedItem(key, "value");
        qi->setBySeqno(seqno++);
        kvstore->set(*ctx, qi);
    }
    // Need a valid snap end for couchstore
    flush.proposedVBState.lastSnapEnd = seqno - 1;

    kvstore->commit(std::move(ctx), flush);

    auto cb = std::make_unique<ReuseSnapshotCallback>(1, 2, 1);
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

    ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    for (int j = 0; j < 2; j++) {
        auto key = makeStoredDocKey("key" + std::to_string(j));
        auto qi = makeCommittedItem(key, "value");
        qi->setBySeqno(seqno++);
        kvstore->set(*ctx, qi);
    }

    flush.proposedVBState.transition.state = vbucket_state_active;
    kvstore->commit(std::move(ctx), flush);

    CompactionConfig compactionConfig;
    compactionConfig.purge_before_seq = 0;
    compactionConfig.purge_before_ts = 0;
    compactionConfig.drop_deletes = false;
    auto vb = TestEPVBucketFactory::makeVBucket(vbid);
    auto cctx = std::make_shared<CompactionContext>(vb, compactionConfig, 0);
    {
        auto lock = getVbLock();
        EXPECT_EQ(CompactDBStatus::Success, kvstore->compactDB(lock, cctx));
    }

    kvstore->scan(*scanCtx);

    // We are picking up a scan which was prematurely stopped in a simulated
    // ENOMEM error. Since we've done a compaction, we have removed all the
    // remaining keys that would have been returned so this test should
    // be verifying that we haven't lost the original snapshot of the scan.
    EXPECT_EQ(callback->nItems, 2);
}

TEST_P(KVStoreParamTest, ScanAborted) {
    uint64_t seqno = 1;

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    for (int j = 0; j < 2; j++) {
        auto key = makeStoredDocKey("key" + std::to_string(j));
        auto qi = makeCommittedItem(key, "value");
        qi->setBySeqno(seqno++);
        kvstore->set(*ctx, qi);
    }
    // Need a valid snap end for couchstore
    flush.proposedVBState.lastSnapEnd = seqno - 1;

    kvstore->commit(std::move(ctx), flush);

    class ValueCallBack : public StatusCallback<GetValue> {
    public:
        ValueCallBack(bool doNotFail) : doNotFail(doNotFail) {
        }

        void callback(GetValue& result) override {
            if (doNotFail) {
                setStatus(cb::engine_errc::success);
            } else {
                EXPECT_EQ(0, calls);
                ++calls;
                setStatus(cb::engine_errc::not_my_vbucket);
            }
        }

        bool doNotFail{false};
        int calls{0};
    };

    class CacheCallBack : public StatusCallback<CacheLookup> {
    public:
        CacheCallBack(bool doNotFail) : doNotFail(doNotFail) {
        }
        void callback(CacheLookup& lookup) override {
            if (doNotFail) {
                setStatus(cb::engine_errc::success);
            } else {
                EXPECT_EQ(0, calls);
                ++calls;
                setStatus(cb::engine_errc::not_my_vbucket);
            }
        }
        bool doNotFail{false};
        int calls{0};
    };

    auto cb = std::make_unique<ValueCallBack>(true);
    auto cl = std::make_unique<CacheCallBack>(false);
    auto scanCtx =
            kvstore->initBySeqnoScanContext(std::move(cb),
                                            std::move(cl),
                                            vbid,
                                            1,
                                            DocumentFilter::ALL_ITEMS,
                                            ValueFilter::VALUES_COMPRESSED,
                                            SnapshotSource::Head);

    ASSERT_NE(nullptr, scanCtx);
    EXPECT_EQ(ScanStatus::Cancelled, kvstore->scan(*scanCtx));

    // Now test the other callback aborts
    cb = std::make_unique<ValueCallBack>(false);
    cl = std::make_unique<CacheCallBack>(true);
    scanCtx = kvstore->initBySeqnoScanContext(std::move(cb),
                                              std::move(cl),
                                              vbid,
                                              1,
                                              DocumentFilter::ALL_ITEMS,
                                              ValueFilter::VALUES_COMPRESSED,
                                              SnapshotSource::Head);

    ASSERT_NE(nullptr, scanCtx);
    EXPECT_EQ(ScanStatus::Cancelled, kvstore->scan(*scanCtx));
}

void KVStoreParamTest::testPerDocumentCompression(bool useJson) {
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    StoredDocKey key = makeStoredDocKey("key");
    // use a value with a little repetition so compression shrinks the value
    // (otherwise the raw value will be stored instead)
    std::string value = useJson ? R"({"foo":"foofoofoofoofoofoofoofoo"})"
                                : "foofoofoofoofoofoofoofoo";
    auto qi = makeCommittedItem(key, value);
    qi->setBySeqno(1);
    if (useJson) {
        qi->setDataType(qi->getDataType() | uint8_t(cb::mcbp::Datatype::JSON));
    }
    ASSERT_FALSE(qi->getDataType() & uint8_t(cb::mcbp::Datatype::Snappy));

    kvstore->set(*ctx, qi);

    EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));

    // "manually" compress the document in the expected format to verify
    // the read version matches
    cb::compression::Buffer expectedValue;
    ASSERT_TRUE(cb::compression::deflateSnappy({value.data(), value.size()},
                                               expectedValue));

    // check the value is read compressed, and is the expected value
    GetValue gv = kvstore->get(
            DiskDocKey{key}, Vbid(0), ValueFilter::VALUES_COMPRESSED);
    EXPECT_TRUE(gv.item->getDataType() & uint8_t(cb::mcbp::Datatype::Snappy));
    EXPECT_EQ(useJson, qi->getDataType() & uint8_t(cb::mcbp::Datatype::JSON));
    EXPECT_EQ(std::string_view(expectedValue),
              gv.item->getValue()->to_string_view());

    // for rigour, check the decompressed version is exactly the original value
    gv.item->decompressValue();
    EXPECT_FALSE(gv.item->getDataType() & uint8_t(cb::mcbp::Datatype::Snappy));
    EXPECT_EQ(useJson, qi->getDataType() & uint8_t(cb::mcbp::Datatype::JSON));
    EXPECT_EQ(value, gv.item->getValue()->to_string_view());
}

TEST_P(KVStoreParamTest, PerDocumentCompressionTest_Binary) {
    // check that an item written without snappy compression will be compressed
    // by magma
    if (config.getBackendString() != "magma") {
        // TODO MB-53859: Run these tests for all kvstores once
        // couchstore decompresses items for VALUES_DECOMPRESSED even if
        // they were compressed _before_ being written to the kvstore
        GTEST_SKIP();
    }
    testPerDocumentCompression(false /* useJson */);
}

TEST_P(KVStoreParamTest, PerDocumentCompressionTest_Json) {
    // check that a json item written without snappy compression will be
    // compressed by magma
    if (config.getBackendString() != "magma") {
        GTEST_SKIP();
    }
    testPerDocumentCompression(true /* useJson */);
}

TEST_P(KVStoreParamTest, PerDocumentCompressionTest_Disabled) {
    // check that a per document compression under magma can be disabled
    // successfully
    if (config.getBackendString() != "magma") {
        GTEST_SKIP();
    }

    config.setMagmaPerDocumentCompressionEnabled(false);

    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    StoredDocKey key = makeStoredDocKey("key");
    std::string value = "foofoofoofoofoofoofoofoo";
    auto qi = makeCommittedItem(key, value);
    qi->setBySeqno(1);
    ASSERT_FALSE(qi->getDataType() & uint8_t(cb::mcbp::Datatype::Snappy));

    kvstore->set(*ctx, qi);

    EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));

    // check the value is read uncompressed, and is the expected value
    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    EXPECT_FALSE(gv.item->getDataType() & uint8_t(cb::mcbp::Datatype::Snappy));
    EXPECT_EQ(value, gv.item->getValue()->to_string_view());
}

TEST_P(KVStoreParamTest, GetBySeqno) {
    const int nItems = 5;
    for (int ii = 1; ii < nItems; ++ii) {
        auto ctx = kvstore->begin(vbid);
        auto key = makeStoredDocKey(std::to_string(ii));
        const std::string value = std::string(ii, 'a');
        std::unique_ptr<Item> item =
                std::make_unique<Item>(key,
                                       0,
                                       0,
                                       value.data(),
                                       value.size(),
                                       PROTOCOL_BINARY_RAW_BYTES,
                                       0,
                                       ii);
        kvstore->set(*ctx, queued_item(std::move(item)));
        kvstore->commit(std::move(ctx), flush);
    }

    auto item = makeCompressibleItem(Vbid(0),
                                     makeStoredDocKey("compressed"),
                                     "" /*body*/,
                                     PROTOCOL_BINARY_RAW_BYTES,
                                     false /*compressed*/,
                                     true /*xattr*/);
    auto ctx = kvstore->begin(vbid);
    item->setBySeqno(nItems);
    kvstore->set(*ctx, queued_item(std::move(item)));
    kvstore->commit(std::move(ctx), flush);

    auto handle = kvstore->makeFileHandle(Vbid(0));

    for (int ii = 1; ii < nItems; ++ii) {
        auto gv = kvstore->getBySeqno(
                *handle, Vbid(0), ii, ValueFilter::KEYS_ONLY);
        EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
        ASSERT_TRUE(gv.item);
        EXPECT_EQ(ii, gv.item->getBySeqno());
        EXPECT_EQ(makeStoredDocKey(std::to_string(ii)), gv.item->getKey());
        EXPECT_EQ(0, gv.item->getNBytes());
    }

    for (int ii = 1; ii < nItems; ++ii) {
        auto gv = kvstore->getBySeqno(
                *handle, Vbid(0), ii, ValueFilter::VALUES_DECOMPRESSED);
        EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
        ASSERT_TRUE(gv.item);
        EXPECT_EQ(ii, gv.item->getBySeqno());
        EXPECT_EQ(makeStoredDocKey(std::to_string(ii)), gv.item->getKey());
        EXPECT_EQ(std::string(ii, 'a'), gv.item->getValueView());
    }

    // Check compressed
    if (isSnappyCompressedAtPersistence()) {
        auto gv = kvstore->getBySeqno(
                *handle, Vbid(0), nItems, ValueFilter::VALUES_COMPRESSED);
        EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
        ASSERT_TRUE(gv.item);
        EXPECT_EQ(nItems, gv.item->getBySeqno());
        EXPECT_EQ(makeStoredDocKey("compressed"), gv.item->getKey());
        EXPECT_TRUE(cb::mcbp::datatype::is_snappy(gv.item->getDataType()));
    }

    // Check an unknown seqno
    auto gv = kvstore->getBySeqno(
            *handle, Vbid(0), ~0, ValueFilter::VALUES_DECOMPRESSED);
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());
    ASSERT_FALSE(gv.item);
}

// Test to ensure that the CompactionContext::max_purged_seq is correctly set
// after calling KVStore::compactDB().
TEST_P(KVStoreParamTest, purgeSeqnoAfterCompaction) {
    uint64_t seqno = 1;
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    auto key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(seqno++);
    qi->setDeleted();
    kvstore->del(*ctx, qi);
    auto key2 = makeStoredDocKey("key2");
    auto qi2 = makeCommittedItem(key2, "value");
    qi2->setBySeqno(seqno++);
    kvstore->set(*ctx, qi2);
    flush.proposedVBState.transition.state = vbucket_state_active;
    ASSERT_TRUE(kvstore->commit(std::move(ctx), flush));

    CompactionConfig compactionConfig;
    compactionConfig.drop_deletes = true;
    auto vb = TestEPVBucketFactory::makeVBucket(vbid);
    auto cctx = std::make_shared<CompactionContext>(vb, compactionConfig, 0);
    {
        auto lock = getVbLock();
        EXPECT_EQ(CompactDBStatus::Success, kvstore->compactDB(lock, cctx));
    }
    EXPECT_EQ(1, cctx->stats.tombstonesPurged);
    EXPECT_EQ(1, cctx->getRollbackPurgeSeqno());
}

static std::string kvstoreTestParams[] = {
#ifdef EP_USE_MAGMA
        "magma",
        "nexus_couchdb_magma",
#endif
        "couchdb"};

INSTANTIATE_TEST_SUITE_P(
        KVStoreParam,
        KVStoreParamTest,
        ::testing::ValuesIn(kvstoreTestParams),
        [](const ::testing::TestParamInfo<std::string>& testInfo) {
            return testInfo.param;
        });

// While these look trivial, GoogleMock classes do a lot in the ctor / dtor
// so outline to speedup build times.
MockGetValueCallback::MockGetValueCallback() = default;
MockGetValueCallback::~MockGetValueCallback() = default;
MockPersistenceCallback::MockPersistenceCallback() = default;
MockPersistenceCallback::~MockPersistenceCallback() = default;
