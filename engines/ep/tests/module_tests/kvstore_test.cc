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

#include "bucket_logger.h"
#include "couch-kvstore/couch-kvstore-config.h"
#include "couch-kvstore/couch-kvstore.h"
#include "item.h"
#include "kvstore.h"
#include "kvstore_config.h"
#ifdef EP_USE_ROCKSDB
#include "rocksdb-kvstore/rocksdb-kvstore_config.h"
#endif
#include "collections/collection_persisted_stats.h"
#ifdef EP_USE_MAGMA
#include "../mock/mock_magma_kvstore.h"
#include "magma-kvstore/magma-kvstore_config.h"
#include "magma-kvstore/magma-kvstore_iorequest.h"
#endif
#include "programs/engine_testapp/mock_server.h"
#include "test_helpers.h"
#include "thread_gate.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"
#include "workload.h"

#include <folly/portability/GTest.h>
#include <platform/dirutils.h>

#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace std::string_literals;
using namespace testing;

// Value to use when testing Snappy compression.
static const std::string COMPRESSIBLE_VALUE = "xxyyzzxxyyzzxxyyzzxxyyzz";

void KVStoreTestCacheCallback::callback(CacheLookup& lookup) {
    EXPECT_EQ(vb, lookup.getVBucketId());
    EXPECT_LE(start, lookup.getBySeqno());
    EXPECT_LE(lookup.getBySeqno(), end);
}

void GetCallback::callback(GetValue& result) {
    EXPECT_EQ(expectedErrorCode, result.getStatus());
    if (result.getStatus() == ENGINE_SUCCESS) {
        if (expectCompressed) {
            EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_SNAPPY,
                      result.item->getDataType());
            result.item->decompressValue();
        }

        EXPECT_EQ(COMPRESSIBLE_VALUE, result.item->getValue()->to_s());
    }
}

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
                   ENGINE_ERROR_CODE expectedErrorCode,
                   bool expectCompressed) {
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

void initialize_kv_store(KVStore* kvstore, Vbid vbid) {
    // simulate the setVbState by incrementing the rev
    kvstore->prepareToCreate(vbid);
    vbucket_state state;
    state.transition.state = vbucket_state_active;
    // simulate the setVbState by incrementing the rev
    kvstore->prepareToCreate(vbid);
    kvstore->snapshotVBucket(vbid, state);
}

std::unique_ptr<KVStore> setup_kv_store(KVStoreConfig& config,
                                        std::vector<Vbid> vbids) {
    auto kvstore = KVStoreFactory::create(config);
    for (auto vbid : vbids) {
        initialize_kv_store(kvstore.rw.get(), vbid);
    }
    return std::move(kvstore.rw);
}

void add_stat_callback(std::string_view key,
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
        auto qi = makeCommittedItem(makeStoredDocKey(key), COMPRESSIBLE_VALUE);
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

void KVStoreBackend::setup(const std::string& dataDir,
                           const std::string& backend) {
    Configuration config;
    // `GetParam` returns the string parameter representing the KVStore
    // implementation.
    auto configStr = "dbname="s + dataDir + ";backend="s + backend + ";";

    if (backend == "magma") {
        configStr += magmaConfig;
    }

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
    if (config.getBackend() == "couchdb") {
        kvsReadOnly =
                static_cast<CouchKVStore*>(kvstore.get())->makeReadOnlyStore();
        kvstoreReadOnly = kvsReadOnly.get();
    } else {
        kvstoreReadOnly = kvstore.get();
    }
}

void KVStoreBackend::teardown() {
    // Under RocksDB, removing the database folder (which is equivalent to
    // calling rocksdb::DestroyDB()) for a live DB is an undefined
    // behaviour. So, close the DB before destroying it.
    kvstore.reset();
}

void KVStoreParamTest::SetUp() {
    KVStoreTest::SetUp();
    KVStoreBackend::setup(data_dir, GetParam());
}

void KVStoreParamTest::TearDown() {
    KVStoreBackend::teardown();
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

// A doc not found should equal a get failure for a get call (used for some
// stats, fetching docs to expire, and rollback)
TEST_P(KVStoreParamTest, GetMissNumGetFailure) {
    GetValue gv = kvstore->get(DiskDocKey{makeStoredDocKey("key")}, Vbid(0));
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    auto stats = kvstore->getKVStoreStat();
    EXPECT_EQ(1, stats.numGetFailure);
    EXPECT_EQ(0, kvstore->getKVStoreStat().io_bg_fetch_docs_read);
    EXPECT_EQ(0, kvstore->getKVStoreStat().io_bgfetch_doc_bytes);
}

// A doc not found doesn't result in a get failure for a getMulti (bgfetch)
TEST_P(KVStoreParamTest, GetMultiMissNumGetFailure) {
    vb_bgfetch_queue_t q;
    vb_bgfetch_item_ctx_t ctx;
    ctx.isMetaOnly = GetMetaOnly::No;
    auto diskDocKey = makeDiskDocKey("key");
    q[diskDocKey] = std::move(ctx);
    kvstore->getMulti(vbid, q);

    for (auto& fetched : q) {
        EXPECT_EQ(ENGINE_KEY_ENOENT, fetched.second.value.getStatus());
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
            [&results](GetValue&& cb) { results.push_back(std::move(cb)); });

    for (auto& fetched : results) {
        EXPECT_EQ(ENGINE_KEY_ENOENT, fetched.getStatus());
    }

    // It wouldn't make sense to report get failures if we don't return anything
    // as who knows what should exist in a range.
    auto stats = kvstore->getKVStoreStat();
    EXPECT_EQ(0, stats.numGetFailure);
}

TEST_P(KVStoreParamTest, SaveDocsHisto) {
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->set(qi);

    StoredDocKey key1 = makeStoredDocKey("key1");
    auto qi1 = makeCommittedItem(key, "value");
    qi1->setBySeqno(2);
    kvstore->set(qi1);

    EXPECT_TRUE(kvstore->commit(flush));

    auto& stats = kvstore->getKVStoreStat();

    auto expectedCount = 1;
    if (GetParam() == "rocksdb") {
        expectedCount = 2;
    }

    EXPECT_EQ(expectedCount, stats.saveDocsHisto.getValueCount());
    EXPECT_EQ(1, stats.commitHisto.getValueCount());
}

TEST_P(KVStoreParamTest, BatchSizeHisto) {
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->set(qi);

    StoredDocKey key1 = makeStoredDocKey("key1");
    auto qi1 = makeCommittedItem(key, "value");
    qi1->setBySeqno(2);
    kvstore->set(qi1);

    EXPECT_TRUE(kvstore->commit(flush));

    auto& stats = kvstore->getKVStoreStat();

    EXPECT_EQ(1, stats.batchSize.getValueCount());
    EXPECT_EQ(2, stats.batchSize.getMaxValue());
}

TEST_P(KVStoreParamTest, DocsCommittedStat) {
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->set(qi);

    EXPECT_TRUE(kvstore->commit(flush));

    auto& stats = kvstore->getKVStoreStat();
    EXPECT_EQ(1, stats.docsCommitted);
}

void KVStoreParamTest::testBgFetchDocsReadGet(bool deleted) {
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);

    if (deleted) {
        qi->setDeleted();
    }

    kvstore->set(qi);

    EXPECT_TRUE(kvstore->commit(flush));

    GetValue gv = kvstore->get(DiskDocKey{key}, Vbid(0));
    checkGetValue(gv);
    EXPECT_EQ(1, kvstore->getKVStoreStat().io_bg_fetch_docs_read);
    EXPECT_NE(0, kvstore->getKVStoreStat().io_bgfetch_doc_bytes);
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGet) {
    SCOPED_TRACE("");
    testBgFetchDocsReadGet(false /*deleted*/);
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGetDeleted) {
    SCOPED_TRACE("");
    testBgFetchDocsReadGet(true /*deleted*/);
}

void KVStoreParamTest::testBgFetchDocsReadGetMulti(bool deleted,
                                                   GetMetaOnly getMeta) {
    kvstore->begin(std::make_unique<TransactionContext>(vbid));
    StoredDocKey key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);

    if (deleted) {
        qi->setDeleted();
    }

    kvstore->set(qi);

    EXPECT_TRUE(kvstore->commit(flush));

    vb_bgfetch_queue_t q;
    vb_bgfetch_item_ctx_t ctx;
    ctx.isMetaOnly = getMeta;
    auto diskDocKey = makeDiskDocKey("key");
    q[diskDocKey] = std::move(ctx);
    kvstore->getMulti(vbid, q);

    for (auto& fetched : q) {
        EXPECT_EQ(ENGINE_SUCCESS, fetched.second.value.getStatus());
    }

    EXPECT_EQ(1, kvstore->getKVStoreStat().io_bg_fetch_docs_read);
    EXPECT_NE(0, kvstore->getKVStoreStat().io_bgfetch_doc_bytes);
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGetMulti) {
    testBgFetchDocsReadGetMulti(false /*deleted*/, GetMetaOnly::No);
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGetMultiDeleted) {
    testBgFetchDocsReadGetMulti(true /*deleted*/, GetMetaOnly::No);
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGetMultiMetaOnly) {
    testBgFetchDocsReadGetMulti(false /*deleted*/, GetMetaOnly::Yes);
}

TEST_P(KVStoreParamTest, BgFetchDocsReadGetMultiDeletedMetaOnly) {
    testBgFetchDocsReadGetMulti(true /*deleted*/, GetMetaOnly::Yes);
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
    }

    kvstore = setup_kv_store(*kvstoreConfig, vbids);

    // Check our loaded vb stat
    EXPECT_EQ(1, kvstore->getKVStoreStat().numLoadedVb);

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
            auto scanCtx = kvstoreReadOnly->initBySeqnoScanContext(
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
        auto cctx = std::make_shared<CompactionContext>(Vbid(0), config, 0);
        for (int i = 0; i < 10; i++) {
            auto lock = getVbLock();
            EXPECT_TRUE(kvstore->compactDB(lock, cctx));
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
    auto cctx = std::make_shared<CompactionContext>(vbid, compactionConfig, 0);
    {
        auto lock = getVbLock();
        EXPECT_TRUE(kvstore->compactDB(lock, cctx));
    }

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
