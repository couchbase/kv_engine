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

#include "../mock/mock_magma_kvstore.h"
#include "configuration.h"
#include "kvstore/magma-kvstore/kv_magma_common/magma-kvstore_metadata.h"
#include "kvstore/magma-kvstore/magma-kvstore_config.h"
#include "kvstore_test.h"
#include "programs/engine_testapp/mock_server.h"
#include "test_helpers.h"
#include "thread_gate.h"
#include <executor/workload.h>

using namespace std::string_literals;
using namespace testing;

// Test fixture for tests which run only on Magma.
class MagmaKVStoreTest : public KVStoreTest {
protected:
    void SetUp() override {
        KVStoreTest::SetUp();

        auto configStr =
                "dbname="s + data_dir + ";"s + "backend=magma;" + magmaConfig;
        if (rollbackTest) {
            configStr += ";" + magmaRollbackConfig;
        }
        Configuration config;
        config.parseConfiguration(configStr.c_str(), get_mock_server_api());
        WorkLoadPolicy workload(config.getMaxNumWorkers(),
                                config.getMaxNumShards());
        kvstoreConfig =
                std::make_unique<MagmaKVStoreConfig>(config,
                                                     config.getBackend(),
                                                     workload.getNumShards(),
                                                     0 /*shardId*/);
        kvstore = std::make_unique<MockMagmaKVStore>(*kvstoreConfig);
    }

    void TearDown() override {
        kvstore.reset();
        KVStoreTest::TearDown();
    }

    std::unique_ptr<MagmaKVStoreConfig> kvstoreConfig;
    std::unique_ptr<MockMagmaKVStore> kvstore;
    void SetRollbackTest() {
        rollbackTest = true;
    }

private:
    bool rollbackTest{false};
};

class MagmaKVStoreRollbackTest : public MagmaKVStoreTest {
protected:
    void SetUp() override {
        MagmaKVStoreTest::SetRollbackTest();
        MagmaKVStoreTest::SetUp();
    }
};

TEST_F(MagmaKVStoreRollbackTest, Rollback) {
    kvstore->prepareToCreateImpl(vbid);

    uint64_t seqno = 1;

    for (int i = 0; i < 2; i++) {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        for (int j = 0; j < 5; j++) {
            auto key = makeStoredDocKey("key" + std::to_string(seqno));
            auto qi = makeCommittedItem(key, "value");
            qi->setBySeqno(seqno++);
            kvstore->set(*ctx, qi);
        }
        kvstore->commit(std::move(ctx), flush);
    }

    auto rv = kvstore->get(makeDiskDocKey("key5"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), cb::engine_errc::success);
    rv = kvstore->get(makeDiskDocKey("key6"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), cb::engine_errc::success);

    auto rollbackResult =
            kvstore->rollback(Vbid(0), 5, std::make_unique<CustomRBCallback>());
    ASSERT_TRUE(rollbackResult.success);

    rv = kvstore->get(makeDiskDocKey("key1"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), cb::engine_errc::success);
    rv = kvstore->get(makeDiskDocKey("key5"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), cb::engine_errc::success);
    rv = kvstore->get(makeDiskDocKey("key6"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), cb::engine_errc::no_such_key);
    rv = kvstore->get(makeDiskDocKey("key10"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), cb::engine_errc::no_such_key);

    auto vbs = kvstore->getCachedVBucketState(Vbid(0));
    ASSERT_EQ(uint64_t(5), vbs->highSeqno);
    ASSERT_EQ(size_t(5), kvstore->getItemCount(Vbid(0)));
}

TEST_F(MagmaKVStoreRollbackTest, RollbackNoValidCheckpoint) {
    uint64_t seqno = 1;

    auto cfg = reinterpret_cast<MagmaKVStoreConfig*>(kvstoreConfig.get());
    auto maxCheckpoints = cfg->getMagmaMaxCheckpoints();

    // Create maxCheckpoints+2 checkpoints
    // Magma may internally retain +1 additional checkpoint for crash recovery
    for (int i = 0; i < int(maxCheckpoints) + 2; i++) {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        for (int j = 0; j < 5; j++) {
            auto key = makeStoredDocKey("key" + std::to_string(seqno));
            auto qi = makeCommittedItem(key, "value");
            qi->setBySeqno(seqno++);
            kvstore->set(*ctx, qi);
        }
        kvstore->commit(std::move(ctx), flush);
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
    ASSERT_EQ(0, int(kvsRev));
    EXPECT_NO_THROW(kvstore->prepareToCreate(Vbid(0)));
}

TEST_F(MagmaKVStoreTest, getStats) {
    constexpr std::array<std::string_view, 40> statNames = {{
            "magma_NCompacts",
            "magma_NFlushes",
            "magma_NTTLCompacts",
            "magma_NFileCountCompacts",
            "magma_NWriterCompacts",
            "magma_BytesOutgoing",
            "magma_NReadBytes",
            "magma_NReadBytesGet",
            "magma_NGets",
            "magma_NSets",
            "magma_NInserts",
            "magma_NReadIO",
            "magma_NReadBytesCompact",
            "magma_BytesIncoming",
            "magma_NWriteBytes",
            "magma_NWriteBytesCompact",
            "magma_LogicalDataSize",
            "magma_LogicalDiskSize",
            "magma_TotalDiskUsage",
            "magma_WALDiskUsage",
            "magma_BlockCacheMemUsed",
            "magma_KeyIndexSize",
            "magma_SeqIndex_IndexBlockSize",
            "magma_WriteCacheMemUsed",
            "magma_WALMemUsed",
            "magma_TableMetaMemUsed",
            "magma_BufferMemUsed",
            "magma_TotalMemUsed",
            "magma_TotalBloomFilterMemUsed",
            "magma_BlockCacheHits",
            "magma_BlockCacheMisses",
            "magma_NTablesDeleted",
            "magma_NTablesCreated",
            "magma_NTableFiles",
            "magma_NSyncs",
            "magma_TableObjectMemUsed",
            "magma_ReadAheadBufferMemUsed",
            "magma_LSMTreeObjectMemUsed",
            "magma_HistogramMemUsed",
            "foo",
    }};
    auto stats = kvstore->getStats(statNames);
    for (auto name : statNames) {
        if (name == "foo") {
            EXPECT_EQ(stats.find(name), stats.end());
        } else {
            EXPECT_NE(stats.find(name), stats.end());
        }
    }
}

TEST_F(MagmaKVStoreTest, getStat) {
    size_t value;
    ASSERT_FALSE(kvstore->getStat("foobar", value));
    ASSERT_TRUE(kvstore->getStat("memory_quota", value));

    ASSERT_TRUE(kvstore->getStat("magma_NSets", value));
    ASSERT_TRUE(kvstore->getStat("magma_NGets", value));
    ASSERT_TRUE(kvstore->getStat("magma_NInserts", value));
    // Compaction counters
    ASSERT_TRUE(kvstore->getStat("magma_NCompacts", value));
    ASSERT_TRUE(kvstore->getStat("magma_NFlushes", value));
    ASSERT_TRUE(kvstore->getStat("magma_NTTLCompacts", value));
    ASSERT_TRUE(kvstore->getStat("magma_NFileCountCompacts", value));
    ASSERT_TRUE(kvstore->getStat("magma_NWriterCompacts", value));
    // Read amp, ReadIOAmp.
    ASSERT_TRUE(kvstore->getStat("magma_BytesOutgoing", value));
    ASSERT_TRUE(kvstore->getStat("magma_NReadBytes", value));
    ASSERT_TRUE(kvstore->getStat("magma_NReadBytesGet", value));
    ASSERT_TRUE(kvstore->getStat("magma_NGets", value));
    ASSERT_TRUE(kvstore->getStat("magma_NReadIO", value));
    ASSERT_TRUE(kvstore->getStat("magma_NReadBytesCompact", value));
    // Write amp.
    ASSERT_TRUE(kvstore->getStat("magma_BytesIncoming", value));
    ASSERT_TRUE(kvstore->getStat("magma_NWriteBytes", value));
    ASSERT_TRUE(kvstore->getStat("magma_NWriteBytesCompact", value));
    // Fragmentation.
    ASSERT_TRUE(kvstore->getStat("magma_LogicalDataSize", value));
    ASSERT_TRUE(kvstore->getStat("magma_LogicalDiskSize", value));
    // Disk usage.
    ASSERT_TRUE(kvstore->getStat("magma_TotalDiskUsage", value));
    ASSERT_TRUE(kvstore->getStat("magma_WALDiskUsage", value));
    // Memory usage.
    ASSERT_TRUE(kvstore->getStat("magma_TableObjectMemUsed", value));
    ASSERT_TRUE(kvstore->getStat("magma_LSMTreeObjectMemUsed", value));
    ASSERT_TRUE(kvstore->getStat("magma_HistogramMemUsed", value));
    ASSERT_TRUE(kvstore->getStat("magma_ReadAheadBufferMemUsed", value));
    ASSERT_TRUE(kvstore->getStat("magma_BlockCacheMemUsed", value));
    ASSERT_TRUE(kvstore->getStat("magma_KeyIndexSize", value));
    ASSERT_TRUE(kvstore->getStat("magma_SeqIndex_IndexBlockSize", value));
    ASSERT_TRUE(kvstore->getStat("magma_WriteCacheMemUsed", value));
    ASSERT_TRUE(kvstore->getStat("magma_WALMemUsed", value));
    ASSERT_TRUE(kvstore->getStat("magma_TableMetaMemUsed", value));
    ASSERT_TRUE(kvstore->getStat("magma_BufferMemUsed", value));
    ASSERT_TRUE(kvstore->getStat("magma_TotalBloomFilterMemUsed", value));
    ASSERT_TRUE(kvstore->getStat("magma_TotalMemUsed", value));
    // Block cache.
    ASSERT_TRUE(kvstore->getStat("magma_BlockCacheHits", value));
    ASSERT_TRUE(kvstore->getStat("magma_BlockCacheMisses", value));
    // SST file counts.
    ASSERT_TRUE(kvstore->getStat("magma_NTablesDeleted", value));
    ASSERT_TRUE(kvstore->getStat("magma_NTablesCreated", value));
    ASSERT_TRUE(kvstore->getStat("magma_NTableFiles", value));
    // NSyncs.
    ASSERT_TRUE(kvstore->getStat("magma_NSyncs", value));
}

TEST_F(MagmaKVStoreTest, setMaxDataSize) {
    uint64_t seqno{1};

    // Magma's memory quota is recalculated on each commit batch.
    auto ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    auto qi = makeCommittedItem(makeStoredDocKey("key"), "value");
    qi->setBySeqno(seqno++);
    kvstore->set(*ctx, qi);
    kvstore->commit(std::move(ctx), flush);

    size_t memQuota;
    ASSERT_TRUE(kvstore->getStat("memory_quota", memQuota));
    size_t writeCacheQuota;
    ASSERT_TRUE(kvstore->getStat("write_cache_quota", writeCacheQuota));

    kvstore->setMaxDataSize(memQuota / 10);

    // Magma's memory quota is recalculated on each commit batch.
    ctx = kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
    qi->setBySeqno(seqno++);
    kvstore->set(*ctx, qi);
    kvstore->commit(std::move(ctx), flush);

    size_t memQuotaAfter;
    ASSERT_TRUE(kvstore->getStat("memory_quota", memQuotaAfter));
    ASSERT_LT(memQuotaAfter, memQuota);

    size_t writeCacheQuotaAfter;
    ASSERT_TRUE(kvstore->getStat("write_cache_quota", writeCacheQuotaAfter));
    ASSERT_LT(writeCacheQuotaAfter, writeCacheQuota);
}

TEST_F(MagmaKVStoreTest, badSetRequest) {
    auto tc = kvstore->begin(Vbid(0),
                             std::make_unique<MockPersistenceCallback>());
    auto& mockPersistenceCallback =
            dynamic_cast<MockPersistenceCallback&>(*tc->cb);

    auto key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    kvstore->set(*tc, qi);

    kvstore->saveDocsErrorInjector = [](VB::Commit& cmt,
                                        kvstats_ctx& ctx) -> int {
        return magma::Status::IOError;
    };

    EXPECT_CALL(mockPersistenceCallback,
                setCallback(_, FlushStateMutation::Failed))
            .Times(1);
    EXPECT_FALSE(kvstore->commit(std::move(tc), flush));
}

TEST_F(MagmaKVStoreTest, badDelRequest) {
    auto tc = kvstore->begin(Vbid(0),
                             std::make_unique<MockPersistenceCallback>());
    auto& mockPersistenceCallback =
            dynamic_cast<MockPersistenceCallback&>(*tc->cb);

    auto key = makeStoredDocKey("key");
    auto qi = makeCommittedItem(key, "value");
    qi->setBySeqno(1);
    qi->setDeleted(DeleteSource::Explicit);
    kvstore->set(*tc, qi);

    kvstore->saveDocsErrorInjector = [](VB::Commit& cmt,
                                        kvstats_ctx& ctx) -> int {
        return magma::Status::IOError;
    };

    EXPECT_CALL(mockPersistenceCallback,
                deleteCallback(_, FlushStateDeletion::Failed))
            .Times(1);
    EXPECT_FALSE(kvstore->commit(std::move(tc), flush));
}

TEST_F(MagmaKVStoreTest, initializeWithHeaderButNoVBState) {
    initialize_kv_store(kvstore.get(), vbid);

    EXPECT_TRUE(kvstore->deleteLocalDoc(vbid, "_vbstate").IsOK());

    vbucket_state defaultState;
    auto* vbstate = kvstore->getCachedVBucketState(vbid);
    EXPECT_NE(defaultState, *vbstate);

    auto res = kvstore->readVBStateFromDisk(vbid);
    EXPECT_FALSE(res.status.IsOK());

    // Recreate the kvstore and the state should equal the default constructed
    // state (and not throw an exception)
    kvstore.reset();
    kvstore = std::make_unique<MockMagmaKVStore>(*kvstoreConfig);

    vbstate = kvstore->getCachedVBucketState(vbid);
    EXPECT_EQ(defaultState, *vbstate);

    res = kvstore->readVBStateFromDisk(vbid);
    EXPECT_FALSE(res.status.IsOK());
    EXPECT_EQ(defaultState, res.vbstate);
}

// Check that if Magma performs an internal (implicit) compaction before
// ep-engine has completed warmup, then any compaction callbacks into
// MagmaKVStore are ignored - until a later compaction after warmup.
TEST_F(MagmaKVStoreTest, MB39669_CompactionBeforeWarmup) {
    // Simulate a compaction callback early on - before Warmup has completed.
    auto newCompaction = kvstoreConfig->magmaCfg.MakeCompactionCallback(
            magma::Magma::KVStoreID(0));
    magma::Slice key;
    magma::Slice value;
    // Require a valid metadata slice to (a) ensure the item isn't just
    // skipped (zero-length meta == local document) and (b) to provide a valid
    // Vbid.
    magmakv::MetaData metadata;
    magma::Slice meta{reinterpret_cast<char*>(&metadata), metadata.getLength()};
    // Compaction callback should return false for anything before warmup.
    EXPECT_FALSE(newCompaction->operator()(key, meta, value));
}

TEST_F(MagmaKVStoreTest, ScanReadsVBStateFromSnapshot) {
    initialize_kv_store(kvstore.get(), vbid);

    ThreadGate tg1(2);
    ThreadGate tg2(2);
    std::unique_ptr<BySeqnoScanContext> scanCtx;

    kvstore->readVBStateFromDiskHook = [&tg1, &tg2]() {
        // Wait until we modify the vbucket_state to test that we are reading
        // the state from the snapshot
        tg1.threadUp();

        // And continue
        tg2.threadUp();
    };

    std::thread t1 = std::thread{[this, &scanCtx]() {
        scanCtx = kvstore->initBySeqnoScanContext(
                std::make_unique<GetCallback>(true /*expectcompressed*/),
                std::make_unique<KVStoreTestCacheCallback>(1, 5, Vbid(0)),
                vbid,
                1,
                DocumentFilter::ALL_ITEMS,
                ValueFilter::VALUES_COMPRESSED,
                SnapshotSource::Head);
        EXPECT_TRUE(scanCtx.get());
    }};

    // Wait until we have grabbed the snapshot in initBySeqnoScanContext
    tg1.threadUp();

    // Change the vBucket state after grabbing the snapshot but before reading
    // the state. If we read the state from the snapshot then it should not
    // see the following change to the maxVisibleSeqno.
    auto vbstate = kvstore->getCachedVBucketState(vbid);
    vbstate->maxVisibleSeqno = 999;
    kvstore->snapshotVBucket(vbid, *vbstate);

    // Finish creating the scanCtx and join up the other thread
    tg2.threadUp();
    t1.join();

    ASSERT_TRUE(scanCtx.get());

    // Max visible seqno should be 0 (i.e. it should not have seen the above
    // change as it should read state from the snapshot). If we read the state
    // again though we should see the updated value.
    EXPECT_EQ(0, scanCtx->maxVisibleSeqno);
    EXPECT_EQ(999, kvstore->getCachedVBucketState(vbid)->maxVisibleSeqno);
}

TEST_F(MagmaKVStoreTest, ScanReadDroppedCollectionsFromSnapshot) {
    initialize_kv_store(kvstore.get(), vbid);

    ThreadGate tg1(2);
    ThreadGate tg2(2);
    std::unique_ptr<BySeqnoScanContext> scanCtx;

    kvstore->readVBStateFromDiskHook = [&tg1, &tg2]() {
        // Wait until we modify the vbucket_state to test that we are reading
        // the state from the snapshot
        tg1.threadUp();

        // And continue
        tg2.threadUp();
    };

    std::thread t1 = std::thread{[this, &scanCtx]() {
        EXPECT_NO_THROW(scanCtx = kvstore->initBySeqnoScanContext(
                                std::make_unique<GetCallback>(
                                        true /*expectcompressed*/),
                                std::make_unique<KVStoreTestCacheCallback>(
                                        1, 5, Vbid(0)),
                                vbid,
                                1,
                                DocumentFilter::ALL_ITEMS,
                                ValueFilter::VALUES_COMPRESSED,
                                SnapshotSource::Head));
        EXPECT_TRUE(scanCtx.get());
    }};

    // Wait until we have grabbed the snapshot in initBySeqnoScanContext
    tg1.threadUp();

    // Dropping a collection in a KVStoreTest is a bit tricky. We could craft
    // the SystemEvent required but we'd just throw when we attempt to update
    // stats in the KVStore as it calls back into the VBucket level manifest.
    // We do have a VBucket level manifest but updating it in a KVStoreTest test
    // is a PITA as it requires a VBucket. For the sake of testing this issue
    // though we can be lazy. To test that we read the dropped collections of
    // the snapshot we can instead write bad data to the droppedCollections
    // local doc which would cause us to throw were we to attempt to parse it
    // as the droppedCollections local doc. Not throwing in
    // initBySeqnoScanContext is the test.
    kvstore->addLocalDoc(vbid, "_collections/dropped", "garbled");

    // Finish creating the scanCtx and join up the other thread
    tg2.threadUp();
    t1.join();
}

TEST_F(MagmaKVStoreTest, MagmaGetExpiryTimeAlive) {
    magmakv::MetaData expiredItem;
    expiredItem.setExptime(10);
    magma::Slice expiredItemSlice = {reinterpret_cast<char*>(&expiredItem),
                                     expiredItem.getLength()};

    EXPECT_EQ(10, kvstore->getExpiryOrPurgeTime(expiredItemSlice));
}

TEST_F(MagmaKVStoreTest, MagmaGetExpiryTimeTombstone) {
    magmakv::MetaData tombstone;
    tombstone.setExptime(10);
    tombstone.setDeleted(true, false /*deleteSource*/);
    magma::Slice tombstoneSlice = {reinterpret_cast<char*>(&tombstone),
                                   tombstone.getLength()};

    EXPECT_EQ(10 + kvstoreConfig->getMetadataPurgeAge(),
              kvstore->getExpiryOrPurgeTime(tombstoneSlice));
}

TEST_F(MagmaKVStoreTest, ReadLocalDocErrorCode) {
    initialize_kv_store(kvstore.get(), vbid);

    auto res = kvstore->readLocalDoc(vbid, "_vbstate");
    EXPECT_EQ(magma::Status::Code::Ok, res.first.ErrorCode());

    EXPECT_TRUE(kvstore->deleteLocalDoc(vbid, "_vbstate").IsOK());
    res = kvstore->readLocalDoc(vbid, "_vbstate");
    EXPECT_EQ(magma::Status::Code::NotFound, res.first.ErrorCode());

    auto kvsRev = kvstore->prepareToDelete(Vbid(0));
    kvstore->delVBucket(vbid, kvsRev);

    res = kvstore->readLocalDoc(vbid, "_vbstate");
    EXPECT_EQ(magma::Status::Code::NotExists, res.first.ErrorCode());
}

TEST_F(MagmaKVStoreTest, KVStoreRevisionAfterReopen) {
    initialize_kv_store(kvstore.get(), vbid);

    auto kvsRev = kvstore->getKVStoreRevision(vbid);
    auto currRev = kvstore->prepareToDelete(vbid);
    EXPECT_EQ(kvsRev, currRev);

    // Reopen kvstore
    kvstore.reset();
    kvstore = std::make_unique<MockMagmaKVStore>(*kvstoreConfig);
    EXPECT_EQ(kvsRev, kvstore->getKVStoreRevision(vbid));
}

class MockDirectory : public magma::Directory {
public:
    MockDirectory(magma::Status status) : status(status) {
    }

    magma::Status Open(magma::Directory::OpenMode mode) override {
        return status;
    }

    magma::Status Sync() override {
        return status;
    }

    magma::Status status;
};

TEST_F(MagmaKVStoreTest, readOnlyMode) {
    initialize_kv_store(kvstore.get(), vbid);

    auto doWrite = [this](uint64_t seqno, bool expected) {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        auto qi = makeCommittedItem(makeStoredDocKey("key"), "value");
        qi->setBySeqno(seqno);
        kvstore->set(*ctx, qi);
        EXPECT_EQ(expected, kvstore->commit(std::move(ctx), flush));
    };

    // Add an item to test that we can read it
    doWrite(1, true /*success*/);

    auto rv = kvstore->get(makeDiskDocKey("key"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), cb::engine_errc::success);

    kvstore.reset();

    // Invoked on first attempt to open magma
    kvstoreConfig->setMakeDirectoryFn(
            [](const std::string& path) -> std::unique_ptr<MockDirectory> {
                return std::make_unique<MockDirectory>(
                        magma::Status(magma::Status::DiskFull, ""));
            });

    // Invoked as we set the config to read only to use the default magma
    // FileSystem
    kvstoreConfig->setReadOnlyHook = [this]() {
        kvstoreConfig->setMakeDirectoryFn(nullptr);
    };

    kvstore = std::make_unique<MockMagmaKVStore>(*kvstoreConfig);

    // Get should be success
    rv = kvstore->get(makeDiskDocKey("key"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), cb::engine_errc::success);

    // So should scan
    auto scanCtx = kvstore->initBySeqnoScanContext(
            std::make_unique<GetCallback>(true /*expectcompressed*/),
            std::make_unique<KVStoreTestCacheCallback>(1, 5, Vbid(0)),
            vbid,
            1,
            DocumentFilter::ALL_ITEMS,
            ValueFilter::VALUES_COMPRESSED,
            SnapshotSource::Head);
    EXPECT_TRUE(scanCtx.get());

    // A write should fail
    doWrite(2, false /*false*/);
}
