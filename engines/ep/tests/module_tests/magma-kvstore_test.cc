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
#include "bucket_logger.h"
#include "collections/collection_persisted_stats.h"
#include "configuration.h"
#include "file_ops_tracker.h"
#include "kvstore/kvstore_iface.h"
#include "kvstore/magma-kvstore/kv_magma_common/magma-kvstore_metadata.h"
#include "kvstore/magma-kvstore/magma-kvstore_config.h"
#include "kvstore/magma-kvstore/magma-kvstore_fs.h"
#include "kvstore/magma-kvstore/magma-kvstore_iorequest.h"
#include "kvstore/storage_common/storage_common/local_doc_constants.h"
#include "kvstore_test.h"
#include "libmagma/file.h"
#include "programs/engine_testapp/mock_server.h"
#include "test_helpers.h"
#include "thread_gate.h"
#include <executor/workload.h>
#include <platform/dirutils.h>

using namespace std::string_literals;
using namespace testing;

// Test fixture for tests which run only on Magma.
class MagmaKVStoreTest : public KVStoreTest {
protected:
    void SetUp() override {
        KVStoreTest::SetUp();

        configStr +=
                "dbname="s + data_dir + ";"s + "backend=magma;" + magmaConfig;
        if (rollbackTest) {
            configStr += ";" + magmaRollbackConfig;
        }
        Configuration config;
        config.parseConfiguration(configStr);
        WorkLoadPolicy workload(
                gsl::narrow_cast<int>(config.getMaxNumWorkers()),
                gsl::narrow_cast<int>(config.getMaxNumShards()));
        kvstoreConfig =
                std::make_unique<MagmaKVStoreConfig>(config,
                                                     config.getBackendString(),
                                                     workload.getNumShards(),
                                                     0 /*shardId*/);
        kvstore = createStore(*kvstoreConfig);
    }

    void TearDown() override {
        kvstore.reset();
        KVStoreTest::TearDown();
    }

    virtual std::unique_ptr<MockMagmaKVStore> createStore(
            MagmaKVStoreConfig& config) {
        return std::make_unique<MockMagmaKVStore>(*kvstoreConfig);
    }

    void SetRollbackTest() {
        rollbackTest = true;
    }

    size_t getCollectionDiskSize(Vbid vbid = Vbid(0),
                                 CollectionID cid = CollectionID::Default) {
        return kvstore->getCollectionStats(vbid, cid).second.diskSize;
    }

    void setAndCommit(queued_item& qi, bool expected) {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        kvstore->set(*ctx, qi);
        flush.proposedVBState.lastSnapStart = qi->getBySeqno();
        flush.proposedVBState.lastSnapEnd = qi->getBySeqno();
        EXPECT_EQ(expected, kvstore->commit(std::move(ctx), flush));
    }

    queued_item doWrite(uint64_t seqno,
                        bool expected,
                        const std::string& key = "key") {
        auto qi = makeCommittedItem(makeStoredDocKey(key),
                                    "value" + std::to_string(seqno));
        qi->setBySeqno(seqno);
        setAndCommit(qi, expected);
        return qi;
    }

    std::unique_ptr<MagmaKVStoreConfig> kvstoreConfig;
    std::unique_ptr<MockMagmaKVStore> kvstore;

    std::string configStr;
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
            flush.proposedVBState.lastSnapStart = seqno;
            flush.proposedVBState.lastSnapEnd = seqno;
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

    auto cfg = kvstoreConfig.get();
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

class MagmaKVStoreFragmentationTest : public MagmaKVStoreTest {
protected:
    void SetUp() override {
        MagmaKVStoreTest::SetUp();

        // Reset kvstore as we're changing the config.
        kvstore.reset();
        if (cb::io::isDirectory(kvstoreConfig->getDBName())) {
            cb::io::rmrf(kvstoreConfig->getDBName());
        }

        // Magma only considers last two levels of LSD for fragmentation
        // computation. Hence only have 2 levels for this test so that data
        // loaded/overwritten directly lands into them.
        kvstoreConfig->magmaCfg.LSDNumLevels = 2;

        // Recreate kvstore with custom Magma config.
        kvstore = std::make_unique<MockMagmaKVStore>(*kvstoreConfig);
    }
};

// Assert couch_docs_fragmentation is inline with Magma reported fragmentation.
TEST_F(MagmaKVStoreFragmentationTest, fragmentationStat) {
    // So that no compactions kick in and we get a stable fragmentation value
    // when comparing the stats.
    kvstore->setMagmaFragmentationPercentage(100);

    // Load data.
    constexpr auto nItems = 100;
    size_t seqno = 1;
    for (auto i = 0; i < nItems; i++) {
        doWrite(seqno, true, "key" + std::to_string(i));
        seqno++;
    }
    ASSERT_TRUE(kvstore->newCheckpoint(vbid));

    // Overwrite data.
    constexpr auto nOverwrite = 10;
    for (auto i = 0; i < nOverwrite; i++) {
        for (auto j = 0; j < nItems; j++) {
            doWrite(seqno, true, "key" + std::to_string(j));
            seqno++;
        }
    }
    ASSERT_TRUE(kvstore->newCheckpoint(vbid));

    auto fileInfo = kvstore->getAggrDbFileInfo();
    auto couchDocsFrag =
            static_cast<float>(fileInfo.fileSize - fileInfo.spaceUsed) /
            fileInfo.fileSize;

    size_t logicalDataSize = 0;
    size_t logicalDiskSize = 0;
    kvstore->getStat("magma_LogicalDataSize", logicalDataSize);
    kvstore->getStat("magma_LogicalDiskSize", logicalDiskSize);
    auto magmaFrag = static_cast<float>(logicalDiskSize - logicalDataSize) /
                     logicalDiskSize;

    if (couchDocsFrag > magmaFrag) {
        std::swap(couchDocsFrag, magmaFrag);
    }

    // Tolerate a difference of 1%.
    auto diff = ((magmaFrag - couchDocsFrag) / magmaFrag) * 100;
    EXPECT_LE(diff, 1);

    // Expect at least 10% fragmentation from overwrites.
    EXPECT_GT(magmaFrag, 0.1);
}

TEST_F(MagmaKVStoreTest, prepareToCreate) {
    Collections::VB::Manifest m{std::make_shared<Collections::Manager>()};
    VB::Commit meta(m);
    meta.proposedVBState.transition.state = vbucket_state_active;
    kvstore->snapshotVBucket(vbid, meta);
    auto kvsRev = kvstore->prepareToDelete(Vbid(0));
    ASSERT_EQ(0, kvsRev->getRevision());
    EXPECT_NO_THROW(kvstore->prepareToCreate(Vbid(0)));
}

TEST_F(MagmaKVStoreTest, getStats) {
    constexpr std::array<std::string_view, 59> statNames = {{
            "magma_MemoryQuotaLowWaterMark",
            "magma_BloomFilterMemoryQuota",
            "magma_WriteCacheQuota",
            "magma_NCompacts",
            "magma_NDropEncryptionKeysCompacts",
            "magma_NDataLevelCompacts",
            "magma_NFlushes",
            "magma_NTTLCompacts",
            "magma_NFileCountCompacts",
            "magma_NWriterCompacts",
            "magma_BytesOutgoing",
            "magma_NReadBytes",
            "magma_NReadBytesGet",
            "magma_KeyIterator_ItemsRead",
            "magma_SeqIterator_ItemsRead",
            "magma_KeyIterator_ItemsSkipped",
            "magma_SeqIterator_ItemsSkipped",
            "magma_NGets",
            "magma_NSets",
            "magma_NInserts",
            "magma_NReadIO",
            "magma_NReadBytesCompact",
            "magma_BytesIncoming",
            "magma_KeyIndex_BytesIncoming",
            "magma_SeqIndex_BytesIncoming",
            "magma_SeqIndex_Delta_BytesIncoming",
            "magma_NWriteBytes",
            "magma_KeyIndex_NWriteBytes",
            "magma_SeqIndex_NWriteBytes",
            "magma_SeqIndex_Delta_NWriteBytes",
            "magma_NWriteBytesCompact",
            "magma_KeyIndex_NWriteBytesFileCountCompact",
            "magma_SeqIndex_NWriteBytesFileCountCompact",
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
            "magma_DataBlocksSize",
            "magma_DataBlocksCompressSize",
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
    ASSERT_TRUE(kvstore->getStat("magma_MemoryQuotaLowWaterMark", value));
    ASSERT_TRUE(kvstore->getStat("magma_BloomFilterMemoryQuota", value));
    ASSERT_TRUE(kvstore->getStat("magma_WriteCacheQuota", value));

    ASSERT_TRUE(kvstore->getStat("magma_NSets", value));
    ASSERT_TRUE(kvstore->getStat("magma_NGets", value));
    ASSERT_TRUE(kvstore->getStat("magma_NInserts", value));
    ASSERT_TRUE(kvstore->getStat("magma_KeyIterator_ItemsRead", value));
    ASSERT_TRUE(kvstore->getStat("magma_SeqIterator_ItemsRead", value));
    ASSERT_TRUE(kvstore->getStat("magma_KeyIterator_ItemsSkipped", value));
    ASSERT_TRUE(kvstore->getStat("magma_SeqIterator_ItemsSkipped", value));
    // Compaction counters
    ASSERT_TRUE(kvstore->getStat("magma_NCompacts", value));
    ASSERT_TRUE(kvstore->getStat("magma_NDropEncryptionKeysCompacts", value));
    ASSERT_TRUE(kvstore->getStat("magma_NDataLevelCompacts", value));
    ASSERT_TRUE(kvstore->getStat("magma_NFlushes", value));
    ASSERT_TRUE(kvstore->getStat("magma_NTTLCompacts", value));
    ASSERT_TRUE(kvstore->getStat("magma_NFileCountCompacts", value));
    ASSERT_TRUE(kvstore->getStat("magma_NWriterCompacts", value));
    // Read amp, ReadIOAmp.
    ASSERT_TRUE(kvstore->getStat("magma_BytesOutgoing", value));
    ASSERT_TRUE(kvstore->getStat("magma_NReadBytes", value));
    ASSERT_TRUE(kvstore->getStat("magma_FSReadBytes", value));
    ASSERT_TRUE(kvstore->getStat("magma_NReadBytesGet", value));
    ASSERT_TRUE(kvstore->getStat("magma_NGets", value));
    ASSERT_TRUE(kvstore->getStat("magma_NReadIO", value));
    ASSERT_TRUE(kvstore->getStat("magma_NReadBytesCompact", value));
    // Write amp.
    ASSERT_TRUE(kvstore->getStat("magma_BytesIncoming", value));
    ASSERT_TRUE(kvstore->getStat("magma_KeyIndex_BytesIncoming", value));
    ASSERT_TRUE(kvstore->getStat("magma_SeqIndex_BytesIncoming", value));
    ASSERT_TRUE(kvstore->getStat("magma_SeqIndex_Delta_BytesIncoming", value));
    ASSERT_TRUE(kvstore->getStat("magma_NWriteBytes", value));
    ASSERT_TRUE(kvstore->getStat("magma_FSWriteBytes", value));
    ASSERT_TRUE(kvstore->getStat("magma_KeyIndex_NWriteBytes", value));
    ASSERT_TRUE(kvstore->getStat("magma_SeqIndex_NWriteBytes", value));
    ASSERT_TRUE(kvstore->getStat("magma_SeqIndex_Delta_NWriteBytes", value));
    ASSERT_TRUE(kvstore->getStat("magma_NWriteBytesCompact", value));
    ASSERT_TRUE(kvstore->getStat("magma_KeyIndex_NWriteBytesFileCountCompact",
                                 value));
    ASSERT_TRUE(kvstore->getStat("magma_SeqIndex_NWriteBytesFileCountCompact",
                                 value));

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
    ASSERT_TRUE(kvstore->getStat("magma_DataBlocksSize", value));
    ASSERT_TRUE(kvstore->getStat("magma_DataBlocksCompressSize", value));
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
    ASSERT_TRUE(kvstore->getStat("magma_WriteCacheQuota", writeCacheQuota));

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
    ASSERT_TRUE(
            kvstore->getStat("magma_WriteCacheQuota", writeCacheQuotaAfter));
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

    EXPECT_TRUE(kvstore->deleteLocalDoc(vbid, LocalDocKey::vbstate).IsOK());

    vbucket_state defaultState;
    auto* vbstate = kvstore->getCachedVBucketState(vbid);
    EXPECT_NE(defaultState, *vbstate);

    auto res = kvstore->readVBStateFromDisk(vbid);
    EXPECT_EQ(res.status, KVStoreIface::ReadVBStateStatus::NotFound);

    // Recreate the kvstore and the state should equal the default constructed
    // state (and not throw an exception)
    kvstore.reset();
    kvstore = std::make_unique<MockMagmaKVStore>(*kvstoreConfig);

    vbstate = kvstore->getCachedVBucketState(vbid);
    EXPECT_FALSE(vbstate);

    res = kvstore->readVBStateFromDisk(vbid);
    EXPECT_EQ(res.status, KVStoreIface::ReadVBStateStatus::NotFound);
    EXPECT_EQ(defaultState, res.state);
}

// Check that if Magma performs an internal (implicit) compaction before
// ep-engine has completed warmup, then we will throw to prevent an implicit
// compaction that can't drop keys from running
TEST_F(MagmaKVStoreTest, MB39669_CompactionBeforeWarmup) {
    // Simulate a compaction callback early on - before Warmup has completed.
    EXPECT_THROW(kvstoreConfig->magmaCfg.MakeCompactionCallback(
                         magma::Magma::KVStoreID(0)),
                 std::invalid_argument);
}

TEST_F(MagmaKVStoreTest, MetadataEncoding) {
    // Set a bunch of fields to various values to test that we can read them
    // back after encoding and decoding.
    magmakv::MetaData metadata;
    metadata.setExptime(111111);
    metadata.setDeleted(true, DeleteSource::TTL);
    metadata.setBySeqno(222222);
    metadata.setValueSize(333333);
    metadata.setDataType(2);
    metadata.setFlags(444444);
    metadata.setCas(555555);
    metadata.setRevSeqno(666666);

    auto encoded = metadata.encode();
    auto decoded = magmakv::MetaData(encoded);

    // Corectness, are we the same after as before
    EXPECT_EQ(metadata, decoded);

    // Size should be smaller encoded (as some values get leb128 encoded and
    // will be smaller due to the values chosen above)
    auto totalSizeOfV0 = 1 /*sizeof(magmakv::MetaData::MetaDataV0*/ + 34;
    EXPECT_LT(encoded.size(), totalSizeOfV0);

    EXPECT_FALSE(decoded.isHistoryEnabled());

    auto v0EncodedSize = encoded.size();

    // Now test V1 (extension of V0 for prepare namespace)
    metadata.setVersion(magmakv::MetaData::Version::V1);
    metadata.setDurabilityDetailsForPrepare(true, 2);

    encoded = metadata.encode();
    decoded = magmakv::MetaData(encoded);

    // Again, should the same after as before
    EXPECT_EQ(metadata, decoded);

    // Again, things should be smaller, but the potential size decrease in V1
    // rom leb128 encoding gets masked by the size decrease in V0...
    auto sizeOfV1 = 6 /*sizeof(magmakv::MetaData::MetaDataV1)*/;
    auto totalSizeOfV1 = totalSizeOfV0 + sizeOfV1;
    EXPECT_LT(encoded.size(), totalSizeOfV1);

    auto v1v0EncodedSizeDiff = encoded.size() - v0EncodedSize;
    EXPECT_LT(v1v0EncodedSizeDiff, sizeOfV1);

    // Now test an abort (with max length prepare seqno)
    metadata.setDurabilityDetailsForAbort(7777777);

    encoded = metadata.encode();
    decoded = magmakv::MetaData(encoded);

    // Again, should the same after as before
    EXPECT_EQ(metadata, decoded);

    // Again, things should be smaller, but the potential size decrease in V1
    // rom leb128 encoding gets masked by the size decrease in V0...
    v1v0EncodedSizeDiff = encoded.size() - v0EncodedSize;
    EXPECT_LT(v1v0EncodedSizeDiff, sizeOfV1);

    EXPECT_FALSE(decoded.isHistoryEnabled());

    metadata.setDeleted(true, DeleteSource::TTL);
    EXPECT_EQ(DeleteSource::TTL, metadata.getDeleteSource());
}

TEST_F(MagmaKVStoreTest, MetadataV2Encoding) {
    // Set the version as V2 and write some of the V0 data.
    magmakv::MetaData metadata;
    metadata.setVersion(magmakv::MetaData::Version::V2);
    metadata.setExptime(111111);
    metadata.setDeleted(true, DeleteSource::TTL);
    metadata.setBySeqno(222222);
    metadata.setValueSize(333333);
    metadata.setDataType(2);
    metadata.setFlags(444444);
    metadata.setCas(555555);
    metadata.setRevSeqno(666666);

    // V2 can record the history setting
    metadata.setHistory(true);

    auto encoded = metadata.encode();
    auto decoded = magmakv::MetaData(encoded);
    EXPECT_TRUE(decoded.isHistoryEnabled());
    EXPECT_FALSE(decoded.isDurabilityDefined());
    EXPECT_EQ(metadata, decoded);

    // This sizing is borrowed from MetadataEncoding test, at this stage expect
    // the size of metadata to be as per V0 expectations.
    // Size should be smaller encoded (as some values get leb128 encoded and
    // will be smaller due to the values chosen above)
    auto totalSizeOfV0 = 1 /*sizeof(magmakv::MetaData::MetaDataV0*/ + 34;
    EXPECT_LT(encoded.size(), totalSizeOfV0);
    auto v0EncodedSize = encoded.size();

    // Now test we can set some durability info
    metadata.setDurabilityDetailsForAbort(7777777);
    encoded = metadata.encode();
    decoded = magmakv::MetaData(encoded);
    EXPECT_TRUE(decoded.isHistoryEnabled());
    EXPECT_TRUE(decoded.isDurabilityDefined());
    EXPECT_EQ(metadata, decoded);

    // Now expect to of grown to match V1 sizing
    auto sizeOfV1 = 6 /*sizeof(magmakv::MetaData::MetaDataV1)*/;
    auto totalSizeOfV1 = totalSizeOfV0 + sizeOfV1;
    EXPECT_LT(encoded.size(), totalSizeOfV1);

    auto v1v0EncodedSizeDiff = encoded.size() - v0EncodedSize;
    EXPECT_LT(v1v0EncodedSizeDiff, sizeOfV1);

    metadata.setHistory(true);

    encoded = metadata.encode();
    decoded = magmakv::MetaData(encoded);
    EXPECT_TRUE(decoded.isDurabilityDefined());
    EXPECT_TRUE(decoded.isHistoryEnabled());
    EXPECT_EQ(metadata, decoded);

    // And finally, no change, setting history and the data is the same
    EXPECT_LT(encoded.size(), totalSizeOfV1);
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
    Collections::VB::Manifest m{std::make_shared<Collections::Manager>()};
    VB::Commit meta(m);
    meta.proposedVBState = *kvstore->getCachedVBucketState(vbid);
    meta.proposedVBState.maxVisibleSeqno = 999;
    kvstore->snapshotVBucket(vbid, meta);

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
    auto encoded = expiredItem.encode();
    magma::Slice expiredItemSlice = {encoded};

    EXPECT_EQ(10, kvstore->getExpiryOrPurgeTime(expiredItemSlice));
}

TEST_F(MagmaKVStoreTest, MagmaGetExpiryTimeTombstone) {
    magmakv::MetaData tombstone;
    tombstone.setExptime(10);
    tombstone.setDeleted(true, DeleteSource::Explicit);
    auto encoded = tombstone.encode();
    magma::Slice tombstoneSlice = {encoded};

    EXPECT_EQ(10 + kvstoreConfig->getMetadataPurgeAge(),
              kvstore->getExpiryOrPurgeTime(tombstoneSlice));
}

TEST_F(MagmaKVStoreTest, ReadLocalDocErrorCode) {
    initialize_kv_store(kvstore.get(), vbid);

    auto res = kvstore->readLocalDoc(vbid, LocalDocKey::vbstate);
    EXPECT_EQ(magma::Status::Code::Ok, res.first.ErrorCode());

    EXPECT_TRUE(kvstore->deleteLocalDoc(vbid, LocalDocKey::vbstate).IsOK());
    res = kvstore->readLocalDoc(vbid, LocalDocKey::vbstate);
    EXPECT_EQ(magma::Status::Code::OkDocNotFound, res.first.ErrorCode());

    auto kvsRev = kvstore->prepareToDelete(Vbid(0));
    kvstore->delVBucket(vbid, std::move(kvsRev));

    res = kvstore->readLocalDoc(vbid, LocalDocKey::vbstate);
    EXPECT_EQ(magma::Status::Code::InvalidKVStore, res.first.ErrorCode());
}

TEST_F(MagmaKVStoreTest, KVStoreRevisionAfterReopen) {
    initialize_kv_store(kvstore.get(), vbid);

    auto kvsRev = kvstore->getKVStoreRevision(vbid);
    auto currRev = kvstore->prepareToDelete(vbid);
    EXPECT_EQ(kvsRev, currRev->getRevision());

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

    // Add an item to test that we can read it
    doWrite(1, true /*success*/);

    auto rv = kvstore->get(makeDiskDocKey("key"), Vbid(0));
    EXPECT_EQ(rv.getStatus(), cb::engine_errc::success);

    kvstore.reset();

    kvstoreConfig->setDefaultFS();

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

TEST_F(MagmaKVStoreTest, implicitCompactionCtxForNonExistantVbucket) {
    kvstore->setMakeCompactionContextCallback(
            [](Vbid vbid, CompactionConfig& config, uint64_t purgeSeqno) {
                return nullptr;
            });
    EXPECT_EQ(nullptr, kvstore->makeImplicitCompactionContext(Vbid(99)));
}

TEST_F(MagmaKVStoreTest, MB_49465) {
    // Set a dummy makeCompactionContextCallback, that will always return
    // nullptr
    kvstore->setMakeCompactionContextCallback(
            [](Vbid vbid, CompactionConfig& config, uint64_t purgeSeqno) {
                return nullptr;
            });
    // Create a MagmaCompactionCB so that we can ensure that we throw a run time
    // error if we're unable to create a compaction context
    EXPECT_THROW(kvstoreConfig->magmaCfg.MakeCompactionCallback(
                         magma::Magma::KVStoreID(99)),
                 std::runtime_error);
}

TEST_F(MagmaKVStoreTest, makeFileHandleSyncFailed) {
    initialize_kv_store(kvstore.get(), vbid);
    auto setupSyncStatus = [this](magma::Status hookStatus) -> void {
        kvstore->setSyncFileHandleStatusHook(
                [hookStatus](magma::Status& status) -> void {
                    status = hookStatus;
                });
    };
    setupSyncStatus(magma::Status(magma::Status::DiskFull, "DiskFull"));
    auto fileHandle = kvstore->makeFileHandle(vbid);
    EXPECT_TRUE(fileHandle);

    setupSyncStatus(magma::Status(magma::Status::ReadOnly, "ReadOnly"));
    fileHandle = kvstore->makeFileHandle(vbid);
    EXPECT_TRUE(fileHandle);

    setupSyncStatus(magma::Status(magma::Status::Internal, "Internal"));
    fileHandle = kvstore->makeFileHandle(vbid);
    EXPECT_FALSE(fileHandle);
}

TEST_F(MagmaKVStoreTest, diskSizeUpdateTracking) {
    // verify that MagmaRequests correctly track whether a new item's size is
    // included in the current disk size
    using namespace testing;

    uint64_t seqno{1};
    auto store = [this, &seqno](queued_item qi) {
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        qi->setBySeqno(seqno++);
        kvstore->set(*ctx, qi);
        VB::Commit commitData(manifest);
        kvstore->commit(std::move(ctx), commitData);
    };

    // forward to a simplified mock function for easier expectations along side
    // tracking the expected stats values
    StrictMock<MockFunction<void(bool)>> cb;

    auto expectedDiskSize = 0;

    kvstore->updateStatsHook = [&](const MagmaRequest& req, size_t oldSize) {
        expectedDiskSize -= oldSize;
        auto isInDiskSize = req.isNewDocReflectedInDiskSize();
        if (isInDiskSize) {
            expectedDiskSize += req.getDocSize();
        }

        cb.AsStdFunction()(isInDiskSize);
    };

    // check the disk size is initially zero
    ASSERT_EQ(getCollectionDiskSize(), 0);

    const auto metadataSize = magmakv::MetaData().encode().size();

    {
        // store a prepare, should _not_ be included in disk size
        // don't even expect the test hook to be called for non-committed
        // items
        EXPECT_CALL(cb, Call(_)).Times(0);
        auto qi = makePendingItem(makeStoredDocKey("key"), "value");
        store(qi);
        EXPECT_EQ(getCollectionDiskSize(), 0);
    }

    {
        // store an item, should be included in disk size
        EXPECT_CALL(cb, Call(true));
        auto qi = makeCommittedItem(makeStoredDocKey("key"), "value");
        store(qi);
        auto minExpectedSize =
                metadataSize + qi->getKey().size() + qi->getNBytes();
        EXPECT_GE(getCollectionDiskSize(), minExpectedSize);
    }

    {
        // delete the same item, tombstones should _not_ be included
        EXPECT_CALL(cb, Call(false));
        auto qi = makeCommittedItem(makeStoredDocKey("key"), "value");
        qi->setDeleted();
        store(qi);
        EXPECT_EQ(getCollectionDiskSize(), 0);
    }

    size_t largerExpectedSize;
    {
        // re-create item (bigger this time), should be included in disk size
        EXPECT_CALL(cb, Call(true));
        auto qi = makeCommittedItem(makeStoredDocKey("key"), "valuefoobarqux");
        store(qi);
        auto minExpectedSize =
                metadataSize + qi->getKey().size() + qi->getNBytes();
        EXPECT_GE(getCollectionDiskSize(), minExpectedSize);
        // stash that size to verify it goes down in the next section
        largerExpectedSize = minExpectedSize;
    }

    {
        // update without deleting (smaller this time), should be included in
        // disk size
        EXPECT_CALL(cb, Call(true));
        auto qi = makeCommittedItem(makeStoredDocKey("key"), "v");
        store(qi);
        auto minExpectedSize =
                metadataSize + qi->getKey().size() + qi->getNBytes();
        EXPECT_GE(getCollectionDiskSize(), minExpectedSize);
        EXPECT_LT(getCollectionDiskSize(), largerExpectedSize);
    }
}

MATCHER_P(FileOpTypeMatcher,
          expectedType,
          fmt::format("Check the type of the FileOp matches '{}'",
                      to_string(expectedType))) {
    return arg.type == expectedType;
}

class MagmaKVStoreFileOpsTest : public MagmaKVStoreTest {
public:
    void TearDown() override {
        tracker->startHook = nullptr;
        MagmaKVStoreTest::TearDown();
        tracker.reset();
    }

    std::unique_ptr<MockMagmaKVStore> createStore(
            MagmaKVStoreConfig& config) override {
        tracker = std::make_unique<FileOpsTracker>();
        config.setFileOpsTracker(*tracker);
        return std::make_unique<MockMagmaKVStore>(*kvstoreConfig);
    }

    std::unique_ptr<FileOpsTracker> tracker;
};

/// Vverify that the tracking FileSystem implementation is used.
TEST_F(MagmaKVStoreFileOpsTest, usesFileOpsTracker) {
    initialize_kv_store(kvstore.get(), vbid);

    StrictMock<MockFunction<void(const FileOp&)>> cb;
    tracker->startHook = cb.AsStdFunction();

    EXPECT_CALL(cb, Call(FileOpTypeMatcher(FileOp::Type::Write)));
    EXPECT_CALL(cb, Call(FileOpTypeMatcher(FileOp::Type::Sync)));

    doWrite(1, true /*success*/);
}

// Verify that the tracking FileSystem implementation defines all the functions
// that are in the base filesystem.
TEST_F(MagmaKVStoreFileOpsTest, trackingFileSystemPassthrough) {
    initialize_kv_store(kvstore.get(), vbid);
    ASSERT_NE(nullptr, kvstoreConfig->magmaCfg.FSHook);

    magma::DefaultFileSystem def;
    kvstoreConfig->magmaCfg.FSHook(def);

    EXPECT_NE(nullptr, def.MakeFile);
    EXPECT_NE(nullptr, def.MakeDirectory);
    EXPECT_NE(nullptr, def.Rename);
    EXPECT_NE(nullptr, def.Link);
    EXPECT_NE(nullptr, def.RemoveAllWithCallback);
}

class MagmaKVStoreOpenFailTest : public MagmaKVStoreTest {
protected:
    struct injected_error : std::runtime_error {
        injected_error() : runtime_error("Injected error") {
        }
    };

    void SetUp() override {
        MagmaKVStoreTest::SetUp();
        injectError = false;
    }

    std::unique_ptr<MockMagmaKVStore> createStore(
            MagmaKVStoreConfig& config) override {
        // FSHook is just a convenient way to force an exception during
        // initialisation.
        config.magmaCfg.FSHook = [this](auto& fs) {
            if (injectError) {
                throw injected_error();
            }
        };
        return std::make_unique<MockMagmaKVStore>(*kvstoreConfig);
    }

    bool injectError{false};
};

// Test that the logger is unregistered if KVStore initialization fails.
TEST_F(MagmaKVStoreOpenFailTest, loggerIsUnregistered) {
    {
        auto logger = kvstore->logger;
        ASSERT_TRUE(logger->isRegistered());
        kvstore.reset();
        ASSERT_FALSE(logger->isRegistered());
    }

    // Force the KVStore to fail during initialisation.
    injectError = true;
    ASSERT_THROW(createStore(*kvstoreConfig), injected_error);
    injectError = false;

    // Check that the new KVStore can register its logger.
    kvstore = createStore(*kvstoreConfig);
    EXPECT_TRUE(kvstore->logger->isRegistered());
}

class MagmaKVStoreHistoryTest : public MagmaKVStoreTest {
protected:
    void SetUp() override {
        configStr = "history_retention_bytes=104857600;";
        MagmaKVStoreTest::SetUp();
    }

    queued_item doWrite(uint64_t seqno,
                        bool expected,
                        const std::string& key = "key") {
        auto qi = makeCommittedItem(makeStoredDocKey(key),
                                    "seqno:" + std::to_string(seqno));
        qi->setBySeqno(seqno);
        qi->setCanDeduplicate(CanDeduplicate::No);
        setAndCommit(qi, expected);
        return qi;
    }

    void TearDown() override {
        MagmaKVStoreTest::TearDown();
    }
};

// Test scanAllVersions returns the expected number of keys and the expected
// history start seqno.
TEST_F(MagmaKVStoreHistoryTest, scanAllVersions1) {
    initialize_kv_store(kvstore.get(), vbid);

    // History is enabled
    flush.historical = CheckpointHistorical::Yes;

    std::vector<queued_item> expectedItems;

    expectedItems.push_back(doWrite(1, true, "k1"));
    expectedItems.push_back(doWrite(2, true, "k2"));
    auto validate = [&expectedItems](GetValue gv) {
        ASSERT_TRUE(gv.item);
        ASSERT_GE(expectedItems.size(), size_t(gv.item->getBySeqno()));
        EXPECT_EQ(*expectedItems[gv.item->getBySeqno() - 1], *gv.item);
    };
    auto bySeq = kvstore->initBySeqnoScanContext(
            std::make_unique<CustomCallback<GetValue>>(validate),
            std::make_unique<CustomCallback<CacheLookup>>(),
            vbid,
            1,
            DocumentFilter::ALL_ITEMS,
            ValueFilter::VALUES_DECOMPRESSED,
            SnapshotSource::Head);
    ASSERT_TRUE(bySeq);
    EXPECT_EQ(1, bySeq->historyStartSeqno);
    EXPECT_EQ(ScanStatus::Success, kvstore->scanAllVersions(*bySeq));

    auto& cb =
            static_cast<CustomCallback<GetValue>&>(bySeq->getValueCallback());
    EXPECT_EQ(2, cb.getProcessedCount());
}

// Validate the ordering by seqno
TEST_F(MagmaKVStoreTest, preparePendingRequestsHistoryEnabled) {
    MagmaKVStoreTransactionContext ctx(*kvstore, vbid, nullptr);

    // seqnos 5, 4, 3, 2, 1
    std::array<std::string, 5> keys = {{"bbb", "c", "bbb", "aaa", "bbb"}};
    uint64_t seqno = 5;
    for (const auto& k : keys) {
        auto qi = makeCommittedItem(makeStoredDocKey(k), "value");
        qi->setBySeqno(seqno--);
        ctx.pendingReqs.push_back(
                std::make_unique<MagmaRequest>(std::move(qi)));
    }

    ctx.preparePendingRequests(magma::Magma::HistoryMode::Enabled);

    std::array<std::pair<uint64_t, std::string>, 5> expected = {
            {{2, "aaa"}, {1, "bbb"}, {3, "bbb"}, {5, "bbb"}, {4, "c"}}};
    ASSERT_EQ(expected.size(), ctx.pendingReqs.size());
    auto itr = expected.begin();
    for (const auto& req : ctx.pendingReqs) {
        EXPECT_EQ(itr->first, req->getItem().getBySeqno()) << req->getItem();
        EXPECT_EQ("cid:0x0:" + itr->second,
                  req->getItem().getKey().to_string());
        ++itr;
    }
}

TEST_F(MagmaKVStoreTest, preparePendingRequestsWithPreparesHistoryEnabled) {
    MagmaKVStoreTransactionContext ctx(*kvstore, vbid, nullptr);

    uint64_t seqno = 11;
    auto qi = makeCommittedItem(makeStoredDocKey("bbb", CollectionID{99}), "v");
    qi->setBySeqno(seqno--);
    ctx.pendingReqs.push_back(std::make_unique<MagmaRequest>(std::move(qi)));

    std::array<std::string, 5> keys = {{"bbb", "c", "bbb", "aaa", "bbb"}};
    for (const auto& k : keys) {
        qi = makeCommittedItem(makeStoredDocKey(k), "value");
        qi->setBySeqno(seqno--);
        ctx.pendingReqs.push_back(
                std::make_unique<MagmaRequest>(std::move(qi)));

        qi = makePendingItem(makeStoredDocKey(k), "value");
        qi->setBySeqno(seqno--);
        ctx.pendingReqs.push_back(
                std::make_unique<MagmaRequest>(std::move(qi)));
    }

    ctx.preparePendingRequests(magma::Magma::HistoryMode::Enabled);
    std::array<std::pair<uint64_t, DiskDocKey>, 11> expected = {
            {{4, DiskDocKey{StoredDocKey{"aaa", CollectionID{0}}, false}},
             {2, DiskDocKey{StoredDocKey{"bbb", CollectionID{0}}, false}},
             {6, DiskDocKey{StoredDocKey{"bbb", CollectionID{0}}, false}},
             {10, DiskDocKey{StoredDocKey{"bbb", CollectionID{0}}, false}},
             {8, DiskDocKey{StoredDocKey{"c", CollectionID{0}}, false}},

             // pending writes are ordered (by-key) above the default collection
             {3, DiskDocKey{StoredDocKey{"aaa", CollectionID{0}}, true}},
             {1, DiskDocKey{StoredDocKey{"bbb", CollectionID{0}}, true}},
             {5, DiskDocKey{StoredDocKey{"bbb", CollectionID{0}}, true}},
             {9, DiskDocKey{StoredDocKey{"bbb", CollectionID{0}}, true}},
             {7, DiskDocKey{StoredDocKey{"c", CollectionID{0}}, true}},

             // but would be below collection 99
             {11, DiskDocKey{StoredDocKey{"bbb", CollectionID{99}}, false}}}};

    ASSERT_EQ(expected.size(), ctx.pendingReqs.size());
    auto itr = expected.begin();
    for (const auto& req : ctx.pendingReqs) {
        EXPECT_EQ(itr->first, req->getItem().getBySeqno()) << req->getItem();
        EXPECT_EQ(req->getItem().getKey(), itr->second.getDocKey());
        ++itr;
    }
}

// Validate the ordering by seqno
TEST_F(MagmaKVStoreTest, preparePendingRequestsHistoryDisabled) {
    MagmaKVStoreTransactionContext ctx(*kvstore, vbid, nullptr);

    // seqnos 5, 4, 3, 2, 1
    std::array<std::string, 5> keys = {{"bbb", "c", "bbb", "aaa", "bbb"}};
    uint64_t seqno = 5;
    for (const auto& k : keys) {
        auto qi = makeCommittedItem(makeStoredDocKey(k), "value");
        qi->setBySeqno(seqno--);
        ctx.pendingReqs.push_back(
                std::make_unique<MagmaRequest>(std::move(qi)));
    }

    ctx.preparePendingRequests(magma::Magma::HistoryMode::Disabled);

    std::array<std::pair<uint64_t, std::string>, 5> expected = {
            {{5, "bbb"}, {4, "c"}, {3, "bbb"}, {2, "aaa"}, {1, "bbb"}}};
    ASSERT_EQ(expected.size(), ctx.pendingReqs.size());
    auto itr = expected.begin();
    for (const auto& req : ctx.pendingReqs) {
        EXPECT_EQ(itr->first, req->getItem().getBySeqno()) << req->getItem();
        EXPECT_EQ("cid:0x0:" + itr->second,
                  req->getItem().getKey().to_string());
        ++itr;
    }
}

// Test scanAllVersions returns the expected number of keys and the expected
// history start seqno. This test uses the same key for all mutations.
TEST_F(MagmaKVStoreHistoryTest, scanAllVersions2) {
    initialize_kv_store(kvstore.get(), vbid);
    flush.historical = CheckpointHistorical::Yes;

    std::vector<queued_item> expectedItems;
    // doWrite writes the same key
    expectedItems.push_back(doWrite(1, true));
    expectedItems.push_back(doWrite(2, true));
    auto validate = [&expectedItems](GetValue gv) {
        ASSERT_TRUE(gv.item);
        ASSERT_GE(expectedItems.size(), size_t(gv.item->getBySeqno()));
        EXPECT_EQ(*expectedItems[gv.item->getBySeqno() - 1], *gv.item);
    };
    auto bySeq = kvstore->initBySeqnoScanContext(
            std::make_unique<CustomCallback<GetValue>>(validate),
            std::make_unique<CustomCallback<CacheLookup>>(),
            vbid,
            1,
            DocumentFilter::ALL_ITEMS,
            ValueFilter::VALUES_DECOMPRESSED,
            SnapshotSource::Head);
    ASSERT_TRUE(bySeq);
    EXPECT_EQ(1, bySeq->historyStartSeqno);
    EXPECT_EQ(ScanStatus::Success, kvstore->scanAllVersions(*bySeq));

    auto& cb =
            static_cast<CustomCallback<GetValue>&>(bySeq->getValueCallback());
    EXPECT_EQ(2, cb.getProcessedCount());
}

// ScanContext now exposes historyStartSeqno which is tracked by magma provided
// it is retaining history.
TEST_F(MagmaKVStoreHistoryTest, historyStartSeqno) {
    initialize_kv_store(kvstore.get(), vbid);

    auto validate = [this](uint64_t expectedHistoryStartSeqno) {
        EXPECT_EQ(expectedHistoryStartSeqno,
                  kvstore->getHistoryStartSeqno(vbid).value_or(~0ull));
        auto bySeq = kvstore->initBySeqnoScanContext(
                std::make_unique<GetCallback>(true /*expectcompressed*/),
                std::make_unique<KVStoreTestCacheCallback>(1, 5, Vbid(0)),
                vbid,
                1,
                DocumentFilter::ALL_ITEMS,
                ValueFilter::VALUES_COMPRESSED,
                SnapshotSource::Head);
        auto byId = kvstore->initByIdScanContext(
                std::make_unique<GetCallback>(true /*expectcompressed*/),
                std::make_unique<KVStoreTestCacheCallback>(1, 5, Vbid(0)),
                vbid,
                {},
                DocumentFilter::ALL_ITEMS,
                ValueFilter::VALUES_COMPRESSED);
        ASSERT_TRUE(bySeq);
        ASSERT_TRUE(byId);

        EXPECT_EQ(expectedHistoryStartSeqno, bySeq->historyStartSeqno);
        EXPECT_EQ(bySeq->historyStartSeqno, byId->historyStartSeqno);
    };

    flush.historical = CheckpointHistorical::No;
    CB_SCOPED_TRACE("History OFF");
    validate(0); // no flush yet - and no history
    doWrite(2, true); // write seqno 2
    validate(0);

    // Now enable history
    flush.historical = CheckpointHistorical::Yes;
    CB_SCOPED_TRACE("History ON flushed seqno 3");
    doWrite(3, true); // write seqno 3
    validate(3);
    CB_SCOPED_TRACE("History ON flushed seqno 4");
    doWrite(4, true); // write seqno 4
    validate(3); // history still starts at 3

    flush.historical = CheckpointHistorical::No;
    doWrite(5, true); // write seqno 5
    validate(0); // back to no history
}

TEST_F(MagmaKVStoreHistoryTest, restartKvStore) {
    initialize_kv_store(kvstore.get(), vbid);

    // Now enable history
    flush.historical = CheckpointHistorical::Yes;
    doWrite(1, true);
    EXPECT_EQ(1, kvstore->getHistoryStartSeqno(vbid).value_or(~0ull));

    kvstore->deinitialize();
    // New KVStore
    kvstore = std::make_unique<MockMagmaKVStore>(*kvstoreConfig);

    // MB-55533: before this bug was closed history was lost
    EXPECT_EQ(1, kvstore->getHistoryStartSeqno(vbid).value_or(~0ull));
}

class MagmaKVStoreConfigTest : public MagmaKVStoreTest {
    void SetUp() override {
        configStr =
                "magma_seq_tree_data_block_size=5555;"
                "magma_min_value_block_size_threshold=131072;"
                "magma_seq_tree_index_block_size=6666;"
                "magma_key_tree_data_block_size=7777;"
                "magma_key_tree_index_block_size=8888;";

        MagmaKVStoreTest::SetUp();
    }
};

TEST_F(MagmaKVStoreConfigTest, TestMagmaConfig) {
    EXPECT_EQ(kvstoreConfig->getMagmaSeqTreeDataBlockSize(), 5555);
    EXPECT_EQ(kvstoreConfig->getMagmaMinValueBlockSizeThreshold(), 131072);
    EXPECT_EQ(kvstoreConfig->getMagmaSeqTreeIndexBlockSize(), 6666);
    EXPECT_EQ(kvstoreConfig->getMagmaKeyTreeDataBlockSize(), 7777);
    EXPECT_EQ(kvstoreConfig->getMagmaKeyTreeIndexBlockSize(), 8888);
}
