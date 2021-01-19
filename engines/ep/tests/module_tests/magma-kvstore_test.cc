/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "../mock/mock_magma_kvstore.h"
#include "configuration.h"
#include "kvstore_test.h"
#include "magma-kvstore/magma-kvstore_config.h"
#include "magma-kvstore/magma-kvstore_metadata.h"
#include "programs/engine_testapp/mock_server.h"
#include "test_helpers.h"
#include "thread_gate.h"
#include "workload.h"

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

    auto vbs = kvstore->getCachedVBucketState(Vbid(0));
    ASSERT_EQ(uint64_t(5), vbs->highSeqno);
    ASSERT_EQ(size_t(5), kvstore->getItemCount(Vbid(0)));
}

TEST_F(MagmaKVStoreRollbackTest, RollbackNoValidCheckpoint) {
    uint64_t seqno = 1;

    auto cfg = reinterpret_cast<MagmaKVStoreConfig*>(kvstoreConfig.get());
    auto maxCheckpoints = cfg->getMagmaMaxCheckpoints();

    for (int i = 0; i < int(maxCheckpoints) + 1; i++) {
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
    ASSERT_EQ(0, int(kvsRev));
    EXPECT_NO_THROW(kvstore->prepareToCreate(Vbid(0)));
}

TEST_F(MagmaKVStoreTest, getStat) {
    size_t val;
    ASSERT_FALSE(kvstore->getStat("foobar", val));
    ASSERT_TRUE(kvstore->getStat("memory_quota", val));
}

// TODO: MB-40008: Disabled as the test has started recently failing.
TEST_F(MagmaKVStoreTest, DISABLED_setMaxDataSize) {
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
    auto newCompaction = kvstoreConfig->magmaCfg.MakeCompactionCallback();
    magma::Slice key;
    magma::Slice value;
    // Require a valid metadata slice to (a) ensure the item isn't just
    // skipped (zero-length meta == local document) and (b) to provide a valid
    // Vbid.
    magmakv::MetaData metadata;
    metadata.vbid = 0;
    magma::Slice meta{reinterpret_cast<char*>(&metadata), sizeof(metadata)};
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

TEST_F(MagmaKVStoreTest, MagmaGetExpiryTimeAlive) {
    magmakv::MetaData expiredItem;
    expiredItem.exptime = 10;
    magma::Slice expiredItemSlice = {reinterpret_cast<char*>(&expiredItem),
                                     sizeof(magmakv::MetaData)};

    EXPECT_EQ(10, kvstore->getExpiryOrPurgeTime(expiredItemSlice));
}

TEST_F(MagmaKVStoreTest, MagmaGetExpiryTimeTombstone) {
    magmakv::MetaData tombstone;
    tombstone.exptime = 10;
    tombstone.deleted = true;
    magma::Slice tombstoneSlice = {reinterpret_cast<char*>(&tombstone),
                                   sizeof(magmakv::MetaData)};

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
