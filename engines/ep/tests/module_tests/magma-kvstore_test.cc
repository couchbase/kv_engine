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
#include "kvstore_test.h"
#include "magma-kvstore/magma-kvstore_config.h"
#include "programs/engine_testapp/mock_server.h"
#include "test_helpers.h"
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
