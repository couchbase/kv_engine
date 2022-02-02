/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "evp_store_single_threaded_test.h"

#include "../couchstore/src/internal.h"
#include "../mock/mock_magma_kvstore.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "collections/collection_persisted_stats.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_bucket.h"
#include "test_helpers.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/test_fileops.h"
#include "vbucket.h"

#include <folly/portability/GMock.h>

using FlushResult = EPBucket::FlushResult;
using MoreAvailable = EPBucket::MoreAvailable;
using WakeCkptRemover = EPBucket::WakeCkptRemover;

/**
 * Error injector interface with implementations for each KVStore that we care
 * to test. This class/test fixture serves a different purpose to the
 * MockKVStore which we use to test Bucket/VBucket code. The purpose of the
 * ErrorInjector class/test fixture is to test changes in KVStore et. al. by
 * injecting errors after we run as much KVStore code as possible.
 */
class ErrorInjector {
public:
    virtual ~ErrorInjector() = default;

    /**
     * Make the next KVStore::commit (flush) operation fail
     */
    virtual void failNextCommit() = 0;
};

class CouchKVStoreErrorInjector : public ErrorInjector {
public:
    CouchKVStoreErrorInjector(KVBucketTest& test)
        : ops(create_default_file_ops()) {
        test.replaceCouchKVStore(ops);
    }

    void failNextCommit() override {
        using namespace testing;
        EXPECT_CALL(ops, sync(_, _))
                .WillOnce(Return(COUCHSTORE_ERROR_WRITE))
                .WillRepeatedly(Return(COUCHSTORE_SUCCESS));
    }

protected:
    ::testing::NiceMock<MockOps> ops;
};

#ifdef EP_USE_MAGMA
class MagmaKVStoreErrorInjector : public ErrorInjector {
public:
    MagmaKVStoreErrorInjector(KVBucketTest& test) {
        test.replaceMagmaKVStore();
        kvstore = dynamic_cast<MockMagmaKVStore*>(
                test.store->getRWUnderlying(test.vbid));
    }

    void failNextCommit() override {
        kvstore->saveDocsErrorInjector = [this](VB::Commit& cmt,
                                                kvstats_ctx& ctx) -> int {
            kvstore->saveDocsErrorInjector = nullptr;
            return magma::Status::IOError;
        };
    }

    MockMagmaKVStore* kvstore;
};
#endif

class KVStoreErrorInjectionTest : public STParamPersistentBucketTest {
public:
    void SetUp() override {
        STParamPersistentBucketTest::SetUp();
        createErrorInjector();
    }

    void createErrorInjector() {
        if (isCouchstore()) {
            errorInjector = std::make_unique<CouchKVStoreErrorInjector>(*this);
#ifdef EP_USE_MAGMA
        } else if (isMagma()) {
            errorInjector = std::make_unique<MagmaKVStoreErrorInjector>(*this);
#endif
        } else {
            throw std::invalid_argument(
                    "KVStoreErrorInjectionTest::createErrorInjector invalid "
                    "backend");
        }
    }

protected:
    std::unique_ptr<ErrorInjector> errorInjector;
};

TEST_P(KVStoreErrorInjectionTest, ItemCountsAndCommitFailure_MB_41321) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value");

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto vb = engine->getKVBucket()->getVBucket(vbid);
    auto stats =
            vb->getManifest().lock(CollectionID::Default).getPersistedStats();
    EXPECT_EQ(1, stats.itemCount);
    EXPECT_EQ(1, stats.highSeqno);
    EXPECT_NE(0, stats.diskSize);

    // Delete our key
    delete_item(vbid, key);

    // Expect the flush of our delete to fail twice. This would see an underflow
    // exception before the fix for MB-41321 as we would decrement the item
    // count from 1 to 0 and then try for -1
    auto flushAndExpectFailure = [this](int expectedCommitFailed) {
        auto flushResult = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
        EXPECT_EQ(EPBucket::MoreAvailable::Yes, flushResult.moreAvailable);
        EXPECT_EQ(0, flushResult.numFlushed);
        EXPECT_EQ(EPBucket::WakeCkptRemover::No, flushResult.wakeupCkptRemover);
        EXPECT_EQ(expectedCommitFailed, engine->getEpStats().commitFailed);
        auto vb = engine->getKVBucket()->getVBucket(vbid);

        // validate the default collection hasn't changed
        auto stats = vb->getManifest()
                             .lock(CollectionID::Default)
                             .getPersistedStats();
        EXPECT_EQ(1, stats.itemCount);
        EXPECT_EQ(1, stats.highSeqno);
        EXPECT_NE(0, stats.diskSize);
    };

    errorInjector->failNextCommit();
    flushAndExpectFailure(1);

    errorInjector->failNextCommit();
    flushAndExpectFailure(2);

    // Now a successful flush which will update the stats
    auto res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
    EXPECT_EQ(EPBucket::MoreAvailable::No, res.moreAvailable);
    EXPECT_EQ(1, res.numFlushed);
    EXPECT_EQ(EPBucket::WakeCkptRemover::No, res.wakeupCkptRemover);
    EXPECT_EQ(2, engine->getEpStats().commitFailed);
    stats = vb->getManifest().lock(CollectionID::Default).getPersistedStats();
    EXPECT_EQ(0, stats.itemCount);
    EXPECT_EQ(2, stats.highSeqno);

    if (isMagma()) {
        // Magma does not track tombstones in disk size as compaction may visit
        // stale values so it's not possible to decrement on purge
        EXPECT_EQ(0, stats.diskSize);
    } else {
        EXPECT_GT(stats.diskSize, 0); // tombstone data remains
    }
}

TEST_P(KVStoreErrorInjectionTest, FlushFailureAtPersistingCollectionChange) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    CollectionsManifest cm(CollectionEntry::dairy);
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));

    // Check nothing persisted to disk, only default collection exists
    auto* kvstore = store->getRWUnderlying(vbid);
    auto [s1, m1] = kvstore->getCollectionsManifest(vbid);
    ASSERT_TRUE(s1);
    EXPECT_EQ(1, m1.collections.size());
    const Collections::CollectionMetaData defaultState;
    EXPECT_EQ(defaultState, m1.collections[0].metaData);
    EXPECT_EQ(0, m1.collections[0].startSeqno);
    // This flush fails, we have not written anything to disk
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    {
        errorInjector->failNextCommit();
        EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
                  epBucket.flushVBucket(vbid));
        // Flush stats not updated
        EXPECT_EQ(1, vb->dirtyQueueSize);
    }

    // Check nothing persisted to disk, only default collection exists
    auto [s2, m2] = kvstore->getCollectionsManifest(vbid);
    ASSERT_TRUE(s2);
    EXPECT_EQ(1, m2.collections.size());
    EXPECT_EQ(defaultState, m2.collections[0].metaData);
    EXPECT_EQ(0, m2.collections[0].startSeqno);

    // This flush succeeds
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats updated
    EXPECT_EQ(0, vb->dirtyQueueSize);

    auto [s3, m3] = kvstore->getCollectionsManifest(vbid);
    ASSERT_TRUE(s3);
    EXPECT_EQ(2, m3.collections.size());

    Collections::CollectionMetaData dairyState{ScopeID::Default,
                                               CollectionEntry::dairy,
                                               CollectionEntry::dairy.name,
                                               {/*no ttl*/}};
    // no ordering of returned collections, both default and dairy must exist
    for (const auto& c : m3.collections) {
        if (c.metaData.cid == CollectionID::Default) {
            EXPECT_EQ(c.metaData, defaultState);
            EXPECT_EQ(0, c.startSeqno);
        } else {
            EXPECT_EQ(c.metaData, dairyState);
            EXPECT_EQ(1, c.startSeqno);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
        CouchstoreOrMagma,
        KVStoreErrorInjectionTest,
        STParameterizedBucketTest::persistentNoNexusConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);