/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "evp_store_single_threaded_test.h"

#include "../couchstore/src/internal.h"
#include "../couchstore/tests/test_fileops.h"
#include "../mock/mock_ep_bucket.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint_utils.h"
#include "collections/collection_persisted_stats.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_vb.h"
#include "kvstore/couch-kvstore/couch-kvstore.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/test_helpers.h"
#include "thread_gate.h"

#include <utilities/test_manifest.h>

#include <thread>

using FlushResult = EPBucket::FlushResult;
using MoreAvailable = EPBucket::MoreAvailable;

class STParamCouchstoreBucketTest : public STParamPersistentBucketTest {};

TEST_P(STParamCouchstoreBucketTest, MB_44098_compactionFailureLeavesNewFile) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Replace compaction completion function with one which throws
    dynamic_cast<MockEPBucket*>(store)->mockMakeCompactionContext =
            [](std::shared_ptr<CompactionContext> ctx) {
                ctx->completionCallback = [](CompactionContext& ctx) {
                    throw std::logic_error("forcing compaction to fail");
                };
                return ctx;
            };

    // Store some data into the vbucket (and flush)
    auto key = makeStoredDocKey("keyB");
    store_item(vbid, key, "value");

    // flush to vbid.couch.1
    flush_vbucket_to_disk(vbid, 1);

    // Run compaction which will fail - but must not leave vbid.couch.2
    runCompaction(vbid);

    // Now delete and recreate the vbucket, the new vb will use vbid.couch.2
    // and the test expects to not be able to read the key
    store->deleteVBucket(vbid, cookie);
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // To demonstrate the MB without to much work, disable the bloomfilter.
    // Prior to the fix, we can fetch the key which was written before the
    // deleteVBucket/createVbucket
    store->getVBucket(vbid)->setFilterStatus(BFILTER_DISABLED);

    auto gv = store->get(key, vbid, cookie, QUEUE_BG_FETCH);

    if (gv.getStatus() == cb::engine_errc::would_block) {
        runBGFetcherTask();
        gv = store->get(key, vbid, cookie, {});
    }

    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());
}

INSTANTIATE_TEST_SUITE_P(STParamCouchstoreBucketTest,
                         STParamCouchstoreBucketTest,
                         STParameterizedBucketTest::couchstoreConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

TEST_P(STParamCouchstoreBucketTest, FlusherMarksCleanBySeqno) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Used to synchronize this-thread (which simulate a frontend thread) and
    // the flusher-thread below (which simulate the flusher running in a bg
    // thread) so that we produce the exec interleaving of a scenario that
    // allows the user reading a stale seqno from disk.
    // Before the fix, that is possible because he flusher marks-clean items in
    // in the HashTable by CAS. Fixed by using Seqno instead.
    // Note: The scenario showed here a perfectly legal case of XDCR setup where
    // 2 different source clusters replicate to the same destination cluster.
    ThreadGate tg{2};

    auto& kvstore = dynamic_cast<CouchKVStore&>(*store->getRWUnderlying(vbid));
    kvstore.setPostFlushHook([&tg]() {
        // The hook is executed after we have flushed to disk but before we call
        // back into the PersistenceCallback. Here we use the hook only for
        // blocking the flusher and allowing a frontend write before it proceeds
        tg.threadUp();
    });

    const std::string key = "key";
    const auto setWithMeta = [this, &key](uint64_t cas,
                                          uint64_t revSeqno,
                                          uint64_t expectedSeqno) -> void {
        const std::string value = "value";
        const auto valBuf = cb::const_byte_buffer{
                reinterpret_cast<const uint8_t*>(key.data()), key.size()};
        uint64_t opCas = 0;
        uint64_t seqno = 0;
        const auto res = engine->public_setWithMeta(
                vbid,
                engine->public_makeDocKey(cookie, key),
                valBuf,
                {cas, revSeqno, 0 /*flags*/, 0 /*exp*/},
                false /*isDeleted*/,
                PROTOCOL_BINARY_RAW_BYTES,
                opCas,
                &seqno,
                cookie,
                {vbucket_state_active} /*permittedVBStates*/,
                CheckConflicts::Yes,
                true /*allowExisting*/,
                GenerateBySeqno::Yes,
                GenerateCas::No,
                {} /*extendedMetaData*/);
        ASSERT_EQ(cb::engine_errc::success, res);
        EXPECT_EQ(cas, opCas); // Note: CAS is not regenerated
        EXPECT_EQ(expectedSeqno, seqno);
    };

    // This-thread issues the first setWithMeta(s:1) and then blocks.
    // It must resume only when the flusher has persisted but not yet executed
    // into the PersistenceCallback.
    const uint64_t cas = 0x0123456789abcdef;
    {
        SCOPED_TRACE("");
        setWithMeta(cas, 1 /*revSeqno*/, 1 /*expectedSeqno*/);
    }
    auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    ASSERT_EQ(1, vb.checkpointManager->getNumItemsForPersistence());

    // Run the flusher in a bg-thread
    const auto flush = [this]() -> void {
        auto& epBucket = dynamic_cast<EPBucket&>(*store);
        const auto res = epBucket.flushVBucket(vbid);
        EXPECT_EQ(FlushResult(MoreAvailable::No, 1), res);
    };
    auto flusher = std::thread(flush);

    // This-thread issues a second setWithMeta(s:2), but only when the the
    // flusher is blocked into the postFlushHook.
    while (tg.getCount() < 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // setWithMeta(s:2) with same CAS and higher revSeqno, so s:2 wins conflict
    // resolution and the operation succeeds.
    {
        SCOPED_TRACE("");
        setWithMeta(cas, 2 /*revSeqno*/, 2 /*expectedSeqno*/);
    }

    // Now I want the flusher to proceed and call into the PersistenceCallback.
    // Before the fix, the flusher uses CAS for identifying the StoredValue to
    // mark clean in the HashTable, so in this scenario that makes clean a
    // StoreValue (s:2) that has never been persisted.
    // Note: The flusher was already running before s:2 was queued for
    // persistence, so s:2 is not being persisted in this flusher run.
    tg.threadUp();
    flusher.join();

    // Now the ItemPager runs. In the HashTable we have s:2: it must be dirty
    // and so not eligible for eviction.
    auto& epVB = dynamic_cast<EPVBucket&>(vb);
    const auto docKey = makeStoredDocKey(key);
    {
        const auto readHandle = vb.lockCollections();
        auto res = vb.ht.findOnlyCommitted(docKey);
        ASSERT_TRUE(res.storedValue);
        ASSERT_EQ(2, res.storedValue->getBySeqno());
        EXPECT_TRUE(res.storedValue->isDirty());
        EXPECT_FALSE(
                epVB.pageOut(readHandle, res.lock, res.storedValue, false));
    }

    // Note: The flusher has never persisted s:2
    ASSERT_EQ(1, vb.getPersistenceSeqno());

    // Try a get, it must fetch s:2 from the HashTable
    const auto res = engine->get(*cookie, docKey, vbid, DocStateFilter::Alive);
    // Note: Before the fix we get EWOULDBLOCK as s:2 would be evicted
    ASSERT_EQ(cb::engine_errc::success, res.first);
    const auto* it = reinterpret_cast<const Item*>(res.second.get());
    EXPECT_EQ(2, it->getBySeqno());
}

TEST_P(STParamCouchstoreBucketTest, DeleteUpdatesPersistedDeletes) {
    store->setVBucketState(vbid, vbucket_state_active);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    EXPECT_EQ(0, vb->getNumPersistedDeletes());

    store_item(vbid,
               makeStoredDocKey("keyA"),
               "value",
               0 /*exptime*/,
               {cb::engine_errc::success} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES);
    delete_item(vbid, makeStoredDocKey("keyA"));

    flushVBucketToDiskIfPersistent(vbid, 1);

    // Before the bug fix the stat would be wrong as we'd read from the RO
    // store but only update the cached value in the RW store.
    EXPECT_EQ(1, vb->getNumPersistedDeletes());
}

void STParamPersistentBucketTest::testCompactionPersistedDeletes(
        bool dropDeletes) {
    store->setVBucketState(vbid, vbucket_state_active);

    flushVBucketToDiskIfPersistent(vbid, 0);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    ASSERT_NE(0, vb->getFilterSize());

    // Stat should be correct and we should populate the cached value
    EXPECT_EQ(0, vb->getNumPersistedDeletes());

    // Persist first delete
    store_item(vbid,
               makeStoredDocKey("keyA"),
               "value",
               0 /*exptime*/,
               {cb::engine_errc::success} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES);
    delete_item(vbid, makeStoredDocKey("keyA"));

    store_item(vbid,
               makeStoredDocKey("keyB"),
               "value",
               0 /*exptime*/,
               {cb::engine_errc::success} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES);
    delete_item(vbid, makeStoredDocKey("keyB"));

    flushVBucketToDiskIfPersistent(vbid, 2);

    EXPECT_EQ(2, vb->getNumPersistedDeletes());

    runCompaction(vbid, 0, dropDeletes);
}

TEST_P(STParamCouchstoreBucketTest, CompactionUpdatesPersistedDeletes) {
    testCompactionPersistedDeletes(true /*dropDeletes*/);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    // Before the bug fix the stat would be wrong as we'd read from the RO
    // store but only update the cached value in the RW store. This won't be 0
    // even though we have 2 deletes as we keep the last item during a
    // compaction.
    EXPECT_EQ(1, vb->getNumPersistedDeletes());
}

TEST_P(STParamCouchstoreBucketTest, CompactionUpdatesBloomFilter) {
    engine->getConfiguration().setBfilterKeyCount(1);

    testCompactionPersistedDeletes(false /*dropDeletes*/);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    // Before the bug fix the stat would be wrong as we'd read from the RO
    // store but only update the cached value in the RW store.
    EXPECT_EQ(2, vb->getNumPersistedDeletes());

    auto expected = 29;
    if (fullEviction()) {
        expected = 10;
    }
    EXPECT_EQ(expected, vb->getFilterSize());
}

/**
 * MB-42224: The test verifies that a failure in the header-sync phase at
 * flush-vbucket causes couchstore auto-retry. Also, the test verifies that
 * relevant stats are correctly updated when finally persistence succeeds.
 */
TEST_P(STParamCouchstoreBucketTest, HeaderSyncFails) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Use mock ops to inject syscall failures
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    // We do 2 syncs per flush. First is for data sync, second is for header
    // sync. In this test we test we fail the second sync, which means that:
    // - All data (docs+local) will be flushed to the OSBC and sync'ed to disk
    // - New header flushed to OSBC but not sync'ed to disk
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::DoDefault())
            .WillOnce(testing::Return(COUCHSTORE_ERROR_WRITE))
            .WillRepeatedly(testing::DoDefault());

    const auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getNumItems());
    auto* kvstore = store->getRWUnderlying(vbid);
    auto diskState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, diskState.status);
    ASSERT_EQ(0, diskState.state.onDiskPrepares);
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.commitFailed);
    ASSERT_EQ(1, stats.flusherCommits);

    // Store a mutation (that is for checking our num-items)
    const auto keyM = makeStoredDocKey("keyM");
    const auto item = makeCommittedItem(keyM, "value");
    EXPECT_EQ(cb::engine_errc::success, store->set(*item, cookie));

    // Store a prepare (that is for checking out on-dick-prepares)
    const auto key = makeStoredDocKey("key");
    const auto prepare =
            makePendingItem(key, "value", cb::durability::Requirements());
    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*prepare, cookie));

    // Try persistence, it fails the first attempt but couchstore auto-retry
    // succeeds at the second attempt.
    auto res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
    EXPECT_EQ(EPBucket::MoreAvailable::No, res.moreAvailable);
    EXPECT_EQ(2, res.numFlushed);
    EXPECT_EQ(1, stats.commitFailed); // This indicates that we failed once
    EXPECT_EQ(2, stats.flusherCommits);
    EXPECT_EQ(1, vb.getNumTotalItems());
    diskState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, diskState.status);
    EXPECT_EQ(1, diskState.state.onDiskPrepares);
    EXPECT_EQ(1, kvstore->getCachedVBucketState(vbid)->onDiskPrepares);
}

/**
 * MB-42224: This test doesn't cover any fix, it just shows how
 * KVStore::snapshotVBuckets behaves if the sync-header fails at commit.
 */
TEST_P(STParamCouchstoreBucketTest, HeaderSyncFails_VBStateOnly) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto* kvstore = dynamic_cast<CouchKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(kvstore);
    auto diskState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, diskState.status);
    ASSERT_EQ(vbucket_state_active, diskState.state.transition.state);
    ASSERT_EQ(vbucket_state_active,
              kvstore->getCachedVBucketState(vbid)->transition.state);
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.commitFailed);
    ASSERT_EQ(1, stats.flusherCommits);

    // Flush and verify nothing in the persistence queue.
    auto& bucket = dynamic_cast<EPBucket&>(*store);
    auto res = bucket.flushVBucket(vbid);
    using MoreAvailable = EPBucket::MoreAvailable;
    EXPECT_EQ(MoreAvailable::No, res.moreAvailable);
    EXPECT_EQ(0, stats.commitFailed);
    EXPECT_EQ(1, stats.flusherCommits);

    // Use mock ops to inject syscall failures.
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::DoDefault()) // data
            .WillOnce(testing::Return(COUCHSTORE_ERROR_WRITE)) // header
            .WillRepeatedly(testing::DoDefault());
    kvstore = dynamic_cast<CouchKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(kvstore);

    // Set new vbstate in memory only
    setVBucketState(vbid, vbucket_state_replica);
    diskState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, diskState.status);
    EXPECT_EQ(vbucket_state_active, diskState.state.transition.state);
    EXPECT_EQ(vbucket_state_active,
              kvstore->getCachedVBucketState(vbid)->transition.state);

    // Flush and verify failure
    res = bucket.flushVBucket(vbid);
    EXPECT_EQ(MoreAvailable::Yes, res.moreAvailable);
    EXPECT_EQ(1, stats.commitFailed);
    EXPECT_EQ(1, stats.flusherCommits);

    // Given that write-data has succeded, we have written the new vbstate to
    // the OS buffer cache, so we do see the new vbstate when we make a VFS read
    diskState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, diskState.status);
    EXPECT_EQ(vbucket_state_replica, diskState.state.transition.state);
    // Note: Cached vbstate is updated only if commit succeeds, so we didn't
    // reach the point where we update it.
    EXPECT_EQ(vbucket_state_active,
              kvstore->getCachedVBucketState(vbid)->transition.state);

    // The next flush attempt succeeds, cached/on-disk vbstate aligned
    res = bucket.flushVBucket(vbid);
    EXPECT_EQ(MoreAvailable::No, res.moreAvailable);
    EXPECT_EQ(1, stats.commitFailed);
    EXPECT_EQ(2, stats.flusherCommits);
    diskState = kvstore->getPersistedVBucketState(vbid);
    ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, diskState.status);
    EXPECT_EQ(vbucket_state_replica, diskState.state.transition.state);
    EXPECT_EQ(vbucket_state_replica,
              kvstore->getCachedVBucketState(vbid)->transition.state);
}

TEST_P(STParamCouchstoreBucketTest,
       BootstrapProcedureLeavesNoCorruptedFileAtFailure) {
    using namespace testing;

    // Always fail the pwrite syscall for inducing the creation of an empty
    // couchstore file when the first flush fails.
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    EXPECT_CALL(ops, pwrite(_, _, _, _, _))
            .Times(2)
            .WillRepeatedly(Return(COUCHSTORE_ERROR_WRITE));
    auto* kvstore = dynamic_cast<CouchKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(kvstore);

    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.commitFailed);
    ASSERT_EQ(0, stats.flusherCommits);

    auto vb = store->getVBucket(vbid);
    ASSERT_FALSE(vb);

    // The store has never received a SetVBstate yet, verify no file on disk
    const auto verifyNoFile = [this, &kvstore]() -> void {
        bool fileNotFound = false;
        try {
            kvstore->getPersistedVBucketState(vbid);
        } catch (const std::logic_error& e) {
            ASSERT_THAT(e.what(), HasSubstr("openDB error:no such file"));
            fileNotFound = true;
        }
        ASSERT_TRUE(fileNotFound);
    };
    verifyNoFile();

    setVBucketState(vbid, vbucket_state_replica);
    vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    verifyNoFile();

    // Flush fails
    auto& ep = dynamic_cast<EPBucket&>(*store);
    const auto res = ep.flushVBucket(vbid);
    EXPECT_EQ(MoreAvailable::Yes, res.moreAvailable);
    EXPECT_EQ(0, res.numFlushed);
    EXPECT_EQ(1, stats.commitFailed);
    EXPECT_EQ(0, stats.flusherCommits);

    // Before the fix we fail with "no header in non-empty file" if we try to
    // open the database at this point, as flush has failed but we have left an
    // empty file behind.
    // A practical example is a restart. We'll try to initialize KVStore and
    // fail, and we'll never recover automatically from that state.
    //
    // After the fix persistence never creates empty files, so we see "no such
    // file" if we try to read from disk here. At restart we are fine as we just
    // don't see any file.
    verifyNoFile();
}
