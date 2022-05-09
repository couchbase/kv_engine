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

#pragma once

#include "collections/collection_persisted_stats.h"
#include "kvstore/kvstore.h"
#include "kvstore/kvstore_config.h"
#include "kvstore/kvstore_transaction_context.h"
#include "rollback_result.h"
#include "vb_commit.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"
#include <folly/portability/GMock.h>

class KVBucket;
struct TransactionContext;

class MockKVStore : public KVStore {
public:
    /**
     * Construct a MockKVStore
     * @param realKVS If non-null; the underlying real KVStore the mock can
     *        delegate calls to.
     */
    MockKVStore(std::unique_ptr<KVStoreIface> realKVS);
    ~MockKVStore() override;

    MOCK_METHOD(void, deinitialize, (), (override));
    MOCK_METHOD(void,
                addStats,
                (const AddStatFn& add_stat, const void* c),
                (const, override));
    MOCK_METHOD(bool,
                getStat,
                (std::string_view name, size_t& value),
                (const, override));
    MOCK_METHOD(GetStatsMap,
                getStats,
                (gsl::span<const std::string_view> keys),
                (const, override));
    MOCK_METHOD(void,
                addTimingStats,
                (const AddStatFn& add_stat, const CookieIface* c),
                (const, override));
    MOCK_METHOD(bool,
                commit,
                (std::unique_ptr<TransactionContext> txnCtx,
                 VB::Commit& commitData),
                (override));
    MOCK_METHOD(StorageProperties, getStorageProperties, (), (const, override));
    MOCK_METHOD(void,
                set,
                (TransactionContext & txnCtx, queued_item item),
                (override));
    MOCK_METHOD(GetValue,
                get,
                (const DiskDocKey& key, Vbid vb, ValueFilter filter),
                (const, override));
    MOCK_METHOD(GetValue,
                getWithHeader,
                (const KVFileHandle& kvFileHandle,
                 const DiskDocKey& key,
                 Vbid vb,
                 ValueFilter filter),
                (const, override));
    MOCK_METHOD(void, setMaxDataSize, (size_t size), (override));
    MOCK_METHOD(void,
                getMulti,
                (Vbid vb, vb_bgfetch_queue_t& itms),
                (const, override));
    MOCK_METHOD(void,
                getRange,
                (Vbid vb,
                 const DiskDocKey& startKey,
                 const DiskDocKey& endKey,
                 ValueFilter filter,
                 const GetRangeCb& cb),
                (const, override));
    MOCK_METHOD(void,
                del,
                (TransactionContext & txnCtx, queued_item item),
                (override));
    MOCK_METHOD(void,
                delVBucket,
                (Vbid vbucket, std::unique_ptr<KVStoreRevision> fileRev),
                (override));
    MOCK_METHOD(std::vector<vbucket_state*>,
                listPersistedVbuckets,
                (),
                (override));
    MOCK_METHOD(bool,
                snapshotVBucket,
                (Vbid vbucketId, const vbucket_state& vbstate),
                (override));
    MOCK_METHOD(CompactDBStatus,
                compactDB,
                (std::unique_lock<std::mutex> & vbLock,
                 std::shared_ptr<CompactionContext> c),
                (override));
    MOCK_METHOD(void,
                abortCompactionIfRunning,
                (std::unique_lock<std::mutex> & vbLock, Vbid vbid),
                (override));
    MOCK_METHOD(vbucket_state*, getCachedVBucketState, (Vbid vbid), (override));
    MOCK_METHOD(KVStoreIface::ReadVBStateResult,
                getPersistedVBucketState,
                (Vbid vbid),
                (const, override));
    MOCK_METHOD(KVStoreIface::ReadVBStateResult,
                getPersistedVBucketState,
                (KVFileHandle & handle, Vbid vbid),
                (const, override));
    MOCK_METHOD(size_t, getNumPersistedDeletes, (Vbid vbid), (override));
    MOCK_METHOD(DBFileInfo, getDbFileInfo, (Vbid dbFileId), (override));
    MOCK_METHOD(DBFileInfo, getAggrDbFileInfo, (), (override));
    MOCK_METHOD(size_t, getItemCount, (Vbid vbid), (override));
    MOCK_METHOD(RollbackResult,
                rollback,
                (Vbid vbid,
                 uint64_t rollbackseqno,
                 std::unique_ptr<RollbackCB>),
                (override));
    MOCK_METHOD(void, pendingTasks, (), (override));
    MOCK_METHOD(cb::engine_errc,
                getAllKeys,
                (Vbid vbid,
                 const DiskDocKey& start_key,
                 uint32_t count,
                 std::shared_ptr<StatusCallback<const DiskDocKey&>> cb),
                (const, override));
    MOCK_METHOD(bool, supportsHistoricalSnapshots, (), (const, override));
    MOCK_METHOD(std::unique_ptr<BySeqnoScanContext>,
                initBySeqnoScanContext,
                (std::unique_ptr<StatusCallback<GetValue>> cb,
                 std::unique_ptr<StatusCallback<CacheLookup>> cl,
                 Vbid vbid,
                 uint64_t startSeqno,
                 DocumentFilter options,
                 ValueFilter valOptions,
                 SnapshotSource source,
                 std::unique_ptr<KVFileHandle> fileHandle),
                (const, override));
    MOCK_METHOD(std::unique_ptr<ByIdScanContext>,
                initByIdScanContext,
                (std::unique_ptr<StatusCallback<GetValue>> cb,
                 std::unique_ptr<StatusCallback<CacheLookup>> cl,
                 Vbid vbid,
                 const std::vector<ByIdRange>& ranges,
                 DocumentFilter options,
                 ValueFilter valOptions,
                 std::unique_ptr<KVFileHandle> handle),
                (const, override));
    MOCK_METHOD(ScanStatus,
                scan,
                (BySeqnoScanContext & sctx),
                (const, override));
    MOCK_METHOD(ScanStatus, scan, (ByIdScanContext & sctx), (const, override));
    MOCK_METHOD(std::unique_ptr<KVFileHandle>,
                makeFileHandle,
                (Vbid vbid),
                (const, override));
    MOCK_METHOD((std::pair<KVStore::GetCollectionStatsStatus,
                           Collections::VB::PersistedStats>),
                getCollectionStats,
                (const KVFileHandle& kvFileHandle, CollectionID collection),
                (const, override));
    MOCK_METHOD((std::pair<KVStore::GetCollectionStatsStatus,
                           Collections::VB::PersistedStats>),
                getCollectionStats,
                (Vbid vbid, CollectionID collection),
                (const, override));
    MOCK_METHOD((std::optional<Collections::ManifestUid>),
                getCollectionsManifestUid,
                (KVFileHandle & kvFileHandle),
                (const, override));
    MOCK_METHOD((std::pair<bool, Collections::KVStore::Manifest>),
                getCollectionsManifest,
                (Vbid vbid),
                (const, override));
    MOCK_METHOD(
            (std::pair<bool,
                       std::vector<Collections::KVStore::DroppedCollection>>),
            getDroppedCollections,
            (Vbid vbid),
            (const, override));
    MOCK_METHOD(const KVStoreConfig&, getConfig, (), (const, override));
    MOCK_METHOD(GetValue,
                getBySeqno,
                (KVFileHandle & handle,
                 Vbid vbid,
                 uint64_t seq,
                 ValueFilter filter),
                (const, override));
    MOCK_METHOD(void,
                setStorageThreads,
                (ThreadPoolConfig::StorageThreadCount num),
                (override));
    MOCK_METHOD(std::unique_ptr<KVStoreRevision>,
                prepareToDeleteImpl,
                (Vbid vbid),
                (override));
    MOCK_METHOD(void, prepareToCreateImpl, (Vbid vbid), (override));
    MOCK_METHOD(std::unique_ptr<TransactionContext>,
                begin,
                (Vbid vbid, std::unique_ptr<PersistenceCallback> pcb),
                (override));
    MOCK_METHOD(bool, snapshotStats, (const nlohmann::json&), (override));
    MOCK_METHOD(std::unique_ptr<KVStoreRevision>,
                prepareToDelete,
                (Vbid vbid),
                (override));
    MOCK_METHOD(void, prepareToCreate, (Vbid vbid), (override));
    MOCK_METHOD(std::unique_ptr<RollbackCtx>,
                prepareToRollback,
                (Vbid vbid),
                (override));

    /**
     * Helper function to replace the existing read-write KVStore in the given
     * bucket & shard with a new MockKVStore instance. Returns a reference to
     * the created mock.
     */
    static MockKVStore& replaceRWKVStoreWithMock(KVBucket& bucket,
                                                 size_t shardId);

    /**
     * Restores the bucket's original read-write KVStore, removing the
     * MockKVStore from between the bucket and original and returning it.
     * Inverse of replaceRWKVStoreWithMock.
     */
    static std::unique_ptr<MockKVStore> restoreOriginalRWKVStore(
            KVBucket& bucket);

private:
    /// Underlying 'real' KVStore object. Will be null if MockKVStore
    /// was constructed standalone, but will be set if MockKVStore was
    /// constructed by replacing an existing one via replaceROKVStoreWithMock()
    /// or replaceRWKVStoreWithMock().
    std::unique_ptr<KVStoreIface> realKVS;
};
