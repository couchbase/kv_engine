/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc
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

#pragma once

#include "kvstore.h"
#include "kvstore_config.h"
#include "rollback_result.h"
#include "vb_commit.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"
#include <folly/portability/GMock.h>

class KVBucket;
struct TransactionContext;

class MockKVStore : public KVStore {
public:
    MockKVStore();
    ~MockKVStore() override;

    MOCK_METHOD(void, deinitialize, (), (override));
    MOCK_METHOD(void,
                addStats,
                (const AddStatFn& add_stat,
                 const void* c,
                 const std::string& args),
                (override));
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
                (const AddStatFn& add_stat, const void* c),
                (override));
    MOCK_METHOD(bool, commit, (VB::Commit & commitData), (override));
    MOCK_METHOD(void, rollback, (), (override));
    MOCK_METHOD(StorageProperties, getStorageProperties, (), (override));
    MOCK_METHOD(void, set, (queued_item item), (override));
    MOCK_METHOD(GetValue,
                get,
                (const DiskDocKey& key, Vbid vb, ValueFilter filter),
                (override));
    MOCK_METHOD(GetValue,
                getWithHeader,
                (const KVFileHandle& kvFileHandle,
                 const DiskDocKey& key,
                 Vbid vb,
                 ValueFilter filter),
                (override));
    MOCK_METHOD(void, setMaxDataSize, (size_t size), (override));
    MOCK_METHOD(void,
                getMulti,
                (Vbid vb, vb_bgfetch_queue_t& itms),
                (override));
    MOCK_METHOD(void,
                getRange,
                (Vbid vb,
                 const DiskDocKey& startKey,
                 const DiskDocKey& endKey,
                 ValueFilter filter,
                 const GetRangeCb& cb),
                (override));
    MOCK_METHOD(void, del, (queued_item item), (override));
    MOCK_METHOD(void, delVBucket, (Vbid vbucket, uint64_t fileRev), (override));
    MOCK_METHOD(std::vector<vbucket_state*>,
                listPersistedVbuckets,
                (),
                (override));
    MOCK_METHOD(bool,
                snapshotVBucket,
                (Vbid vbucketId, const vbucket_state& vbstate),
                (override));
    MOCK_METHOD(bool,
                compactDB,
                (std::unique_lock<std::mutex> & vbLock,
                 std::shared_ptr<CompactionContext> c),
                (override));
    MOCK_METHOD(void,
                abortCompactionIfRunning,
                (std::unique_lock<std::mutex> & vbLock, Vbid vbid),
                (override));
    MOCK_METHOD(vbucket_state*, getCachedVBucketState, (Vbid vbid), (override));
    MOCK_METHOD(vbucket_state,
                getPersistedVBucketState,
                (Vbid vbid),
                (override));
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
                (override));
    MOCK_METHOD(bool, supportsHistoricalSnapshots, (), (const, override));
    MOCK_METHOD(std::unique_ptr<BySeqnoScanContext>,
                initBySeqnoScanContext,
                (std::unique_ptr<StatusCallback<GetValue>> cb,
                 std::unique_ptr<StatusCallback<CacheLookup>> cl,
                 Vbid vbid,
                 uint64_t startSeqno,
                 DocumentFilter options,
                 ValueFilter valOptions,
                 SnapshotSource source),
                (override));
    MOCK_METHOD(std::unique_ptr<ByIdScanContext>,
                initByIdScanContext,
                (std::unique_ptr<StatusCallback<GetValue>> cb,
                 std::unique_ptr<StatusCallback<CacheLookup>> cl,
                 Vbid vbid,
                 const std::vector<ByIdRange>& ranges,
                 DocumentFilter options,
                 ValueFilter valOptions),
                (override));
    MOCK_METHOD(scan_error_t, scan, (BySeqnoScanContext & sctx), (override));
    MOCK_METHOD(scan_error_t, scan, (ByIdScanContext & sctx), (override));
    MOCK_METHOD(std::unique_ptr<KVFileHandle>,
                makeFileHandle,
                (Vbid vbid),
                (override));
    MOCK_METHOD((std::pair<KVStore::GetCollectionStatsStatus,
                           Collections::VB::PersistedStats>),
                getCollectionStats,
                (const KVFileHandle& kvFileHandle, CollectionID collection),
                (override));
    MOCK_METHOD(std::optional<Collections::ManifestUid>,
                getCollectionsManifestUid,
                (KVFileHandle & kvFileHandle),
                (override));
    MOCK_METHOD((std::pair<bool, Collections::KVStore::Manifest>),
                getCollectionsManifest,
                (Vbid vbid),
                (override));
    MOCK_METHOD(
            (std::pair<bool,
                       std::vector<Collections::KVStore::DroppedCollection>>),
            getDroppedCollections,
            (Vbid vbid),
            (override));
    MOCK_METHOD(const KVStoreConfig&, getConfig, (), (const, override));
    MOCK_METHOD(void,
                setStorageThreads,
                (ThreadPoolConfig::StorageThreadCount num),
                (override));
    MOCK_METHOD(GetValue,
                getBySeqno,
                (KVFileHandle & handle,
                 Vbid vbid,
                 uint64_t seq,
                 ValueFilter filter),
                (override));
    MOCK_METHOD(uint64_t, prepareToDeleteImpl, (Vbid vbid), (override));
    MOCK_METHOD(void, prepareToCreateImpl, (Vbid vbid), (override));

    /**
     * Helper function to replace the existing read-only KVStore in the given
     * bucket & shard with a new MockKVStore instance. Returns a reference to
     * the created mock.
     */
    static MockKVStore& replaceROKVStoreWithMock(KVBucket& bucket,
                                                 size_t shardId);
};
