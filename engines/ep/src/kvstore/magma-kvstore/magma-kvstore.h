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

#pragma once

#include "fusion_uploader.h"
#include "kv_magma_common/magma-kvstore_magma_db_stats.h"
#include "kvstore/kvstore.h"
#include "kvstore/kvstore_transaction_context.h"
#include "libmagma/magma.h"
#include "rollback_result.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"

#include <folly/Synchronized.h>
#include <platform/non_negative_counter.h>

#include <filesystem>
#include <map>
#include <queue>
#include <shared_mutex>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace magma {
class Slice;
class Status;
} // namespace magma

namespace cb::compression {
class Buffer;
}

class MagmaKVStoreConfig;
class MagmaMemoryTrackingProxy;
class MagmaRequest;
struct kvstats_ctx;
struct MagmaKVStoreTransactionContext;
class MagmaRequest;
struct vbucket_state;

/**
 * A persistence store based on magma.
 */
class MagmaKVStore : public KVStore {
public:
    MagmaKVStore(MagmaKVStoreConfig& config,
                 EncryptionKeyProvider* encryptionKeyProvider,
                 std::string_view chronicleAuthToken);

    ~MagmaKVStore() override;

    using WriteOps = std::vector<magma::Magma::WriteOperation>;

    /**
     * A localDb request is used to scope the memory required for
     * inserting a localDb update into the kvstore localDb.
     */
    class MagmaLocalReq {
    public:
        MagmaLocalReq(std::string_view key,
                      std::string&& value,
                      bool deleted = false)
            : key(key), value(std::move(value)), deleted(deleted) {
        }

        // Some localDb Reqs come from flatbuffers. We need to convert
        // the flatbuffer into a string.
        MagmaLocalReq(std::string_view key,
                      const flatbuffers::DetachedBuffer& buf);

        static MagmaLocalReq makeDeleted(std::string_view key,
                                         std::string&& value = {}) {
            return {std::move(key), std::move(value), true /*deleted*/};
        }

        std::string key;
        std::string value;
        bool deleted{false};
    };

    using LocalDbReqs = std::vector<MagmaLocalReq>;

    /**
     * Add localDbReqs to the WriteOps vector to be inserted into
     * the kvstore.
     *
     * @param localDbReqs vector of localDb updates
     * @param writeOps vector of Magma::WriteOperations's
     */
    void addLocalDbReqs(const LocalDbReqs& localDbReqs, WriteOps& writeOps);

    /**
     * Add MagmaDbStats to the WriteOps vector to be inserted into the kvstore.
     * @param stats The MagmaDbStats object
     * @param writeOps vector of Magma::WriteOperations
     */
    void addStatUpdateToWriteOps(MagmaDbStats& stats, WriteOps& writeOps) const;

    void deinitialize() override;

    bool pause() override;
    void resume() override;

    void operator=(MagmaKVStore& from) = delete;

    bool commit(std::unique_ptr<TransactionContext> txnCtx,
                VB::Commit& commitData) override;

    StorageProperties getStorageProperties() const override;

    void setMaxDataSize(size_t size) override;

    std::variant<cb::engine_errc, std::unordered_set<std::string>>
    getEncryptionKeyIds() const override;

    /**
     * Get magma stats
     *
     * @param name stat name
     * @param value returned value when function return is true
     * @return true if stat found, value is set
     */
    bool getStat(std::string_view name, size_t& value) const override;

    GetStatsMap getStats(gsl::span<const std::string_view> keys) const override;

    /**
     * Adds a request to a queue for batch processing at commit()
     */
    void set(TransactionContext& ctx, queued_item itm) override;

    GetValue get(const DiskDocKey& key,
                 Vbid vb,
                 ValueFilter filter) const override;
    using KVStore::get;

    GetValue getWithHeader(const KVFileHandle& kvFileHandle,
                           const DiskDocKey& key,
                           Vbid vb,
                           ValueFilter filter) const override;

    void getMulti(Vbid vb,
                  vb_bgfetch_queue_t& itms,
                  CreateItemCB createItemCb) const override;

    void getRange(Vbid vb,
                  const DiskDocKey& startKey,
                  const DiskDocKey& endKey,
                  ValueFilter filter,
                  const GetRangeCb& cb) const override;

    void del(TransactionContext& txnCtx, queued_item itm) override;

    void delVBucket(Vbid vbucket,
                    std::unique_ptr<KVStoreRevision> kvstoreRev) override;

    std::vector<vbucket_state*> listPersistedVbuckets() override;

    /**
     * When the continuous backup feature is enabled, we disable history
     * eviction until we've definitively called StartBackup for all warmed up
     * vBuckets. We re-enable history eviction from this API.
     */
    void completeLoadingVBuckets() override;

    bool snapshotVBucket(Vbid vbucketId, const VB::Commit& meta) override;

    // Compaction in magma is asynchronous. Its triggered by 3 conditions:
    //  - Level compaction
    //  - Expiry compaction
    //    In magma, a histogram of when items will expire is maintained.
    //    Periodically, magma will trigger a compaction to visit those
    //    sstables which have expired items to have them removed.
    //  - Dropped collections removal
    //    At the end of the flusher loop in kv_engine, a call to magma will be
    //    made to trigger asynchronous PurgeRange scans to purge the store
    //    of any dropped collections. For each collectionID, 2 scans will be
    //    triggered, [Default+CollectionID] and
    //    [DurabilityPrepare+CollectionID].
    //    While a purge scan will identify the sstables containing the scan
    //    key ex.[Default+CollectionID], all items for that CollectionID will
    //    be removed ie. both Default & DurabilityPrepare. This guarantees that
    //    the data from each sstable containing the collectionID is visited
    //    once.
    //    Also during this call from the bg flusher, any completed compaction
    //    data will be picked up. The max_purged_seq will update the vbstate
    //    and any dropped collections will trigger 2 scans (same as above) to
    //    determine if a collection has been removed and if so, the collection
    //    manifest is updated.
    //
    //    Regardless of which type of compaction is running, all compactions
    //    required a kv_engine callback to pick up CompactionContext and all
    //    compactions can remove expired items or dropped collection items.
    //
    //    Synchronous compaction is supported for testing. Normally, kv_engine
    //    with magma store should never call compactDB. But for testing, we
    //    need to support a synchronous call. When compactDB is called, it will
    //    save the CompactionContext passed in to compactDB and will use it
    //    to perform compaction.
    CompactDBStatus compactDB(std::unique_lock<std::mutex>& vbLock,
                              std::shared_ptr<CompactionContext> ctx) override;

    size_t getNumPersistedDeletes(Vbid vbid) override {
        // TODO
        return 0;
    }

    DBFileInfo getDbFileInfo(Vbid vbid) override;

    DBFileInfo getAggrDbFileInfo() override;

    size_t getItemCount(Vbid vbid) override;

    uint64_t getPurgeSeqno(Vbid vbid) override;

    RollbackResult rollback(Vbid vbid,
                            uint64_t rollbackSeqno,
                            std::unique_ptr<RollbackCB>) override;

    void pendingTasks() override;

    cb::engine_errc getAllKeys(
            Vbid vbid,
            const DiskDocKey& start_key,
            uint32_t count,
            std::shared_ptr<StatusCallback<const DiskDocKey&>> cb)
            const override;

    std::unique_ptr<BySeqnoScanContext> initBySeqnoScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            uint64_t startSeqno,
            DocumentFilter options,
            ValueFilter valOptions,
            SnapshotSource source,
            std::unique_ptr<KVFileHandle> fileHandle = nullptr) const override;

    std::unique_ptr<ByIdScanContext> initByIdScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            const std::vector<ByIdRange>& ranges,
            DocumentFilter options,
            ValueFilter valOptions,
            std::unique_ptr<KVFileHandle> handle = nullptr) const override;

    ScanStatus scan(BySeqnoScanContext& sctx) const override;
    ScanStatus scanAllVersions(BySeqnoScanContext& sctx) const override;
    ScanStatus scan(ByIdScanContext& ctx) const override;

    std::unique_ptr<KVFileHandle> makeFileHandle(Vbid vbid) const override;

    std::pair<GetCollectionStatsStatus, Collections::VB::PersistedStats>
    getCollectionStats(const KVFileHandle& kvFileHandle,
                       CollectionID collection) const override;

    std::pair<GetCollectionStatsStatus, Collections::VB::PersistedStats>
    getCollectionStats(Vbid, CollectionID collection) const override;

    /**
     * Get the collection stats for the given key
     *
     * @param vbid
     * @param keySlice Stats key to lookup
     * @param snapshot if not null collection stats will be collected from the
     * local store of the snapshot
     * @return pair of status and if success valid collection PersistedStats
     */
    std::pair<GetCollectionStatsStatus, Collections::VB::PersistedStats>
    getCollectionStats(Vbid,
                       magma::Slice keySlice,
                       magma::Magma::Snapshot* snapshot = nullptr) const;

    /**
     * Get the dropped collection item count for the given collection
     * @param vbid Vbid
     * @param collection to find stats for
     * @return Bool status and item count (default to 0 if not found)
     */
    std::pair<GetCollectionStatsStatus, uint64_t> getDroppedCollectionItemCount(
            Vbid vbid, CollectionID collection) const;

    /**
     * Increment the kvstore revision.
     */
    void prepareToCreateImpl(Vbid vbid) override;

    /**
     * Soft delete the kvstore.
     */
    std::unique_ptr<KVStoreRevision> prepareToDeleteImpl(Vbid vbid) override;

    std::unique_ptr<RollbackCtx> prepareToRollback(Vbid vbid) override;

    /**
     * Re-enable implicit compaction for the given vBucket
     *
     * @param vbid to re-enable implicit compaction for
     */
    void resumeImplicitCompaction(Vbid vbid);

    std::optional<Collections::ManifestUid> getCollectionsManifestUid(
            KVFileHandle& kvFileHandle) const override;

    /**
     * Retrieve the manifest from the local db.
     * MagmaKVStore implements this method as a read of 4 _local documents
     * manifest, open collections, open scopes, dropped collections.
     *
     * @param vbid vbucket id
     * @return pair of bool status and the persisted manifest data for the given
     *         vbid
     */
    std::pair<bool, Collections::KVStore::Manifest> getCollectionsManifest(
            Vbid vbid) const override;

    /**
     * Retrieve the manifest from the snapshot.
     * MagmaKVStore implements this method as a read of 4 _local documents
     * manifest, open collections, open scopes, dropped collections.
     *
     * @param vbid vbucket id
     * @return pair of Status and the persisted manifest data for the given
     *         vbid
     */
    std::pair<magma::Status, Collections::KVStore::Manifest>
    getCollectionsManifest(Vbid vbid, magma::Magma::Snapshot& snapshot) const;

    /**
     * Read local doc to get a vector of open collections from the given
     * snapshot
     *
     * @param vbid vbucket id
     * @param snapshot The magma snapshot from which we want to read
     * @return a pair of Status and vector of open collections (can be empty)
     */
    std::pair<magma::Status, std::vector<Collections::KVStore::OpenCollection>>
    getOpenCollections(Vbid vbid, magma::Magma::Snapshot& snapshot) const;

    /**
     * Read local document to get the vector of dropped collections from the
     * latest snapshot
     *
     * @param vbid vbucket id
     * @return a pair of bool status and vector of dropped collections (can be
     *         empty)
     */
    std::pair<bool, std::vector<Collections::KVStore::DroppedCollection>>
    getDroppedCollections(Vbid vbid) const override;

    /**
     * Read local doc to get the vector of dropped collections from the given
     * snapshot
     *
     * @param vbid vbucket id
     * @param snapshot The magma snapshot from which we want to read
     * @return a pair of Status and vector of dropped collections (can be empty)
     */
    std::pair<magma::Status,
              std::vector<Collections::KVStore::DroppedCollection>>
    getDroppedCollections(Vbid vbid, magma::Magma::Snapshot& snapshot) const;

    /**
     * This function maintains the set of open collections, adding newly opened
     * collections and removing those which are dropped. To validate the
     * creation of new collections, this method must read the dropped
     * collections.
     *
     * @param vbid vbucket id
     * @param localDbReqs vector of localDb updates
     * @param collectionsFlush flush object for a single 'flush/commit'
     * @param dbStats MagmaDbStats updated during this flush batch
     * @return status
     */
    magma::Status updateCollectionsMeta(
            Vbid vbid,
            LocalDbReqs& localDbReqs,
            Collections::VB::Flush& collectionsFlush,
            MagmaDbStats& dbStats);

    /**
     * Maintain the current uid committed
     *
     * @param localDbReqs vector of localDb updates
     * @param collectionsFlush flush object for a single 'flush/commit'
     */
    void updateManifestUid(LocalDbReqs& localDbReqs,
                           Collections::VB::Flush& collectionsFlush);

    /**
     * Maintain the list of open collections. The maintenance requires
     * reading the dropped collections which is passed back to avoid
     * a reread.
     *
     * @param vbid vbucket id
     * @param localDbReqs vector of localDb updates
     * @param collectionsFlush flush object for a single 'flush/commit'
     * @return status
     */
    magma::Status updateOpenCollections(
            Vbid vbid,
            LocalDbReqs& localDbReqs,
            Collections::VB::Flush& collectionsFlush);

    /**
     * Maintain the list of dropped collections
     *
     * @param vbid vbucket id
     * @param localDbReqs vector of localDb updates
     * @param collectionsFlush flush object for a single 'flush/commit'
     * @param dbStats MagmaDbStats updated during this flush batch
     * @return status
     */
    magma::Status updateDroppedCollections(
            Vbid vbid,
            LocalDbReqs& localDbReqs,
            Collections::VB::Flush& collectionsFlush,
            MagmaDbStats& dbStats);

    /**
     * Maintain the list of open scopes
     *
     * @param vbid vbucket id
     * @param localDbReqs vector of localDb updates
     * @param collectionsFlush flush object for a single 'flush/commit'
     * @return status
     */
    magma::Status updateScopes(Vbid vbid,
                               LocalDbReqs& localDbReqs,
                               Collections::VB::Flush& collectionsFlush);

    /**
     * Given a collection id, return the key used to maintain the
     * collection stats in the local db.
     *
     * @param cid Collection ID
     */
    std::string getCollectionsStatsKey(CollectionID cid) const;

    /**
     * Given a collection id, return the key used to maintain the dropped
     * collection stats in the local db.
     *
     * @param cid Collection ID
     */
    std::string getDroppedCollectionsStatsKey(CollectionID cid) const;

    /**
     * Save stats for collection cid
     *
     * @param localDbReqs vector of localDb updates
     * @param cid Collection ID
     * @param stats The stats that should be applied and persisted
     */
    void saveCollectionStats(LocalDbReqs& localDbReqs,
                             CollectionID cid,
                             const Collections::VB::PersistedStats& stats);

    /**
     * Delete the collection stats for the given collection id
     * @param localDbReqs vector of localDb updates
     * @param cid Collection ID
     */
    void deleteCollectionStats(LocalDbReqs& localDbReqs, CollectionID cid);

    /**
     * Read from local DB
     */
    std::pair<magma::Status, std::string> readLocalDoc(
            Vbid vbid, const magma::Slice& keySlice) const;

    /**
     * Read local doc from given snapshot
     */
    std::pair<magma::Status, std::string> readLocalDoc(
            Vbid vbid,
            magma::Magma::Snapshot& snapshot,
            const magma::Slice& keySlice) const;

    /**
     * Processes the result of readLocalDoc adding information to the returned
     * Status and logging if necessary
     */
    std::pair<magma::Status, std::string> processReadLocalDocResult(
            magma::Status,
            Vbid vbid,
            const magma::Slice& keySlice,
            std::string_view value) const;

    /**
     * Encode the cached vbucket_state into a JSON string
     */
    std::string encodeVBState(const vbucket_state& vbstate) const;

    /**
     * Read the vbstate from disk and load into cache
     */
    ReadVBStateStatus loadVBStateCache(Vbid vbid, bool resetKVStoreRev = false);

    /**
     * Write the vbucket_state to disk.
     * This is done outside an atomic batch so we need to
     * create a WriteDocs batch to write it.
     *
     * @param vbid vbucket id
     * @param meta Information to be passed to the storage
     * @return status
     */
    magma::Status writeVBStateToDisk(Vbid vbid, const VB::Commit& meta);

    /**
     * Read the encoded vstate + docCount from the local db.
     */
    ReadVBStateResult readVBStateFromDisk(Vbid vbid) const;

    /**s
     * Read the encoded vbstate from the given snapshot.
     */
    virtual ReadVBStateResult readVBStateFromDisk(
            Vbid vbid, magma::Magma::Snapshot& snapshot) const;

    /**
     * Write the encoded vbstate to localDb.
     */
    void addVBStateUpdateToLocalDbReqs(LocalDbReqs& localDbReqs,
                                       const vbucket_state& vbs,
                                       uint64_t kvstoreRev);

    /**
     * Whenever we go to cachedVBStates, we need to merge in
     * the MagmaDbStats stats to make the vbstate current. We read
     * the MagmaDbStats stats from magma which are kept on the state file.
     */
    void mergeMagmaDbStatsIntoVBState(vbucket_state& vbstate, Vbid vbid) const;

    /**
     * Get vbstate from cache.
     *
     * Note: When getting the vbstate, we merge in the MagmaDbStats stats.
     * See mergeMagmaDbStatsIntoVBState() above.
     */
    vbucket_state* getCachedVBucketState(Vbid vbucketId) override;

    ReadVBStateResult getPersistedVBucketState(Vbid vbid) const override;

    ReadVBStateResult getPersistedVBucketState(KVFileHandle& handle,
                                               Vbid vbid) const override;

    ReadVBStateResult loadVBucketSnapshot(
            Vbid vbid,
            vbucket_state_t state,
            const nlohmann::json& topology) override;

    /**
     * Populate kvstore stats with magma specific stats
     */
    void addStats(const AddStatFn& add_stat,
                  CookieIface& cookie) const override;

    /**
     * Populate magma specific timing stats.
     *
     * @param add_stat the callback function to add statistics
     * @param c the cookie to pass to the callback function
     */
    void addTimingStats(const AddStatFn& add_stat,
                        CookieIface& c) const override;

    /**
     * Construct a compaction context for use with implicit compactions. Calls
     * back up to the bucket to do so as we need certain callbacks and config.
     */
    std::shared_ptr<CompactionContext> makeImplicitCompactionContext(Vbid vbid);

    const KVStoreConfig& getConfig() const override;

    void setMagmaFragmentationPercentage(size_t value);

    void setMagmaEnableBlockCache(bool enable);

    void setMagmaSeqTreeDataBlockSize(size_t value);

    void setMagmaMinValueBlockSizeThreshold(size_t value);

    void setMagmaSeqTreeIndexBlockSize(size_t value);

    void setMagmaKeyTreeDataBlockSize(size_t value);

    void setMagmaKeyTreeIndexBlockSize(size_t value);

    void setStorageThreads(ThreadPoolConfig::StorageThreadCount num) override;

    /**
     * Set the number of magma flushers and compactors based on configuration
     * settings of number of backend threads, number of writer threads, and
     * percentage of flusher threads.
     */
    void calculateAndSetMagmaThreads();

    /**
     * Returns the expiry time of alive documents or the time at which
     * tombstones should be purged. Used by magma to track expiry/purge times
     * in histograms used to determine when to run compaction.
     */
    uint32_t getExpiryOrPurgeTime(const magma::Slice& slice);

    /**
     * Return magma kvstore revision
     * @param vbid
     *
     * @return revision returns 0 (default) if the kvstore does not exist
     */
    uint32_t getKVStoreRevision(Vbid vbid) const;

    GetValue getBySeqno(KVFileHandle& handle,
                        Vbid vbid,
                        uint64_t seq,
                        ValueFilter filter) const override;

    std::unique_ptr<TransactionContext> begin(
            Vbid vbid, std::unique_ptr<PersistenceCallback> pcb) override;

    /**
     * Informs magma of how much history must be retained using
     * Magma::SetHistoryRetentionTime
     */
    void setHistoryRetentionBytes(size_t size, size_t nVbuckets) override;
    void setHistoryRetentionSeconds(std::chrono::seconds secs) override;
    std::optional<uint64_t> getHistoryStartSeqno(Vbid vbid) override;

    void setContinuousBackupInterval(std::chrono::seconds interval);

    /// Overload for testing.
    std::pair<magma::Status, std::string> onContinuousBackupCallback(
            const KVFileHandle& kvFileHandle);

    /**
     * Continuous backup callback called from Magma.
     * This callback serialises some of the state of the vBucket into a
     * flatbuffer, to be stored in the backup file as additional metadata.
     *
     * @param vbid the vBucket
     * @param snapshot Magma snapshot from which to read the vBucket state
     * @return status code and the serialized metadata
     */
    virtual std::pair<magma::Status, std::string> onContinuousBackupCallback(
            Vbid vbid, magma::Magma::Snapshot& snapshot);

    std::pair<cb::engine_errc, nlohmann::json> getFusionStats(
            FusionStat stat, Vbid vbid) override;

    std::pair<cb::engine_errc, nlohmann::json> getFusionStorageSnapshot(
            std::string_view fusionNamespace,
            Vbid vbid,
            std::string_view snapshotUuid,
            std::time_t validity) override;

    cb::engine_errc releaseFusionStorageSnapshot(
            std::string_view fusionNamespace,
            Vbid vbid,
            std::string_view snapshotUuid) override;

    cb::engine_errc setChronicleAuthToken(std::string_view token) override;
    std::string getChronicleAuthToken() const override;

    std::pair<cb::engine_errc, std::vector<std::string>> mountVBucket(
            Vbid vbid,
            VBucketSnapshotSource source,
            const std::vector<std::string>& paths) override;

    cb::engine_errc syncFusionLogstore(Vbid vbid) override;

    /**
     * Schedules a task for starting the fusion uploader in bg-thread.
     * The call fails by key_already_exists if any start/stop task is already
     * scheduled.
     */
    cb::engine_errc startFusionUploader(Vbid vbid, uint64_t term) override;

    /**
     * Calls into magma::StartFusionUploader. This is a blocking call.
     */
    cb::engine_errc doStartFusionUploader(Vbid vbid, uint64_t term);

    /**
     * Schedules a task for stopping the fusion uploader in bg-thread.
     * The call fails by key_already_exists if any start/stop task is already
     * scheduled.
     */
    cb::engine_errc stopFusionUploader(Vbid vbid) override;

    /**
     * Calls into magma::StopFusionUploader. This is a blocking call.
     */
    cb::engine_errc doStopFusionUploader(Vbid vbid);

    /**
     * Calls into magma::IsFusionUploader(vbid).
     *
     * @return True if the uploader is enabled, false otherwise.
     */
    bool isFusionUploader(Vbid vbid) const;

    /**
     * Calls into magma::GetFusionUploaderTerm(vbid).
     *
     * @return The current uplaoder term.
     */
    uint64_t getFusionUploaderTerm(Vbid vbid) const;

    FusionUploaderState getFusionUploaderState(Vbid vbid) const;

    std::chrono::seconds getFusionUploadInterval() const;
    std::chrono::seconds getFusionLogCheckpointInterval() const;

    cb::engine_errc setFusionLogStoreURI(std::string_view uri);
    std::string getFusionLogStoreURI() const;

    cb::engine_errc setFusionMetadataStoreURI(std::string_view uri);
    std::string getFusionMetadataStoreURI() const;

    std::chrono::seconds getMagmaFusionUploadInterval() const;
    void setMagmaFusionUploadInterval(std::chrono::seconds value);

    void setMagmaFusionLogstoreFragmentationThreshold(float value);
    float getMagmaFusionLogstoreFragmentationThreshold() const;

    std::variant<cb::engine_errc, cb::snapshot::Manifest> prepareSnapshotImpl(
            const std::filesystem::path& snapshotDirectory,
            Vbid vb,
            std::string_view uuid) override;

    // Magma uses a unique logger with a prefix of magma so that all logging
    // calls from the wrapper thru magma will be prefixed with magma.
    std::shared_ptr<BucketLogger> logger;

protected:
    // Opens the Magma instance and finishes initialisation.
    void initialize(EncryptionKeyProvider* encryptionKeyProvider,
                    std::string_view chronicleAuthToken);

    /**
     * CompactDB implementation. See comments on public compactDB.
     */
    CompactDBStatus compactDBInternal(std::unique_lock<std::mutex>& vbLock,
                                      std::shared_ptr<CompactionContext> ctx);

    GetValue makeItem(Vbid vb,
                      const magma::Slice& keySlice,
                      const magma::Slice& metaSlice,
                      const magma::Slice& valueSlice,
                      ValueFilter filter,
                      CreateItemCB createItemCb) const;

    GetValue makeGetValue(Vbid vb,
                          const magma::Slice& keySlice,
                          const magma::Slice& metaSlice,
                          const magma::Slice& valueSlice,
                          ValueFilter filter,
                          CreateItemCB createItemCb) const;

    virtual int saveDocs(MagmaKVStoreTransactionContext& txnCtx,
                         VB::Commit& commitData,
                         kvstats_ctx& kvctx,
                         magma::Magma::HistoryMode historyMode =
                                 magma::Magma::HistoryMode::Disabled);

    void commitCallback(MagmaKVStoreTransactionContext& txnCtx,
                        int status,
                        kvstats_ctx& kvctx);

    /// private getWithHeader shared with public get and getWithHeader
    GetValue getWithHeader(const DiskDocKey& key,
                           Vbid vbid,
                           ValueFilter filter) const;

    bool keyMayExist(Vbid vbid, const DocKeyView& key) const override;

    /**
     * MagmaCompactionCB is the class invoked by magma compactions,
     * both implicit and explicit. For explict compactions, which come
     * through compactDB, we pass the CompactionContext thru to the callback
     * routine. For implicit compaction, we call makeCompactionCtx to
     * create the CompactionContext on the fly.
     *
     * Since implicit compactions can run in a thread other than the
     * BG Writer thread, we keep track of MagmaDbStats stats during
     * compaction and magma will call the GetUserStats() routine and
     * merge them with the existing MagmaDbStats stats.
     */
    class MagmaCompactionCB : public magma::Magma::CompactionCallback {
    public:
        MagmaCompactionCB(
                MagmaKVStore& magmaKVStore,
                Vbid vbid,
                std::shared_ptr<CompactionContext> compactionContext = nullptr,
                std::optional<CollectionID> cid = std::nullopt);

        ~MagmaCompactionCB() override;
        bool operator()(const magma::Slice& keySlice,
                        const magma::Slice& metaSlice,
                        const magma::Slice& valueSlice) override;
        const magma::UserStats* GetUserStats() override {
            return &magmaDbStats;
        }

        /**
         * @return true if this collection can be purged
         */
        bool canPurge(CollectionID collection);

        /**
         * Vbucket being compacted - required so that we can work out which
         * vBucket is being compacted for implicit (magma driven) compactions.
         */
        Vbid vbid;

        /**
         * Ctx may be passed at construction (explicit, kv/ns_server driven
         * compactions) or set later (implicit magma driven compactions)
         */
        std::shared_ptr<CompactionContext> ctx;

        /**
         * highSeqno of the oldest checkpoint to which magma can rollback
         */
        uint64_t oldestRollbackableHighSeqno{0};

    private:
        MagmaKVStore& magmaKVStore;
        /**
         * Stats updates made during compaction
         */
        MagmaDbStats magmaDbStats;

        /**
         * Optionally only collection-purge items from this collection
         */
        std::optional<CollectionID> onlyThisCollection;
    };

    /**
     * Add a doc count delta to the the underlying MagmaDbStats. Used to
     * decerement docCount by the size of a collection when we compact a
     * range and remove the dropped collection stats
     *
     * @param magmaDbStats stats to update
     * @param cid Collection id
     * @param delta to add
     */
    void processCollectionPurgeDelta(MagmaDbStats& magmaDbStats,
                                     CollectionID cid,
                                     int64_t delta);

    /**
     * Called for each item during compaction to determine whether we should
     * keep or drop the item (and drive expiry).
     *
     * @return magma status and true if the item should be dropped
     */
    std::pair<magma::Status, bool> compactionCallBack(
            MagmaKVStore::MagmaCompactionCB& cbCtx,
            const magma::Slice& keySlice,
            const magma::Slice& metaSlice,
            const magma::Slice& valueSlice) const;

    /**
     * Called from compactionCallback and operates on the primary memory domain
     */
    std::pair<magma::Status, bool> compactionCore(
            MagmaKVStore::MagmaCompactionCB& cbCtx,
            const magma::Slice& keySlice,
            const magma::Slice& metaSlice,
            const magma::Slice& valueSlice,
            std::string_view userSanitizedItemStr) const;

    /**
     * Get highSeqno of the oldest checkpoint to which magma can rollback
     * @param vbid
     * @return status
     * @return Oldest rollbackable sequence number
     */
    std::pair<magma::Status, uint64_t> getOldestRollbackableHighSeqno(
            Vbid vbid);

    /**
     * Get the MagmaDbStats from the Magma::KVStore
     * @param vbid
     * @return The MagmaDbStats (empty optional returned if they don't exist in
     * magma).
     * @throws std::runtime_error if the DbStats are in an invalid format.
     */
    std::optional<MagmaDbStats> getMagmaDbStats(Vbid vbid) const;

    /**
     * Get the manifest UID which is stored as a metadata document, this returns
     * the decoded value
     *
     *  @return pair, first is the status and second is the data if status is
     *          true.
     */
    std::pair<bool, Collections::ManifestUid> getCollectionsManifestUid(
            Vbid vbid) const;

    /**
     * Scan the seqno index - where mode selects a history scan that includes
     * all retained versions, or a normal scan which returns the most recent
     * updates.
     * @param ctx object which holds the required scan configuration
     * @param mode selects history or non history
     */
    ScanStatus scan(BySeqnoScanContext& ctx,
                    magma::Magma::SeqIterator::Mode mode) const;

    /**
     * Scan a single ById range (key iterator)
     */
    ScanStatus scan(ByIdScanContext& ctx, const ByIdRange& range) const;

    /**
     * Run the ScanContext callbacks for a single key/value (when scanning)
     *
     * @param ctx The ScanContext owning the callbacks to use
     * @param keySlice Slice "pointing" at the scanned key
     * @param seqno The seqno of the scanned key
     * @param metaSlice Slice "pointing" at the key's metadata
     * @oaram valSlice Slice "pointing" at the key's value. This Slice can be
     *        can be empty in which case the valueRead function will be used
     *        to obtain the value (when ctx.cacheCallback fails to find a value)
     * @param valRead a function that can read the value (for the case when
     *        valSlice is empty).
     * @return ScanStatus
     */
    ScanStatus scanOne(
            ScanContext& ctx,
            const magma::Slice& keySlice,
            uint64_t seqno,
            const magma::Slice& metaSlice,
            const magma::Slice& valSlice,
            std::function<magma::Status(magma::Slice&)> valueRead) const;

    /**
     * @returns true if the continuous backup feature has been started
     */
    bool isContinuousBackupStarted(Vbid vbid);

    /**
     * @returns true if history eviction is currently paused.
     */
    bool isHistoryEvictionPaused() {
        return historyEvictionPaused;
    }

    /**
     * Called after a VBState is flushed to disk. Starts/stops continuous backup
     * as needed.
     *
     * Note: Also called from snapshotVBucket when !needsToBePersisted() and we
     * skip persistence.
     */
    void postVBStateFlush(Vbid vbid, const vbucket_state& committedState);

    /**
     * Constructs the continuous backup path for the given vBucket.
     * @return the continuous backup path
     */
    std::filesystem::path getContinuousBackupPath(
            Vbid vbid, const vbucket_state& committedState);

    /**
     * If the provided @p op does not already have datatype Snappy, attempt
     * to compress the contained value, storing the result in @p
     * newValueStorage.
     *
     * If compression did reduce the size of the value, updated metadata will
     * be stored in @p newMetaStorage, @p result will be populated with the
     * updated operation, and true will be returned.
     *
     * If the value is already compressed, @p result will be unmodified
     * and false will be returned.
     *
     * @param commitData data used to update on disk stats at commit
     * @param newMetaStorage temporary storage for updated metadata
     *                       if the value is compressed
     * @param newValueStorage temporary storage for compressed values
     * @param op the operation to consider compressing
     * @param result contains the updated write operation pointing to the
     *               temporary meta/value storage if the value was compressed
     *               (and returned true)
     * @return whether the value in the write operation was compressed.
     *         If false, @p result is unmodified, and the original @p op
     *         should be used.
     */
    static bool maybeCompressValue(VB::Commit& commitData,
                                   std::string& newMetaStorage,
                                   cb::compression::Buffer& newValueStorage,
                                   const magma::Magma::WriteOperation& op,
                                   magma::Magma::WriteOperation& result);

    static magma::Magma::HistoryMode toHistoryMode(
            CheckpointHistorical historical) {
        switch (historical) {
        case CheckpointHistorical::No:
            return magma::Magma::HistoryMode::Disabled;
        case CheckpointHistorical::Yes:
            return magma::Magma::HistoryMode::Enabled;
        }
        folly::assume_unreachable();
    }

    cb::engine_errc checkFusionStatCallStatus(FusionStat stat,
                                              Vbid vbid,
                                              magma::Status status) const;

    MagmaKVStoreConfig& configuration;

    /**
     * Wrapped magma instance for a shard
     */
    std::unique_ptr<MagmaMemoryTrackingProxy> magma;

    // Path to magma files. Include shardID.
    const std::string magmaPath;

    std::atomic<size_t> scanCounter; // atomic counter for generating scan id

    // Keep track of the vbucket revision. Magma API can only accept revisions
    // upto u32
    std::vector<Monotonic<uint32_t>> kvstoreRevList;

    // For testing, we need to simulate couchstore where every batch
    // is a potential rollback point. We do this by Syncing after every batch
    // but that only creates a Sync checkpoint rather than a Rollback checkpoint
    // in magma. To create Rollback checkpoints on every flush we need to also
    // set the checkpoint_interval to 0.
    bool doSyncEveryBatch{false};

    // Using upsert for Set means we can't keep accurate document totals.
    // This is used for testing only!
    bool useUpsertForSet{false};

    folly::Synchronized<std::queue<std::tuple<Vbid, uint64_t>>>
            pendingVbucketDeletions;

    /// Status of continuous backup.
    enum class BackupStatus : char {
        Stopped,
        Started,
    };

    /**
     * Status of cont bk per vBucket. We only have maxVBuckets / maxShards
     * slots for efficiency. Does not use vector<bool> to avoid the bitset
     * optimisation, which is not thread-safe for concurrent access to different
     * elements.
     */
    std::vector<BackupStatus> continuousBackupStatus;

    /**
     * Set to true if we paused history eviction when opening the Magma DB.
     * We do this to ensure that continuous backup does not lose history due to
     * WAL replay pushing causing the history window to move at startup. We
     * should unpause history eviction after populating all vBuckets for this
     * shard and initialising continuous backup.
     */
    std::atomic<bool> historyEvictionPaused{false};

    /**
     * Testing hook called with the result of CompactKVStore when dropping
     * collections.
     */
    TestingHook<magma::Status&> compactionStatusHook;

    /**
     * Testing hook called before we call CompactKVStore.
     */
    TestingHook<> preCompactKVStoreHook;

    /**
     * Testing hook called with the result of Sync when creating a file handle
     */
    TestingHook<magma::Status&> fileHandleSyncStatusHook;

    /**
     * Testing hook called after collection stats are updated in the commit
     * data.
     * Hook is provided the request and the old item size.
     */
    TestingHook<const MagmaRequest&, size_t> updateStatsHook;

private:
    EventuallyPersistentEngine* currEngine;

    FusionUploaderManager fusionUploaderManager;
};

struct MagmaKVStoreTransactionContext : public TransactionContext {
    MagmaKVStoreTransactionContext(KVStore& kvstore,
                                   Vbid vbid,
                                   std::unique_ptr<PersistenceCallback> cb);

    /**
     * Prepare the requests before handing them over to Magma. This currently
     * orders by seqno (ascending) see MB-55199
     *
     * @param batchHistoryMode If the batch is a history batch
     *        (which can include duplicates)
     */
    void preparePendingRequests(magma::Magma::HistoryMode batchHistoryMode);

    /**
     * Container for pending Magma requests.
     *
     * Storing the pointer as preparePendingRequests will sort this container.
     */
    using PendingRequestQueue = std::deque<std::unique_ptr<MagmaRequest>>;

    PendingRequestQueue pendingReqs;
};
