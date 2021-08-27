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

#include "kvstore/kvstore.h"

class NexusKVStoreConfig;
class NexusKVStoreSecondaryPersistenceCallback;
class NexusRollbackCB;
class NexusKVStoreSecondaryGetAllKeysCallback;

struct NexusCompactionContext;
struct NexusRollbackContext;

/**
 * Testing harness for two KVStore implementations that runs both KVStores in
 * parallel and compares the results of interesting operations.
 */
class NexusKVStore : public KVStoreIface {
public:
    NexusKVStore(NexusKVStoreConfig& config);

    void deinitialize() override;
    void addStats(const AddStatFn& add_stat,
                  const void* c,
                  const std::string& args) const override;
    bool getStat(std::string_view name, size_t& value) const override;
    GetStatsMap getStats(gsl::span<const std::string_view> keys) const override;
    void addTimingStats(const AddStatFn& add_stat,
                        const CookieIface* c) const override;
    void resetStats() override;
    size_t getMemFootPrint() const override;
    bool commit(std::unique_ptr<TransactionContext> txnCtx,
                VB::Commit& commitData) override;
    StorageProperties getStorageProperties() const override;
    void set(TransactionContext& txnCtx, queued_item item) override;
    GetValue get(const DiskDocKey& key,
                 Vbid vb,
                 ValueFilter filter) const override;
    GetValue getWithHeader(const KVFileHandle& kvFileHandle,
                           const DiskDocKey& key,
                           Vbid vb,
                           ValueFilter filter) const override;
    void setMaxDataSize(size_t size) override;
    void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) const override;
    void getRange(Vbid vb,
                  const DiskDocKey& startKey,
                  const DiskDocKey& endKey,
                  ValueFilter filter,
                  const GetRangeCb& cb) const override;
    void del(TransactionContext& txnCtx, queued_item item) override;
    void delVBucket(Vbid vbucket, uint64_t fileRev) override;
    std::vector<vbucket_state*> listPersistedVbuckets() override;
    bool snapshotVBucket(Vbid vbucketId, const vbucket_state& vbstate) override;
    bool compactDB(std::unique_lock<std::mutex>& vbLock,
                   std::shared_ptr<CompactionContext> c) override;
    void abortCompactionIfRunning(std::unique_lock<std::mutex>& vbLock,
                                  Vbid vbid) override;
    vbucket_state* getCachedVBucketState(Vbid vbid) override;
    vbucket_state getPersistedVBucketState(Vbid vbid) override;
    size_t getNumPersistedDeletes(Vbid vbid) override;
    DBFileInfo getDbFileInfo(Vbid dbFileId) override;
    DBFileInfo getAggrDbFileInfo() override;
    size_t getItemCount(Vbid vbid) override;
    RollbackResult rollback(Vbid vbid,
                            uint64_t rollbackseqno,
                            std::unique_ptr<RollbackCB> ptr) override;
    void pendingTasks() override;
    cb::engine_errc getAllKeys(
            Vbid vbid,
            const DiskDocKey& start_key,
            uint32_t count,
            std::shared_ptr<StatusCallback<const DiskDocKey&>> cb)
            const override;
    bool supportsHistoricalSnapshots() const override;
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
            ValueFilter valOptions) const override;
    scan_error_t scan(BySeqnoScanContext& sctx) const override;
    scan_error_t scan(ByIdScanContext& sctx) const override;
    std::unique_ptr<KVFileHandle> makeFileHandle(Vbid vbid) const override;
    std::pair<GetCollectionStatsStatus, Collections::VB::PersistedStats>
    getCollectionStats(const KVFileHandle& kvFileHandle,
                       CollectionID collection) const override;
    std::pair<GetCollectionStatsStatus, Collections::VB::PersistedStats>
    getCollectionStats(Vbid vbid, CollectionID collection) const override;
    std::optional<Collections::ManifestUid> getCollectionsManifestUid(
            KVFileHandle& kvFileHandle) override;
    std::pair<bool, Collections::KVStore::Manifest> getCollectionsManifest(
            Vbid vbid) const override;
    std::pair<bool, std::vector<Collections::KVStore::DroppedCollection>>
    getDroppedCollections(Vbid vbid) const override;
    const KVStoreConfig& getConfig() const override;
    GetValue getBySeqno(KVFileHandle& handle,
                        Vbid vbid,
                        uint64_t seq,
                        ValueFilter filter) override;
    void setStorageThreads(ThreadPoolConfig::StorageThreadCount num) override;
    std::unique_ptr<TransactionContext> begin(
            Vbid vbid, std::unique_ptr<PersistenceCallback> pcb) override;
    const KVStoreStats& getKVStoreStat() const override;
    void setMakeCompactionContextCallback(
            MakeCompactionContextCallback cb) override;
    void setPostFlushHook(std::function<void()> hook) override;
    nlohmann::json getPersistedStats() const override;
    bool snapshotStats(const nlohmann::json& stats) override;
    void prepareToCreate(Vbid vbid) override;
    uint64_t prepareToDelete(Vbid vbid) override;
    uint64_t getLastPersistedSeqno(Vbid vbid) override;
    void prepareForDeduplication(std::vector<queued_item>& items) override;
    void setSystemEvent(TransactionContext& txnCtx,
                        const queued_item item) override;
    void delSystemEvent(TransactionContext& txnCtx,
                        const queued_item item) override;
    void endTransaction(Vbid vbid) override;

protected:
    uint64_t prepareToDeleteImpl(Vbid vbid) override;
    void prepareToCreateImpl(Vbid vbid) override;

    void handleError(std::string_view msg) const;

    /**
     * Create a Collections::VB::Manifest for the secondary KVStore to use
     * during this flush batch.
     *
     * @param vbid Vbid
     * @param commitData passed to ::commit and includes a reference to the
     *                   manifest via Collections::Flush
     * @return A copy of the Collections::VB::Manifest
     */
    Collections::VB::Manifest generateSecondaryVBManifest(
            Vbid vbid, const VB::Commit& commitData);

    /**
     * Check a various state after updating both primary and secondary KVStores
     *
     * @param vbid Vbid
     * @param primaryVBManifest in memory primary manifest (passing nullptr
     *        skips the checks against the in memory manifest)
     * @param secondaryVBManifest in memory secondary manifest (passing nullptr
     *        skips the checks against the in memory manifest)
     */
    void doCollectionsMetadataChecks(
            Vbid vbid,
            const Collections::VB::Manifest* primaryVBManifest,
            const Collections::VB::Manifest* secondaryVBManifest);

    /**
     * We cache locks per-vBucket and to save memory usage we only allocate
     * num vBuckets / num shards slots in the array. Acquire and return the
     * desired lock to the caller.
     *
     * @param vbid Vbid of the lock we want
     * @return acquired unique lock for the vBucket
     */
    [[nodiscard]] std::unique_lock<std::mutex> getLock(Vbid vbid) const;

    /**
     * Compare get values of the primary against the secondary. Compares status
     * and the resulting item.
     *
     * @param caller string to log
     * @param vbid vbucket
     * @param key to log
     * @param primaryGetValue
     * @param secondaryGetValue
     */
    void doPostGetChecks(std::string_view caller,
                         Vbid vbid,
                         const DiskDocKey& key,
                         const GetValue& primaryGetValue,
                         const GetValue& secondaryGetValue) const;

    /**
     * Compare the two items returns from the kvstores. Can't use the typical
     * Item comparator as that compares datatype and value which may not be the
     * same if one KVStore automatically compresses items (couchstore) and
     * another does not (magma).
     *
     * @param primaryItem not const as may need to decompress, but we are taking
     *                    a copy to not change the result
     * @param secondaryItem not const as may need to decompress, but we are
     *                      taking a copy to not change the result
     * @return true if logically equivalent
     */
    bool compareItem(Item primaryItem, Item secondaryItem) const;

    /**
     * Work out which order to run compaction in
     * @param primaryCtx
     * @param secondaryCtx
     * @return A NexusCompactionContext containing the KVStore order and KVStore
     *         specific contexts
     */
    NexusCompactionContext calculateCompactionOrder(
            std::shared_ptr<CompactionContext> primaryCtx,
            std::shared_ptr<CompactionContext> secondaryCtx);

    /**
     * Work out which order to run rollback in
     * @return A NexusRollbackContext containing the KVStore order
     */
    NexusRollbackContext calculateRollbackOrder();

    /**
     * Comapre the two vbucket states returned from the kvstores.
     *
     * @param primaryState not const as may need to overwrite prepare fields if
     *                     not comparable
     * @param secondaryState not const as may need to overwrite prepare fields
     *                       if not compared
     * @return true if logically equivalent
     */
    bool compareVBucketState(vbucket_state primaryVbState,
                             vbucket_state secondaryVbState) const;

    // Friended to let us call handleError to error from associated classes
    friend NexusKVStoreSecondaryPersistenceCallback;
    friend NexusKVStoreSecondaryGetAllKeysCallback;
    friend NexusRollbackCB;

protected:
    NexusKVStoreConfig& configuration;
    std::unique_ptr<KVStoreIface> primary;
    std::unique_ptr<KVStoreIface> secondary;

    /**
     * During rollback we make a call to getWithHeader in EPDiskRollbackCB for
     * each item we are attempting to rollback to compare the before and after
     * state of the document. When we make this call, the underlying file has
     * already been reverted to the previous state and we are just iterating the
     * changes. As such, a call to getWithHeader for the rolling back KVStore
     * will have a different result (if it's the one done first) to the other
     * KVStore. This means we have to skip the checks we'd normally do in
     * getWithHeader for this portion of a rollback.
     */
    bool skipGetWithHeaderChecksForRollback = false;

    /**
     * Mutexes that allow us to lock interesting actions for some particular
     * vBucket. For example, because we want to be able to do a get against
     * both KVStores and compare the results we need to be able to prevent
     * flushing from happening at the same time to ensure that the comparison
     * is valid. Logically we could use KVBucket::vb_mutexes to the same effect,
     * but given that the purpose of NexusKVStore is to test, it makes sense
     * to use a system as close to the typical KVStore usage as possible.
     *
     * Mutable so that we can take the lock in const functions such as get.
     */
    mutable std::vector<std::mutex> vbMutexes;
};