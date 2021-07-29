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

/**
 * Testing harness for two KVStore implementations that runs both KVStores in
 * parallel and compares the results of interesting operations.
 */
class NexusKVStore : public KVStoreIface {
public:
    NexusKVStore(KVStoreConfig& config);

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
            SnapshotSource source) const override;
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
    std::pair<bool, Collections::VB::PersistedStats> getCollectionStats(
            const KVFileHandle& kvFileHandle,
            CollectionID collection) const override;
    std::pair<bool, Collections::KVStore::Manifest> getCollectionsManifest(
            Vbid vbid) const override;
    std::pair<bool, std::vector<Collections::KVStore::DroppedCollection>>
    getDroppedCollections(Vbid vbid) override;
    const KVStoreConfig& getConfig() const override;
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

protected:
    uint64_t prepareToDeleteImpl(Vbid vbid) override;
    void prepareToCreateImpl(Vbid vbid) override;

protected:
    std::unique_ptr<KVStoreIface> primary;
};