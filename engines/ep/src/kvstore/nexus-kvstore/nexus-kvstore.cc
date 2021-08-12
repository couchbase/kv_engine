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

#include "nexus-kvstore.h"

#include "bucket_logger.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "kvstore/kvstore_transaction_context.h"
#include "nexus-kvstore-config.h"
#include "nexus-kvstore-transaction-context.h"
#include "rollback_result.h"
#include "vb_commit.h"
#include "vbucket_state.h"

NexusKVStore::NexusKVStore(NexusKVStoreConfig& config) : configuration(config) {
    primary = KVStoreFactory::create(configuration.getPrimaryConfig());
    secondary = KVStoreFactory::create(configuration.getSecondaryConfig());
}

void NexusKVStore::deinitialize() {
    primary->deinitialize();
    secondary->deinitialize();
}

void NexusKVStore::addStats(const AddStatFn& add_stat,
                            const void* c,
                            const std::string& args) const {
    primary->addStats(add_stat, c, args);
}

bool NexusKVStore::getStat(std::string_view name, size_t& value) const {
    return primary->getStat(name, value);
}

GetStatsMap NexusKVStore::getStats(
        gsl::span<const std::string_view> keys) const {
    return primary->getStats(keys);
}

void NexusKVStore::addTimingStats(const AddStatFn& add_stat,
                                  const CookieIface* c) const {
    primary->addTimingStats(add_stat, c);
}

void NexusKVStore::resetStats() {
    primary->resetStats();
    secondary->resetStats();
}

size_t NexusKVStore::getMemFootPrint() const {
    return primary->getMemFootPrint() + secondary->getMemFootPrint();
}

Collections::VB::Manifest NexusKVStore::generateSecondaryVBManifest(
        Vbid vbid, const VB::Commit& primaryCommitData) {
    // Need to create the manifest for the secondary KVStore
    auto secondaryManifest = primaryCommitData.collections.getManifest();

    // Having generated the Manifest object we now need to correct the disk
    // sizes as they may differ between KVStores. We'll load the disk sizes of
    // each collection now...
    auto secondaryFileHandle = secondary->makeFileHandle(vbid);
    if (secondaryFileHandle) {
        auto collections = secondaryManifest.wlock();
        for (auto& itr : collections) {
            auto& [cid, entry] = itr;
            auto [statsSuccess, stats] =
                    secondary->getCollectionStats(*secondaryFileHandle, cid);
            if (statsSuccess) {
                collections.setDiskSize(cid, stats.diskSize);
            }
        }
    }

    return secondaryManifest;
}

void NexusKVStore::doPostFlushSanityChecks(
        Vbid vbid,
        const Collections::VB::Manifest& primaryVBManifest,
        const Collections::VB::Manifest& secondaryVBManifest) {
    // 1) Compare on disk manifests
    auto [primaryManifestResult, primaryKVStoreManifest] =
            primary->getCollectionsManifest(vbid);
    auto [secondaryManifestResult, secondaryKVStoreManifest] =
            secondary->getCollectionsManifest(vbid);
    if (primaryManifestResult != secondaryManifestResult) {
        auto msg = fmt::format(
                "NexusKVStore::doPostFlushSanityChecks: {}: issue getting "
                "collections manifest primary:{} secondary:{}",
                vbid,
                primaryManifestResult,
                secondaryManifestResult);
        handleError(msg);
    }

    // @TODO MB-47604: currently we don't check droppedCollectionsExist when
    // comparing the two manifests because we don't currently run compaction
    // against the secondary KVStore.
    if (primaryKVStoreManifest.manifestUid !=
        secondaryKVStoreManifest.manifestUid) {
        auto msg = fmt::format(
                "NexusKVStore::doPostFlushSanityChecks: {}: collections "
                "manifest uid not equal primary:{} secondary: {}",
                vbid,
                primaryKVStoreManifest.manifestUid,
                secondaryKVStoreManifest.manifestUid);
        handleError(msg);
    }

    if (!primaryKVStoreManifest.compareCollections(secondaryKVStoreManifest)) {
        auto msg = fmt::format(
                "NexusKVStore::doPostFlushSanityChecks: {}: collections "
                "manifest collections not equal primary:{} secondary:{}",
                vbid,
                primaryKVStoreManifest,
                secondaryKVStoreManifest);
        handleError(msg);
    }

    if (!primaryKVStoreManifest.compareScopes(secondaryKVStoreManifest)) {
        auto msg = fmt::format(
                "NexusKVStore::doPostFlushSanityChecks: {}: collections "
                "manifest scopes not equal primary:{} secondary:{}",
                vbid,
                primaryKVStoreManifest,
                secondaryKVStoreManifest);
        handleError(msg);
    }

    // 2) Compare collections stats doc values
    for (const auto& collection : primaryKVStoreManifest.collections) {
        auto& cid = collection.metaData.cid;

        auto primaryHandle = primary->makeFileHandle(vbid);
        auto [primaryResult, primaryStats] =
                primary->getCollectionStats(*primaryHandle, cid);
        auto secondaryHandle = secondary->makeFileHandle(vbid);
        auto [secondaryResult, secondaryStats] =
                secondary->getCollectionStats(*secondaryHandle, cid);
        if (primaryResult != secondaryResult) {
            auto msg = fmt::format(
                    "NexusKVStore::doPostFlushSanityChecks: {}: issue getting "
                    "collection stats primary:{} secondary:{}",
                    vbid,
                    primaryResult,
                    secondaryResult);
            handleError(msg);
        }

        if (primaryStats.itemCount != secondaryStats.itemCount) {
            auto msg = fmt::format(
                    "NexusKVStore::doPostFlushSanityChecks: {}: cid:{} item "
                    "count mismatch primary:{} secondary:{}",
                    vbid,
                    cid,
                    primaryStats.itemCount,
                    secondaryStats.itemCount);
            handleError(msg);
        }
        if (primaryStats.itemCount !=
            primaryVBManifest.lock(cid).getItemCount()) {
            auto msg = fmt::format(
                    "NexusKVStore::doPostFlushSanityChecks: {}: cid:{} item "
                    "count mismatch for primary disk:{} VBManifest:{}",
                    vbid,
                    cid,
                    primaryStats.itemCount,
                    primaryVBManifest.lock(cid).getItemCount());
            handleError(msg);
        }
        if (secondaryStats.itemCount !=
            secondaryVBManifest.lock(cid).getItemCount()) {
            auto msg = fmt::format(
                    "NexusKVStore::doPostFlushSanityChecks: {}: cid:{} item "
                    "count mismatch for secondary disk:{} VBManifest:{}",
                    vbid,
                    cid,
                    secondaryStats.itemCount,
                    secondaryVBManifest.lock(cid).getItemCount());
            handleError(msg);
        }

        if (primaryStats.highSeqno != secondaryStats.highSeqno) {
            auto msg = fmt::format(
                    "NexusKVStore::doPostFlushSanityChecks: {}: cid:{} high "
                    "seqno mismatch primary:{} secondary:{}",
                    vbid,
                    cid,
                    primaryStats.highSeqno,
                    secondaryStats.highSeqno);
            handleError(msg);
        }
        if (primaryStats.highSeqno !=
            primaryVBManifest.lock(cid).getPersistedHighSeqno()) {
            auto msg = fmt::format(
                    "NexusKVStore::doPostFlushSanityChecks: {}: cid:{} high "
                    "seqno mismatch for primary disk:{} VBManifest:{}",
                    vbid,
                    cid,
                    primaryStats.highSeqno,
                    primaryVBManifest.lock(cid).getPersistedHighSeqno());
            handleError(msg);
        }
        if (secondaryStats.highSeqno !=
            secondaryVBManifest.lock(cid).getPersistedHighSeqno()) {
            auto msg = fmt::format(
                    "NexusKVStore::doPostFlushSanityChecks: {}: cid:{} high "
                    "seqno mismatch for secondary disk:{} VBManifest:{}",
                    vbid,
                    cid,
                    secondaryStats.highSeqno,
                    secondaryVBManifest.lock(cid).getPersistedHighSeqno());
            handleError(msg);
        }

        // We can't compare disk size between primary and secondary as they
        // will differ if the underlying KVStore type is different. We can
        // check them against the VB Manifest though.
        if (primaryStats.diskSize !=
            primaryVBManifest.lock(cid).getDiskSize()) {
            auto msg = fmt::format(
                    "NexusKVStore::doPostFlushSanityChecks: {}: cid:{} disk "
                    "size mismatch for primary disk:{} VBManifest:{}",
                    vbid,
                    cid,
                    primaryStats.diskSize,
                    primaryVBManifest.lock(cid).getDiskSize());
            handleError(msg);
        }
        if (secondaryStats.diskSize !=
            secondaryVBManifest.lock(cid).getDiskSize()) {
            auto msg = fmt::format(
                    "NexusKVStore::doPostFlushSanityChecks: {}: cid:{} disk "
                    "size mismatch for secondary disk:{} VBManifest:{}",
                    vbid,
                    cid,
                    secondaryStats.diskSize,
                    secondaryVBManifest.lock(cid).getDiskSize());
            handleError(msg);
        }
    }
}

bool NexusKVStore::commit(std::unique_ptr<TransactionContext> txnCtx,
                          VB::Commit& primaryCommitData) {
    auto& nexusTxnCtx = dynamic_cast<NexusKVStoreTransactionContext&>(*txnCtx);
    auto vbid = txnCtx->vbid;

    // Need to create the manifest for the secondary KVStore
    auto secondaryVBManifest =
            generateSecondaryVBManifest(vbid, primaryCommitData);

    // Copy the flush tracking before we go and update it for each doc we see.
    // We need this copy for the secondary to start the counts from the same
    // place. We need to swap the manifest that the collections flush object
    // points to though so that it doesn't try to update the original manifest
    // for the secondary KVStore. We'll check that manifest against the disk
    // state to make sure the secondary KVStore is correct.
    VB::Commit secondaryCommitData = primaryCommitData;
    secondaryCommitData.collections.setManifest(secondaryVBManifest);

    auto primaryResult = primary->commit(std::move(nexusTxnCtx.primaryContext),
                                         primaryCommitData);
    auto secondaryResult = secondary->commit(
            std::move(nexusTxnCtx.secondaryContext), secondaryCommitData);
    if (primaryResult != secondaryResult) {
        auto msg = fmt::format(
                "NexusKVStore::commit: {}: primaryResult:{} secondaryResult:{}",
                vbid,
                primaryResult,
                secondaryResult);
        handleError(msg);
    }

    doPostFlushSanityChecks(vbid,
                            primaryCommitData.collections.getManifest(),
                            secondaryVBManifest);

    return primaryResult;
}

StorageProperties NexusKVStore::getStorageProperties() const {
    auto primaryProperties = primary->getStorageProperties();
    auto secondaryProperties = secondary->getStorageProperties();

    // ByIdScan adds an extra DCP feature that clients should not assume
    // exists (so we should only enable it if bost KVStores support it).
    auto byIdScan = StorageProperties::ByIdScan::No;
    if (primaryProperties.hasByIdScan() && secondaryProperties.hasByIdScan()) {
        byIdScan = StorageProperties::ByIdScan::Yes;
    }

    // Auto de-dupe will change the flush batches and not all KVStores can deal
    // with that so we can only set it to true if it's true for both. All
    // KVStores should be able to deal with a deduped flush batch.
    auto autoDedupe = StorageProperties::AutomaticDeduplication::No;
    if (primaryProperties.hasAutomaticDeduplication() &&
        secondaryProperties.hasAutomaticDeduplication()) {
        autoDedupe = StorageProperties::AutomaticDeduplication::Yes;
    }

    return StorageProperties(byIdScan, autoDedupe);
}

void NexusKVStore::set(TransactionContext& txnCtx, queued_item item) {
    auto& nexusTxnCtx = dynamic_cast<NexusKVStoreTransactionContext&>(txnCtx);
    primary->set(*nexusTxnCtx.primaryContext, item);
    secondary->set(*nexusTxnCtx.secondaryContext, item);
}

GetValue NexusKVStore::get(const DiskDocKey& key,
                           Vbid vb,
                           ValueFilter filter) const {
    return primary->get(key, vb, filter);
}

GetValue NexusKVStore::getWithHeader(const KVFileHandle& kvFileHandle,
                                     const DiskDocKey& key,
                                     Vbid vb,
                                     ValueFilter filter) const {
    return primary->getWithHeader(kvFileHandle, key, vb, filter);
}

void NexusKVStore::setMaxDataSize(size_t size) {
    primary->setMaxDataSize(size);
    secondary->setMaxDataSize(size);
}

void NexusKVStore::getMulti(Vbid vb, vb_bgfetch_queue_t& itms) const {
    primary->getMulti(vb, itms);
}

void NexusKVStore::getRange(Vbid vb,
                            const DiskDocKey& startKey,
                            const DiskDocKey& endKey,
                            ValueFilter filter,
                            const KVStoreIface::GetRangeCb& cb) const {
    primary->getRange(vb, startKey, endKey, filter, cb);
}

void NexusKVStore::del(TransactionContext& txnCtx, queued_item item) {
    auto& nexusTxnCtx = dynamic_cast<NexusKVStoreTransactionContext&>(txnCtx);
    primary->del(*nexusTxnCtx.primaryContext, item);
    secondary->del(*nexusTxnCtx.secondaryContext, item);
}

void NexusKVStore::delVBucket(Vbid vbucket, uint64_t fileRev) {
    primary->delVBucket(vbucket, fileRev);
    secondary->delVBucket(vbucket, fileRev);
}

std::vector<vbucket_state*> NexusKVStore::listPersistedVbuckets() {
    return primary->listPersistedVbuckets();
}

bool NexusKVStore::snapshotVBucket(Vbid vbucketId,
                                   const vbucket_state& vbstate) {
    return primary->snapshotVBucket(vbucketId, vbstate);
}

bool NexusKVStore::compactDB(std::unique_lock<std::mutex>& vbLock,
                             std::shared_ptr<CompactionContext> c) {
    return primary->compactDB(vbLock, c);
}

void NexusKVStore::abortCompactionIfRunning(
        std::unique_lock<std::mutex>& vbLock, Vbid vbid) {
    primary->abortCompactionIfRunning(vbLock, vbid);
    secondary->abortCompactionIfRunning(vbLock, vbid);
}

vbucket_state* NexusKVStore::getCachedVBucketState(Vbid vbid) {
    return primary->getCachedVBucketState(vbid);
}

vbucket_state NexusKVStore::getPersistedVBucketState(Vbid vbid) {
    return primary->getPersistedVBucketState(vbid);
}

size_t NexusKVStore::getNumPersistedDeletes(Vbid vbid) {
    return primary->getNumPersistedDeletes(vbid);
}

DBFileInfo NexusKVStore::getDbFileInfo(Vbid dbFileId) {
    return primary->getDbFileInfo(dbFileId);
}

DBFileInfo NexusKVStore::getAggrDbFileInfo() {
    return primary->getAggrDbFileInfo();
}

size_t NexusKVStore::getItemCount(Vbid vbid) {
    return primary->getItemCount(vbid);
}

RollbackResult NexusKVStore::rollback(Vbid vbid,
                                      uint64_t rollbackseqno,
                                      std::unique_ptr<RollbackCB> ptr) {
    return primary->rollback(vbid, rollbackseqno, std::move(ptr));
}

void NexusKVStore::pendingTasks() {
    primary->pendingTasks();
    secondary->pendingTasks();
}

cb::engine_errc NexusKVStore::getAllKeys(
        Vbid vbid,
        const DiskDocKey& start_key,
        uint32_t count,
        std::shared_ptr<StatusCallback<const DiskDocKey&>> cb) const {
    return primary->getAllKeys(vbid, start_key, count, cb);
}

bool NexusKVStore::supportsHistoricalSnapshots() const {
    return primary->supportsHistoricalSnapshots() &&
           secondary->supportsHistoricalSnapshots();
}

std::unique_ptr<BySeqnoScanContext> NexusKVStore::initBySeqnoScanContext(
        std::unique_ptr<StatusCallback<GetValue>> cb,
        std::unique_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        uint64_t startSeqno,
        DocumentFilter options,
        ValueFilter valOptions,
        SnapshotSource source) const {
    return primary->initBySeqnoScanContext(std::move(cb),
                                           std::move(cl),
                                           vbid,
                                           startSeqno,
                                           options,
                                           valOptions,
                                           source);
}

std::unique_ptr<ByIdScanContext> NexusKVStore::initByIdScanContext(
        std::unique_ptr<StatusCallback<GetValue>> cb,
        std::unique_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        const std::vector<ByIdRange>& ranges,
        DocumentFilter options,
        ValueFilter valOptions) const {
    return primary->initByIdScanContext(
            std::move(cb), std::move(cl), vbid, ranges, options, valOptions);
}

scan_error_t NexusKVStore::scan(BySeqnoScanContext& sctx) const {
    return primary->scan(sctx);
}

scan_error_t NexusKVStore::scan(ByIdScanContext& sctx) const {
    return primary->scan(sctx);
}

std::unique_ptr<KVFileHandle> NexusKVStore::makeFileHandle(Vbid vbid) const {
    return primary->makeFileHandle(vbid);
}

std::pair<bool, Collections::VB::PersistedStats>
NexusKVStore::getCollectionStats(const KVFileHandle& kvFileHandle,
                                 CollectionID collection) const {
    return primary->getCollectionStats(kvFileHandle, collection);
}

std::pair<bool, Collections::KVStore::Manifest>
NexusKVStore::getCollectionsManifest(Vbid vbid) const {
    return primary->getCollectionsManifest(vbid);
}

std::pair<bool, std::vector<Collections::KVStore::DroppedCollection>>
NexusKVStore::getDroppedCollections(Vbid vbid) {
    return primary->getDroppedCollections(vbid);
}

const KVStoreConfig& NexusKVStore::getConfig() const {
    return primary->getConfig();
}

void NexusKVStore::setStorageThreads(ThreadPoolConfig::StorageThreadCount num) {
    primary->setStorageThreads(num);
    secondary->setStorageThreads(num);
}

std::unique_ptr<TransactionContext> NexusKVStore::begin(
        Vbid vbid, std::unique_ptr<PersistenceCallback> pcb) {
    auto primaryContext = primary->begin(vbid, std::move(pcb));
    auto secondaryContext =
            secondary->begin(vbid, std::make_unique<PersistenceCallback>());

    return std::make_unique<NexusKVStoreTransactionContext>(
            *this,
            vbid,
            std::move(primaryContext),
            std::move(secondaryContext));
}

const KVStoreStats& NexusKVStore::getKVStoreStat() const {
    return primary->getKVStoreStat();
}

void NexusKVStore::setMakeCompactionContextCallback(
        MakeCompactionContextCallback cb) {
    primary->setMakeCompactionContextCallback(cb);
}

void NexusKVStore::setPostFlushHook(std::function<void()> hook) {
    primary->setPostFlushHook(hook);
}

nlohmann::json NexusKVStore::getPersistedStats() const {
    return primary->getPersistedStats();
}

bool NexusKVStore::snapshotStats(const nlohmann::json& stats) {
    return primary->snapshotStats(stats);
}

void NexusKVStore::prepareToCreate(Vbid vbid) {
    primary->prepareToCreate(vbid);
    secondary->prepareToCreate(vbid);
}

uint64_t NexusKVStore::prepareToDelete(Vbid vbid) {
    auto primaryResult = primary->prepareToDelete(vbid);
    auto secondaryResult = secondary->prepareToDelete(vbid);

    if (primaryResult != secondaryResult) {
        auto msg = fmt::format(
                "NexusKVStore::prepareToDelete: {}: primaryResult:{} "
                "secondaryResult:{}",
                vbid,
                primaryResult,
                secondaryResult);
        handleError(msg);
    }

    return primaryResult;
}

uint64_t NexusKVStore::getLastPersistedSeqno(Vbid vbid) {
    return primary->getLastPersistedSeqno(vbid);
}

void NexusKVStore::prepareForDeduplication(std::vector<queued_item>& items) {
    primary->prepareForDeduplication(items);
}

void NexusKVStore::setSystemEvent(TransactionContext& txnCtx,
                                  const queued_item item) {
    auto& nexusTxnCtx = dynamic_cast<NexusKVStoreTransactionContext&>(txnCtx);
    primary->setSystemEvent(*nexusTxnCtx.primaryContext, item);
    secondary->setSystemEvent(*nexusTxnCtx.secondaryContext, item);
}

void NexusKVStore::delSystemEvent(TransactionContext& txnCtx,
                                  const queued_item item) {
    auto& nexusTxnCtx = dynamic_cast<NexusKVStoreTransactionContext&>(txnCtx);
    primary->delSystemEvent(*nexusTxnCtx.primaryContext, item);
    secondary->delSystemEvent(*nexusTxnCtx.secondaryContext, item);
}

uint64_t NexusKVStore::prepareToDeleteImpl(Vbid vbid) {
    auto primaryResult = primary->prepareToDeleteImpl(vbid);
    auto secondaryResult = secondary->prepareToDeleteImpl(vbid);

    if (primaryResult != secondaryResult) {
        auto msg = fmt::format(
                "NexusKVStore::prepareToDeleteImpl: {}: primaryResult:{} "
                "secondaryResult:{}",
                vbid,
                primaryResult,
                secondaryResult);
        handleError(msg);
    }

    return primaryResult;
}

void NexusKVStore::prepareToCreateImpl(Vbid vbid) {
    primary->prepareToCreateImpl(vbid);
    secondary->prepareToCreateImpl(vbid);
}

void NexusKVStore::handleError(std::string_view msg) {
    // Always worth logging
    EP_LOG_CRITICAL("{}", msg);

    switch (configuration.getErrorHandlingMethod()) {
    case NexusErrorHandlingMethod::Log:
        return;
    case NexusErrorHandlingMethod::Abort:
        std::abort();
    case NexusErrorHandlingMethod::Throw:
        throw std::logic_error(std::string(msg));
    }
}

void NexusKVStore::endTransaction(Vbid vbid) {
    primary->endTransaction(vbid);
    secondary->endTransaction(vbid);
}
