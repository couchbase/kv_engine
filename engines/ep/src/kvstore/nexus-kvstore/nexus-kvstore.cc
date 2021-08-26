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
#include "collections/collection_persisted_stats.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "kvstore/kvstore_transaction_context.h"
#ifdef EP_USE_MAGMA
#include "kvstore/magma-kvstore/magma-kvstore.h"
#endif
#include "nexus-kvstore-config.h"
#include "nexus-kvstore-persistence-callback.h"
#include "nexus-kvstore-transaction-context.h"
#include "rollback_result.h"
#include "vb_commit.h"
#include "vbucket.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"

#include <utilities/logtags.h>

#include <utility>

NexusKVStore::NexusKVStore(NexusKVStoreConfig& config) : configuration(config) {
    primary = KVStoreFactory::create(configuration.getPrimaryConfig());
    secondary = KVStoreFactory::create(configuration.getSecondaryConfig());

    auto cacheSize = configuration.getCacheSize();
    vbMutexes = std::vector<std::mutex>(cacheSize);
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

    auto collections = secondaryManifest.wlock();
    for (auto& itr : collections) {
        auto& [cid, entry] = itr;
        auto [status, stats] = secondary->getCollectionStats(vbid, cid);
        if (status == GetCollectionStatsStatus::Success) {
            collections.setDiskSize(cid, stats.diskSize);
        }
    }

    return secondaryManifest;
}

void NexusKVStore::doCollectionsMetadataChecks(
        Vbid vbid,
        const Collections::VB::Manifest* primaryVBManifest,
        const Collections::VB::Manifest* secondaryVBManifest) {
    // 1) Compare on disk manifests
    auto [primaryManifestResult, primaryKVStoreManifest] =
            primary->getCollectionsManifest(vbid);
    auto [secondaryManifestResult, secondaryKVStoreManifest] =
            secondary->getCollectionsManifest(vbid);
    if (primaryManifestResult != secondaryManifestResult) {
        auto msg = fmt::format(
                "NexusKVStore::doCollectionsMetadataChecks: {}: issue getting "
                "collections manifest primary:{} secondary:{}",
                vbid,
                primaryManifestResult,
                secondaryManifestResult);
        handleError(msg);
    }

    if (primaryKVStoreManifest != secondaryKVStoreManifest) {
        auto msg = fmt::format(
                "NexusKVStore::doCollectionsMetadataChecks: {}: collections "
                "manifest not equal primary:{} secondary: {}",
                vbid,
                primaryKVStoreManifest.manifestUid,
                secondaryKVStoreManifest.manifestUid);
        handleError(msg);
    }

    // 2) Compare collections stats doc values
    for (const auto& collection : primaryKVStoreManifest.collections) {
        auto& cid = collection.metaData.cid;

        auto [primaryResult, primaryStats] =
                primary->getCollectionStats(vbid, cid);

        auto [secondaryResult, secondaryStats] =
                secondary->getCollectionStats(vbid, cid);
        if (primaryResult != secondaryResult) {
            auto msg = fmt::format(
                    "NexusKVStore::doCollectionsMetadataChecks: {}: issue "
                    "getting "
                    "collection stats primary:{} secondary:{}",
                    vbid,
                    primaryResult,
                    secondaryResult);
            handleError(msg);
        }

        if (primaryStats.itemCount != secondaryStats.itemCount) {
            auto msg = fmt::format(
                    "NexusKVStore::doCollectionsMetadataChecks: {}: cid:{} "
                    "item "
                    "count mismatch primary:{} secondary:{}",
                    vbid,
                    cid,
                    primaryStats.itemCount,
                    secondaryStats.itemCount);
            handleError(msg);
        }
        if (primaryVBManifest &&
            primaryStats.itemCount !=
                    primaryVBManifest->lock(cid).getItemCount()) {
            auto msg = fmt::format(
                    "NexusKVStore::doCollectionsMetadataChecks: {}: cid:{} "
                    "item "
                    "count mismatch for primary disk:{} VBManifest:{}",
                    vbid,
                    cid,
                    primaryStats.itemCount,
                    primaryVBManifest->lock(cid).getItemCount());
            handleError(msg);
        }
        if (secondaryVBManifest &&
            secondaryStats.itemCount !=
                    secondaryVBManifest->lock(cid).getItemCount()) {
            auto msg = fmt::format(
                    "NexusKVStore::doCollectionsMetadataChecks: {}: cid:{} "
                    "item "
                    "count mismatch for secondary disk:{} VBManifest:{}",
                    vbid,
                    cid,
                    secondaryStats.itemCount,
                    secondaryVBManifest->lock(cid).getItemCount());
            handleError(msg);
        }

        if (primaryStats.highSeqno != secondaryStats.highSeqno) {
            auto msg = fmt::format(
                    "NexusKVStore::doCollectionsMetadataChecks: {}: cid:{} "
                    "high "
                    "seqno mismatch primary:{} secondary:{}",
                    vbid,
                    cid,
                    primaryStats.highSeqno,
                    secondaryStats.highSeqno);
            handleError(msg);
        }
        if (primaryVBManifest &&
            primaryStats.highSeqno !=
                    primaryVBManifest->lock(cid).getPersistedHighSeqno()) {
            auto msg = fmt::format(
                    "NexusKVStore::doCollectionsMetadataChecks: {}: cid:{} "
                    "high "
                    "seqno mismatch for primary disk:{} VBManifest:{}",
                    vbid,
                    cid,
                    primaryStats.highSeqno,
                    primaryVBManifest->lock(cid).getPersistedHighSeqno());
            handleError(msg);
        }
        if (secondaryVBManifest &&
            secondaryStats.highSeqno !=
                    secondaryVBManifest->lock(cid).getPersistedHighSeqno()) {
            auto msg = fmt::format(
                    "NexusKVStore::doCollectionsMetadataChecks: {}: cid:{} "
                    "high "
                    "seqno mismatch for secondary disk:{} VBManifest:{}",
                    vbid,
                    cid,
                    secondaryStats.highSeqno,
                    secondaryVBManifest->lock(cid).getPersistedHighSeqno());
            handleError(msg);
        }

        // We can't compare disk size between primary and secondary as they
        // will differ if the underlying KVStore type is different. We can
        // check them against the VB Manifest though.
        if (primaryVBManifest &&
            primaryStats.diskSize !=
                    primaryVBManifest->lock(cid).getDiskSize()) {
            auto msg = fmt::format(
                    "NexusKVStore::doCollectionsMetadataChecks: {}: cid:{} "
                    "disk "
                    "size mismatch for primary disk:{} VBManifest:{}",
                    vbid,
                    cid,
                    primaryStats.diskSize,
                    primaryVBManifest->lock(cid).getDiskSize());
            handleError(msg);
        }
        if (secondaryVBManifest &&
            secondaryStats.diskSize !=
                    secondaryVBManifest->lock(cid).getDiskSize()) {
            auto msg = fmt::format(
                    "NexusKVStore::doCollectionsMetadataChecks: {}: cid:{} "
                    "disk "
                    "size mismatch for secondary disk:{} VBManifest:{}",
                    vbid,
                    cid,
                    secondaryStats.diskSize,
                    secondaryVBManifest->lock(cid).getDiskSize());
            handleError(msg);
        }
    }
}

bool NexusKVStore::commit(std::unique_ptr<TransactionContext> txnCtx,
                          VB::Commit& primaryCommitData) {
    auto& nexusTxnCtx = dynamic_cast<NexusKVStoreTransactionContext&>(*txnCtx);
    auto vbid = txnCtx->vbid;

    auto lh = getLock(vbid);

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

    // Secondary commit data needs some tweaking in
    auto* secondaryVbState = secondary->getCachedVBucketState(vbid);
    if (!primary->getStorageProperties().hasPrepareCounting() &&
        secondary->getStorageProperties().hasPrepareCounting() &&
        secondaryVbState) {
        // Secondary supports prepare counting but primary doesn't. This means
        // that flushes which call getCachedVBucketState and other things that
        // call getPersistedVBucketState will have incorrect numbers for
        // prepares as we'll reset counters on every flush. We can fix this by
        // passing back the secondary count (under the assumption that the
        // primary doesn't care about it).
        secondaryCommitData.proposedVBState.onDiskPrepares =
                secondaryVbState->onDiskPrepares;
        secondaryCommitData.proposedVBState.setOnDiskPrepareBytes(
                secondaryVbState->getOnDiskPrepareBytes());
    }

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

    doCollectionsMetadataChecks(vbid,
                                &primaryCommitData.collections.getManifest(),
                                &secondaryVBManifest);

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

    // Not all KVStores can count prepares
    auto prepareCounting = StorageProperties::PrepareCounting::No;
    if (primaryProperties.hasPrepareCounting() &&
        secondaryProperties.hasPrepareCounting()) {
        prepareCounting = StorageProperties::PrepareCounting::Yes;
    }

    // Nexus calls back from compaction with the callbacks from the primary.
    // The bucket should be able to deal with either.
    auto compactionStaleItemCallbacks =
            primary->getStorageProperties().hasCompactionStaleItemCallbacks()
                    ? StorageProperties::CompactionStaleItemCallbacks::Yes
                    : StorageProperties::CompactionStaleItemCallbacks::No;

    return StorageProperties(byIdScan,
                             autoDedupe,
                             prepareCounting,
                             compactionStaleItemCallbacks);
}

void NexusKVStore::set(TransactionContext& txnCtx, queued_item item) {
    auto& nexusTxnCtx = dynamic_cast<NexusKVStoreTransactionContext&>(txnCtx);
    primary->set(*nexusTxnCtx.primaryContext, item);
    secondary->set(*nexusTxnCtx.secondaryContext, item);
}

void NexusKVStore::doPostGetChecks(std::string_view caller,
                                   Vbid vb,
                                   const DiskDocKey& key,
                                   const GetValue& primaryGetValue,
                                   const GetValue& secondaryGetValue) const {
    if (primaryGetValue.getStatus() != secondaryGetValue.getStatus()) {
        auto msg = fmt::format(
                "NexusKVStore::{}: {} key:{} status mismatch primary:{} "
                "secondary:{}",
                caller,
                vb,
                cb::UserData(key.to_string()),
                primaryGetValue.getStatus(),
                secondaryGetValue.getStatus());
        handleError(msg);
    }

    if (primaryGetValue.getStatus() == cb::engine_errc::success &&
        !compareItem(*primaryGetValue.item, *secondaryGetValue.item)) {
        auto msg = fmt::format(
                "NexusKVStore::{}: {} key:{} item mismatch primary:{} "
                "secondary:{}",
                caller,
                vb,
                cb::UserData(key.to_string()),
                *primaryGetValue.item,
                *secondaryGetValue.item);
        handleError(msg);
    }
}

bool NexusKVStore::compareItem(Item primaryItem, Item secondaryItem) const {
    // We can't use the Item comparator as that's going to check datatype and
    // value fields which may be different if we asked for a compressed item and
    // the KVStore returned it de-compressed because it stored it decompressed.
    if (primaryItem.isCommitted() != secondaryItem.isCommitted() ||
        primaryItem.getOperation() != secondaryItem.getOperation() ||
        primaryItem.getRevSeqno() != secondaryItem.getRevSeqno() ||
        primaryItem.getVBucketId() != secondaryItem.getVBucketId() ||
        primaryItem.getCas() != secondaryItem.getCas() ||
        primaryItem.getExptime() != secondaryItem.getExptime() ||
        primaryItem.getPrepareSeqno() != secondaryItem.getPrepareSeqno() ||
        primaryItem.getBySeqno() != secondaryItem.getBySeqno() ||
        primaryItem.getKey() != secondaryItem.getKey() ||
        primaryItem.isDeleted() != secondaryItem.isDeleted()) {
        return false;
    }

    if (primaryItem.isDeleted() &&
        primaryItem.deletionSource() != secondaryItem.deletionSource()) {
        // If deleted, source should be the same
        return false;
    }

    if (primaryItem.getDataType() == secondaryItem.getDataType()) {
        // Direct comparison of value is possible
        return primaryItem.getValueView() == secondaryItem.getValueView();
    }

    // Datatypes not the same... we want to check the value but we're going to
    // have to make sure that both items are in the same state of compression to
    // compare them.
    std::string decompressedValue;
    if (mcbp::datatype::is_snappy(primaryItem.getDataType())) {
        primaryItem.decompressValue();
        decompressedValue = primaryItem.getValueView();
    } else {
        decompressedValue = primaryItem.getValueView();
    }

    std::string otherDecompressedValue;
    if (mcbp::datatype::is_snappy(secondaryItem.getDataType())) {
        secondaryItem.decompressValue();
        otherDecompressedValue = secondaryItem.getValueView();
    } else {
        otherDecompressedValue = secondaryItem.getValueView();
    }

    if (decompressedValue != otherDecompressedValue) {
        return false;
    }

    return true;
}

GetValue NexusKVStore::get(const DiskDocKey& key,
                           Vbid vb,
                           ValueFilter filter) const {
    auto lh = getLock(vb);
    auto primaryGetValue = primary->get(key, vb, filter);
    auto secondaryGetValue = secondary->get(key, vb, filter);

    doPostGetChecks(__FUNCTION__, vb, key, primaryGetValue, secondaryGetValue);
    return primaryGetValue;
}

GetValue NexusKVStore::getWithHeader(const KVFileHandle& kvFileHandle,
                                     const DiskDocKey& key,
                                     Vbid vb,
                                     ValueFilter filter) const {
    auto lh = getLock(vb);

    auto primaryGetValue =
            primary->getWithHeader(kvFileHandle, key, vb, filter);

    if (skipGetWithHeaderChecksForRollback) {
        // We're calling this from rollback, and the primary KVStore will have
        // been rolled back already and we're looking for the pre-rollback seqno
        // state of a doc in the callback in EPBucket. Any comparison here would
        // be invalid so we just return early.
        return primaryGetValue;
    }

    auto secondaryHandle = secondary->makeFileHandle(vb);
    auto secondaryGetValue =
            secondary->getWithHeader(*secondaryHandle, key, vb, filter);

    doPostGetChecks(__FUNCTION__, vb, key, primaryGetValue, secondaryGetValue);
    return primaryGetValue;
}

void NexusKVStore::setMaxDataSize(size_t size) {
    primary->setMaxDataSize(size);
    secondary->setMaxDataSize(size);
}

/**
 * BGFetchItem created by NexusKVStore to perform the same BGFetch operation
 * against the secondary KVStore in NexusKVStore.
 */
class NexusBGFetchItem : public BGFetchItem {
public:
    explicit NexusBGFetchItem(std::chrono::steady_clock::time_point initTime,
                              ValueFilter filter)
        : BGFetchItem(initTime), filter(filter) {
    }

    void complete(EventuallyPersistentEngine& engine,
                  VBucketPtr& vb,
                  std::chrono::steady_clock::time_point startTime,
                  const DiskDocKey& key) const override {
        // Do nothing, we will compare the GetValues later
    }

    void abort(EventuallyPersistentEngine& engine,
               cb::engine_errc status,
               std::map<const CookieIface*, cb::engine_errc>& toNotify)
            const override {
        // Same as above
    }

    ValueFilter getValueFilter() const override {
        return filter;
    }

private:
    ValueFilter filter;
};

void NexusKVStore::getMulti(Vbid vb, vb_bgfetch_queue_t& primaryQueue) const {
    auto lh = getLock(vb);
    vb_bgfetch_queue_t secondaryQueue;
    for (const auto& [key, primaryCtx] : primaryQueue) {
        auto [itr, inserted] =
                secondaryQueue.emplace(key, vb_bgfetch_item_ctx_t());
        Expects(inserted);

        for (const auto& bgFetchItem : primaryCtx.getRequests()) {
            itr->second.addBgFetch(std::make_unique<NexusBGFetchItem>(
                    bgFetchItem->initTime, bgFetchItem->getValueFilter()));
        }
    }

    primary->getMulti(vb, primaryQueue);
    secondary->getMulti(vb, secondaryQueue);

    if (primaryQueue.size() != secondaryQueue.size()) {
        auto msg = fmt::format(
                "NexusKVStore::getMulti: {}: primary queue and secondary "
                "queue are different sizes",
                vb);
        handleError(msg);
    }

    for (auto& [key, value] : primaryQueue) {
        auto secondaryItr = secondaryQueue.find(key);
        if (secondaryItr == secondaryQueue.end()) {
            auto msg = fmt::format(
                    "NexusKVStore::getMulti: {}: found key:{} in primary queue "
                    "but not secondary",
                    vb,
                    cb::UserData(key.to_string()));
            handleError(msg);
        }

        doPostGetChecks(
                __FUNCTION__, vb, key, value.value, secondaryItr->second.value);
    }
}

void NexusKVStore::getRange(Vbid vb,
                            const DiskDocKey& startKey,
                            const DiskDocKey& endKey,
                            ValueFilter filter,
                            const KVStoreIface::GetRangeCb& cb) const {
    auto lh = getLock(vb);

    std::deque<GetValue> primaryGetValues;
    auto primaryCb = [&primaryGetValues](GetValue&& gv) {
        primaryGetValues.emplace_back(std::move(gv));
    };

    primary->getRange(vb, startKey, endKey, filter, primaryCb);

    std::deque<GetValue> secondaryGetValues;
    auto secondaryCb = [&secondaryGetValues](GetValue&& gv) {
        secondaryGetValues.emplace_back(std::move(gv));
    };

    secondary->getRange(vb, startKey, endKey, filter, secondaryCb);

    if (primaryGetValues.size() != secondaryGetValues.size()) {
        auto msg = fmt::format(
                "NexusKVStore::getMulti: {}: primary getvalues  and secondary "
                "get values are different sizes",
                vb);
        handleError(msg);
    }

    auto size = primaryGetValues.size();
    for (size_t i = 0; i < size; i++) {
        // Check primary vs secondary
        if (primaryGetValues.front().getStatus() !=
            secondaryGetValues.front().getStatus()) {
            // Might be able to log key if one is success
            std::string key = "";
            if (primaryGetValues.front().getStatus() ==
                cb::engine_errc::success) {
                key = primaryGetValues.front().item->getKey().to_string();
            }
            if (secondaryGetValues.front().getStatus() ==
                cb::engine_errc::success) {
                key = secondaryGetValues.front().item->getKey().to_string();
            }

            auto msg = fmt::format(
                    "NexusKVStore::getRange: {}: different result for item "
                    "with key {}"
                    "primary:{} secondary:{}",
                    vb,
                    key,
                    primaryGetValues.front().getStatus(),
                    secondaryGetValues.front().getStatus());
            handleError(msg);
        }

        if (primaryGetValues.front().getStatus() == cb::engine_errc::success) {
            doPostGetChecks(__FUNCTION__,
                            vb,
                            DiskDocKey(primaryGetValues.front().item->getKey()),
                            primaryGetValues.front(),
                            secondaryGetValues.front());
        }

        cb(std::move(primaryGetValues.front()));
        primaryGetValues.pop_front();
        secondaryGetValues.pop_front();
    }
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
    auto primaryResult = primary->snapshotVBucket(vbucketId, vbstate);
    auto secondaryResult = secondary->snapshotVBucket(vbucketId, vbstate);

    if (primaryResult != secondaryResult) {
        auto msg = fmt::format(
                "NexusKVStore::snapshotVBucket: {} primaryResult:{} "
                "secondaryResult:{}",
                vbucketId,
                primaryResult,
                secondaryResult);
        handleError(msg);
    }

    auto primaryVbState = primary->getPersistedVBucketState(vbucketId);
    auto secondaryVbState = secondary->getPersistedVBucketState(vbucketId);

    if (!getStorageProperties().hasPrepareCounting()) {
        // Can't compare prepare counts so zero them out
        primaryVbState.onDiskPrepares = 0;
        secondaryVbState.onDiskPrepares = 0;
        primaryVbState.setOnDiskPrepareBytes(0);
        secondaryVbState.setOnDiskPrepareBytes(0);
    }

    if (primaryVbState != secondaryVbState) {
        auto msg = fmt::format(
                "NexusKVStore::snapshotVBucket: {} difference in vbstate "
                "primary:{} secondary:{}",
                vbucketId,
                primaryVbState,
                secondaryVbState);
        handleError(msg);
    }

    return primaryResult;
}

/**
 * Expiry callback variant that stores a set of callback invocations and
 * (if supplied) forwards the callback on to the real expiry callback
 */
class NexusExpiryCB : public Callback<Item&, time_t&> {
public:
    explicit NexusExpiryCB(std::shared_ptr<Callback<Item&, time_t&>> cb = {})
        : cb(std::move(cb)) {
    }

    void callback(Item& it, time_t& startTime) override {
        // Time is not interesting here
        callbacks.emplace(it);
        if (cb) {
            cb->callback(it, startTime);
        }
    }

    std::unordered_set<DiskDocKey> callbacks;
    std::shared_ptr<Callback<Item&, time_t&>> cb;
};

struct NexusCompactionContext {
    KVStoreIface* kvStoreToCompactFirst;
    KVStoreIface* kvStoreToCompactSecond;
    std::shared_ptr<CompactionContext> firstCtx;
    std::shared_ptr<CompactionContext> secondCtx;

    bool attemptToPruneStaleCallbacks;
};

NexusCompactionContext NexusKVStore::calculateCompactionOrder(
        std::shared_ptr<CompactionContext> primaryCtx,
        std::shared_ptr<CompactionContext> secondaryCtx) {
    auto primaryStaleCallbacks =
            primary->getStorageProperties().hasCompactionStaleItemCallbacks();
    auto secondaryStaleCallbacks =
            secondary->getStorageProperties().hasCompactionStaleItemCallbacks();

    if (!primaryStaleCallbacks && !secondaryStaleCallbacks) {
        // Couchstore + Couchstore
        // Neither has stale call backs, comparisons are simple and it should
        // not matter in which order we run compaction
        return {primary.get(),
                secondary.get(),
                primaryCtx,
                secondaryCtx,
                false};
    } else if (primaryStaleCallbacks && !secondaryStaleCallbacks) {
        // Magma + Couchstore
        // Run primary first to attempt to prune the stale callbacks
        return {primary.get(), secondary.get(), primaryCtx, secondaryCtx, true};
    } else if (!primaryStaleCallbacks && secondaryStaleCallbacks) {
        // Couchstore + Magma
        // Run secondary first to attempt to prune the stale callbacks
        return {secondary.get(), primary.get(), secondaryCtx, primaryCtx, true};
    } else {
        // Magma + Magma
        // Order shouldn't matter, the stale callback pruning may/may not work
        // depending on how/when files are compacted in magma
        return {primary.get(),
                secondary.get(),
                primaryCtx,
                secondaryCtx,
                false};
    }
}

bool NexusKVStore::compactDB(std::unique_lock<std::mutex>& vbLock,
                             std::shared_ptr<CompactionContext> primaryCtx) {
    auto vbid = primaryCtx->vbid;

    // Need to take the lock for this vBucket to prevent concurrent flushes
    // from changing the on disk state that a compaction might see (and
    // concurrent gets from seeing a different state should compaction change
    // things).
    // @TODO MB-47604: Getting concurrent flushes and compaction working would
    // be good as it more closely behaves like the real system
    auto lh = getLock(vbid);

    // We can't pass the vbLock to the underlying KVStores as they may unlock it
    // if they don't need to hold it to inter-lock flushing and compaction. At
    // a glance that's fine as we are inter-locking flushing and compaction in
    // NexusKVStore with our own lock, but the presence of that lock can cause
    // lock order cycles if we attempt to re-acquire the vbLock whilst holding
    // the NexusKVStore lock. We have to acqurie the vbLock first for flushing,
    // so the only way out of this is to just pass a dummy lock to the
    // underlying KVStores. We don't care about unlocking the vbLock as we're
    // inter-locking flushing/gets/compactions already.
    std::mutex dummyLock;
    auto dummyLh = std::unique_lock<std::mutex>(dummyLock);

    // Create a new context to avoid calling things like the completion callback
    // which sets in memory state after the secondary compacts
    auto secondaryCtx = std::make_shared<CompactionContext>(
            vbid, primaryCtx->compactConfig, primaryCtx->max_purged_seq);

    // Don't set the NexusExpiryCB cb member to avoid forwarding expiries to
    // the engine (we will do so for the primary)
    auto secondaryExpiryCb = std::make_shared<NexusExpiryCB>();
    secondaryCtx->expiryCallback = secondaryExpiryCb;

    // Replace the ExpiredItemsCallback with our own that stores the result for
    // later comparison with the secondary and forwards the result on
    auto primaryExpiryCb =
            std::make_shared<NexusExpiryCB>(primaryCtx->expiryCallback);
    primaryCtx->expiryCallback = primaryExpiryCb;

    std::unordered_map<DiskDocKey, int64_t> primaryDrops;
    std::unordered_map<DiskDocKey, int64_t> secondaryDrops;
    Collections::KVStore::DroppedCb originalDroppedKeyCb =
            primaryCtx->droppedKeyCb;
    primaryCtx->droppedKeyCb = [&primaryDrops, &originalDroppedKeyCb](
                                       const DiskDocKey& key,
                                       int64_t seqno,
                                       bool aborted,
                                       int64_t pcs) {
        auto [itr, inserted] = primaryDrops.try_emplace(key, seqno);
        itr->second = std::max<uint64_t>(itr->second, seqno);

        originalDroppedKeyCb(key, seqno, aborted, pcs);
    };

    secondaryCtx->droppedKeyCb = [&secondaryDrops](const DiskDocKey& key,
                                                   int64_t seqno,
                                                   bool aborted,
                                                   int64_t pcs) {
        auto itr = secondaryDrops.find(key);
        if (itr != secondaryDrops.end()) {
            itr->second = std::max<uint64_t>(itr->second, seqno);
        } else {
            secondaryDrops[key] = seqno;
        }
    };

    // Comparisons in the callbacks made may be difficult to make if one KVStore
    // may call back with stale items but the other does not. If we know that
    // one of the KVStores will do so then we can run the compaction for that
    // KVStore first and check the item against the other to see if it is stale
    // or not. If the callback is for a stale item, we remove it from the list
    // to compare.
    auto nexusCompactionContext =
            calculateCompactionOrder(primaryCtx, secondaryCtx);

    auto firstResult = nexusCompactionContext.kvStoreToCompactFirst->compactDB(
            dummyLh, nexusCompactionContext.firstCtx);

    if (nexusCompactionContext.attemptToPruneStaleCallbacks) {
        // Iterate over the callbacks made by the first compaction and run
        // a get against the other KVStore to check if the item exists. If it
        // does and the seqno of the drop is lower than that of the primary
        // then we should just ignore the callback invocation as it's probably
        // a stale key.
        auto& firstDrops =
                nexusCompactionContext.kvStoreToCompactFirst == primary.get()
                        ? primaryDrops
                        : secondaryDrops;
        for (auto itr = firstDrops.begin(); itr != firstDrops.end();) {
            auto key = itr->first;
            auto seqno = itr->second;

            auto gv = nexusCompactionContext.kvStoreToCompactSecond->get(key,
                                                                         vbid);
            if (gv.getStatus() == cb::engine_errc::success &&
                gv.item->getBySeqno() > seqno) {
                // Remove
                firstDrops.erase(itr++);
            } else {
                itr++;
            }
        }
    }

    // Might have to re-acquire the lock, depending what the first kvstore does
    // with it...
    if (!dummyLh.owns_lock()) {
        dummyLh.lock();
    }
    auto secondResult =
            nexusCompactionContext.kvStoreToCompactSecond->compactDB(
                    dummyLh, nexusCompactionContext.secondCtx);

    if (firstResult != secondResult) {
        auto msg = fmt::format(
                "NexusKVStore::compactDB: {}: compaction result mismatch "
                "first:{} second:{}",
                vbid,
                firstResult,
                secondResult);
        handleError(msg);
    }

    // The expiration callback invocations should be the same
    for (auto& cb : primaryExpiryCb->callbacks) {
        if (secondaryExpiryCb->callbacks.find(cb) ==
            secondaryExpiryCb->callbacks.end()) {
            auto msg = fmt::format(
                    "NexusKVStore::compactDB: {}: Expiry callback found with "
                    "key:{} for primary but not secondary",
                    vbid,
                    cb::UserData(cb.to_string()));
            handleError(msg);
        } else {
            secondaryExpiryCb->callbacks.erase(cb);
        }
    }

    if (!secondaryExpiryCb->callbacks.empty()) {
        std::stringstream ss;
        for (auto& cb : secondaryExpiryCb->callbacks) {
            ss << cb::UserData(cb.to_string()) << ",";
        }
        ss.unget();

        auto msg = fmt::format(
                "NexusKVStore::compactDB: {}: secondary expiry callbacks not "
                "made by primary:{}",
                vbid,
                ss.str());
        handleError(msg);
    }

    for (auto& [key, seqno] : primaryDrops) {
        auto itr = secondaryDrops.find(key);
        if (itr == secondaryDrops.end()) {
            auto msg = fmt::format(
                    "NexusKVStore::compactDB: {}: drop callback found with "
                    "key:{} for primary but not secondary",
                    vbid,
                    cb::UserData(key.to_string()));
            handleError(msg);
        } else if (seqno != itr->second) {
            auto msg = fmt::format(
                    "NexusKVStore::compactDB: {}: drop callback found with "
                    "key:{} and different seqno primary:{} secondary:{}",
                    vbid,
                    cb::UserData(key.to_string()),
                    seqno,
                    itr->second);
            handleError(msg);
        } else {
            secondaryDrops.erase(key);
        }
    }

    if (!secondaryDrops.empty()) {
        std::stringstream ss;
        for (auto& [key, seqno] : secondaryDrops) {
            ss << "[key:" << cb::UserData(key.to_string()) << ",seqno:" << seqno
               << "],";
        }
        ss.unget();

        auto msg = fmt::format(
                "NexusKVStore::compactDB: {}: secondary callbacks not made by "
                "primary:{}",
                vbid,
                ss.str());
        handleError(msg);
    }

    // Compare the collections state if successful
    if (firstResult) {
        doCollectionsMetadataChecks(vbid, nullptr, nullptr);
    }

    return nexusCompactionContext.kvStoreToCompactFirst == primary.get()
                   ? firstResult
                   : secondResult;
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

/**
 * Rollback callback for NexusKVStore. This callback:
 *
 * a) forwards the callback invocation on to the original callback (if the
 *    original callback is supplied during construction)
 * b) forwards gets and sets of the file handle on to the original callback (if
 *    original callback is supplied during construction)
 * b) stores a copy of the key and seqno for comparison of the callback
 *    invocations between primary and secondary KVStores
 */
class NexusRollbackCB : public RollbackCB {
public:
    NexusRollbackCB(NexusKVStore& kvstore,
                    Vbid vbid,
                    std::unordered_map<DiskDocKey, uint64_t>& rollbacks,
                    std::unique_ptr<RollbackCB> originalCb = {})
        : kvstore(kvstore),
          vbid(vbid),
          rolledBack(rollbacks),
          originalCb(std::move(originalCb)) {
    }

    void callback(GetValue& val) override {
        // The item passed in here is the post-rollback item. We should compare
        // it to the items rolled back by the primary.
        Expects(val.item);
        auto [itr, emplaceResult] = rolledBack.try_emplace(
                DiskDocKey(*val.item), val.item->getBySeqno());
        if (!emplaceResult) {
            auto msg = fmt::format(
                    "NexusRollbackCB::callback: {}: called back for {} with "
                    "seqno {} but callback already exists with seqno {}",
                    vbid,
                    cb::UserData(val.item->getKey().to_string()),
                    val.item->getBySeqno(),
                    itr->second);
            kvstore.handleError(msg);
        }

        if (originalCb) {
            originalCb->callback(val);
        }
    }

    void setKVFileHandle(std::unique_ptr<KVFileHandle> handle) override {
        // Give the file handle to the original (if it exists), otherwise we
        // need to store it ourselves.
        if (originalCb) {
            originalCb->setKVFileHandle(std::move(handle));
            return;
        }

        RollbackCB::setKVFileHandle(std::move(handle));
    }

    const KVFileHandle* getKVFileHandle() const override {
        // Get the file handle from the original if it exists (we should have
        // given it to the orignal via setKVFileHandle), otherwise we should
        // have store it in the parent so we should return that one.
        if (originalCb) {
            return originalCb->getKVFileHandle();
        }

        return RollbackCB::getKVFileHandle();
    }

    // Used for logging errors
    NexusKVStore& kvstore;

    // Used for logging errors
    Vbid vbid;

    /**
     * Map of DiskDocKey (includes prepare namespace) to seqno
     */
    std::unordered_map<DiskDocKey, uint64_t>& rolledBack;

    /**
     * Original callback to be invoked if set
     */
    std::unique_ptr<RollbackCB> originalCb;
};

struct NexusRollbackContext {
    KVStoreIface* kvstoreToRollbackFirst;
    KVStoreIface* kvstoreToRollbackSecond;
};

NexusRollbackContext NexusKVStore::calculateRollbackOrder() {
#ifdef EP_USE_MAGMA

    bool primaryIsMagma = dynamic_cast<MagmaKVStore*>(primary.get());
    bool secondaryIsMagma = dynamic_cast<MagmaKVStore*>(secondary.get());

    if (!primaryIsMagma && secondaryIsMagma) {
        // Got to do magma (secondary) first
        return {secondary.get(), primary.get()};
    }
#endif

    return {primary.get(), secondary.get()};
}

RollbackResult NexusKVStore::rollback(Vbid vbid,
                                      uint64_t rollbackseqno,
                                      std::unique_ptr<RollbackCB> ptr) {
    // We're not taking the lock for the vBucket here because the callback in
    // EPDiskRollbackCB is going to call getWithHeader for each item we roll
    // back. We're protected from getting into odd states though as:
    //
    // 1) This vBucket must be a replica so no bg fetches
    // 2) During rollback we take the vBucket write lock so no flushes

    // Skip checks, see member declaration for more details.
    skipGetWithHeaderChecksForRollback = true;
    auto guard = folly::makeGuard(
            [this] { skipGetWithHeaderChecksForRollback = false; });

    std::unordered_map<DiskDocKey, uint64_t> primaryRollbacks;
    auto primaryCb = std::make_unique<NexusRollbackCB>(
            *this, vbid, primaryRollbacks, std::move(ptr));

    std::unordered_map<DiskDocKey, uint64_t> secondaryRollbacks;
    auto secondaryCb =
            std::make_unique<NexusRollbackCB>(*this, vbid, secondaryRollbacks);

    // Magma is only going to keep n checkpoints (i.e. n rollback points) in
    // memory and as we're checkpointing every flush batch (to ensure that
    // rollback points are consistent between magma and couchstore) that's
    // effectively n flush batches. Best thing to do here is to do the magma
    // rollback first (assuming we are running magma) and then assert later that
    // couchstore rolls back to the same seqno. Should magma be unable to roll
    // back to anything other than 0 then there's no point rolling back
    // couchstore.
    auto nexusRollbackContext = calculateRollbackOrder();

    auto firstResult = nexusRollbackContext.kvstoreToRollbackFirst->rollback(
            vbid,
            rollbackseqno,
            nexusRollbackContext.kvstoreToRollbackFirst == primary.get()
                    ? std::move(primaryCb)
                    : std::move(secondaryCb));

    if (!firstResult.success) {
        // Need to roll back to zero, may as well just return now
        return firstResult;
    }

    auto secondResult = nexusRollbackContext.kvstoreToRollbackSecond->rollback(
            vbid,
            rollbackseqno,
            nexusRollbackContext.kvstoreToRollbackSecond == primary.get()
                    ? std::move(primaryCb)
                    : std::move(secondaryCb));

    if (firstResult != secondResult) {
        auto msg = fmt::format(
                "NexusKVStore::rollback: {}: rollback result not equal "
                "first:{} second:{}",
                vbid,
                firstResult,
                secondResult);
        handleError(msg);
    }

    for (const auto& [key, seqno] : primaryRollbacks) {
        auto itr = secondaryRollbacks.find(key);
        if (itr == secondaryRollbacks.end()) {
            auto msg = fmt::format(
                    "NexusKVStore::rollback: {}: primary invoked rollback "
                    "callback for {} at seqno {} but secondary did not",
                    vbid,
                    cb::UserData(key.to_string()),
                    seqno);
            handleError(msg);
        }
        secondaryRollbacks.erase(itr);
    }

    if (!secondaryRollbacks.empty()) {
        std::stringstream ss;
        for (const auto& [key, seqno] : secondaryRollbacks) {
            ss << "[key:" << cb::UserData(key.to_string()) << ",seqno:" << seqno
               << "],";
        }
        ss.unget();

        auto msg = fmt::format(
                "NexusKVStoer::rollback: {}: secondary callbacks invocations "
                "not made by primary:{}",
                vbid,
                ss.str());
    }

    doCollectionsMetadataChecks(vbid, nullptr, nullptr);

    return nexusRollbackContext.kvstoreToRollbackFirst == primary.get()
                   ? firstResult
                   : secondResult;
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

std::pair<KVStore::GetCollectionStatsStatus, Collections::VB::PersistedStats>
NexusKVStore::getCollectionStats(const KVFileHandle& kvFileHandle,
                                 CollectionID collection) const {
    return primary->getCollectionStats(kvFileHandle, collection);
}

std::pair<KVStore::GetCollectionStatsStatus, Collections::VB::PersistedStats>
NexusKVStore::getCollectionStats(Vbid vbid, CollectionID collection) const {
    return primary->getCollectionStats(vbid, collection);
}

std::optional<Collections::ManifestUid> NexusKVStore::getCollectionsManifestUid(
        KVFileHandle& kvFileHandle) {
    return primary->getCollectionsManifestUid(kvFileHandle);
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

GetValue NexusKVStore::getBySeqno(KVFileHandle& handle,
                                  Vbid vbid,
                                  uint64_t seq,
                                  ValueFilter filter) {
    return primary->getBySeqno(handle, vbid, seq, filter);
}

void NexusKVStore::setStorageThreads(ThreadPoolConfig::StorageThreadCount num) {
    primary->setStorageThreads(num);
    secondary->setStorageThreads(num);
}

std::unique_ptr<TransactionContext> NexusKVStore::begin(
        Vbid vbid, std::unique_ptr<PersistenceCallback> pcb) {
    auto ctx = std::make_unique<NexusKVStoreTransactionContext>(*this, vbid);

    ctx->primaryContext = primary->begin(
            vbid,
            std::make_unique<NexusKVStorePrimaryPersistenceCallback>(
                    std::move(pcb), ctx->primarySets, ctx->primaryDeletions));
    ctx->secondaryContext = secondary->begin(
            vbid,
            std::make_unique<NexusKVStoreSecondaryPersistenceCallback>(
                    *this, ctx->primarySets, ctx->primaryDeletions));

    return ctx;
}

const KVStoreStats& NexusKVStore::getKVStoreStat() const {
    return primary->getKVStoreStat();
}

void NexusKVStore::setMakeCompactionContextCallback(
        MakeCompactionContextCallback cb) {
    // The makeCompactionContextCallback function stored in KVStore is used to
    // allow magma to run "implicit" background compactions. Couchstore
    // doesn't support these (and RocksDBKVStore is barely implemented) so we
    // can just return here instead of setting the callback to effectively
    // disable magma's implicit compactions. This lets us run compaction in
    // lockstep between primary and secondary as only externally driven
    // compactions will run.
    // TODO MB-47604 eventually getting magma's implicit background compactions
    // running would be beneficial here.
    return;
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

void NexusKVStore::handleError(std::string_view msg) const {
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

std::unique_lock<std::mutex> NexusKVStore::getLock(Vbid vbid) const {
    return std::unique_lock<std::mutex>(
            vbMutexes[vbid.get() / configuration.getMaxShards()]);
}
