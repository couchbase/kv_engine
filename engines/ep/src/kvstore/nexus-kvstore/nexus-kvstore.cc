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

#include "kvstore/kvstore_transaction_context.h"
#include "nexus-kvstore-config.h"
#include "rollback_result.h"
#include "vbucket_state.h"

NexusKVStore::NexusKVStore(NexusKVStoreConfig& config) : configuration(config) {
    primary = KVStoreFactory::create(configuration.getPrimaryConfig());
    secondary = KVStoreFactory::create(configuration.getSecondaryConfig());
}

void NexusKVStore::deinitialize() {
    primary->deinitialize();
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
}

size_t NexusKVStore::getMemFootPrint() const {
    return primary->getMemFootPrint();
}

bool NexusKVStore::commit(std::unique_ptr<TransactionContext> txnCtx,
                          VB::Commit& commitData) {
    return primary->commit(std::move(txnCtx), commitData);
}

StorageProperties NexusKVStore::getStorageProperties() const {
    return primary->getStorageProperties();
}

void NexusKVStore::set(TransactionContext& txnCtx, queued_item item) {
    primary->set(txnCtx, item);
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
    primary->del(txnCtx, item);
}

void NexusKVStore::delVBucket(Vbid vbucket, uint64_t fileRev) {
    primary->delVBucket(vbucket, fileRev);
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
    return primary->abortCompactionIfRunning(vbLock, vbid);
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
}

cb::engine_errc NexusKVStore::getAllKeys(
        Vbid vbid,
        const DiskDocKey& start_key,
        uint32_t count,
        std::shared_ptr<StatusCallback<const DiskDocKey&>> cb) const {
    return primary->getAllKeys(vbid, start_key, count, cb);
}

bool NexusKVStore::supportsHistoricalSnapshots() const {
    return primary->supportsHistoricalSnapshots();
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
}

std::unique_ptr<TransactionContext> NexusKVStore::begin(
        Vbid vbid, std::unique_ptr<PersistenceCallback> pcb) {
    return primary->begin(vbid, std::move(pcb));
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
}

uint64_t NexusKVStore::prepareToDelete(Vbid vbid) {
    return primary->prepareToDelete(vbid);
}

uint64_t NexusKVStore::getLastPersistedSeqno(Vbid vbid) {
    return primary->getLastPersistedSeqno(vbid);
}

void NexusKVStore::prepareForDeduplication(std::vector<queued_item>& items) {
    primary->prepareForDeduplication(items);
}

void NexusKVStore::setSystemEvent(TransactionContext& txnCtx,
                                  const queued_item item) {
    primary->setSystemEvent(txnCtx, item);
}

void NexusKVStore::delSystemEvent(TransactionContext& txnCtx,
                                  const queued_item item) {
    primary->delSystemEvent(txnCtx, item);
}

uint64_t NexusKVStore::prepareToDeleteImpl(Vbid vbid) {
    return primary->prepareToDeleteImpl(vbid);
}

void NexusKVStore::prepareToCreateImpl(Vbid vbid) {
    primary->prepareToCreateImpl(vbid);
}
