/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "magma-memory-tracking-proxy.h"

#include "kvstore/kvstore.h"

#include <cbcrypto/key_store.h>
#include <fmt/format.h>
#include <gsl/gsl-lite.hpp>
#include <platform/cb_arena_malloc.h>

DomainAwareFetchBuffer::DomainAwareFetchBuffer() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    buffer = std::make_unique<magma::Magma::FetchBuffer>();
}

DomainAwareFetchBuffer::~DomainAwareFetchBuffer() {
    // Force destruct in the secondary domain
    cb::UseArenaMallocSecondaryDomain domainGuard;
    buffer.reset();
}

DomainAwareSeqIterator::~DomainAwareSeqIterator() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    itr.reset();
}

magma::Status DomainAwareSeqIterator::Initialize(
        const magma::Magma::SeqNo startSeqno,
        const magma::Magma::SeqNo endSeqno,
        magma::Magma::SeqIterator::Mode mode) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return itr->Initialize(startSeqno, endSeqno, mode);
}

magma::Status DomainAwareSeqIterator::GetStatus() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return itr->GetStatus();
}

void DomainAwareSeqIterator::Next() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return itr->Next();
}

bool DomainAwareSeqIterator::Valid() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return itr->Valid();
}

void DomainAwareSeqIterator::GetRecord(magma::Slice& key,
                                       magma::Slice& meta,
                                       magma::Slice& value,
                                       magma::Magma::SeqNo& seqno) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return itr->GetRecord(key, meta, value, seqno);
}

void DomainAwareSeqIterator::Seek(const magma::Magma::SeqNo startSeqno,
                                  const magma::Magma::SeqNo endSeqno) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return itr->Seek(startSeqno, endSeqno);
}

std::string DomainAwareSeqIterator::to_string() const {
    return fmt::format("{:p}", fmt::ptr(itr.get()));
}

DomainAwareKeyIterator::~DomainAwareKeyIterator() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    itr.reset();
}

void DomainAwareKeyIterator::Seek(const magma::Slice& startKey) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    itr->Seek(startKey);
}

bool DomainAwareKeyIterator::Valid() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return itr->Valid();
}

magma::Status DomainAwareKeyIterator::GetStatus() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return itr->GetStatus();
}

void DomainAwareKeyIterator::Next() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    itr->Next();
}

const magma::Slice DomainAwareKeyIterator::GetKey() {
    cb::UseArenaMallocSecondaryDomain domainGuard;

    return itr->GetKey();
}

const magma::Slice DomainAwareKeyIterator::GetMeta() {
    cb::UseArenaMallocSecondaryDomain domainGuard;

    return itr->GetMeta();
}

magma::Magma::SeqNo DomainAwareKeyIterator::GetSeqno() const {
    cb::UseArenaMallocSecondaryDomain domainGuard;

    return itr->GetSeqno();
}

magma::Status DomainAwareKeyIterator::GetValue(magma::Slice& value) {
    cb::UseArenaMallocSecondaryDomain domainGuard;

    return itr->GetValue(value);
}

std::string DomainAwareKeyIterator::to_string() const {
    return fmt::format("{:p}", fmt::ptr(itr.get()));
}

template <>
void DomainAwareDelete<magma::UserStats>::operator()(magma::UserStats* p) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    delete p;
}

template <>
void DomainAwareDelete<DomainAwareSeqIterator>::operator()(
        DomainAwareSeqIterator* p) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    delete p;
}

template <>
void DomainAwareDelete<DomainAwareKeyIterator>::operator()(
        DomainAwareKeyIterator* p) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    delete p;
}

template <>
void DomainAwareDelete<magma::Magma::MagmaStats>::operator()(
        magma::Magma::MagmaStats* p) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    delete p;
}

template <>
void DomainAwareDelete<std::string>::operator()(std::string* p) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    delete p;
}

template <>
void DomainAwareDelete<magma::Magma::Snapshot>::operator()(
        magma::Magma::Snapshot* p) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    delete p;
}

MagmaMemoryTrackingProxy::MagmaMemoryTrackingProxy(
        magma::Magma::Config& config) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma = std::make_unique<magma::Magma>(config);
}

MagmaMemoryTrackingProxy::~MagmaMemoryTrackingProxy() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma.reset();
}

magma::Status MagmaMemoryTrackingProxy::Pause() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->Pause();
}

void MagmaMemoryTrackingProxy::Resume() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->Resume();
}

void MagmaMemoryTrackingProxy::Close() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->Close();
}

magma::Status MagmaMemoryTrackingProxy::CompactKVStore(
        const magma::Magma::KVStoreID kvID,
        const magma::Slice& lowKey,
        const magma::Slice& highKey,
        const magma::Magma::CompactionCallbackBuilder& makeCallback,
        const std::vector<std::string>& obsoleteKeys) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->CompactKVStore(
            kvID, lowKey, highKey, makeCallback, obsoleteKeys);
}

magma::Status MagmaMemoryTrackingProxy::RunImplicitCompactKVStore(
        const magma::Magma::KVStoreID kvID) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->RunImplicitCompactKVStore(kvID);
}

magma::Status MagmaMemoryTrackingProxy::DeleteKVStore(
        const magma::Magma::KVStoreID kvID,
        const magma::Magma::KVStoreRevision kvsRev) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->DeleteKVStore(kvID, kvsRev);
}

magma::Status MagmaMemoryTrackingProxy::Get(const magma::Magma::KVStoreID kvID,
                                            const magma::Slice& key,
                                            DomainAwareFetchBuffer& idxBuf,
                                            DomainAwareFetchBuffer& seqBuf,
                                            magma::Slice& meta,
                                            magma::Slice& value) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->Get(
            kvID, key, idxBuf.getBuffer(), seqBuf.getBuffer(), meta, value);
}

bool MagmaMemoryTrackingProxy::KeyMayExist(const magma::Magma::KVStoreID kvID,
                                           const magma::Slice& key) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->KeyMayExist(
            kvID,
            key,
            false // Perform non-blocking version of the bloom-filter check
    );
}

magma::Status MagmaMemoryTrackingProxy::GetDiskSnapshot(
        const magma::Magma::KVStoreID kvID,
        DomainAwareUniquePtr<magma::Magma::Snapshot>& snap) {
    Expects(!snap);
    cb::UseArenaMallocSecondaryDomain domainGuard;
    // Call magma with its unique_ptr type and then hand any pointer over to
    // the domain aware type
    std::unique_ptr<magma::Magma::Snapshot> snapshot;
    auto status = magma->GetDiskSnapshot(kvID, snapshot);
    snap.reset(snapshot.release());
    return status;
}

magma::Status MagmaMemoryTrackingProxy::GetOldestDiskSnapshot(
        const magma::Magma::KVStoreID kvID,
        DomainAwareUniquePtr<magma::Magma::Snapshot>& snap) {
    Expects(!snap);
    cb::UseArenaMallocSecondaryDomain domainGuard;
    std::unique_ptr<magma::Magma::Snapshot> snapshot;
    auto status = magma->GetOldestDiskSnapshot(kvID, snapshot);
    snap.reset(snapshot.release());
    return status;
}

magma::Status MagmaMemoryTrackingProxy::GetSnapshot(
        const magma::Magma::KVStoreID kvID,
        DomainAwareUniquePtr<magma::Magma::Snapshot>& snap) {
    Expects(!snap);
    cb::UseArenaMallocSecondaryDomain domainGuard;
    std::unique_ptr<magma::Magma::Snapshot> snapshot;
    auto status = magma->GetSnapshot(kvID, snapshot);
    snap.reset(snapshot.release());
    return status;
}

magma::Status MagmaMemoryTrackingProxy::GetDocs(
        const magma::Magma::KVStoreID kvID,
        magma::Operations<magma::Magma::GetOperation>& getOps,
        magma::Magma::GetDocCallback cb) {
    auto wrappedCallback = [&cb](magma::Status status,
                                 const magma::Magma::GetOperation& op,
                                 const magma::Slice& metaSlice,
                                 const magma::Slice& valueSlice) {
        // Run the callers callback in primary domain
        cb::UseArenaMallocPrimaryDomain domainGuard;
        cb(status, op, metaSlice, valueSlice);
    };

    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->GetDocs(kvID, getOps, wrappedCallback);
}
void MagmaMemoryTrackingProxy::executeOnKVStoreList(
        std::function<void(const std::vector<magma::Magma::KVStoreID>&)>
                callback) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    callback(magma->GetKVStoreList());
}

std::tuple<magma::Status, magma::Magma::KVStoreRevision>
MagmaMemoryTrackingProxy::GetKVStoreRevision(
        const magma::Magma::KVStoreID kvID) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->GetKVStoreRevision(kvID);
}

std::tuple<magma::Status, DBFileInfo>
MagmaMemoryTrackingProxy::GetStatsForDbInfo(
        const magma::Magma::KVStoreID kvid) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    DBFileInfo rv;
    auto [status, kvstats] = magma->GetKVStoreStats(kvid);
    if (status) {
        rv.spaceUsed = kvstats.ActiveDiskUsage;
        rv.fileSize = kvstats.TotalDiskUsage;
        rv.historyDiskSize = kvstats.HistoryDiskUsage;
        rv.historyStartTimestamp =
                std::chrono::seconds(kvstats.HistoryStartTimestamp);
    }
    return {status, rv};
}

magma::DBSizeInfo MagmaMemoryTrackingProxy::GetDBSizeInfo() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->GetDBSizeInfo();
}

DomainAwareUniquePtr<magma::UserStats>
MagmaMemoryTrackingProxy::GetKVStoreUserStats(
        const magma::Magma::KVStoreID kvid) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return DomainAwareUniquePtr<magma::UserStats>{
            magma->GetKVStoreUserStats(kvid).release()};
}

DomainAwareUniquePtr<magma::UserStats>
MagmaMemoryTrackingProxy::GetKVStoreUserStats(
        magma::Magma::Snapshot& snapshot) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return DomainAwareUniquePtr<magma::UserStats>{
            magma->GetKVStoreUserStats(snapshot).release()};
}

std::pair<magma::Status, DomainAwareUniquePtr<std::string>>
MagmaMemoryTrackingProxy::GetLocal(const magma::Magma::KVStoreID kvID,
                                   const magma::Slice& key) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    DomainAwareUniquePtr<std::string> stringPtr(new std::string{});
    auto status = magma->GetLocal(kvID, key, *stringPtr);
    return {status, std::move(stringPtr)};
}

std::pair<magma::Status, DomainAwareUniquePtr<std::string>>
MagmaMemoryTrackingProxy::GetLocal(magma::Magma::Snapshot& snapshot,
                                   const magma::Slice& key) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    DomainAwareUniquePtr<std::string> stringPtr(new std::string{});
    auto status = magma->GetLocal(snapshot, key, *stringPtr);
    return {status, std::move(stringPtr)};
}

magma::Status MagmaMemoryTrackingProxy::GetMaxSeqno(
        const magma::Magma::KVStoreID kvID, magma::Magma::SeqNo& seqno) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->GetMaxSeqno(kvID, seqno);
}

magma::Status MagmaMemoryTrackingProxy::GetRange(
        const magma::Magma::KVStoreID kvID,
        const magma::Slice& startKey,
        const magma::Slice& endKey,
        magma::Magma::GetRangeCB itemCb,
        bool returnValue,
        uint64_t count) {
    auto wrappedCallback = [&itemCb](magma::Slice& key,
                                     magma::Slice& meta,
                                     magma::Slice& value) {
        cb::UseArenaMallocPrimaryDomain domainGuard;
        return itemCb(key, meta, value);
    };

    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->GetRange(
            kvID, startKey, endKey, wrappedCallback, returnValue, count);
}

DomainAwareUniquePtr<magma::Magma::MagmaStats>
MagmaMemoryTrackingProxy::GetStats(std::chrono::milliseconds cacheDuration) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    // MagmaStats internally has at least one std::vector which needs to
    // be destroyed in the correct domain. Manager the MagmaStats object so it
    // destructs against the second domain.
    DomainAwareUniquePtr<magma::Magma::MagmaStats> stats(
            new magma::Magma::MagmaStats{});
    magma->GetStats(*stats, cacheDuration);
    return stats;
}

void MagmaMemoryTrackingProxy::GetFileStats(magma::MagmaFileStats& fileStats) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->GetFileStats(fileStats);
}

void MagmaMemoryTrackingProxy::GetHistogramStats(
        magma::MagmaHistogramStats& histogramStats) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->GetHistogramStats(histogramStats);
}

std::tuple<magma::Status,
           DomainAwareUniquePtr<std::string>,
           DomainAwareUniquePtr<std::string>,
           DomainAwareUniquePtr<std::string>>
MagmaMemoryTrackingProxy::GetBySeqno(magma::Magma::Snapshot& snapshot,
                                     const magma::Magma::SeqNo seqno) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    DomainAwareUniquePtr<std::string> key(new std::string{});
    DomainAwareUniquePtr<std::string> meta(new std::string{});
    DomainAwareUniquePtr<std::string> value(new std::string{});
    auto status = magma->GetBySeqno(snapshot, seqno, *key, *meta, *value);
    return {status, std::move(key), std::move(meta), std::move(value)};
}

DomainAwareUniquePtr<DomainAwareSeqIterator>
MagmaMemoryTrackingProxy::NewSeqIterator(magma::Magma::Snapshot& snapshot) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    auto itr = std::make_unique<DomainAwareSeqIterator>(
            magma->NewSeqIterator(snapshot));
    return DomainAwareUniquePtr<DomainAwareSeqIterator>(itr.release());
}

DomainAwareUniquePtr<DomainAwareKeyIterator>
MagmaMemoryTrackingProxy::NewKeyIterator(magma::Magma::Snapshot& snapshot) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    auto itr = std::make_unique<DomainAwareKeyIterator>(
            magma->NewKeyIterator(snapshot));
    return DomainAwareUniquePtr<DomainAwareKeyIterator>(itr.release());
}

magma::Status MagmaMemoryTrackingProxy::Open() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->Open();
}

magma::Status MagmaMemoryTrackingProxy::Rollback(
        const magma::Magma::KVStoreID kvID,
        magma::Magma::SeqNo rollbackSeqno,
        magma::Magma::RollbackCallback callback) {
    auto wrappedCallback = [&callback](const magma::Slice& keySlice,
                                       const uint64_t seqno,
                                       const magma::Slice& metaSlice) {
        // Run the callers callback in primary domain
        cb::UseArenaMallocPrimaryDomain domainGuard;
        callback(keySlice, seqno, metaSlice);
    };

    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->Rollback(kvID, rollbackSeqno, wrappedCallback);
}

void MagmaMemoryTrackingProxy::SetFragmentationRatio(double fragRatio) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetFragmentationRatio(fragRatio);
}

void MagmaMemoryTrackingProxy::EnableBlockCache(bool enable) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->EnableBlockCache(enable);
}

void MagmaMemoryTrackingProxy::SetMaxOpenFiles(size_t n, bool blocking) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetMaxOpenFiles(n, blocking);
}

void MagmaMemoryTrackingProxy::SetMemoryQuota(const size_t quota) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetMemoryQuota(quota);
}

void MagmaMemoryTrackingProxy::SetNumThreads(
        magma::Magma::ThreadType threadType, size_t nThreads) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetNumThreads(threadType, nThreads);
}

void MagmaMemoryTrackingProxy::SetHistoryRetentionSize(size_t historyBytes) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetHistoryRetentionSize(historyBytes);
}

void MagmaMemoryTrackingProxy::SetHistoryRetentionTime(
        std::chrono::seconds historySeconds) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetHistoryRetentionTime(historySeconds);
}

magma::Status MagmaMemoryTrackingProxy::Sync(bool flushAll) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->Sync(flushAll);
}

magma::Status MagmaMemoryTrackingProxy::SyncKVStore(
        const magma::Magma::KVStoreID kvID) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->SyncKVStore(kvID);
}

magma::Status MagmaMemoryTrackingProxy::WriteDocs(
        const magma::Magma::KVStoreID kvID,
        const std::vector<magma::Magma::WriteOperation>& docOperations,
        const magma::Magma::KVStoreRevision kvsRev,
        const magma::Magma::HistoryMode historyMode,
        const magma::Magma::WriteDocsCallback docCallback,
        const magma::Magma::PostWriteDocsCallback postCallback,
        const magma::Magma::DocTransformCallback docTransformCallback) {
    magma::Magma::WriteDocsCallback wrappedDocCallback = nullptr;
    magma::Magma::PostWriteDocsCallback wrappedPostCallback = nullptr;
    magma::Magma::DocTransformCallback wrappedDocTransformCallback = nullptr;
    if (docCallback) {
        wrappedDocCallback = [&docCallback](
                                     const magma::Magma::WriteOperation& op,
                                     const bool docExists,
                                     const magma::Slice oldMeta) {
            // Run the callers callback in primary domain
            cb::UseArenaMallocPrimaryDomain domainGuard;
            docCallback(op, docExists, oldMeta);
        };
    }

    if (postCallback) {
        wrappedPostCallback = [&postCallback]() {
            // Run the callers callback in primary domain
            cb::UseArenaMallocPrimaryDomain domainGuard;
            return postCallback();
        };
    }

    if (docTransformCallback) {
        wrappedDocTransformCallback =
                [&docTransformCallback](
                        const magma::Magma::WriteOperation& op,
                        magma::Magma::WriteOperation& outputOp) {
                    // Run the callers callback in primary domain
                    cb::UseArenaMallocPrimaryDomain domainGuard;
                    return docTransformCallback(op, outputOp);
                };
    }

    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->WriteDocs(kvID,
                            docOperations,
                            kvsRev,
                            wrappedDocCallback,
                            wrappedPostCallback,
                            wrappedDocTransformCallback,
                            historyMode);
}

magma::Status MagmaMemoryTrackingProxy::NewCheckpoint(
        const magma::Magma::KVStoreID kvID) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->NewCheckpoint(kvID);
}

magma::Status MagmaMemoryTrackingProxy::StopBGCompaction(
        const magma::Magma::KVStoreID kvID) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->StopBGCompaction(kvID);
}

magma::Status MagmaMemoryTrackingProxy::ResumeBGCompaction(
        const magma::Magma::KVStoreID kvID) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->ResumeBGCompaction(kvID);
}

magma::Magma::SeqNo MagmaMemoryTrackingProxy::GetOldestHistorySeqno(
        magma::Magma::KVStoreID kvid) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->GetOldestHistorySeqno(kvid);
}

magma::Magma::SeqNo MagmaMemoryTrackingProxy::GetOldestHistorySeqno(
        magma::Magma::Snapshot& snapshot) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->GetOldestHistorySeqno(snapshot);
}

void MagmaMemoryTrackingProxy::SetSeqTreeDataBlockSize(size_t value) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetSeqTreeDataBlockSize(value);
}

void MagmaMemoryTrackingProxy::SetMinValueBlockSizeThreshold(size_t value) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetSeqTreeValueBlockSize(value);
}

void MagmaMemoryTrackingProxy::SetSeqTreeIndexBlockSize(size_t value) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetSeqTreeIndexBlockSize(value);
}

void MagmaMemoryTrackingProxy::SetKeyTreeDataBlockSize(size_t value) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetKeyTreeDataBlockSize(value);
}

void MagmaMemoryTrackingProxy::SetKeyTreeIndexBlockSize(size_t value) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetKeyTreeIndexBlockSize(value);
}

void MagmaMemoryTrackingProxy::SetBackupInterval(
        std::chrono::minutes interval) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetBackupInterval(interval);
}

magma::Status MagmaMemoryTrackingProxy::StartBackup(
        const magma::Magma::KVStoreID kvID, const std::string& backupPath) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->StartBackup(kvID, backupPath);
}

magma::Status MagmaMemoryTrackingProxy::StopBackup(
        const magma::Magma::KVStoreID kvID) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->StopBackup(kvID);
}

void MagmaMemoryTrackingProxy::EnableHistoryEviction() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->EnableHistoryEviction();
}

void MagmaMemoryTrackingProxy::DisableHistoryEviction() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->DisableHistoryEviction();
}

void MagmaMemoryTrackingProxy::setActiveEncryptionKeys(
        const cb::crypto::KeyStore& keyStore) {
    // magma only cares about the current key
    cb::UseArenaMallocSecondaryDomain domainGuard;
    auto active = keyStore.getActiveKey();
    if (!active) {
        magma->SetCurrentEncryptionKey({});
    } else {
        magma->SetCurrentEncryptionKey(*active);
    }
}

nlohmann::json MagmaMemoryTrackingProxy::getVbucketEncryptionKeyIds() const {
    // this is a const method in magma so it'll only allocate memory in this
    // domain (and not cause any allocation / free in magmas domain) so we
    // can just return the allocated memory as it'll be freed in the same
    // domain
    return magma->GetActiveEncryptionKeyIDs();
}
