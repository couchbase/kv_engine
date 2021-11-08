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

#include "magma-memory-tracking-proxy.h"

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

template <>
void DomainAwareDelete<magma::UserStats>::operator()(magma::UserStats* p) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    delete p;
}

template <>
void DomainAwareDelete<magma::Magma::SeqIterator>::operator()(
        magma::Magma::SeqIterator* p) {
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

void MagmaMemoryTrackingProxy::Close() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->Close();
}

magma::Status MagmaMemoryTrackingProxy::CompactKVStore(
        const magma::Magma::KVStoreID kvID,
        const magma::Slice& lowKey,
        const magma::Slice& highKey,
        magma::Magma::CompactionCallbackBuilder makeCallback) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->CompactKVStore(kvID, lowKey, highKey, makeCallback);
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
                                            magma::Slice& value,
                                            bool& found) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->Get(kvID,
                      key,
                      idxBuf.getBuffer(),
                      seqBuf.getBuffer(),
                      meta,
                      value,
                      found);
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
    auto wrappedCallback = [&cb](bool found,
                                 magma::Status status,
                                 const magma::Magma::GetOperation& op,
                                 const magma::Slice& metaSlice,
                                 const magma::Slice& valueSlice) {
        // Run the callers callback in primary domain
        cb::UseArenaMallocPrimaryDomain domainGuard;
        cb(found, status, op, metaSlice, valueSlice);
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

std::tuple<magma::Status, magma::KVStoreStats>
MagmaMemoryTrackingProxy::GetKVStoreStats(const magma::Magma::KVStoreID kvid) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->GetKVStoreStats(kvid);
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
                                   const magma::Slice& key,
                                   bool& found) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    DomainAwareUniquePtr<std::string> stringPtr(new std::string{});
    auto status = magma->GetLocal(kvID, key, *stringPtr, found);
    return {status, std::move(stringPtr)};
}

std::pair<magma::Status, DomainAwareUniquePtr<std::string>>
MagmaMemoryTrackingProxy::GetLocal(magma::Magma::Snapshot& snapshot,
                                   const magma::Slice& key,
                                   bool& found) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    DomainAwareUniquePtr<std::string> stringPtr(new std::string{});
    auto status = magma->GetLocal(snapshot, key, *stringPtr, found);
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
        std::function<void(magma::Slice& key,
                           magma::Slice& meta,
                           magma::Slice& value)> itemCb,
        bool returnValue,
        uint64_t count) {
    auto wrappedCallback = [&itemCb](magma::Slice& key,
                                     magma::Slice& meta,
                                     magma::Slice& value) {
        cb::UseArenaMallocPrimaryDomain domainGuard;
        itemCb(key, meta, value);
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
                                     const magma::Magma::SeqNo seqno,
                                     bool& found) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    DomainAwareUniquePtr<std::string> key(new std::string{});
    DomainAwareUniquePtr<std::string> meta(new std::string{});
    DomainAwareUniquePtr<std::string> value(new std::string{});
    auto status =
            magma->GetBySeqno(snapshot, seqno, *key, *meta, *value, found);
    return {status, std::move(key), std::move(meta), std::move(value)};
}

DomainAwareUniquePtr<magma::Magma::SeqIterator>
MagmaMemoryTrackingProxy::NewSeqIterator(magma::Magma::Snapshot& snapshot) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return DomainAwareUniquePtr<magma::Magma::SeqIterator>{
            magma->NewSeqIterator(snapshot).release()};
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

void MagmaMemoryTrackingProxy::SetMemoryQuota(const size_t quota) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetMemoryQuota(quota);
}

void MagmaMemoryTrackingProxy::SetNumThreads(
        magma::Magma::ThreadType threadType, size_t nThreads) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    magma->SetNumThreads(threadType, nThreads);
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
        const magma::Magma::WriteDocsCallback docCallback,
        const magma::Magma::PostWriteDocsCallback postCallback) {
    magma::Magma::WriteDocsCallback wrappedDocCallback = nullptr;
    magma::Magma::PostWriteDocsCallback wrappedPostCallback = nullptr;

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

    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->WriteDocs(kvID,
                            docOperations,
                            kvsRev,
                            wrappedDocCallback,
                            wrappedPostCallback);
}

magma::Status MagmaMemoryTrackingProxy::NewCheckpoint(
        const magma::Magma::KVStoreID kvID) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->NewCheckpoint(kvID);
}
