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
        std::unique_ptr<magma::Magma::Snapshot>& snap) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->GetDiskSnapshot(kvID, snap);
}

magma::Status MagmaMemoryTrackingProxy::GetDocs(
        const magma::Magma::KVStoreID kvID,
        magma::Operations<magma::Magma::GetOperation>& getOps,
        magma::Magma::GetDocCallback cb) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->GetDocs(kvID, getOps, cb);
}

std::vector<magma::Magma::KVStoreID>
MagmaMemoryTrackingProxy::GetKVStoreList() {
    // cb::UseArenaMallocSecondaryDomain domainGuard;
    // @todo: Return a type which can be destroyed in the correct domain
    return magma->GetKVStoreList();
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

std::unique_ptr<magma::UserStats> MagmaMemoryTrackingProxy::GetKVStoreUserStats(
        const magma::Magma::KVStoreID kvid) {
    // cb::UseArenaMallocSecondaryDomain domainGuard;
    // @todo: Return a type which can be destroyed in the correct domain
    return magma->GetKVStoreUserStats(kvid);
}

std::unique_ptr<magma::UserStats> MagmaMemoryTrackingProxy::GetKVStoreUserStats(
        magma::Magma::Snapshot& snapshot) {
    // cb::UseArenaMallocSecondaryDomain domainGuard;
    // @todo: Return a type which can be destroyed in the correct domain
    return magma->GetKVStoreUserStats(snapshot);
}

magma::Status MagmaMemoryTrackingProxy::GetLocal(
        const magma::Magma::KVStoreID kvID,
        const magma::Slice& key,
        std::string& value,
        bool& found) {
    // cb::UseArenaMallocSecondaryDomain domainGuard;
    // @todo: change value type
    return magma->GetLocal(kvID, key, value, found);
}
magma::Status MagmaMemoryTrackingProxy::GetLocal(
        magma::Magma::Snapshot& snapshot,
        const magma::Slice& key,
        std::string& value,
        bool& found) {
    // cb::UseArenaMallocSecondaryDomain domainGuard;
    // @todo: change value type
    return magma->GetLocal(snapshot, key, value, found);
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
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->GetRange(kvID, startKey, endKey, itemCb, returnValue, count);
}

void MagmaMemoryTrackingProxy::GetStats(
        magma::Magma::MagmaStats& magmaStats,
        std::chrono::milliseconds cacheDuration) {
    // cb::UseArenaMallocSecondaryDomain domainGuard;
    // @todo: MagmaStats internally has at least one std::vector which needs to
    // be destroyed in the correct domain
    magma->GetStats(magmaStats, cacheDuration);
}

std::unique_ptr<magma::Magma::SeqIterator>
MagmaMemoryTrackingProxy::NewSeqIterator(magma::Magma::Snapshot& snapshot) {
    // cb::UseArenaMallocSecondaryDomain domainGuard;
    // @todo: Return a type which can be destroyed in the correct domain
    return magma->NewSeqIterator(snapshot);
}

magma::Status MagmaMemoryTrackingProxy::Open() {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->Open();
}

magma::Status MagmaMemoryTrackingProxy::Rollback(
        const magma::Magma::KVStoreID kvID,
        magma::Magma::SeqNo rollbackSeqno,
        magma::Magma::RollbackCallback callback) {
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->Rollback(kvID, rollbackSeqno, callback);
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
    cb::UseArenaMallocSecondaryDomain domainGuard;
    return magma->WriteDocs(
            kvID, docOperations, kvsRev, docCallback, postCallback);
}
