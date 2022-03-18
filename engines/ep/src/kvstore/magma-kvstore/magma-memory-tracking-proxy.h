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

/**
 * The primary class in this file is "MagmaMemoryTrackingProxy". This is a
 * proxy around our usage of Magma. The purpose of the class is to set
 * the memory domain before crossing over to libmagma, the result is that the
 * Secondary MemoryDomain will account for libmagma memory usage. The API
 * exposed here is a clone of libmagma with some additions where extra memory
 * management is needed.
 */

#pragma once

#include "libmagma/magma.h"

/**
 * DomainAwareFetchBuffer is a supporting class that allocates and frees in
 * MemoryDomain::Secondary
 */
class DomainAwareFetchBuffer {
public:
    DomainAwareFetchBuffer();
    ~DomainAwareFetchBuffer();

    // Obtain the real object needed for calling Magma
    magma::Magma::FetchBuffer& getBuffer() {
        return *buffer;
    }

private:
    std::unique_ptr<magma::Magma::FetchBuffer> buffer;
};

// DomainAwareSeqIterator is a helper that switches domains before invoking
// methods of SeqIterator.
class DomainAwareSeqIterator : public magma::Magma::SeqIterator {
public:
    DomainAwareSeqIterator(std::unique_ptr<magma::Magma::SeqIterator> itr)
        : itr(std::move(itr)) {
    }
    ~DomainAwareSeqIterator() override;
    void Seek(const magma::Magma::SeqNo startSeqno,
              const magma::Magma::SeqNo endSeqno) override;
    bool Valid() override;
    magma::Status GetStatus() override;
    void Next() override;
    void GetRecord(magma::Slice& key,
                   magma::Slice& meta,
                   magma::Slice& value,
                   magma::Magma::SeqNo& seqno) override;
    std::string to_string() const;

private:
    std::unique_ptr<magma::Magma::SeqIterator> itr;
};

/**
 * Helper/Deleter for std::unique_ptr types, will delete in
 * MemoryDomain::Secondary
 */
template <class T>
struct DomainAwareDelete {
    void operator()(T* p);
};

template <class T>
using DomainAwareUniquePtr = std::unique_ptr<T, DomainAwareDelete<T>>;

class MagmaMemoryTrackingProxy {
public:
    /**
     * Constructs a Magma instance using the secondary domain
     */
    MagmaMemoryTrackingProxy(magma::Magma::Config& config);

    /**
     * Destructs the Magma instance using the secondary domain
     */
    ~MagmaMemoryTrackingProxy();

    // For all of the following functions, please see libmagma for documentation

    void Close();
    magma::Status CompactKVStore(
            const magma::Magma::KVStoreID kvID,
            const magma::Slice& lowKey,
            const magma::Slice& highKey,
            magma::Magma::CompactionCallbackBuilder makeCallback = nullptr);
    magma::Status RunImplicitCompactKVStore(const magma::Magma::KVStoreID kvID);
    magma::Status DeleteKVStore(const magma::Magma::KVStoreID kvID,
                                const magma::Magma::KVStoreRevision kvsRev = 1);
    magma::Status Get(const magma::Magma::KVStoreID kvID,
                      const magma::Slice& key,
                      DomainAwareFetchBuffer& idxBuf,
                      DomainAwareFetchBuffer& seqBuf,
                      magma::Slice& meta,
                      magma::Slice& value,
                      bool& found);
    magma::Status GetDiskSnapshot(
            const magma::Magma::KVStoreID kvID,
            DomainAwareUniquePtr<magma::Magma::Snapshot>& snap);
    magma::Status GetOldestDiskSnapshot(
            const magma::Magma::KVStoreID kvID,
            DomainAwareUniquePtr<magma::Magma::Snapshot>& snap);
    magma::Status GetSnapshot(
            const magma::Magma::KVStoreID kvID,
            DomainAwareUniquePtr<magma::Magma::Snapshot>& snap);
    magma::Status GetDocs(const magma::Magma::KVStoreID kvID,
                          magma::Operations<magma::Magma::GetOperation>& getOps,
                          magma::Magma::GetDocCallback cb);

    /**
     * Invokes the given callback with the result of calling
     * magma::GetKVStoreList, this allows the caller to switch domains and work
     * with the returned vector
     *
     * @param callback function to invoke for each KVStore
     */
    void executeOnKVStoreList(
            std::function<void(const std::vector<magma::Magma::KVStoreID>&)>
                    callback);

    std::tuple<magma::Status, magma::Magma::KVStoreRevision> GetKVStoreRevision(
            const magma::Magma::KVStoreID kvID);
    std::tuple<magma::Status, magma::KVStoreStats> GetKVStoreStats(
            const magma::Magma::KVStoreID kvid);

    DomainAwareUniquePtr<magma::UserStats> GetKVStoreUserStats(
            const magma::Magma::KVStoreID kvid);
    DomainAwareUniquePtr<magma::UserStats> GetKVStoreUserStats(
            magma::Magma::Snapshot& snapshot);

    std::pair<magma::Status, DomainAwareUniquePtr<std::string>> GetLocal(
            const magma::Magma::KVStoreID kvID,
            const magma::Slice& key,
            bool& found);

    std::pair<magma::Status, DomainAwareUniquePtr<std::string>> GetLocal(
            magma::Magma::Snapshot& snapshot,
            const magma::Slice& key,
            bool& found);

    magma::Status GetMaxSeqno(const magma::Magma::KVStoreID kvID,
                              magma::Magma::SeqNo& seqno);
    magma::Status GetRange(const magma::Magma::KVStoreID kvID,
                           const magma::Slice& startKey,
                           const magma::Slice& endKey,
                           magma::Magma::GetRangeCB itemCb,
                           bool returnValue = false,
                           uint64_t count = 0);

    std::tuple<magma::Status,
               DomainAwareUniquePtr<std::string>,
               DomainAwareUniquePtr<std::string>,
               DomainAwareUniquePtr<std::string>>
    GetBySeqno(magma::Magma::Snapshot& snapshot,
               const magma::Magma::SeqNo seqno,
               bool& found);
    DomainAwareUniquePtr<magma::Magma::MagmaStats> GetStats(
            std::chrono::milliseconds cacheDuration = std::chrono::seconds(0));
    void GetFileStats(magma::MagmaFileStats& fileStats);
    void GetHistogramStats(magma::MagmaHistogramStats& histogramStats);

    DomainAwareUniquePtr<DomainAwareSeqIterator> NewSeqIterator(
            magma::Magma::Snapshot& snapshot);
    magma::Status Open();
    magma::Status Rollback(const magma::Magma::KVStoreID kvID,
                           magma::Magma::SeqNo rollbackSeqno,
                           magma::Magma::RollbackCallback callback);
    void SetMaxOpenFiles(size_t n, bool blocking = false);
    void SetFragmentationRatio(double fragRatio);
    void EnableBlockCache(bool enable);
    void SetMemoryQuota(const size_t quota);
    void SetNumThreads(magma::Magma::ThreadType threadType, size_t nThreads);
    magma::Status Sync(bool flushAll);
    magma::Status SyncKVStore(const magma::Magma::KVStoreID kvID);
    magma::Status WriteDocs(
            const magma::Magma::KVStoreID kvID,
            const std::vector<magma::Magma::WriteOperation>& docOperations,
            const magma::Magma::KVStoreRevision kvsRev = 1,
            const magma::Magma::WriteDocsCallback docCallback = nullptr,
            const magma::Magma::PostWriteDocsCallback postCallback = nullptr);
    magma::Status NewCheckpoint(const magma::Magma::KVStoreID kvID);
    magma::Status StopBGCompaction(const magma::Magma::KVStoreID kvID);
    magma::Status ResumeBGCompaction(const magma::Magma::KVStoreID kvID);

private:
    std::unique_ptr<magma::Magma> magma;
};