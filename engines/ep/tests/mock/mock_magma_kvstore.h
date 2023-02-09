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

#ifdef EP_USE_MAGMA

#include "kvstore/magma-kvstore/magma-kvstore.h"

class MockMagmaKVStore : public MagmaKVStore {
public:
    explicit MockMagmaKVStore(MagmaKVStoreConfig& config);

    /**
     * See base-class comments for usage. However this "mock" version will
     * override the ScanContext::historyStartSeqno if this class defines
     * historyStartSeqno (see historyStartSeqno member below).
     */
    std::unique_ptr<BySeqnoScanContext> initBySeqnoScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            uint64_t startSeqno,
            DocumentFilter options,
            ValueFilter valOptions,
            SnapshotSource source,
            std::unique_ptr<KVFileHandle> fileHandle = nullptr) const override;

    /**
     * See base-class comments for usage. However this "mock" version will
     * override the ScanContext::historyStartSeqno if this class defines
     * historyStartSeqno (see historyStartSeqno member below).
     */
    std::unique_ptr<ByIdScanContext> initByIdScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            const std::vector<ByIdRange>& ranges,
            DocumentFilter options,
            ValueFilter valOptions) const override;

    ReadVBStateResult readVBStateFromDisk(Vbid vbid);

    ReadVBStateResult readVBStateFromDisk(
            Vbid vbid, magma::Magma::Snapshot& snapshot) const override;

    int saveDocs(MagmaKVStoreTransactionContext& txnCtx,
                 VB::Commit& commitData,
                 kvstats_ctx& kvctx,
                 magma::Magma::HistoryMode historyMode =
                         magma::Magma::HistoryMode::Disabled) override;

    bool snapshotVBucket(Vbid vbid, const VB::Commit& meta) override;

    magma::Status addLocalDoc(Vbid vbid,
                              std::string_view key,
                              std::string value);

    magma::Status deleteLocalDoc(Vbid vbid, std::string_view key);

    /*
     * Perform implicit compactions for all keyIndex sstables in the vbucket
     * These compactions will use the same context as Magma's implicit
     * compactions
     */
    magma::Status runImplicitCompactKVStore(Vbid vbid);

    /*
     * Create new rollback-able checkpoint for the specified vbucket
     * @param vbid
     */
    magma::Status newCheckpoint(Vbid vbid);

    void setCompactionStatusHook(std::function<void(magma::Status&)> hook) {
        compactionStatusHook = hook;
    }

    void setPreCompactKVStoreHook(std::function<void()> hook) {
        preCompactKVStoreHook = hook;
    }

    std::optional<MagmaDbStats> public_getMagmaDbStats(Vbid vbid) {
        return getMagmaDbStats(vbid);
    }

    void setSyncFileHandleStatusHook(std::function<void(magma::Status&)> hook) {
        fileHandleSyncStatusHook = hook;
    }

    StorageProperties getStorageProperties() const override {
        return storageProperties;
    }

    TestingHook<> readVBStateFromDiskHook;

    std::function<int(VB::Commit&, kvstats_ctx&)> saveDocsErrorInjector;
    std::function<bool()> snapshotVBucketErrorInjector;

    StorageProperties storageProperties;

    /**
     * The historyStartSeqno in this class when set will override the value
     * MagmaKVStore sets. This allows arbitrary placement of the history range
     * for testing of various two-phase backfills.
     */
    std::optional<uint64_t> historyStartSeqno;
};

#endif
