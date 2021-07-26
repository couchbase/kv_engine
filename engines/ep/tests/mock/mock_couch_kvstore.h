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

#include "couch-kvstore/couch-kvstore.h"

class MockCouchRequest : public CouchRequest {
public:
    class MetaData {
    public:
        MetaData()
            : cas(0), expiry(0), flags(0), ext1(0), ext2(0), legacyDeleted(0) {
        }

        uint64_t cas;
        uint32_t expiry;
        uint32_t flags;
        uint8_t ext1;
        uint8_t ext2;
        uint8_t legacyDeleted; // allow testing via 19byte meta document

        static const size_t sizeofV0 = 16;
        static const size_t sizeofV1 = 18;
        static const size_t sizeofV2 = 19;
    };

    explicit MockCouchRequest(const queued_item it) : CouchRequest(it) {
    }

    // Update what will be written as 'metadata'
    void writeMetaData(MetaData& meta, size_t size) {
        std::memcpy(dbDocInfo.rev_meta.buf, &meta, size);
        dbDocInfo.rev_meta.size = size;
    }
};

class MockCouchKVStore : public CouchKVStore {
public:
    /// Read-write constructor
    explicit MockCouchKVStore(const CouchKVStoreConfig& config)
        : CouchKVStore(config) {
    }

    /// Read-Only constructor where we are given a RevisionMap
    MockCouchKVStore(const CouchKVStoreConfig& config,
                     std::shared_ptr<RevisionMap> dbFileRevMap)
        : CouchKVStore(
                  config, *couchstore_get_default_file_ops(), dbFileRevMap) {
    }

    using CouchKVStore::setConcurrentCompactionPostLockHook;
    using CouchKVStore::setConcurrentCompactionPreLockHook;
    using CouchKVStore::setMb40415RegressionHook;

    /**
     * Mocks original code but returns the IORequest for fuzzing.
     *
     * NOTE: Returned pointer is only valid until the next request is added to
     * the pendingReqsQ.
     */
    MockCouchRequest* setAndReturnRequest(TransactionContext& txnCtx,
                                          queued_item& itm) {
        if (!inTransaction) {
            throw std::invalid_argument(
                    "MockCouchKVStore::set: inTransaction must be "
                    "true to perform a set operation.");
        }

        // each req will be de-allocated after commit
        auto& ctx = dynamic_cast<CouchKVStoreTransactionContext&>(txnCtx);
        ctx.pendingReqsQ.emplace_back(itm);
        return static_cast<MockCouchRequest*>(&ctx.pendingReqsQ.back());
    }

    bool commit(TransactionContext& txnCtx, VB::Commit& commitData) override {
        preCommitHook();
        return CouchKVStore::commit(txnCtx, commitData);
    }

    /**
     * Register a callback to be triggered immediately before
     * CouchKVStore::commit is executed. Will replace any previously set hook.
     */
    void setPreCommitHook(std::function<void()> cb) {
        preCommitHook = std::move(cb);
    }

    std::unordered_map<Vbid, std::unordered_set<uint64_t>>
    public_getVbucketRevisions(
            const std::vector<std::string>& filenames) const {
        return getVbucketRevisions(filenames);
    }

    uint64_t public_getDbRevision(Vbid vbucketId) const {
        return getDbRevision(vbucketId);
    }

    TestingHook<> preCommitHook;
};
