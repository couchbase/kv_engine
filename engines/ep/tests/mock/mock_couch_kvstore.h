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

#include "kvstore/couch-kvstore/couch-kvstore.h"

class MockCouchRequest : public CouchRequest {
public:
    class MetaData {
    public:
        MetaData() : cas(0), expiry(0), flags(0), ext1(0), ext2(0) {
        }

        uint64_t cas;
        uint32_t expiry;
        uint32_t flags;
        uint8_t ext1;
        uint8_t ext2;
        // V2 and V3 overlay each other. V2 is now unused but can
        // potentially still be read. V3 Adds an additional 7 bytes for
        // Durability-related fields.
        union V2V3Union {
            // Need to supply a default constructor or the compiler will
            // complain about cb::uint48_t
            V2V3Union() : v3{} {};

            struct {
                uint8_t conflictResMode;
            } v2;
            struct V3 {
                uint8_t operation;
                // Durability operation details - see MetaData::MetaDataV3.
                // Not further defined here (other than correct size) as
                // only need correct size for tests thus far.
                cb::uint48_t details;
            } v3;
        } metaV2V3;

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

    MockCouchKVStore(const CouchKVStoreConfig& config, FileOpsInterface& ops)
        : CouchKVStore(config, ops) {
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
        // each req will be de-allocated after commit
        auto& ctx = dynamic_cast<CouchKVStoreTransactionContext&>(txnCtx);
        ctx.pendingReqsQ.emplace_back(itm);
        return static_cast<MockCouchRequest*>(&ctx.pendingReqsQ.back());
    }

    std::unordered_map<Vbid, std::unordered_set<uint64_t>>
    public_getVbucketRevisions(
            const std::vector<std::string>& filenames) const {
        return getVbucketRevisions(filenames);
    }

    uint64_t public_getDbRevision(Vbid vbucketId) const {
        return getDbRevision(vbucketId);
    }

    bool deleteLocalDoc(Vbid vbid, std::string_view doc);
};
