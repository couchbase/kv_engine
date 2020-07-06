/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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
    explicit MockCouchKVStore(CouchKVStoreConfig& config)
        : CouchKVStore(config) {
    }

    /// Read-Only constructor where we are given a RevisionMap
    MockCouchKVStore(CouchKVStoreConfig& config,
                     std::shared_ptr<RevisionMap> dbFileRevMap)
        : CouchKVStore(CreateReadOnly{},
                       config,
                       *couchstore_get_default_file_ops(),
                       dbFileRevMap) {
    }

    using CouchKVStore::compactDBInternal;

    /**
     * Mocks original code but returns the IORequest for fuzzing.
     *
     * NOTE: Returned pointer is only valid until the next request is added to
     * the pendingReqsQ.
     */
    MockCouchRequest* setAndReturnRequest(queued_item& itm) {
        if (isReadOnly()) {
            throw std::logic_error(
                    "MockCouchKVStore::set: Not valid on a read-only "
                    "object.");
        }
        if (!inTransaction) {
            throw std::invalid_argument(
                    "MockCouchKVStore::set: inTransaction must be "
                    "true to perform a set operation.");
        }

        // each req will be de-allocated after commit
        pendingReqsQ.emplace_back(itm);
        return static_cast<MockCouchRequest*>(&pendingReqsQ.back());
    }

    bool commit(VB::Commit& commitData) override {
        preCommitHook();
        return CouchKVStore::commit(commitData);
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

    std::unique_ptr<MockCouchKVStore> makeReadOnlyStore() const {
        return std::make_unique<MockCouchKVStore>(configuration, dbFileRevMap);
    }

    std::function<void()> preCommitHook = [] {};
};