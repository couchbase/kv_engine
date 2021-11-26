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

#include "libcouchstore/couch_db.h"

class CouchKVStore;

/**
 * Internal RAII class for managing a Db* and having it closed when
 * the DbHolder goes out of scope.
 */
class DbHolder {
public:
    DbHolder(const CouchKVStore& kvs) : kvstore(kvs), db(nullptr), fileRev(0) {
    }

    DbHolder(const DbHolder&) = delete;
    DbHolder& operator=(const DbHolder&) = delete;

    // Move-construction is allowed.
    DbHolder(DbHolder&& other);

    // Move-assignment is required for folly::EvictingCacheMap::set in the
    // CouchKVStoreFileCache
    DbHolder& operator=(DbHolder&& other);

    Db** getDbAddress() {
        return &db;
    }

    Db* getDb() {
        return db;
    }

    Db* getDb() const {
        return db;
    }

    operator Db*() {
        return db;
    }

    Db* releaseDb() {
        auto* result = db;
        db = nullptr;
        return result;
    }

    void setFileRev(uint64_t rev) {
        fileRev = rev;
    }

    uint64_t getFileRev() const {
        return fileRev;
    }

    void setFilename(std::string name) {
        filename = std::move(name);
    }

    const std::string& getFilename() const {
        return filename;
    }

    // Allow a non-RAII close, needed for some use-cases.
    // Note non-const; when this method returns it no longer owns the
    // Db.
    void close();

    ~DbHolder() {
        close();
    }

protected:
    std::reference_wrapper<const CouchKVStore> kvstore;
    Db* db;
    uint64_t fileRev;
    std::string filename;
};
