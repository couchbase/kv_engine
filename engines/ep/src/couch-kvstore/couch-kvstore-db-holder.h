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

#include "libcouchstore/couch_db.h"

class CouchKVStore;

/**
 * Internal RAII class for managing a Db* and having it closed when
 * the DbHolder goes out of scope.
 */
class DbHolder {
public:
    DbHolder(CouchKVStore& kvs) : kvstore(kvs), db(nullptr), fileRev(0) {
    }

    DbHolder(const DbHolder&) = delete;
    DbHolder& operator=(const DbHolder&) = delete;

    // Move-construction is allowed.
    DbHolder(DbHolder&& other);

    DbHolder& operator=(DbHolder&& other) = delete;

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

    // Allow a non-RAII close, needed for some use-cases.
    // Note non-const; when this method returns it no longer owns the
    // Db.
    void close();

    ~DbHolder() {
        close();
    }

protected:
    CouchKVStore& kvstore;
    Db* db;
    uint64_t fileRev;
};
