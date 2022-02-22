/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "diskdockey.h"

#include <memcached/dockey.h>
#include <memcached/engine_error.h>
#include <memcached/vbucket.h>

#include <memory>

class Item;

class CacheLookup {
public:
    CacheLookup(const DiskDocKey& k, int64_t s, Vbid vb)
        : key(k), bySeqno(s), vbid(vb) {
    }

    ~CacheLookup() {}

    const DiskDocKey& getKey() {
        return key;
    }

    int64_t getBySeqno() { return bySeqno; }

    Vbid getVBucketId() {
        return vbid;
    }

    bool operator==(const CacheLookup& other) const {
        return key == other.key && bySeqno == other.bySeqno &&
               vbid == other.vbid;
    }
    bool operator!=(const CacheLookup& other) const {
        return !operator==(other);
    }

private:
    DiskDocKey key;
    int64_t bySeqno;
    Vbid vbid;
};

/**
 * Value for callback for GET operations.
 */
class GetValue {
public:
    GetValue();

    explicit GetValue(std::unique_ptr<Item> v,
                      cb::engine_errc s = cb::engine_errc::success,
                      uint64_t i = -1,
                      bool incomplete = false);

    /// Cannot copy GetValues (cannot copy underlying Item).
    GetValue(const GetValue&) = delete;
    GetValue& operator=(const GetValue&&) = delete;

    /// Can move GetValues
    GetValue(GetValue&& other);
    GetValue& operator=(GetValue&& other);

    ~GetValue();

    /**
     * Engine code describing what happened.
     */
    cb::engine_errc getStatus() const {
        return status;
    }

    /**
     * Set the status code
     */
    void setStatus(cb::engine_errc s) {
        status = s;
    }

    /**
     * Get the item's underlying ID (if applicable).
     */
    uint64_t getId() { return id; }

    /**
     * Set the item's underlying ID.
     */
    void setId(uint64_t newId) { id = newId; }

    bool isPartial() const { return partial; }

    void setPartial() { partial = true; }

    std::unique_ptr<Item> item;

private:
    uint64_t id;
    cb::engine_errc status;
    bool partial;
};

/**
 * Abstract interface for callbacks from storage APIs.
 */
template <typename... RV>
class Callback {
public:
    virtual ~Callback() {}

    /**
     * Method called on callback.
     */
    virtual void callback(RV&... value) = 0;
};

/**
 * Interface for callbacks which return a status code.
 */
template <typename... RV>
class StatusCallback : public Callback<RV...> {
public:
    StatusCallback() : myStatus(cb::engine_errc::success) {
    }

    virtual void setStatus(cb::engine_errc status) {
        myStatus = status;
    }

    virtual cb::engine_errc getStatus() const {
        return myStatus;
    }

    void yield() {
        myStatus = cb::engine_errc::temporary_failure;
    }

    bool shouldYield() const {
        return myStatus == cb::engine_errc::temporary_failure;
    }

private:
    cb::engine_errc myStatus;
};
