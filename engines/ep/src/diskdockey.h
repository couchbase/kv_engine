/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/dockey.h>
#include <string>

struct DocKey;
class Item;

/**
 * Represents a document key which has been / will be written to disk.
 *
 * Objects of this type are created when we make a request to a KVStore to
 * read or write a document.
 *
 * This is similar to DocKey (and related classes), except that may have a
 * 'DurabilityPrepare' namespace prefix prepended before the normal
 * collectionID if it refers to a document in the separate prepare namespace.
 * As such, it *cannot* be implicitly converted to DocKey - the user must
 * consider if they explicitly want to ignore the DurabilityPrepare
 * namespace - see getDocKey() method.
 */
class DiskDocKey {
public:
    /**
     * Create a DiskDocKey from a DocKey and a pending flag.
     *
     * @param key DocKey that is to be copied-in
     * @param prepared Prefix for prepared SyncWrite?
     */
    explicit DiskDocKey(const DocKey& key, bool prepared = false);

    /**
     * Create a DiskDocKey from an Item.
     *
     * Adds a DurabilityPrepare namespace if the Item is a pending SyncWrite.
     */
    explicit DiskDocKey(const Item& item);

    /**
     * Create a DiskDocKey from a raw char* and length.
     *
     * Used when constructing DiskDocKeys from on-disk key spans / views (e.g.
     * couchstore sized_buf). Accepts valid namespace prefixes on the key span.
     */
    explicit DiskDocKey(const char* ptr, size_t len);

    bool operator==(const DiskDocKey& rhs) const {
        return keydata == rhs.keydata;
    }

    bool operator!=(const DiskDocKey& rhs) const {
        return !(*this == rhs);
    }

    bool operator<(const DiskDocKey& rhs) const {
        return keydata < rhs.keydata;
    }

    /// Hash function for use with STL containers.
    std::size_t hash() const;

    const uint8_t* data() const {
        return reinterpret_cast<const uint8_t*>(keydata.data());
    }

    size_t size() const {
        return keydata.size();
    }

    /**
     * Explicitly convert to a DocKey, ignoring any DurabilityPrepare prefix
     * which may be present.
     */
    DocKey getDocKey() const;

    /**
     * @returns true if the DocKey refers to a committed (not DurabilityPrepare)
     * key.
     */
    bool isCommitted() const;

    /**
     * @returns true if the DocKey refers to a prepared (DurabilityPrepare) key.
     */
    bool isPrepared() const;

    /**
     * Intended for debug use only
     * @returns cid:key
     */
    std::string to_string() const;

    /**
     * Intended for RangeScan key manipulation, append character to key
     */
    void append(char c) {
        keydata.push_back(c);
    }

protected:
    std::string keydata;
};

/// A hash function for DiskDocKey so they can be used in std::map and friends.
namespace std {
template <>
struct hash<DiskDocKey> {
    std::size_t operator()(const DiskDocKey& key) const {
        return key.hash();
    }
};
} // namespace std
