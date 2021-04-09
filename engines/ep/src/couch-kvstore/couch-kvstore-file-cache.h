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

#include "couch-kvstore-db-holder.h"

#include <folly/Synchronized.h>
#include <folly/container/EvictingCacheMap.h>

#include <string>

/**
 * FileCache is a static process wide cache for the file descriptors in use in
 * CouchKVStore.
 *
 * @TODO MB-39302: hook this up for dynamic FD limit changes later
 */
class CouchKVStoreFileCache {
public:
    using CacheMap =
            folly::EvictingCacheMap<std::string,
                                    folly::Synchronized<DbHolder, std::mutex>>;

    /**
     * Handle to access the FileCacheHandle map. This is a separate struct so
     * that we can easily guard access with folly::Synchronized. folly's
     * EvictingCacheMap isn't thread safe by default so this is necessary. Not
     * all functions are a direct map to a function on the CacheMap so it's not
     * ideal to simply expose a folly::Synchronized<CacheMap>.
     */
    struct Handle {
        Handle(size_t cacheSize);

        // Need to explicitly default the move ctor as the dtor declaration will
        // implicit delete it.
        Handle(Handle&& other) = default;

        ~Handle();

        CacheMap::const_iterator begin() const;
        CacheMap::const_iterator end() const;
        CacheMap::iterator find(const std::string& key);

        void resize(size_t value);
        void clear();

        CacheMap::mapped_type::LockedPtr get(const std::string& key);
        void set(const std::string& key, DbHolder&& holder);
        std::pair<CacheMap::iterator, bool> insert(const std::string& key,
                                                   DbHolder&& holder);
        void erase(const std::string& key);

        size_t numFiles() const;

    protected:
        CacheMap cache;
    };

    static CouchKVStoreFileCache& get();

    auto getHandle() {
        return cache.lock();
    }

protected:
    CouchKVStoreFileCache();

    folly::Synchronized<Handle, std::mutex> cache;
};
