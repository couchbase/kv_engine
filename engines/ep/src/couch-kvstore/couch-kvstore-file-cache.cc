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

#include "couch-kvstore-file-cache.h"

#include "bucket_logger.h"
#include "environment.h"

#include <gsl/gsl-lite.hpp>

// Destruction of the mapped type (eviction from the cache) requires that we
// close the opened file. If we don't take the lock when destructing the
// mapped typed though then we can destroy it while another instance has a
// LockedPtr on the mapped type. We need to acquire the lock here to prevent
// these races.
static void cacheEvictionHandler(
        CouchKVStoreFileCache::CacheMap::key_type key,
        CouchKVStoreFileCache::CacheMap::mapped_type&& value) {
    auto handle = value.lock();
    handle->close();
}

CouchKVStoreFileCache& CouchKVStoreFileCache::get() {
    static CouchKVStoreFileCache fc;
    return fc;
}

CouchKVStoreFileCache::CouchKVStoreFileCache() : cache(1) {
}

CouchKVStoreFileCache::Handle::Handle(size_t cacheSize) : cache(cacheSize) {
    cache.setPruneHook(&cacheEvictionHandler);
}

CouchKVStoreFileCache::Handle::~Handle() {
    clear();
}

CouchKVStoreFileCache::CacheMap::const_iterator
CouchKVStoreFileCache::Handle::begin() const {
    return cache.begin();
}

CouchKVStoreFileCache::CacheMap::const_iterator
CouchKVStoreFileCache::Handle::end() const {
    return cache.end();
}

CouchKVStoreFileCache::CacheMap::iterator CouchKVStoreFileCache::Handle::find(
        const std::string& key) {
    return cache.find(key);
}

void CouchKVStoreFileCache::Handle::resize(size_t value) {
    // Size should be at least 0 as this is a special case in folly that removes
    // the size limit and stops the cache from evicting things
    Expects(value > 0);

    auto envLimit = Environment::get().getMaxBackendFileDescriptors();
    auto newLimit = std::min(value, envLimit);
    if (newLimit != cache.getMaxSize()) {
        EP_LOG_INFO("CouchKVStoreFileCache::resize: oldSize:{}, newSize:{}",
                    cache.getMaxSize(),
                    newLimit);
        cache.setMaxSize(newLimit);
    }
}

void CouchKVStoreFileCache::Handle::clear() {
    // All the files should be closed now, nuke the cache.
    cache.clear(&cacheEvictionHandler);
}

CouchKVStoreFileCache::CacheMap::mapped_type::LockedPtr
CouchKVStoreFileCache::Handle::get(const std::string& key) {
    return cache.get(key).lock();
}

void CouchKVStoreFileCache::Handle::set(const std::string& key,
                                        DbHolder&& holder) {
    cache.set(key,
              CouchKVStoreFileCache::CacheMap::mapped_type(std::move(holder)));
}

std::pair<CouchKVStoreFileCache::CacheMap::iterator, bool>
CouchKVStoreFileCache::Handle::insert(const std::string& key,
                                      DbHolder&& holder) {
    return cache.insert(
            key,
            CouchKVStoreFileCache::CacheMap::mapped_type(std::move(holder)));
}

void CouchKVStoreFileCache::Handle::erase(const std::string& key) {
    auto itr = cache.find(key);
    if (itr == cache.end()) {
        return;
    }

    {
        auto handle = itr->second.lock();
        handle->close();
    }

    cache.erase(itr);
}

size_t CouchKVStoreFileCache::Handle::numFiles() const {
    return cache.size();
}
