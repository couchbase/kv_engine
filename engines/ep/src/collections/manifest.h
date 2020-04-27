/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include <nlohmann/json_fwd.hpp>
#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>

#include "collections/collections_types.h"
#include "memcached/engine_common.h"

class KVBucket;

namespace Collections {

static const size_t MaxCollectionNameSize = 30;

struct CollectionEntry {
    CollectionID id;
    cb::ExpiryLimit maxTtl;
    bool operator==(const CollectionEntry& other) const;
    bool operator!=(const CollectionEntry& other) const {
        return !(*this == other);
    }
};

struct Scope {
    std::string name;
    std::vector<CollectionEntry> collections;
    bool operator==(const Scope& other) const;
    bool operator!=(const Scope& other) const {
        return !(*this == other);
    }
};

/**
 * Manifest is an object that is constructed from JSON data as per
 * a set_collections command
 *
 * Users of this class can then obtain the UID and all collections that are
 * included in the manifest.
 */
class Manifest {
public:
    Manifest() = default;

    /*
     * Create a manifest from json.
     * Validates the json as per SET_COLLECTIONS rules.
     * @param json a buffer containing the JSON manifest data
     * @param maxNumberOfScopes an upper limit on the number of scopes allowed
     *        defaults to 100.
     * @param maxNumberOfCollections an upper limit on the number of collections
     *        allowed, defaults to 1000.
     */
    Manifest(std::string_view json,
             size_t maxNumberOfScopes = 100,
             size_t maxNumberOfCollections = 1000);

    Manifest(const std::string& json,
             size_t maxNumberOfScopes = 100,
             size_t maxNumberOfCollections = 1000)
        : Manifest(std::string_view{json},
                   maxNumberOfScopes,
                   maxNumberOfCollections) {
    }

    bool doesDefaultCollectionExist() const {
        return defaultCollectionExists;
    }

    // This manifest object stores UID to Scope mappings
    using scopeContainer = std::unordered_map<ScopeID, Scope>;

    // This manifest object stores CID mapped to SID and collection name
    struct Collection {
        ScopeID sid;
        std::string name;
        bool operator==(const Collection& other) const;
        bool operator!=(const Collection& other) const {
            return !(*this == other);
        }
    };
    using collectionContainer = std::unordered_map<CollectionID, Collection>;

    collectionContainer::const_iterator begin() const {
        return collections.begin();
    }

    collectionContainer::const_iterator end() const {
        return collections.end();
    }

    scopeContainer::const_iterator beginScopes() const {
        return scopes.begin();
    }

    scopeContainer::const_iterator endScopes() const {
        return scopes.end();
    }

    size_t size() const {
        return collections.size();
    }

    /**
     * @return the unique ID of the Manifest which constructed this
     */
    ManifestUid getUid() const {
        return uid;
    }

    /**
     * Search for a collection by CollectionID
     *
     * @param cid CollectionID to search for.
     * @return iterator to the matching entry or end() if not found.
     */
    collectionContainer::const_iterator findCollection(CollectionID cid) const {
        return collections.find(cid);
    }

    /**
     * Search for a collection by name (requires a scope name also)
     *
     * @param collectionName Name of the collection to search for.
     * @param scopeName Name of the scope in which to search. Defaults to the
     * default scope.
     * @return iterator to the matching entry or end() if not found.
     */
    collectionContainer::const_iterator findCollection(
            const std::string& collectionName,
            std::string_view scopeName = DefaultScopeIdentifier) const {
        for (auto& scope : scopes) {
            if (scope.second.name == scopeName) {
                for (auto& scopeCollection : scope.second.collections) {
                    auto collection = collections.find(scopeCollection.id);

                    if (collection != collections.end() &&
                        collection->second.name == collectionName) {
                        return collection;
                    }
                }
            }
        }

        return collections.end();
    }

    /**
     * Search for a scope by ScopeID
     *
     * @param sid ScopeID to search for.
     * @return iterator to the matching entry or end() if not found.
     */
    scopeContainer::const_iterator findScope(ScopeID sid) const {
        return scopes.find(sid);
    }

    /**
     * Attempt to lookup the collection-id of the "path" note that this method
     * skips/ignores the scope part of the path and requires the caller to
     * specify the scope for the actual ID lookup. getScopeID(path) exists for
     * this purpose.
     *
     * A path defined as "scope.collection"
     *
     * _default collection can be specified by name or by omission
     * e.g. "." == "_default._default"
     *      "c1." == "c1._default" (which would fail to find an ID)
     *
     * @param scope The scopeID of the scope part of the path
     * @param path The full path, the scope part is not used
     * @return optional CollectionID, undefined if nothing found
     * @throws cb::engine_error(invalid_argument) for invalid input
     */
    std::optional<CollectionID> getCollectionID(ScopeID scope,
                                                std::string_view path) const;

    /**
     * Attempt to lookup the scope-id of the "path", note that this method
     * ignores the collection part of the path.
     *
     * A path defined as either "scope.collection" or "scope"
     *
     * _default scope can be specified by name or by omission
     * e.g. ".beer" == _default scope
     *      ".      == _default scope
     *      ""      == _default scope
     *
     * @return optional ScopeID, undefined if nothing found
     * @throws cb::engine_error(invalid_argument) for invalid input
     */
    std::optional<ScopeID> getScopeID(std::string_view path) const;

    /**
     * Attempt to lookup the scope-id of the "key" (using the collection-ID)
     * @return an optional ScopeID, undefined if nothing found
     */
    std::optional<ScopeID> getScopeID(const DocKey& key) const;

    /**
     * Attempt to lookup the scope-id of the "key" (using the collection-ID)
     * @return an optional ScopeID, undefined if nothing found
     */
    std::optional<ScopeID> getScopeID(CollectionID cid) const;

    /**
     * @returns this manifest as nlohmann::json object
     */
    nlohmann::json toJson(
            const Collections::IsVisibleFunction& isVisible) const;

    /**
     * Add stats for collection. Each collection is tested for
     * Privilege::SimpleStats and 'added' if the user has the privilege.
     * @param bucket The bucket so we can call engine::testPrivilege
     * @param cookie The cookie so for the connection
     * @param function to call to add stats
     */
    void addCollectionStats(KVBucket& bucket,
                            const void* cookie,
                            const AddStatFn& add_stat) const;
    /**
     * Add stats for scopes. Each scope is tested for
     * Privilege::SimpleStats and 'added' if the user has the privilege.
     * @param bucket The bucket so we can call engine::testPrivilege
     * @param cookie The cookie so for the connection
     * @param function to call to add stats
     */
    void addScopeStats(KVBucket& bucket,
                       const void* cookie,
                       const AddStatFn& add_stat) const;

    /**
     * Write to std::cerr this
     */
    void dump() const;

    bool operator==(const Manifest& other) const;
    bool operator!=(const Manifest& other) const {
        return !(*this == other);
    }

private:
    /**
     * Set defaultCollectionExists to true if identifier matches
     * CollectionID::Default
     * @param identifier ID to check
     */
    void enableDefaultCollection(CollectionID identifier);

    /**
     * Check if the std::string represents a legal collection name.
     * Current validation is to ensure we block creation of _ prefixed
     * collections and only accept $default for $ prefixed names.
     *
     * @param name a collection or scope name.
     */
    static bool validName(std::string_view name);

    /**
     * Check if the CollectionID is invalid for a Manifest
     */
    static bool invalidCollectionID(CollectionID identifier);

    friend std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

    bool defaultCollectionExists{true};
    scopeContainer scopes = {
            {ScopeID::Default, {DefaultScopeName, {{CollectionID::Default}}}}};
    collectionContainer collections = {
            {CollectionID::Default, {ScopeID::Default, DefaultCollectionName}}};
    ManifestUid uid{0};
};

std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

} // end namespace Collections
