/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <nlohmann/json_fwd.hpp>
#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>

#include "collections/collections_types.h"
#include "memcached/engine_common.h"
#include "memcached/engine_error.h"

class KVBucket;
class BucketStatCollector;
class StatCollector;

namespace flatbuffers {
class DetachedBuffer;
}

namespace Collections {

static const size_t MaxScopeOrCollectionNameSize = 251;

struct CollectionEntry {
    CollectionID cid;
    std::string name;
    cb::ExpiryLimit maxTtl;
    ScopeID sid;
    bool metered;
    bool operator==(const CollectionEntry& other) const;
    bool operator!=(const CollectionEntry& other) const {
        return !(*this == other);
    }
};

struct Scope {
    /**
     * Store the dataLimit we will use (which is the clusters value / nVbuckets)
     * This is so we don't need to re-calculate it everytime we read it.
     */
    DataLimit dataLimit;

    /**
     * Store the 'pristine' value from the cluster manager - currently used so
     * cbstats can just display the pristine value for scope stats and the
     * get manifest command.
     */
    size_t dataLimitFromCluster{0};

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
    /**
     * Constructs an epoch manifest
     * Default scope, default collection and uid of 0
     */
    Manifest();

    /*
     * Create a manifest from json.
     * Validates the json as per SET_COLLECTIONS rules.
     * @param json a buffer containing the JSON manifest data
     */
    explicit Manifest(std::string_view json, size_t numVbuckets = 1024);

    struct FlatBuffers {};
    explicit Manifest(std::string_view flatbufferData, FlatBuffers tag);

    Manifest(Manifest&&);
    /**
     * Assignment operator that is aware of a forced assign (so uid can go back)
     */
    Manifest& operator=(Manifest&& other);

    bool doesDefaultCollectionExist() const {
        return defaultCollectionExists;
    }

    // This manifest object stores UID to Scope mappings
    using scopeContainer = std::unordered_map<ScopeID, Scope>;

    // And a map from cid to the CollectionEntry
    using collectionContainer =
            std::unordered_map<CollectionID, CollectionEntry&>;

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

    size_t getCollectionCount() const {
        return collections.size();
    }

    /**
     * Is the Manifest 'successor' a valid successor of this Manifest?
     * If the successor is not a forced update, it must be 'sane' progression
     * of the Manifest, for example the manifest uid must be incrementing and
     * immutable properties of scopes/collections must remain that way
     */
    cb::engine_error isSuccessor(const Manifest& successor) const;

    /**
     * Is this the epoch state of collections?
     * uid 0, default collection and default scope only
     */
    bool isEpoch() const;

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
            // Look for the correct scope
            if (scope.second.name == scopeName) {
                for (auto& scopeCollection : scope.second.collections) {
                    auto collection = collections.find(scopeCollection.cid);

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
     * Get the data limit for the scope that is to be used for vbucket limit
     * This is the value ns_server gave us divided by the number of vbuckets
     */
    DataLimit getScopeDataLimit(ScopeID sid) const;

    /**
     * @returns this manifest as nlohmann::json object
     */
    nlohmann::json toJson(
            const Collections::IsVisibleFunction& isVisible) const;

    /**
     * @return flatbuffer representation of this object
     */
    flatbuffers::DetachedBuffer toFlatbuffer() const;

    /**
     * Add stats for collection. Each collection is tested for
     * Privilege::SimpleStats and 'added' if the user has the privilege.
     * @param bucket The bucket so we can call engine::testPrivilege
     * @param cookie The cookie so for the connection
     * @param function to call to add stats
     */
    void addCollectionStats(KVBucket& bucket,
                            const BucketStatCollector& collector) const;
    /**
     * Add stats for scopes. Each scope is tested for
     * Privilege::SimpleStats and 'added' if the user has the privilege.
     * @param bucket The bucket so we can call engine::testPrivilege
     * @param cookie The cookie so for the connection
     * @param function to call to add stats
     */
    void addScopeStats(KVBucket& bucket,
                       const BucketStatCollector& collector) const;

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
     * Construction helper - builds the collection map and will detect duplicate
     * cid
     */
    void buildCollectionIdToEntryMap();

    /**
     * Compare this against other and return true if the all but the uid are
     * equal (the uid is not compared).
     */
    bool isEqualContent(const Manifest& other) const;

    /**
     * Parse the optional limits section of a scope object
     * @return The value of 'data_size' if found, the number divided by vbuckets
     *         and the raw value
     */
    std::pair<DataLimit, uint64_t> processLimits(
            std::optional<nlohmann::json> limits, size_t numVbuckets);

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

    /**
     * scopes stores all of the known scopes and the 'epoch' Manifest i.e.
     * default initialisation stores just the default scope.
     */
    scopeContainer scopes = {{ScopeID::Default,
                              {NoDataLimit,
                               0,
                               DefaultScopeName,
                               {{CollectionID::Default,
                                 DefaultCollectionName,
                                 cb::NoExpiryLimit,
                                 ScopeID::Default,
                                 true}}}}};
    collectionContainer collections;
    ManifestUid uid{0};
};

std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

} // end namespace Collections
