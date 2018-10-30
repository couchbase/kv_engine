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

#include <nlohmann/json.hpp>
#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>

#include "collections/collections_types.h"
#include "memcached/engine_common.h"

namespace Collections {

// strings used in JSON parsing
static constexpr char const* ScopesKey = "scopes";
static constexpr nlohmann::json::value_t ScopesType =
        nlohmann::json::value_t::array;
static constexpr char const* CollectionsKey = "collections";
static constexpr nlohmann::json::value_t CollectionsType =
        nlohmann::json::value_t::array;
static constexpr char const* NameKey = "name";
static constexpr nlohmann::json::value_t NameType =
        nlohmann::json::value_t::string;
static constexpr char const* UidKey = "uid";
static constexpr nlohmann::json::value_t UidType =
        nlohmann::json::value_t::string;
static constexpr char const* MaxTtlKey = "max_ttl";
static constexpr nlohmann::json::value_t MaxTtlType =
        nlohmann::json::value_t::number_unsigned;
static const size_t MaxCollectionNameSize = 30;

struct CollectionEntry {
    CollectionID id;
    cb::ExpiryLimit maxTtl;
};

struct Scope {
    std::string name;
    std::vector<CollectionEntry> collections;
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
    /*
     * Create a manifest from json.
     * Validates the json as per SET_COLLECTIONS rules.
     * @param json a buffer containing the JSON manifest data
     * @param maxNumberOfScopes an upper limit on the number of scopes allowed
     *        defaults to 100.
     * @param maxNumberOfCollections an upper limit on the number of collections
     *        allowed, defaults to 1000.
     */
    Manifest(cb::const_char_buffer json,
             size_t maxNumberOfScopes = 100,
             size_t maxNumberOfCollections = 1000);

    Manifest(const std::string& json,
             size_t maxNumberOfScopes = 100,
             size_t maxNumberOfCollections = 1000)
        : Manifest(cb::const_char_buffer{json},
                   maxNumberOfScopes,
                   maxNumberOfCollections) {
    }

    bool doesDefaultCollectionExist() const {
        return defaultCollectionExists;
    }

    // This manifest object stores UID to Scope mappings
    using scopeContainer = std::unordered_map<ScopeID, Scope>;

    // This manifest object stores CID to name mappings for collections
    using collectionContainer = std::unordered_map<CollectionID, std::string>;

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
            const std::string& scopeName =
                    cb::to_string(DefaultScopeIdentifier)) const {
        for (auto& scope : scopes) {
            if (scope.second.name == scopeName) {
                for (auto& scopeCollection : scope.second.collections) {
                    auto collection = collections.find(scopeCollection.id);

                    if (collection != collections.end() &&
                        collection->second == collectionName) {
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
     * @returns this manifest as a std::string (JSON formatted)
     */
    std::string toJson() const;

    void addStats(const void* cookie, ADD_STAT add_stat) const;

    /**
     * Write to std::cerr this
     */
    void dump() const;

private:
    /**
     * Set defaultCollectionExists to true if identifier matches
     * CollectionID::Default
     * @param identifier ID to check
     */
    void enableDefaultCollection(CollectionID identifier);

    /**
     * Get json sub-object from the json object for key and check the type.
     * @param json The parent object in which to find key.
     * @param key The key to look for.
     * @param expectedType The type the found object must be.
     * @return A json object for key.
     * @throws std::invalid_argument if key is not found or the wrong type.
     */
    nlohmann::json getJsonObject(const nlohmann::json& object,
                                 const std::string& key,
                                 nlohmann::json::value_t expectedType);

    /**
     * Constructor helper function, throws invalid_argument with a string
     * indicating if the expectedType.
     *
     * @param errorKey the JSON key being looked up
     * @param object object to check
     * @param expectedType the type we expect object to be
     * @throws std::invalid_argument if !expectedType
     */
    static void throwIfWrongType(const std::string& errorKey,
                                 const nlohmann::json& object,
                                 nlohmann::json::value_t expectedType);

    /**
     * Check if the std::string represents a legal collection name.
     * Current validation is to ensure we block creation of _ prefixed
     * collections and only accept $default for $ prefixed names.
     *
     * @param name a std::string representing a collection or scope name.
     */
    static bool validName(const std::string& name);

    /**
     * Check if the CollectionID is invalid for a Manifest
     */
    static bool invalidCollectionID(CollectionID identifier);

    friend std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

    bool defaultCollectionExists;
    scopeContainer scopes;
    collectionContainer collections;
    ManifestUid uid;
};

std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

} // end namespace Collections
