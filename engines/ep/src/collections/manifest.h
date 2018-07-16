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

namespace Collections {

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
     * @param maxNumberOfCollections an upper limit on the number of collections
     *        allowed, defaults to 1000.
     */
    Manifest(cb::const_char_buffer json, size_t maxNumberOfCollections = 1000);

    Manifest(const std::string& json, size_t maxNumberOfCollections = 1000)
        : Manifest(cb::const_char_buffer{json}, maxNumberOfCollections) {
    }

    bool doesDefaultCollectionExist() const {
        return defaultCollectionExists;
    }

    // This manifest object stores CID to name mappings
    using container = std::unordered_map<CollectionID, std::string>;

    container::const_iterator begin() const {
        return collections.begin();
    }

    container::const_iterator end() const {
        return collections.end();
    }

    size_t size() const {
        return collections.size();
    }

    /// @return the unique ID of the Manifest which constructed this
    uid_t getUid() const {
        return uid;
    }

    /**
     * @return iterator to the matching entry or end() if not found.
     */
    container::const_iterator find(CollectionID cid) const {
        return collections.find(cid);
    }

    /**
     * @returns this manifest as a std::string (JSON formatted)
     */
    std::string toJson() const;

    /**
     * Write to std::cerr this
     */
    void dump() const;

private:
    /**
     * Set defaultCollectionExists to true if identifier matches
     * CollectionID::DefaultCollection
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
     * @param collection a std::string representing a collection name.
     */
    static bool validCollection(const std::string& collection);

    /**
     * Check if the identifier is valid
     */
    static bool validUid(CollectionID identifier);

    friend std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

    bool defaultCollectionExists;
    container collections;
    uid_t uid;

    // strings used in JSON parsing
    static constexpr char const* CollectionsKey = "collections";
    static constexpr nlohmann::json::value_t CollectionsType =
            nlohmann::json::value_t::array;
    static constexpr char const* CollectionNameKey = "name";
    static constexpr nlohmann::json::value_t CollectionNameType =
            nlohmann::json::value_t::string;
    static constexpr char const* CollectionUidKey = "uid";
    static constexpr nlohmann::json::value_t CollectionUidType =
            nlohmann::json::value_t::string;
};

std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

} // end namespace Collections
