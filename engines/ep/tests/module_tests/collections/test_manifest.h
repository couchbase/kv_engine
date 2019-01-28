/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

/**
 * Predefined data to help build collections tests
 *
 * CollectionID::abc abstracts how we identify abc, by name or by ID?
 * CollectionManifest provides a way to manage a JSON manifest without having
 * to work with strings directly.
 *
 */

#pragma once

#include "collections/collections_types.h"

#include <memcached/dockey.h>
#include <nlohmann/json.hpp>

// For building CollectionEntry we need a name
namespace CollectionName {
constexpr char defaultC[] = "_default";
constexpr char meat[] = "meat";
constexpr char fruit[] = "fruit";
constexpr char vegetable[] = "vegetable";
constexpr char vegetable2[] = "vegetable";
constexpr char dairy[] = "dairy";
constexpr char dairy2[] = "dairy";
} // namespace CollectionName

// For building CollectionEntry we need a UID
namespace CollectionUid {
const CollectionID defaultC = 0x0;
const CollectionID meat = 0x8;
const CollectionID fruit = 0x9;
const CollectionID vegetable = 0xa;
const CollectionID vegetable2 = 0xb;
const CollectionID dairy = 0xc;
const CollectionID dairy2 = 0xd;
} // namespace CollectionUid

namespace CollectionEntry {
struct Entry {
    std::string name;
    CollectionID uid;

    // Define the ID of a collection to be its CollectionID
    CollectionID getId() const {
        return uid;
    }

    operator CollectionID() const {
        return uid;
    }
};

#define Entry_(name) \
    static Entry name = {CollectionName::name, CollectionUid::name}
Entry_(defaultC);
Entry_(meat);
Entry_(fruit);
Entry_(vegetable);
Entry_(vegetable2);
Entry_(dairy);
Entry_(dairy2);

#undef Entry_
} // namespace CollectionEntry

// For build ScopeEntry we need a name
namespace ScopeName {
constexpr char defaultS[] = "_default";
constexpr char shop1[] = "supermarket";
constexpr char shop2[] = "minimart";
}

// For building ScopeEntry we need a UID
namespace ScopeUid {
const ScopeID defaultS = 0;
const ScopeID shop1 = 8;
const ScopeID shop2 = 9;
} // namespace ScopeUid

namespace ScopeEntry {
struct Entry {
    std::string name;
    ScopeID uid;
    std::vector<CollectionEntry::Entry> collections;

    std::vector<CollectionEntry::Entry> getCollections() const {
        return collections;
    }

    // Define the ID of a scope to be its ScopeID
    ScopeID getId() const {
        return uid;
    }

    operator ScopeID() const {
        return uid;
    }
};

#define Entry_(name)                      \
    static Entry name = {ScopeName::name, \
                         ScopeUid::name,  \
                         std::vector<CollectionEntry::Entry>{}}
Entry_(defaultS);
Entry_(shop1);
Entry_(shop2);
#undef Entry_
} // namespace ScopeEntry

struct NoDefault {};

/**
 * Lightweight JSON wrapper to abstract the creation and manipulation of the
 * JSON which we use to manage the collection state.
 */
class CollectionsManifest {
public:
    /// construct with default only
    CollectionsManifest();

    /// construct with no collections
    CollectionsManifest(NoDefault);

    /// construct with default and one other
    CollectionsManifest(const CollectionEntry::Entry& entry);

    /// Add the scope entry - allows duplicates
    CollectionsManifest& add(const ScopeEntry::Entry& entry);

    /// Add the collection entry to the given scope - allows duplicates
    /// caller specifies the collection max_ttl
    /// Adds the collection to the default scope if not are specified
    CollectionsManifest& add(
            const CollectionEntry::Entry& collectionEntry,
            cb::ExpiryLimit maxTtl,
            const ScopeEntry::Entry& scopeEntry = ScopeEntry::defaultS);

    /// Add the collection entry to the given scope - allows duplicates
    /// Adds the collection to the default scope if not are specified
    CollectionsManifest& add(
            const CollectionEntry::Entry& collectionEntry,
            const ScopeEntry::Entry& scopeEntry = ScopeEntry::defaultS);

    /// Remove the entry if found (the first found entry is removed)
    CollectionsManifest& remove(const ScopeEntry::Entry& scopeEntry);

    /// Remove the entry if found (the first found entry is removed)
    CollectionsManifest& remove(
            const CollectionEntry::Entry& collectionEntry,
            const ScopeEntry::Entry& scopeEntry = ScopeEntry::defaultS);

    /// Return the manifest UID
    Collections::ManifestUid getUid() const {
        return uid;
    }

    /// Set the uid, useful for tests which may want to assert uid values
    void setUid(Collections::ManifestUid uid) {
        this->uid = uid;
    }

    void setUid(const std::string& uid);

    /// Most interfaces require std::string manifest
    operator std::string() const {
        return toJson();
    }

    // Convert the manifest to a vector of objects
    // primarily for CollectionsKVStore tests
    std::vector<Collections::CollectionMetaData> getCreateEventVector() const;
    std::vector<ScopeID> getScopeIdVector() const;

private:
    void updateUid();
    std::string toJson() const;
    nlohmann::json json;
    Collections::ManifestUid uid = 0;
};
