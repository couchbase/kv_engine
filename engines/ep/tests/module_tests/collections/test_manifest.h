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

/**
 * Predefined data to help build collections tests
 *
 * CollectionID::abc abstracts how we identify abc, by name or by ID?
 * CollectionManifest provides a way to manage a JSON manifest without having
 * to work with strings directly.
 *
 */

#include "collections/collections_types.h"

#include <memcached/dockey.h>
#include <nlohmann/json.hpp>

// For building CollectionEntry we need a name
namespace CollectionName {
constexpr char defaultC[] = "$default";
constexpr char meat[] = "meat";
constexpr char fruit[] = "fruit";
constexpr char vegetable[] = "vegetable";
constexpr char vegetable2[] = "vegetable";
constexpr char dairy[] = "dairy";
constexpr char dairy2[] = "dairy";
} // namespace CollectionName

// For building CollectionEntry we need a UID
namespace CollectionUid {
const CollectionID defaultC = 0;
const CollectionID meat = 2;
const CollectionID fruit = 3;
const CollectionID vegetable = 4;
const CollectionID vegetable2 = 5;
const CollectionID dairy = 6;
const CollectionID dairy2 = 7;
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

    /// Add the entry - allows duplicates
    CollectionsManifest& add(const CollectionEntry::Entry& entry);

    /// Remove the entry if found (the first found entry is removed)
    CollectionsManifest& remove(const CollectionEntry::Entry& entry);

    /// Return the manifest UID
    Collections::uid_t getUid() const {
        return uid;
    }

    /// Set the uid, useful for tests which may want to assert uid values
    void setUid(Collections::uid_t uid) {
        this->uid = uid;
    }

    void setUid(const std::string& uid);

    /// Most interfaces require std::string manifest
    operator std::string() const {
        return toJson();
    }

private:
    void updateUid();
    std::string toJson() const;
    nlohmann::json json;
    Collections::uid_t uid = 0;
};