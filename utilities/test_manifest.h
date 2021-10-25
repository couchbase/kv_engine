/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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

#include <memcached/dockey.h>
#include <memcached/types.h>
#include <nlohmann/json.hpp>
#include <spdlog/fmt/fmt.h>

// For building CollectionEntry we need a name
namespace CollectionName {
constexpr char defaultC[] = "_default";
constexpr char meat[] = "meat";
constexpr char fruit[] = "fruit";
constexpr char vegetable[] = "vegetable";
constexpr char vegetable2[] = "vegetable";
constexpr char dairy[] = "dairy";
constexpr char dairy2[] = "dairy";
constexpr char customer1[] = "customer_collection1";
constexpr char maxCollection[] =
        "collectionaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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
const CollectionID customer1 = 0xb;
const CollectionID maxCollection = 0xe;
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
Entry_(customer1);
Entry_(maxCollection);

#undef Entry_
} // namespace CollectionEntry

// For build ScopeEntry we need a name
namespace ScopeName {
constexpr char defaultS[] = "_default";
constexpr char shop1[] = "supermarket";
constexpr char shop2[] = "minimart";
constexpr char customer[] = "customer_scope";
constexpr char maxScope[] =
        "scopeaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

} // namespace ScopeName

// For building ScopeEntry we need a UID
namespace ScopeUid {
const ScopeID defaultS = 0;
const ScopeID shop1 = 8;
const ScopeID shop2 = 9;
const ScopeID customer = 8;
const ScopeID maxScope = 10;
} // namespace ScopeUid

namespace ScopeEntry {
struct Entry {
    std::string name;
    ScopeID uid;

    // Define the ID of a scope to be its ScopeID
    ScopeID getId() const {
        return uid;
    }

    operator ScopeID() const {
        return uid;
    }
};

#define Entry_(name) static Entry name = {ScopeName::name, ScopeUid::name}
Entry_(defaultS);
Entry_(shop1);
Entry_(shop2);
Entry_(customer);
Entry_(maxScope);
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
    explicit CollectionsManifest(NoDefault);

    /// construct with default and one other
    explicit CollectionsManifest(const CollectionEntry::Entry& entry);

    /// Add the scope entry - allows duplicates
    CollectionsManifest& add(const ScopeEntry::Entry& entry);

    /// Add the scope entry and set a limit
    CollectionsManifest& add(const ScopeEntry::Entry& entry, size_t dataLimit);

    /// Add the collection entry to the given scope - allows duplicates
    /// caller specifies the collection maxTTL
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

    /// Rename the scope
    CollectionsManifest& rename(const ScopeEntry::Entry& scopeEntry,
                                const std::string& newName);

    /// Rename the collection in the given scope
    CollectionsManifest& rename(const CollectionEntry::Entry& collectionEntry,
                                const ScopeEntry::Entry& scopeEntry,
                                const std::string& newName);

    /// @return true if collection exists
    bool exists(
            const CollectionEntry::Entry& collectionEntry,
            const ScopeEntry::Entry& scopeEntry = ScopeEntry::defaultS) const;

    /// @return true if scope exists
    bool exists(const ScopeEntry::Entry& scopeEntry) const;

    /// Return the manifest UID
    uint64_t getUid() const {
        return uid;
    }

    /// Return the manifest UID
    std::string getUidString() const {
        return fmt::format("{0:x}", uid);
    }

    /// Set the uid, useful for tests which may want to assert uid values
    void setUid(uint64_t uid) {
        this->uid = uid;
    }

    void updateUid(uint64_t uid);

    void setUid(const std::string& uid);

    void setForce(bool force);

    /// Most interfaces require std::string manifest
    operator std::string() const {
        return toJson();
    }

    const nlohmann::json& getJson() const {
        return json;
    }

private:
    void updateUid();
    std::string toJson() const;
    nlohmann::json json;
    uint64_t uid = 0;
};
