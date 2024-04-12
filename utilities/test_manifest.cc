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

#include "test_manifest.h"

#include <memcached/dockey.h>

#include <nlohmann/json.hpp>
#include <spdlog/fmt/fmt.h>
#include <iomanip>
#include <iostream>

CollectionsManifest::CollectionsManifest() {
    add(ScopeEntry::defaultS);
    add(CollectionEntry::defaultC);
    updateUid(0);
}

CollectionsManifest::CollectionsManifest(NoDefault) {
    add(ScopeEntry::defaultS);
}

CollectionsManifest::CollectionsManifest(const CollectionEntry::Entry& entry)
    : CollectionsManifest() {
    add(entry);
}

CollectionsManifest& CollectionsManifest::add(const ScopeEntry::Entry& entry,
                                              std::optional<size_t> dataLimit) {
    updateUid();

    nlohmann::json jsonEntry;
    jsonEntry["name"] = entry.name;
    jsonEntry["uid"] = entry.uid.to_string(false);
    jsonEntry["collections"] = std::vector<nlohmann::json>();

    if (dataLimit) {
        nlohmann::json limitObject;
        limitObject["kv"]["data_size"] = dataLimit.value();
        jsonEntry["limits"] = limitObject;
    }

    json["scopes"].push_back(jsonEntry);

    return *this;
}

CollectionsManifest& CollectionsManifest::add(
        const CollectionEntry::Entry& collectionEntry,
        cb::ExpiryLimit maxTtl,
        std::optional<bool> history,
        const ScopeEntry::Entry& scopeEntry,
        std::optional<uint64_t> flushUid) {
    nlohmann::json jsonEntry;
    jsonEntry["name"] = collectionEntry.name;
    jsonEntry["uid"] = collectionEntry.uid.to_string(false);

    if (maxTtl) {
        jsonEntry["maxTTL"] = maxTtl.value().count();
    }

    if (collectionEntry.metered) {
        jsonEntry["metered"] = true;
    }

    if (history) {
        jsonEntry["history"] = history.value();
    }

    if (flushUid) {
        jsonEntry["flush_uid"] = fmt::format("{:x}", flushUid.value());
    }

    // Add the new collection to the set of collections belonging to the
    // given scope
    auto scope = findScope(scopeEntry);
    if (!scope) {
        throw std::invalid_argument(
                "CollectionsManifest::add(collection) failed to find scope");
    }

    updateUid();
    (*scope)["collections"].push_back(jsonEntry);
    return *this;
}

CollectionsManifest& CollectionsManifest::update(
        const CollectionEntry::Entry& collectionEntry,
        cb::ExpiryLimit maxTtl,
        std::optional<bool> history,
        const ScopeEntry::Entry& scopeEntry) {
    remove(collectionEntry, scopeEntry);
    add(collectionEntry, maxTtl, history, scopeEntry);
    return *this;
}

CollectionsManifest& CollectionsManifest::add(
        const CollectionEntry::Entry& collectionEntry,
        const ScopeEntry::Entry& scopeEntry) {
    return add(collectionEntry,
               {/*no ttl*/},
               {/* no history setting defined*/},
               scopeEntry);
}

CollectionsManifest& CollectionsManifest::remove(
        const ScopeEntry::Entry& entry) {
    const auto sid = entry.uid.to_string(false);
    for (auto itr = json["scopes"].begin(); itr != json["scopes"].end();
         itr++) {
        if ((*itr)["name"] == entry.name && (*itr)["uid"] == sid) {
            updateUid();
            json["scopes"].erase(itr);
            return *this;
        }
    }

    throw std::invalid_argument(
            "CollectionsManifest::remove(scope) did not remove anything");
}

CollectionsManifest& CollectionsManifest::remove(
        const CollectionEntry::Entry& collectionEntry,
        const ScopeEntry::Entry& scopeEntry) {
    const auto cid = collectionEntry.uid.to_string(false);

    // Iterate on all scopes, find the one matching the passed scopeEntry
    for (auto itr = json["scopes"].begin(); itr != json["scopes"].end();
         itr++) {
        if ((*itr)["name"] == scopeEntry.name) {
            // Iterate on all collections within the scope, find the one
            // matching the passed collectionEntry
            for (auto citr = (*itr)["collections"].begin();
                 citr != (*itr)["collections"].end();
                 citr++) {
                if ((*citr)["name"] == collectionEntry.name &&
                    (*citr)["uid"] == cid) {
                    updateUid();
                    (*itr)["collections"].erase(citr);
                    return *this;
                }
            }
            break;
        }
    }

    throw std::invalid_argument(
            "CollectionsManifest::remove(collection) did not remove "
            "anything");
}

CollectionsManifest& CollectionsManifest::rename(
        const ScopeEntry::Entry& scopeEntry, const std::string& newName) {
    auto* scope = findScope(scopeEntry);
    if (scope) {
        updateUid();
        (*scope)["name"] = newName;
        return *this;
    }

    throw std::invalid_argument(
            "CollectionsManifest::rename(scope) did not rename "
            "anything");
}

CollectionsManifest& CollectionsManifest::rename(
        const CollectionEntry::Entry& collectionEntry,
        const ScopeEntry::Entry& scopeEntry,
        const std::string& newName) {
    auto* scope = findScope(scopeEntry);
    if (scope) {
        auto* collection = findCollection(collectionEntry, *scope);
        if (collection) {
            updateUid();
            (*collection)["name"] = newName;
            return *this;
        }
    }

    throw std::invalid_argument(
            "CollectionsManifest::rename(collection) did not rename "
            "anything");
}

CollectionsManifest& CollectionsManifest::flush(
        const CollectionEntry::Entry& collectionEntry,
        const ScopeEntry::Entry& scopeEntry) {
    auto scope = findScope(scopeEntry);
    if (scope) {
        auto* collection = findCollection(collectionEntry, *scope);
        if (collection) {
            updateUid();
            (*collection)["flush_uid"] = json["uid"];
            return *this;
        }
    }
    throw std::invalid_argument(
            "CollectionsManifest::flush(collection) did not update "
            "anything");
}

bool CollectionsManifest::exists(const CollectionEntry::Entry& collectionEntry,
                                 const ScopeEntry::Entry& scopeEntry) const {
    auto scope = findScope(scopeEntry);
    if (scope && findCollection(collectionEntry, scope.value()).has_value()) {
        return true;
    }
    return false;
}

bool CollectionsManifest::exists(const ScopeEntry::Entry& scopeEntry) const {
    return findScope(scopeEntry).has_value();
}

void CollectionsManifest::updateUid() {
    updateUid(uid + 1);
}

void CollectionsManifest::updateUid(uint64_t uid) {
    this->uid = uid;
    json["uid"] = fmt::format("{:x}", uid);
}

std::string CollectionsManifest::to_json() const {
    return json.dump();
}

void CollectionsManifest::setUid(const std::string& uid) {
    this->uid = strtoull(uid.c_str(), nullptr, 16);
    updateUid();
}

std::optional<nlohmann::json> CollectionsManifest::findScope(
        const ScopeEntry::Entry& scopeEntry) const {
    const auto sid = scopeEntry.uid.to_string(false);
    for (auto& scope : json["scopes"]) {
        if (scope["name"] == scopeEntry.name && scope["uid"] == sid) {
            return scope;
        }
    }
    return std::nullopt;
}

std::optional<nlohmann::json> CollectionsManifest::findCollection(
        const CollectionEntry::Entry& collectionEntry,
        const nlohmann::json& scopes) const {
    const auto cid = collectionEntry.uid.to_string(false);
    for (const auto& entry : scopes["collections"]) {
        if (entry["name"] == collectionEntry.name && entry["uid"] == cid) {
            return entry;
        }
    }
    return std::nullopt;
}

nlohmann::json* CollectionsManifest::findScope(
        const ScopeEntry::Entry& scopeEntry) {
    const auto sid = scopeEntry.uid.to_string(false);
    for (auto& entry : json["scopes"]) {
        if (entry["name"] == scopeEntry.name && entry["uid"] == sid) {
            return &entry;
        }
    }
    return nullptr;
}

nlohmann::json* CollectionsManifest::findCollection(
        const CollectionEntry::Entry& collectionEntry, nlohmann::json& scopes) {
    const auto cid = collectionEntry.uid.to_string(false);
    for (auto& entry : scopes["collections"]) {
        if (entry["name"] == collectionEntry.name && entry["uid"] == cid) {
            return &entry;
        }
    }
    return nullptr;
}
