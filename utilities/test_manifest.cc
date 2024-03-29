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
#include <iomanip>
#include <iostream>
#include <sstream>

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
    std::stringstream ss;
    ss << std::hex << uint32_t(entry.uid);

    jsonEntry["name"] = entry.name;
    jsonEntry["uid"] = ss.str();
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
        const ScopeEntry::Entry& scopeEntry) {
    updateUid();

    nlohmann::json jsonEntry;
    std::stringstream ss;
    ss << std::hex << uint32_t(collectionEntry.uid);

    jsonEntry["name"] = collectionEntry.name;
    jsonEntry["uid"] = ss.str();

    if (maxTtl) {
        jsonEntry["maxTTL"] = maxTtl.value().count();
    }

    if (collectionEntry.metered) {
        jsonEntry["metered"] = true;
    }

    if (history) {
        jsonEntry["history"] = history.value();
    }

    // Add the new collection to the set of collections belonging to the
    // given scope
    for (auto itr = json["scopes"].begin(); itr != json["scopes"].end();
         itr++) {
        if ((*itr)["name"] == scopeEntry.name) {
            (*itr)["collections"].push_back(jsonEntry);
            break;
        }
    }

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
    updateUid();
    std::stringstream sid;
    sid << std::hex << uint32_t(entry.uid);
    for (auto itr = json["scopes"].begin(); itr != json["scopes"].end();
         itr++) {
        if ((*itr)["name"] == entry.name && (*itr)["uid"] == sid.str()) {
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
    updateUid();

    std::stringstream cid;
    cid << std::hex << uint32_t(collectionEntry.uid);

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
                    (*citr)["uid"] == cid.str()) {
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
    updateUid();
    std::stringstream sidString;
    sidString << std::hex << uint32_t(scopeEntry.uid);
    for (auto& scope : json["scopes"]) {
        if (scope["name"] == scopeEntry.name &&
            scope["uid"] == sidString.str()) {
            scope["name"] = newName;
            return *this;
        }
    }

    throw std::invalid_argument(
            "CollectionsManifest::rename(scope) did not rename "
            "anything");
}

CollectionsManifest& CollectionsManifest::rename(
        const CollectionEntry::Entry& collectionEntry,
        const ScopeEntry::Entry& scopeEntry,
        const std::string& newName) {
    updateUid();
    std::stringstream sidString, cidString;
    sidString << std::hex << uint32_t(scopeEntry.uid);
    cidString << std::hex << uint32_t(collectionEntry.uid);
    for (auto& scope : json["scopes"]) {
        if (scope["name"] == scopeEntry.name &&
            scope["uid"] == sidString.str()) {
            for (auto& collection : scope["collections"]) {
                if (collection["name"] == collectionEntry.name &&
                    collection["uid"] == cidString.str()) {
                    collection["name"] = newName;
                    return *this;
                }
            }
        }
    }
    throw std::invalid_argument(
            "CollectionsManifest::rename(collection) did not rename "
            "anything");
}

bool CollectionsManifest::exists(const CollectionEntry::Entry& collectionEntry,
                                 const ScopeEntry::Entry& scopeEntry) const {
    std::stringstream cid;
    cid << std::hex << uint32_t(collectionEntry.uid);
    std::stringstream sid;
    sid << std::hex << uint32_t(scopeEntry.uid);

    for (auto itr = json["scopes"].begin(); itr != json["scopes"].end();
         itr++) {
        if ((*itr)["name"] == scopeEntry.name && (*itr)["uid"] == sid.str()) {
            for (auto citr = (*itr)["collections"].begin();
                 citr != (*itr)["collections"].end();
                 citr++) {
                if ((*citr)["name"] == collectionEntry.name &&
                    (*citr)["uid"] == cid.str()) {
                    return true;
                }
            }
            break;
        }
    }
    return false;
}

bool CollectionsManifest::exists(const ScopeEntry::Entry& scopeEntry) const {
    std::stringstream sid;
    sid << std::hex << uint32_t(scopeEntry.uid);
    for (auto itr = json["scopes"].begin(); itr != json["scopes"].end();
         itr++) {
        if ((*itr)["name"] == scopeEntry.name && (*itr)["uid"] == sid.str()) {
            return true;
        }
    }
    return false;
}

void CollectionsManifest::updateUid() {
    uid++;

    std::stringstream ss;
    ss << std::hex << uid;
    json["uid"] = ss.str();
}

void CollectionsManifest::updateUid(uint64_t uid) {
    this->uid = uid;

    std::stringstream ss;
    ss << std::hex << uid;
    json["uid"] = ss.str();
}

void CollectionsManifest::setForce(bool force) {
    json["force"] = force;
}

std::string CollectionsManifest::to_json() const {
    return json.dump();
}

void CollectionsManifest::setUid(const std::string& uid) {
    this->uid = strtoull(uid.c_str(), nullptr, 16);
    updateUid();
}
