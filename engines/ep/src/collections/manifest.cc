/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "collections/manifest.h"
#include "bucket_logger.h"
#include "collections/collections_constants.h"
#include "collections/collections_types.h"
#include "collections/manifest_generated.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "utility.h"

#include <json_utilities.h>

#include <gsl/gsl-lite.hpp>
#include <memcached/engine_error.h>
#include <nlohmann/json.hpp>
#include <platform/checked_snprintf.h>
#include <statistics/cbstat_collector.h>
#include <statistics/labelled_collector.h>

#include <cctype>
#include <cstring>
#include <iostream>
#include <sstream>

namespace Collections {

// strings used in JSON parsing
static constexpr char const* ScopesKey = "scopes";
static constexpr nlohmann::json::value_t ScopesType =
        nlohmann::json::value_t::array;
static constexpr char const* CollectionsKey = "collections";
static constexpr char const* NameKey = "name";
static constexpr nlohmann::json::value_t NameType =
        nlohmann::json::value_t::string;
static constexpr char const* UidKey = "uid";
static constexpr char const* MaxTtlKey = "maxTTL";
static constexpr nlohmann::json::value_t MaxTtlType =
        nlohmann::json::value_t::number_unsigned;
static constexpr char const* LimitsKey = "limits";
static constexpr nlohmann::json::value_t LimitsType =
        nlohmann::json::value_t::object;
static constexpr char const* KvKey = "kv";
static constexpr nlohmann::json::value_t KvType =
        nlohmann::json::value_t::object;
static constexpr char const* DataSizeKey = "data_size";
static constexpr nlohmann::json::value_t DataSizeType =
        nlohmann::json::value_t::number_unsigned;

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

Manifest::Manifest() {
    buildCollectionIdToEntryMap();
}

Manifest::Manifest(std::string_view json, size_t numVbuckets)
    : defaultCollectionExists(false), scopes(), collections(), uid(0) {
    auto throwInvalid = [](const std::string& detail) {
        throw std::invalid_argument("Manifest::Manifest: " + detail);
    };

    nlohmann::json parsed;
    try {
        parsed = nlohmann::json::parse(json);
    } catch (const nlohmann::json::exception& e) {
        throwInvalid("nlohmann cannot parse json:" + std::string(json) +
                     ", e:" + e.what());
    }

    // Read the Manifest UID e.g. "uid" : "5fa1"
    auto jsonUid = getJsonObject(parsed, UidKey, UidType);
    uid = makeUid(jsonUid.get<std::string>());

    // Read the scopes within the Manifest
    auto scopes = getJsonObject(parsed, ScopesKey, ScopesType);

    for (const auto& scope : scopes) {
        throwIfWrongType(
                std::string(ScopesKey), scope, nlohmann::json::value_t::object);

        auto name = getJsonObject(scope, NameKey, NameType);
        auto uid = getJsonObject(scope, UidKey, UidType);

        auto nameValue = name.get<std::string>();
        if (!validName(nameValue)) {
            throwInvalid("scope name: " + nameValue + " is not valid.");
        }

        // Construction of ScopeID checks for invalid values
        ScopeID sidValue = makeScopeID(uid.get<std::string>());

        // 1) Default scope has an expected name.
        // 2) Scope identifiers must be unique.
        // 3) Scope names must be unique.
        if (sidValue.isDefaultScope() && nameValue != DefaultScopeIdentifier) {
            throwInvalid("default scope with wrong name:" + nameValue);
        } else if (this->scopes.count(sidValue) > 0) {
            // Scope uids must be unique
            throwInvalid("duplicate scope uid:" + sidValue.to_string() +
                         ", name:" + nameValue);
        }

        // iterate scopes and compare names, fail for a match.
        for (const auto& itr : this->scopes) {
            if (itr.second.name == nameValue) {
                throwInvalid("duplicate scope name:" + sidValue.to_string() +
                             ", name:" + nameValue);
            }
        }

        std::vector<CollectionEntry> scopeCollections = {};

        // Read the collections within this scope
        auto collections =
                getJsonObject(scope, CollectionsKey, CollectionsType);

        for (const auto& collection : collections) {
            throwIfWrongType(std::string(CollectionsKey),
                             collection,
                             nlohmann::json::value_t::object);

            auto cname = getJsonObject(collection, NameKey, NameType);
            auto cuid = getJsonObject(collection, UidKey, UidType);
            auto cmaxttl = cb::getOptionalJsonObject(
                    collection, MaxTtlKey, MaxTtlType);

            auto cnameValue = cname.get<std::string>();
            if (!validName(cnameValue)) {
                throwInvalid("collection name:" + cnameValue + " is not valid");
            }

            CollectionID cidValue = makeCollectionID(cuid.get<std::string>());

            // 1) The default collection must be within the default scope and
            // have the expected name.
            // 2) The constructor of CollectionID checked for invalid values,
            // but we need to check to ensure System (1) wasn't present in the
            // Manifest.
            // 3) Collection identifiers must be unique.
            // 4) Collection names must be unique within the scope.
            if (cidValue.isDefaultCollection()) {
                if (cnameValue != DefaultCollectionIdentifier) {
                    throwInvalid(
                            "the default collection name is unexpected name:" +
                            cnameValue);
                } else if (!sidValue.isDefaultScope()) {
                    throwInvalid(
                            "the default collection is not in the default "
                            "scope");
                }
            } else if (invalidCollectionID(cidValue)) {
                throwInvalid("collection uid: " + cidValue.to_string() +
                             " is not valid.");
            }

            // Collection names must be unique within the scope
            for (const auto& collection : scopeCollections) {
                if (collection.name == cnameValue) {
                    throwInvalid("duplicate collection name:" +
                                 cidValue.to_string() +
                                 ", name: " + cnameValue);
                }
            }

            cb::ExpiryLimit maxTtl;
            if (cmaxttl) {
                // Don't exceed 32-bit max
                auto value = cmaxttl.value().get<uint64_t>();
                if (value > std::numeric_limits<uint32_t>::max()) {
                    throwInvalid("maxTTL:" + std::to_string(value));
                }
                maxTtl = std::chrono::seconds(value);
            }

            enableDefaultCollection(cidValue);
            scopeCollections.push_back(
                    CollectionEntry{cidValue, cnameValue, maxTtl, sidValue});
        }

        // Check for limits - only support for data_size
        auto dataLimit = processLimits(
                cb::getOptionalJsonObject(scope, LimitsKey, LimitsType),
                numVbuckets);

        this->scopes.emplace(sidValue,
                             Scope{dataLimit.first,
                                   dataLimit.second,
                                   nameValue,
                                   std::move(scopeCollections)});
    }

    // Now build the collection id to collection-entry map
    buildCollectionIdToEntryMap();

    // Final checks...
    // uid of 0 -> this must be the 'epoch' state
    // else no scopes is invalid and we must always have default scope
    if (uid == 0 && !isEpoch()) {
        throwInvalid("uid of 0 but not the expected 'epoch' manifest");
    } else if (this->scopes.empty()) {
        throwInvalid("no scopes were defined in the manifest");
    } else if (findScope(ScopeID::Default) == this->scopes.end()) {
        throwInvalid("the default scope was not defined");
    }
}

std::pair<DataLimit, uint64_t> Manifest::processLimits(
        std::optional<nlohmann::json> limits, size_t numVbuckets) {
    if (!limits) {
        return {std::nullopt, 0};
    }
    auto kv = cb::getOptionalJsonObject(*limits, KvKey, KvType);
    if (!kv) {
        return {std::nullopt, 0};
    }

    auto dataSize = cb::getOptionalJsonObject(*kv, DataSizeKey, DataSizeType);
    if (!dataSize) {
        return {std::nullopt, 0};
    }

    // The data_limit from the manifest is bucket total data, so divide it down
    // by how many active vbuckets will exist.
    Expects(numVbuckets > 0);
    auto value = dataSize.value().get<uint64_t>();
    return {value / numVbuckets, value};
}

Manifest::Manifest(Manifest&& other) {
    *this = std::move(other);
}

Manifest& Manifest::operator=(Manifest&& other) {
    if (this != &other) {
        defaultCollectionExists = other.defaultCollectionExists;
        scopes = std::move(other.scopes);
        collections = std::move(other.collections);
            uid = other.uid;
    }

    return *this;
}

void Manifest::buildCollectionIdToEntryMap() {
    for (auto& scope : this->scopes) {
        for (auto& collection : scope.second.collections) {
            auto [itr, emplaced] =
                    collections.try_emplace(collection.cid, collection);
            if (!emplaced) {
                // Only 1 of each ID is allowed!
                throw std::invalid_argument(
                        "Manifest::buildCollectionIdToEntryMap: duplicate "
                        "collection uid:" +
                        collection.cid.to_string() + ", name1:" +
                        itr->second.name + ", name2:" + collection.name);
            }
        }
    }
}

nlohmann::json getJsonObject(const nlohmann::json& object,
                             const std::string& key,
                             nlohmann::json::value_t expectedType) {
    return cb::getJsonObject(object, key, expectedType, "Manifest");
}

void throwIfWrongType(const std::string& errorKey,
                      const nlohmann::json& object,
                      nlohmann::json::value_t expectedType) {
    cb::throwIfWrongType(errorKey, object, expectedType, "Manifest");
}

void Manifest::enableDefaultCollection(CollectionID identifier) {
    if (identifier == CollectionID::Default) {
        defaultCollectionExists = true;
    }
}

bool Manifest::validName(std::string_view name) {
    // $ prefix is currently reserved for future use
    // Name cannot be empty
    if (name.empty() || name.size() > MaxScopeOrCollectionNameSize ||
        name[0] == '$') {
        return false;
    }
    // Check rest of the characters for validity
    for (const auto& c : name) {
        // Collection names are allowed to contain
        // A-Z, a-z, 0-9 and _ - % $
        // system collections are _ prefixed, but not enforced here
        if (!(std::isdigit(c) || std::isalpha(c) || c == '_' || c == '-' ||
              c == '%' || c == '$')) {
            return false;
        }
    }
    return true;
}

bool Manifest::invalidCollectionID(CollectionID identifier) {
    // System cannot appear in a manifest
    return identifier == CollectionID::System;
}

nlohmann::json Manifest::toJson(
        const Collections::IsVisibleFunction& isVisible) const {
    nlohmann::json manifest;
    manifest["uid"] = fmt::format("{0:x}", uid);
    manifest["scopes"] = nlohmann::json::array();

    // scope check is correct to see an empty scope
    // collection check is correct as well, if you have no visible collections
    // and no access to the scope - no scope

    for (const auto& s : scopes) {
        nlohmann::json scope;
        scope["collections"] = nlohmann::json::array();
        bool visible = isVisible(s.first, {});
        for (const auto& c : s.second.collections) {
            // Include if the collection is visible
            if (isVisible(s.first, c.cid)) {
                nlohmann::json collection;
                collection["name"] = c.name;
                collection["uid"] = fmt::format("{0:x}", uint32_t{c.cid});
                if (c.maxTtl) {
                    collection["maxTTL"] = c.maxTtl.value().count();
                }
                scope["collections"].push_back(collection);
            }
        }
        if (!scope["collections"].empty() || visible) {
            scope["name"] = s.second.name;
            scope["uid"] = fmt::format("{0:x}", uint32_t(s.first));
            if (s.second.dataLimit) {
                nlohmann::json jsonDataLimit;
                jsonDataLimit["kv"]["data_size"] =
                        s.second.dataLimitFromCluster;
                scope["limits"] = jsonDataLimit;
            }
            manifest["scopes"].push_back(scope);
        }
    }
    return manifest;
}

flatbuffers::DetachedBuffer Manifest::toFlatbuffer() const {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<Collections::Persist::Scope>> fbScopes;

    for (const auto& scope : scopes) {
        std::vector<flatbuffers::Offset<Collections::Persist::Collection>>
                fbCollections;

        for (const auto& c : scope.second.collections) {
            auto newEntry = Collections::Persist::CreateCollection(
                    builder,
                    uint32_t(c.cid),
                    c.maxTtl.has_value(),
                    c.maxTtl.value_or(std::chrono::seconds(0)).count(),
                    builder.CreateString(c.name));
            fbCollections.push_back(newEntry);
        }
        auto collectionVector = builder.CreateVector(fbCollections);

        if (scope.second.dataLimit) {
            auto limits = Collections::Persist::CreateScopeLimits(
                    builder,
                    true,
                    scope.second.dataLimit.value(),
                    scope.second.dataLimitFromCluster);
            auto newEntry = Collections::Persist::CreateScope(
                    builder,
                    uint32_t(scope.first),
                    builder.CreateString(scope.second.name),
                    collectionVector,
                    limits);
            fbScopes.push_back(newEntry);
        } else {
            auto newEntry = Collections::Persist::CreateScope(
                    builder,
                    uint32_t(scope.first),
                    builder.CreateString(scope.second.name),
                    collectionVector);
            fbScopes.push_back(newEntry);
        }
    }

    auto scopeVector = builder.CreateVector(fbScopes);
    auto toWrite = Collections::Persist::CreateManifest(
            builder, uid, false, scopeVector);
    builder.Finish(toWrite);
    return builder.Release();
}

// sibling of toFlatbuffer, construct a Manifest from a flatbuffer format
Manifest::Manifest(std::string_view flatbufferData, Manifest::FlatBuffers tag)
    : defaultCollectionExists(false), scopes(), uid(0) {
    flatbuffers::Verifier v(
            reinterpret_cast<const uint8_t*>(flatbufferData.data()),
            flatbufferData.size());
    if (!v.VerifyBuffer<Collections::Persist::Manifest>(nullptr)) {
        std::stringstream ss;
        ss << "Collections::Manifest::Manifest(FlatBuffers): flatbufferData "
              "invalid, ptr:"
           << reinterpret_cast<const void*>(flatbufferData.data())
           << ", size:" << flatbufferData.size();

        throw std::invalid_argument(ss.str());
    }

    auto manifest = flatbuffers::GetRoot<Collections::Persist::Manifest>(
            reinterpret_cast<const uint8_t*>(flatbufferData.data()));

    uid = manifest->uid();

    for (const Collections::Persist::Scope* scope : *manifest->scopes()) {
        std::vector<CollectionEntry> scopeCollections;

        for (const Collections::Persist::Collection* collection :
             *scope->collections()) {
            cb::ExpiryLimit maxTtl;
            CollectionID cid(collection->collectionId());
            if (collection->ttlValid()) {
                maxTtl = std::chrono::seconds(collection->maxTtl());
            }

            enableDefaultCollection(cid);
            scopeCollections.push_back(CollectionEntry{
                    cid, collection->name()->str(), maxTtl, scope->scopeId()});
        }

        std::optional<size_t> dataSize;
        uint64_t pristineValue = 0;

        if (scope->limits() && scope->limits()->dataSizeEnabled()) {
            dataSize = scope->limits()->dataSize();
            pristineValue = scope->limits()->dataSizeClusterValue();
        }

        this->scopes.emplace(scope->scopeId(),
                             Scope{dataSize,
                                   pristineValue,
                                   scope->name()->str(),
                                   std::move(scopeCollections)});
    }

    // Now build the collection id to collection-entry map
    buildCollectionIdToEntryMap();
}

void Manifest::addCollectionStats(KVBucket& bucket,
                                  const BucketStatCollector& collector) const {
    try {
        using namespace cb::stats;
        // manifest_uid is always permitted (e.g. get_collections_manifest
        // exposes this too). It reveals nothing about scopes or collections but
        // is useful for assisting in access failures
        collector.addStat(Key::manifest_uid, uid);
        for (const auto& scope : scopes) {
            std::string_view scopeName = scope.second.name;
            auto scopeC = collector.forScope(scopeName, scope.first);
            for (const auto& entry : scope.second.collections) {
                auto collectionC = scopeC.forCollection(entry.name, entry.cid);
                // The inclusion of each collection requires an appropriate
                // privilege
                if (collectionC.testPrivilegeForStat(scope.first, entry.cid) !=
                    cb::engine_errc::success) {
                    continue; // skip this collection
                }

                collectionC.addStat(Key::collection_name, entry.name);

                if (entry.maxTtl) {
                    collectionC.addStat(Key::collection_maxTTL, entry.maxTtl->count());
                }
            }
        }
    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "Manifest::addCollectionStats failed to build stats "
                "exception:{}",
                e.what());
    }
}

// Generates the stats for the 'scopes' key
void Manifest::addScopeStats(KVBucket& bucket,
                             const BucketStatCollector& collector) const {
    try {
        using namespace cb::stats;
        // manifest_uid is always permitted (e.g. get_collections_manifest
        // exposes this too). It reveals nothing about scopes or collections but
        // is useful for assisting in access failures
        collector.addStat(Key::manifest_uid, uid);

        for (const auto& entry : scopes) {
            std::string_view scopeName = entry.second.name;
            auto scopeC = collector.forScope(scopeName, entry.first);
            // The inclusion of each scope requires an appropriate
            // privilege
            if (scopeC.testPrivilegeForStat(entry.first, {}) !=
                cb::engine_errc::success) {
                continue; // skip this scope
            }
            const auto name = entry.second.name;

            scopeC.addStat(Key::scope_name, name);
            scopeC.addStat(Key::scope_collection_count,
                           entry.second.collections.size());

            if (entry.second.dataLimit) {
                scopeC.addStat(Key::scope_data_limit,
                               entry.second.dataLimitFromCluster);
            }

            // add each collection name and id
            for (const auto& colEntry : entry.second.collections) {
                auto collectionC =
                        scopeC.forCollection(colEntry.name, colEntry.cid);
                collectionC.addStat(Key::collection_name, colEntry.name);
            }
        }
    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "Manifest::addScopeStats failed to build stats "
                "exception:{}",
                e.what());
    }
}

std::optional<CollectionID> Manifest::getCollectionID(
        ScopeID scope, std::string_view path) const {
    int pos = path.find_first_of('.');
    auto collection = path.substr(pos + 1);

    // Empty collection part of the path means default collection.
    if (collection.empty()) {
        collection = DefaultCollectionIdentifier;
    }

    if (!validName(collection)) {
        throw cb::engine_error(cb::engine_errc::invalid_arguments,
                               "Manifest::getCollectionID invalid collection:" +
                                       std::string(collection));
    }

    auto scopeItr = scopes.find(scope);
    if (scopeItr == scopes.end()) {
        // Assumption is that a valid scope will be given because it was looked
        // up first via getScopeId(path) - so it is invalid to give a bad scope.
        throw std::invalid_argument(
                "Manifest::getCollectionID given unknown scope:" +
                scope.to_string());
    }
    for (const auto& c : scopeItr->second.collections) {
        if (c.name == collection) {
            return c.cid;
        }
    }

    return {};
}

std::optional<ScopeID> Manifest::getScopeID(std::string_view path) const {
    int pos = path.find_first_of('.');
    auto scope = path.substr(0, pos);

    // Empty scope part of the path means default scope.
    if (scope.empty()) {
        scope = DefaultScopeIdentifier;
    }

    if (!(validName(scope))) {
        throw cb::engine_error(
                cb::engine_errc::invalid_arguments,
                "Manifest::getScopeID invalid scope:" + std::string(scope));
    }

    for (const auto& s : scopes) {
        if (s.second.name == scope) {
            return s.first;
        }
    }

    return {};
}

std::optional<ScopeID> Manifest::getScopeID(const DocKey& key) const {
    return getScopeID(key.getCollectionID());
}

std::optional<ScopeID> Manifest::getScopeID(CollectionID cid) const {
    if (cid.isDefaultCollection() && defaultCollectionExists) {
        return ScopeID{ScopeID::Default};
    } else {
        auto itr = collections.find(cid);
        if (itr != collections.end()) {
            return itr->second.sid;
        }
    }
    return {};
}

DataLimit Manifest::getScopeDataLimit(ScopeID sid) const {
    auto scopeItr = scopes.find(sid);
    if (scopeItr == scopes.end()) {
        return std::nullopt;
    }
    return scopeItr->second.dataLimit;
}

void Manifest::dump() const {
    std::cerr << *this << std::endl;
}

bool CollectionEntry::operator==(const CollectionEntry& other) const {
    return cid == other.cid && name == other.name && sid == other.sid &&
           maxTtl == other.maxTtl;
}

bool Scope::operator==(const Scope& other) const {
    bool equal = name == other.name &&
                 collections.size() == other.collections.size() &&
                 dataLimit == other.dataLimit &&
                 dataLimitFromCluster == other.dataLimitFromCluster;
    if (equal) {
        for (const auto& c : collections) {
            equal &= std::find(other.collections.begin(),
                               other.collections.end(),
                               c) != other.collections.end();
            if (!equal) {
                break;
            }
        }
    }
    return equal;
}

bool Manifest::operator==(const Manifest& other) const {
    return (uid == other.uid) && isEqualContent(other);
}

// tests the equality of contents, i.e. everything but the uid
bool Manifest::isEqualContent(const Manifest& other) const {
    bool equal = defaultCollectionExists == other.defaultCollectionExists;
    equal &= scopes == other.scopes &&
             collections.size() == other.collections.size();
    return equal;
}

cb::engine_error Manifest::isSuccessor(const Manifest& successor) const {
    // else must be a > uid and any changes must be valid or equal uid with no
    // changes
    if (successor.getUid() > uid) {
        // For each scope-id in this is it in successor?
        for (const auto& [sid, scope] : scopes) {
            auto itr = successor.findScope(sid);
            // If the sid still exists it must have the same name
            if (itr != successor.endScopes()) {
                if (scope.name != itr->second.name) {
                    return cb::engine_error(
                            cb::engine_errc::cannot_apply_collections_manifest,
                            "invalid name change detected on scope "
                            "sid:" + sid.to_string() +
                                    ", name:" + scope.name +
                                    ", new-name:" + itr->second.name);
                }
            } // else this sid has been removed and that's fine
        }

        // For each collection in this is it in successor?
        for (const auto& [cid, collection] : collections) {
            auto itr = successor.findCollection(cid);
            if (itr != successor.end()) {
                // CollectionEntry must be equal (no maxTTL changes either)
                if (collection != itr->second) {
                    return cb::engine_error(
                            cb::engine_errc::cannot_apply_collections_manifest,
                            "invalid collection change detected "
                            "cid:" + cid.to_string() +
                                    ", name:" + collection.name + ", sid:" +
                                    collection.sid.to_string() + ", maxTTL:" +
                                    (collection.maxTtl.has_value() ? "yes"
                                                                   : "no") +
                                    ":" +
                                    std::to_string(
                                            collection.maxTtl
                                                    .value_or(
                                                            std::chrono::
                                                                    seconds{0})
                                                    .count()) +
                                    ", new-name:" + itr->second.name +
                                    ", new-sid: " +
                                    itr->second.sid.to_string() +
                                    ", new-maxTTL:" +
                                    (itr->second.maxTtl.has_value() ? "yes"
                                                                    : "no") +
                                    ":" +
                                    std::to_string(
                                            itr->second.maxTtl
                                                    .value_or(
                                                            std::chrono::
                                                                    seconds{0})
                                                    .count()));
                }
            } // else this cid has been removed and that's fine
        }
    } else if (uid == successor.getUid()) {
        if (*this != successor) {
            return cb::engine_error(
                    cb::engine_errc::cannot_apply_collections_manifest,
                    "equal uid but not an equal manifest");
        }
    } else {
        return cb::engine_error(
                cb::engine_errc::cannot_apply_collections_manifest,
                "uid must be >= current-uid:" + std::to_string(uid) +
                        ", new-uid:" + std::to_string(successor.getUid()));
    }

    return cb::engine_error(cb::engine_errc::success, "");
}

bool Manifest::isEpoch() const {
    // uid of 0, 1 scope and 1 collection
    if (uid == 0 && scopes.size() == 1 && collections.size() == 1) {
        // The default scope and the default collection
        const auto scope = findScope(ScopeID::Default);
        return defaultCollectionExists && scope != scopes.end() &&
               scope->second.name == DefaultScopeIdentifier;
    }
    return false;
}

std::ostream& operator<<(std::ostream& os, const Manifest& manifest) {
    os << "Collections::Manifest"
       << ", defaultCollectionExists:" << manifest.defaultCollectionExists
       << ", uid:" << manifest.uid
       << ", collections.size:" << manifest.collections.size() << std::endl;
    for (const auto& entry : manifest.scopes) {
        os << "scope:{" << entry.first << ", " << entry.second.name;

        if (entry.second.dataLimit) {
            os << ", dataLimit:" << entry.second.dataLimit.value()
               << ", dataLimitFromCluster:"
               << entry.second.dataLimitFromCluster;
        }
        os << ", collections:[";

        for (const auto& collection : entry.second.collections) {
            os << "{cid:" << collection.cid << ", sid:" << collection.sid
               << ", " << collection.name
               << ", ttl:" << (collection.maxTtl.has_value() ? "yes" : "no")
               << ":"
               << collection.maxTtl.value_or(std::chrono::seconds(0)).count()
               << "}";
        }
        os << "]\n";
    }

    for (const auto& [key, collection] : manifest.collections) {
        os << "{cid key:" << key.to_string() << ", value:" << collection.cid
           << ", sid:" << collection.sid << ", " << collection.name
           << ", ttl:" << (collection.maxTtl.has_value() ? "yes" : "no") << ":"
           << collection.maxTtl.value_or(std::chrono::seconds(0)).count()
           << "}";
        os << "]\n";
    }
    return os;
}
}
