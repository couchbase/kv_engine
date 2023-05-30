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

#include "collections/collections_types.h"
#include "ep_types.h"
#include "systemevent_factory.h"

#include <fmt/ostream.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/dockey.h>
#include <nlohmann/json.hpp>
#include <spdlog/fmt/fmt.h>

#include <cctype>
#include <cstring>
#include <iostream>
#include <sstream>
#include <utility>

namespace Collections {

ManifestUid makeManifestUid(std::string_view uid) {
    // note makeUid comes from the DocKey code as ManifestUid and CollectionUid
    // are very similar
    return ManifestUid(makeUid(uid, 16));
}

std::string makeCollectionIdIntoString(CollectionID collection) {
    cb::mcbp::unsigned_leb128<CollectionIDType> leb128(uint32_t{collection});
    return std::string(reinterpret_cast<const char*>(leb128.data()),
                       leb128.size());
}

CollectionID getCollectionIDFromKey(const DocKey& key) {
    if (!key.isInSystemCollection()) {
        throw std::invalid_argument("getCollectionIDFromKey: non-system key");
    }
    return SystemEventFactory::getCollectionIDFromKey(key);
}

ScopeID getScopeIDFromKey(const DocKey& key) {
    if (!key.isInSystemCollection()) {
        throw std::invalid_argument("getScopeIDFromKey: non-system key");
    }
    return SystemEventFactory::getScopeIDFromKey(key);
}

AccumulatedStats& AccumulatedStats::operator+=(const AccumulatedStats& other) {
    itemCount += other.itemCount;
    diskSize += other.diskSize;
    opsStore += other.opsStore;
    opsDelete += other.opsDelete;
    opsGet += other.opsGet;
    return *this;
}

bool AccumulatedStats::operator==(const AccumulatedStats& other) const {
    return itemCount == other.itemCount && diskSize == other.diskSize &&
           opsStore == other.opsStore && opsDelete == other.opsDelete &&
           opsGet == other.opsGet;
}

bool AccumulatedStats::operator!=(const AccumulatedStats& other) const {
    return !(*this == other);
}

std::string to_string(Metered metered) {
    switch (metered) {
    case Metered::Yes:
        return "Metered";
    case Metered::No:
        return "Unmetered";
    }
    folly::assume_unreachable();
}

std::ostream& operator<<(std::ostream& os, Metered metered) {
    return os << to_string(metered);
}

namespace VB {
std::string to_string(ManifestUpdateStatus status) {
    switch (status) {
    case ManifestUpdateStatus::Success:
        return "Success";
    case ManifestUpdateStatus::Behind:
        return "Behind";
    case ManifestUpdateStatus::EqualUidWithDifferences:
        return "EqualUidWithDifferences";
    case ManifestUpdateStatus::ImmutablePropertyModified:
        return "ImmutablePropertyModified";
    }
    return "Unknown " + std::to_string(int(status));
}

CollectionSharedMetaDataView::CollectionSharedMetaDataView(
        std::string_view name, ScopeID scope, Metered metered)
    : name(name), scope(scope), metered(metered) {
}

CollectionSharedMetaDataView::CollectionSharedMetaDataView(
        const CollectionSharedMetaData& meta)
    : name(meta.name),
      scope(meta.scope),
      metered(meta.metered) {
}

std::string CollectionSharedMetaDataView::to_string() const {
    return "Collection: name:" + std::string(name) +
           ", scope:" + scope.to_string() + ", " +
           Collections::to_string(metered);
}

CollectionSharedMetaData::CollectionSharedMetaData(std::string_view name,
                                                   ScopeID scope,
                                                   Metered metered)
    : name(name), scope(scope), metered(metered) {
}

CollectionSharedMetaData::CollectionSharedMetaData(
        const CollectionSharedMetaDataView& view)
    : name(view.name),
      scope(view.scope),
      metered(view.metered) {
}

bool CollectionSharedMetaData::operator==(
        const CollectionSharedMetaDataView& view) const {
    return name == view.name && scope == view.scope && metered == view.metered;
}

bool CollectionSharedMetaData::operator==(
        const CollectionSharedMetaData& meta) const {
    return *this == CollectionSharedMetaDataView(meta);
}

std::ostream& operator<<(std::ostream& os,
                         const CollectionSharedMetaData& meta) {
    return os << " name:" << meta.name << ", scope:" << meta.scope << ", "
              << meta.metered;
}

ScopeSharedMetaDataView::ScopeSharedMetaDataView(
        const ScopeSharedMetaData& meta)
    : name(meta.name) {
}

std::string ScopeSharedMetaDataView::to_string() const {
    std::string rv = "Scope: name:" + std::string(name);

    if (dataLimit) {
        rv += ", limit:" + std::to_string(dataLimit.value());
    }
    return rv;
}

ScopeSharedMetaData::ScopeSharedMetaData(const ScopeSharedMetaDataView& view)
    : name(view.name), dataLimit(view.dataLimit) {
}

bool ScopeSharedMetaData::operator==(
        const ScopeSharedMetaDataView& view) const {
    // Note: deliberately not including the dataLimit in the compare. Not going
    // to consider the dataLimit a primary part of the scope identity. This
    // means we can create a scope on a replica (which isn't told the limit) and
    // it will share the metadata of any scope created by an active (which has
    // the data limit).
    return name == view.name;
}

bool ScopeSharedMetaData::operator==(const ScopeSharedMetaData& meta) const {
    return *this == ScopeSharedMetaDataView(meta);
}

std::ostream& operator<<(std::ostream& os, const ScopeSharedMetaData& meta) {
    os << " name:" << meta.name;

    meta.dataLimit.withRLock([&os](auto& dataLimit) {
        if (dataLimit) {
            os << ",dataLimit:" << dataLimit.value();
        }
    });
    return os;
}

} // namespace VB

std::string to_string(const CollectionMetaData& collection) {
    return fmt::format(
            "cid:{}, name:{}, ttl:{{{}, {}}}, sid:{}, {}, {}",
            collection.cid.to_string(),
            collection.name,
            collection.maxTtl.has_value(),
            collection.maxTtl.value_or(std::chrono::seconds(0)).count(),
            collection.sid.to_string(),
            collection.metered,
            collection.canDeduplicate);
}

std::ostream& operator<<(std::ostream& os, const CollectionMetaData& meta) {
    return os << to_string(meta);
}

std::ostream& operator<<(std::ostream& os, const ScopeMetaData& meta) {
    os << "sid:" << meta.sid << ",name:" << meta.name;

    return os;
}

} // end namespace Collections
