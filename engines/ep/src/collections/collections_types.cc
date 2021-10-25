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
#include "systemevent_factory.h"

#include <mcbp/protocol/unsigned_leb128.h>
#include <nlohmann/json.hpp>
#include <spdlog/fmt/fmt.h>

#include <cctype>
#include <cstring>
#include <iostream>
#include <sstream>
#include <utility>

namespace Collections {

ManifestUid makeUid(const char* uid, size_t len) {
    if (std::strlen(uid) == 0 || std::strlen(uid) > len) {
        throw std::invalid_argument(
                "Collections::makeUid uid must be > 0 and <=" +
                std::to_string(len) +
                " characters: "
                "strlen(uid):" +
                std::to_string(std::strlen(uid)));
    }

    // verify that the input characters satisfy isxdigit
    for (size_t ii = 0; ii < std::strlen(uid); ii++) {
        if (uid[ii] == 0) {
            break;
        } else if (!std::isxdigit(uid[ii])) {
            throw std::invalid_argument("Collections::makeUid: uid:" +
                                        std::string(uid) + ", index:" +
                                        std::to_string(ii) + " fails isxdigit");
        }
    }

    return ManifestUid(std::strtoul(uid, nullptr, 16));
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
        std::string_view name, ScopeID scope, cb::ExpiryLimit maxTtl)
    : name(name), scope(scope), maxTtl(std::move(maxTtl)) {
}

CollectionSharedMetaDataView::CollectionSharedMetaDataView(
        const CollectionSharedMetaData& meta)
    : name(meta.name), scope(meta.scope), maxTtl(meta.maxTtl) {
}

std::string CollectionSharedMetaDataView::to_string() const {
    std::string rv = "Collection: name:" + std::string(name) +
                     ", scope:" + scope.to_string();
    if (maxTtl) {
        rv += " maxTtl:" + std::to_string(maxTtl.value().count());
    }
    return rv;
}

CollectionSharedMetaData::CollectionSharedMetaData(std::string_view name,
                                                   ScopeID scope,
                                                   cb::ExpiryLimit maxTtl)
    : name(name), scope(scope), maxTtl(std::move(maxTtl)) {
}

CollectionSharedMetaData::CollectionSharedMetaData(
        const CollectionSharedMetaDataView& view)
    : name(view.name), scope(view.scope), maxTtl(view.maxTtl) {
}

bool CollectionSharedMetaData::operator==(
        const CollectionSharedMetaDataView& view) const {
    return name == view.name && scope == view.scope && maxTtl == view.maxTtl;
}

bool CollectionSharedMetaData::operator==(
        const CollectionSharedMetaData& meta) const {
    return *this == CollectionSharedMetaDataView(meta);
}

std::ostream& operator<<(std::ostream& os,
                         const CollectionSharedMetaData& meta) {
    os << " name:" << meta.name << ", scope:" << meta.scope;
    if (meta.maxTtl) {
        os << ", maxTtl:" << meta.maxTtl.value().count();
    }
    return os;
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
    if (meta.dataLimit) {
        os << ",dataLimit:" << meta.dataLimit.value();
    }
    return os;
}

} // namespace VB

std::ostream& operator<<(std::ostream& os, const CollectionMetaData& meta) {
    os << "sid:" << meta.sid << ",cid:" << meta.cid << ",name:" << meta.name;

    if (meta.maxTtl) {
        os << ",maxTTl:" << meta.maxTtl->count();
    }

    return os;
}

std::ostream& operator<<(std::ostream& os, const ScopeMetaData& meta) {
    os << "sid:" << meta.sid << ",name:" << meta.name;

    return os;
}

} // end namespace Collections
