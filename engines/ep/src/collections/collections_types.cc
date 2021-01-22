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

std::string to_string(const CreateEventData& event) {
    return fmt::format(
            fmt("CreateCollection{{uid:{:#x} scopeID:{} collectionID:{} "
                "name:'"
                "{}' maxTTLEnabled:{} maxTTL:{}}}"),
            event.manifestUid.load(),
            event.metaData.sid.to_string(),
            event.metaData.cid.to_string(),
            event.metaData.name,
            event.metaData.maxTtl.has_value(),
            event.metaData.maxTtl.has_value() ? event.metaData.maxTtl->count()
                                              : 0);
}

std::string to_string(const DropEventData& event) {
    return fmt::format(
            fmt("DropCollection{{uid:{:#x} scopeID:{} collectionID:{}}}"),
            event.manifestUid.load(),
            event.sid.to_string(),
            event.cid.to_string());
}

std::string to_string(const CreateScopeEventData& event) {
    return fmt::format(fmt("CreateScope{{uid:{:#x} scopeID:{} name:'{}'}}"),
                       event.manifestUid.load(),
                       event.metaData.sid.to_string(),
                       event.metaData.name);
}

std::string to_string(const DropScopeEventData& event) {
    return fmt::format(fmt("DropScope{{uid:{:#x} scopeID:{}}}"),
                       event.manifestUid.load(),
                       event.sid.to_string());
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
    return "Scope: name:" + std::string(name);
}

ScopeSharedMetaData::ScopeSharedMetaData(const ScopeSharedMetaDataView& view)
    : name(view.name) {
}

bool ScopeSharedMetaData::operator==(
        const ScopeSharedMetaDataView& view) const {
    return name == view.name;
}

bool ScopeSharedMetaData::operator==(const ScopeSharedMetaData& meta) const {
    return *this == ScopeSharedMetaDataView(meta);
}

std::ostream& operator<<(std::ostream& os, const ScopeSharedMetaData& meta) {
    os << " name:" << meta.name;
    return os;
}

} // namespace VB

} // end namespace Collections
