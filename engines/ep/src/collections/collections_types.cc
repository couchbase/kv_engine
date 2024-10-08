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
#include <memcached/dockey_view.h>
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
    return {reinterpret_cast<const char*>(leb128.data()), leb128.size()};
}

CollectionID getCollectionIDFromKey(const DocKeyView& key) {
    if (!key.isInSystemEventCollection()) {
        throw std::invalid_argument("getCollectionIDFromKey: non-system key");
    }
    return SystemEventFactory::getCollectionIDFromKey(key);
}

ScopeID getScopeIDFromKey(const DocKeyView& key) {
    if (!key.isInSystemEventCollection()) {
        throw std::invalid_argument("getScopeIDFromKey: non-system key");
    }
    return SystemEventFactory::getScopeIDFromKey(key);
}

AccumulatedStats& AccumulatedStats::operator+=(const AccumulatedStats& other) {
    itemCount += other.itemCount;
    diskSize += other.diskSize;
    return *this;
}

bool AccumulatedStats::operator==(const AccumulatedStats& other) const {
    return itemCount == other.itemCount && diskSize == other.diskSize;
}

bool AccumulatedStats::operator!=(const AccumulatedStats& other) const {
    return !(*this == other);
}

OperationCounts& OperationCounts::operator+=(const OperationCounts& other) {
    opsStore += other.opsStore;
    opsDelete += other.opsDelete;
    opsGet += other.opsGet;
    return *this;
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
        std::string_view name, ScopeID scope)
    : name(name), scope(scope) {
}

CollectionSharedMetaDataView::CollectionSharedMetaDataView(
        const CollectionSharedMetaData& meta)
    : name(meta.name), scope(meta.scope) {
}

std::string CollectionSharedMetaDataView::to_string() const {
    return "Collection: name:" + std::string(name) +
           ", scope:" + scope.to_string();
}

CollectionSharedMetaData::CollectionSharedMetaData(std::string_view name,
                                                   ScopeID scope)
    : name(name), scope(scope) {
}

CollectionSharedMetaData::CollectionSharedMetaData(
        const CollectionSharedMetaDataView& view)
    : name(view.name), scope(view.scope) {
}

bool CollectionSharedMetaData::operator==(
        const CollectionSharedMetaDataView& view) const {
    // note: this operator does not compare the operation counters
    return name == view.name && scope == view.scope;
}

bool CollectionSharedMetaData::operator==(
        const CollectionSharedMetaData& meta) const {
    return *this == CollectionSharedMetaDataView(meta);
}

void CollectionSharedMetaData::incrementOpsStore() {
    coreLocal.get()->numOpsStore++;
}

void CollectionSharedMetaData::incrementOpsDelete() {
    coreLocal.get()->numOpsDelete++;
}

void CollectionSharedMetaData::incrementOpsGet() {
    coreLocal.get()->numOpsGet++;
}

OperationCounts CollectionSharedMetaData::getOperationCounts() const {
    OperationCounts counts;
    // sum the core local RelaxedAtomic counters
    for (const auto& core : coreLocal) {
        counts.opsStore += core->numOpsStore;
        counts.opsDelete += core->numOpsDelete;
        counts.opsGet += core->numOpsGet;
    }
    return counts;
}

std::ostream& operator<<(std::ostream& os,
                         const CollectionSharedMetaData& meta) {
    return os << " name:" << meta.name << ", scope:" << meta.scope;
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

std::string to_string(const CollectionMetaData& collection) {
    return fmt::format(
            "cid:{}, name:{}, ttl:{{{}, {}}}, sid:{}, {}, {}, flushUid:{}",
            collection.cid.to_string(),
            collection.name,
            collection.maxTtl.has_value(),
            collection.maxTtl.value_or(std::chrono::seconds(0)).count(),
            collection.sid.to_string(),
            collection.metered,
            to_string(collection.canDeduplicate),
            collection.flushUid);
}

std::ostream& operator<<(std::ostream& os, const CollectionMetaData& meta) {
    return os << to_string(meta);
}

std::ostream& operator<<(std::ostream& os, const ScopeMetaData& meta) {
    return os << format_as(meta);
}

std::string format_as(const ScopeMetaData& meta) {
    return fmt::format("sid:{},name:{}", meta.sid, meta.name);
}

} // end namespace Collections
