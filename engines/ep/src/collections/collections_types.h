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

#pragma once

#include <platform/monotonic.h>

#include <gsl/gsl-lite.hpp>
#include <memcached/types.h>
#include <platform/atomic.h>

#include <functional>
#include <unordered_map>

struct DocKey;

namespace Collections {

// The reserved name of the system owned, default collection.
const char* const DefaultCollectionName = "_default";
static std::string_view DefaultCollectionIdentifier(DefaultCollectionName);

const char* const DefaultScopeName = "_default";
static std::string_view DefaultScopeIdentifier(DefaultScopeName);

// The SystemEvent keys are given some human readable tags to make disk or
// memory dumps etc... more helpful.
const char* const CollectionEventDebugTag = "_collection";
const char* const ScopeEventDebugTag = "_scope";

// Couchstore private file name for manifest data
const char CouchstoreManifest[] = "_local/collections_manifest";

// Name of file where the manifest will be kept on persistent buckets
const char* const ManifestFile = "collections.manifest";
static std::string_view ManifestFileName(ManifestFile);

// Length of the string excluding the zero terminator (i.e. strlen)
const size_t CouchstoreManifestLen = sizeof(CouchstoreManifest) - 1;

using ManifestUid = WeaklyMonotonic<uint64_t>;

// Struct/Map used in summary stat collecting (where we do vb accumulation)
struct AccumulatedStats {
    AccumulatedStats& operator+=(const AccumulatedStats& other);
    bool operator==(const AccumulatedStats& other) const;
    bool operator!=(const AccumulatedStats& other) const;

    uint64_t itemCount{0};
    uint64_t diskSize{0};
    uint64_t opsStore{0};
    uint64_t opsDelete{0};
    uint64_t opsGet{0};
};
using Summary = std::unordered_map<CollectionID, AccumulatedStats>;

struct ManifestUidNetworkOrder {
    explicit ManifestUidNetworkOrder(ManifestUid uid) : uid(htonll(uid)) {
    }
    ManifestUid to_host() const {
        return ManifestUid(ntohll(uid));
    }
    ManifestUid::value_type uid;
};
static_assert(sizeof(ManifestUidNetworkOrder) == 8,
              "ManifestUidNetworkOrder must have fixed size of 8 bytes as "
              "written to disk.");

/**
 * Return a ManifestUid from a C-string.
 * A valid ManifestUid is a C-string where each character satisfies
 * std::isxdigit and can be converted to a ManifestUid by std::strtoull.
 *
 * @param uid C-string uid
 * @param len a length for validation purposes
 * @throws std::invalid_argument if uid is invalid
 */
ManifestUid makeUid(const char* uid, size_t len = 16);

/**
 * Return a ManifestUid from a std::string
 * A valid ManifestUid is a std::string where each character satisfies
 * std::isxdigit and can be converted to a ManifestUid by std::strtoull.
 *
 * @param uid std::string
 * @throws std::invalid_argument if uid is invalid
 */
static inline ManifestUid makeUid(const std::string& uid) {
    return makeUid(uid.c_str());
}

/**
 * Return a CollectionID from a C-string.
 * A valid CollectionID is a C-string where each character satisfies
 * std::isxdigit and can be converted to a CollectionID by std::strtoul.
 *
 * @param uid C-string uid
 * @throws std::invalid_argument if uid is invalid
 */
static inline CollectionID makeCollectionID(const char* uid) {
    // CollectionID is 8 characters max and smaller than a ManifestUid
    try {
        return gsl::narrow<CollectionIDType>(makeUid(uid, 8));
    } catch (const gsl::narrowing_error& e) {
        throw std::invalid_argument("Cannot narrow uid:'" + std::string(uid) +
                                    "' to a CollectionID");
    }
}

/**
 * Return a CollectionID from a std::string
 * A valid CollectionID is a std::string where each character satisfies
 * std::isxdigit and can be converted to a CollectionID by std::strtoul
 *
 * @param uid std::string
 * @throws std::invalid_argument if uid is invalid
 */
static inline CollectionID makeCollectionID(const std::string& uid) {
    return makeCollectionID(uid.c_str());
}

/**
 * Return a ScopeID from a C-string.
 * A valid CollectionID is a std::string where each character satisfies
 * std::isxdigit and can be converted to a CollectionID by std::strtoul
 * @param uid C-string uid
 * @throws std::invalid_argument if uid is invalid
 */
static inline ScopeID makeScopeID(const char* uid) {
    try {
        // ScopeId is 8 characters max and smaller than a ManifestUid
        return gsl::narrow<ScopeIDType>(makeUid(uid, 8));
    } catch (const gsl::narrowing_error& e) {
        throw std::invalid_argument("Cannot narrow uid:'" + std::string(uid) +
                                    "' to a ScopeID");
    }
}

/**
 * Return a ScopeID from a std::string
 * A valid ScopeID is a std::string where each character satisfies
 * std::isxdigit and can be converted to a CollectionID by std::strtoul
 * @param uid std::string
 * @return std::invalid_argument if uid is invalid
 */
static inline ScopeID makeScopeID(const std::string& uid) {
    return makeScopeID(uid.c_str());
}

/**
 * The metadata of a single collection
 *
 * Default construction yields the default collection
 */
struct CollectionMetaData {
    ScopeID sid{ScopeID::Default}; // The scope that the collection belongs to
    CollectionID cid{CollectionID::Default}; // The collection's ID
    std::string name{DefaultCollectionName}; // The collection's name
    cb::ExpiryLimit maxTtl{}; // The collection's maxTTL

    bool operator==(const CollectionMetaData& other) const {
        return sid == other.sid && cid == other.cid && name == other.name &&
               maxTtl == other.maxTtl;
    }
};

std::ostream& operator<<(std::ostream& os, const CollectionMetaData& meta);

/**
 * The metadata of a single scope
 *
 * Default construction yields the default scope
 */
struct ScopeMetaData {
    ScopeID sid{ScopeID::Default}; // The scope's ID
    std::string name{DefaultScopeName}; // The scope's name

    bool operator==(const ScopeMetaData& other) const {
        return sid == other.sid && name == other.name;
    }
};

std::ostream& operator<<(std::ostream& os, const ScopeMetaData& meta);

/**
 * For creation of collection SystemEvents - The SystemEventFactory
 * glues the CollectionID into the event key (so create of x doesn't
 * collide with create of y). This method yields the 'keyExtra' parameter
 *
 * @param collection The value to turn into a string
 * @return the keyExtra parameter to be passed to SystemEventFactory
 */
std::string makeCollectionIdIntoString(CollectionID collection);

/**
 * For creation of collection SystemEvents - The SystemEventFactory
 * glues the CollectionID into the event key (so create of x doesn't
 * collide with create of y). This method basically reverses
 * makeCollectionIdIntoString so we can get a CollectionID from a
 * SystemEvent key
 *
 * @param key DocKey from an Item in the System namespace and is a collection
 *        event
 * @return the ID which was in the event
 */
CollectionID getCollectionIDFromKey(const DocKey& key);

/// Same as getCollectionIDFromKey but for events changing scopes
ScopeID getScopeIDFromKey(const DocKey& key);

/**
 * Callback function for processing against dropped collections in an ephemeral
 * vb, returns true if the key at seqno should be dropped
 *
 * @param DocKey the key of the item we should process
 * @param int64_t the seqno of the item
 */
using IsDroppedEphemeralCb = std::function<bool(const DocKey&, int64_t)>;

/**
 * A function for determining if a collection is visible
 */
using IsVisibleFunction =
        std::function<bool(ScopeID, std::optional<CollectionID>)>;

/**
 * A data limit - optional
 */
using DataLimit = std::optional<size_t>;

static const DataLimit NoDataLimit{};

namespace VB {
enum class ManifestUpdateStatus {
    Success,
    // The new Manifest has a 'UID' that is < current.
    Behind,
    // The new Manifest has a 'UID' that is == current, but adds/drops
    // collections/scopes.
    EqualUidWithDifferences,
    // The new Manifest changes scopes or collections immutable properties, e.g.
    // current has {id:8, name:"c1"} and new has {id:8,name:"c2"}.
    ImmutablePropertyModified
};
std::string to_string(ManifestUpdateStatus);

/// values required by the flusher to calculate new collection statistics
struct StatsForFlush {
    uint64_t itemCount;
    size_t diskSize;
    uint64_t highSeqno;
};

// Following classes define the metadata that will be held by the Manager but
// referenced from VB::Manifest
class CollectionSharedMetaData;
class CollectionSharedMetaDataView {
public:
    CollectionSharedMetaDataView(std::string_view name,
                                 ScopeID scope,
                                 cb::ExpiryLimit maxTtl);
    CollectionSharedMetaDataView(const CollectionSharedMetaData&);
    std::string to_string() const;
    std::string_view name;
    const ScopeID scope;
    const cb::ExpiryLimit maxTtl;
};

// The type stored by the Manager SharedMetaDataTable
class CollectionSharedMetaData : public RCValue {
public:
    CollectionSharedMetaData(std::string_view name,
                             ScopeID scope,
                             cb::ExpiryLimit maxTtl);
    CollectionSharedMetaData(const CollectionSharedMetaDataView& view);
    bool operator==(const CollectionSharedMetaDataView& view) const;
    bool operator!=(const CollectionSharedMetaDataView& view) const {
        return !(*this == view);
    }
    bool operator==(const CollectionSharedMetaData& meta) const;
    bool operator!=(const CollectionSharedMetaData& meta) const {
        return !(*this == meta);
    }

    const std::string name;
    const ScopeID scope;
    const cb::ExpiryLimit maxTtl;
};
std::ostream& operator<<(std::ostream& os,
                         const CollectionSharedMetaData& meta);

class ScopeSharedMetaData;
class ScopeSharedMetaDataView {
public:
    ScopeSharedMetaDataView(const ScopeSharedMetaData&);
    ScopeSharedMetaDataView(std::string_view name, DataLimit dataLimit)
        : name(name), dataLimit(dataLimit) {
    }
    std::string to_string() const;
    std::string_view name;
    DataLimit dataLimit;
};

// The type stored by the Manager SharedMetaDataTable
class ScopeSharedMetaData : public RCValue {
public:
    ScopeSharedMetaData(const ScopeSharedMetaDataView& view);
    bool operator==(const ScopeSharedMetaDataView& view) const;
    bool operator!=(const ScopeSharedMetaDataView& view) const {
        return !(*this == view);
    }
    bool operator==(const ScopeSharedMetaData& meta) const;
    bool operator!=(const ScopeSharedMetaData& meta) const {
        return !(*this == meta);
    }

    // scope name is fixed
    const std::string name;
    // scope limit we will allow changes
    DataLimit dataLimit;
};
std::ostream& operator<<(std::ostream& os, const ScopeSharedMetaData& meta);

} // namespace VB

} // end namespace Collections
