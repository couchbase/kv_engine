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

#include "ep_types.h"

#include <folly/Synchronized.h>
#include <gsl/gsl-lite.hpp>
#include <memcached/types.h>
#include <platform/atomic.h>
#include <platform/monotonic.h>

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
 * Return a ManifestUid from a string_view
 * A valid ManifestUid is a string where each character satisfies
 * std::isxdigit and can be converted to a ManifestUid by std::strtoull.
 *
 * @param uid view to convert
 * @throws std::invalid_argument if uid is invalid
 */
ManifestUid makeManifestUid(std::string_view uid);

/// Collection metering yes/no
enum class Metered : bool { Yes, No };

std::string to_string(Metered);
std::ostream& operator<<(std::ostream&, Metered);

static inline Metered getMetered(bool metered) {
    return metered ? Metered::Yes : Metered::No;
}

static inline bool getMeteredFromEnum(Metered metered) {
    return metered == Metered::Yes ? true : false;
}

/**
 * The metadata of a single collection. This represents the data we persist
 * in KVStore meta and is used to communicate back to kv from KVStore
 *
 * Default construction yields the default collection
 */
struct CollectionMetaData {
    CollectionMetaData() = default;
    CollectionMetaData(ScopeID sid,
                       CollectionID cid,
                       std::string_view name,
                       cb::ExpiryLimit maxTtl,
                       Metered metered,
                       CanDeduplicate canDeduplicate)
        : sid(sid),
          cid(cid),
          name(name),
          maxTtl(maxTtl),
          metered(metered),
          canDeduplicate(canDeduplicate) {
    }

    ScopeID sid{ScopeID::Default}; // The scope that the collection belongs to
    CollectionID cid{CollectionID::Default}; // The collection's ID
    std::string name{DefaultCollectionName}; // The collection's name
    cb::ExpiryLimit maxTtl{}; // The collection's maxTTL
    Metered metered{Metered::Yes};
    CanDeduplicate canDeduplicate{CanDeduplicate::Yes};

    bool operator==(const CollectionMetaData& other) const {
        return compareImmutableProperties(other) &&
               compareMutableProperties(other);
    }

    bool operator!=(const CollectionMetaData& other) const {
        return !(*this == other);
    }

    // compare only the properties which are not permitted to change
    bool compareImmutableProperties(const CollectionMetaData& other) const {
        return sid == other.sid && cid == other.cid && name == other.name &&
               metered == other.metered;
    }

    bool compareMutableProperties(const CollectionMetaData& other) const {
        return canDeduplicate == other.canDeduplicate && maxTtl == other.maxTtl;
    }
};

std::string to_string(const CollectionMetaData&);
std::ostream& operator<<(std::ostream& os, const CollectionMetaData& meta);

/**
 * The metadata of a single scope. This represents the data we persist
 * in KVStore meta and is used to communicate back to kv from KVStore
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
                                 Metered metered);
    CollectionSharedMetaDataView(const CollectionSharedMetaData&);
    std::string to_string() const;
    std::string_view name;
    const ScopeID scope;
    Metered metered;
};

// The type stored by the Manager SharedMetaDataTable
class CollectionSharedMetaData : public RCValue {
public:
    CollectionSharedMetaData(std::string_view name,
                             ScopeID scope,
                             Metered metered);
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
    // can be updated (corrected) post creation from any thread
    std::atomic<Metered> metered;
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

    // A scope limit can be changed by any thread post creation of the
    // ScopeSharedMetaData, hence is synchronised
    folly::Synchronized<DataLimit> dataLimit;
};
std::ostream& operator<<(std::ostream& os, const ScopeSharedMetaData& meta);

/**
 * Mutation "type" enum used in collection high-seqno updates
 */
enum class HighSeqnoType { Committed, PrepareAbort, SystemEvent };

/// Key to locate default collection legacy data from an xattr blob
constexpr std::string_view LegacyXattrKey = "_default_collection_legacy";
/// The JSON key which stores the value of defaultCollectionMaxLegacyDCPSeqno
constexpr std::string_view LegacyMaxSeqnoKey = "max_dcp_seqno";
/// Formatting string to create the JSON value of the LegacyMaxSeqnoKey
constexpr std::string_view LegacyJSONFormat = R"({{"max_dcp_seqno":"{}"}})";

} // namespace VB

} // end namespace Collections
