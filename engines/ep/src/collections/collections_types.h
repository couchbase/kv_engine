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

#pragma once

#include "monotonic.h"

#include <memcached/types.h>
#include <gsl/gsl>

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
    return gsl::narrow_cast<CollectionID>(makeUid(uid, 8));
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
 * @return std::invalid_argument if uid is invalid
 */
static inline ScopeID makeScopeID(const char* uid) {
    // ScopeId is 8 characters max and smaller than a ManifestUid
    return gsl::narrow_cast<ScopeID>(makeUid(uid, 8));
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

/**
 * The metadata of a single scope
 *
 * Default construction yields the default collection
 */
struct ScopeMetaData {
    ScopeID sid{ScopeID::Default}; // The scope's ID
    std::string name{DefaultScopeName}; // The scope's name

    bool operator==(const ScopeMetaData& other) const {
        return sid == other.sid && name == other.name;
    }
};

/**
 * All of the data a system event needs
 */
struct CreateEventData {
    ManifestUid manifestUid; // The Manifest which generated the event
    CollectionMetaData metaData; // The data of the new collection
};

struct DropEventData {
    ManifestUid manifestUid; // The Manifest which generated the event
    ScopeID sid; // The scope that the collection belonged to
    CollectionID cid; // The collection the event belongs to
};

struct CreateScopeEventData {
    ManifestUid manifestUid; // The Manifest which generated the event
    ScopeMetaData metaData; // The data of the new scope
};

struct DropScopeEventData {
    ManifestUid manifestUid; // The Manifest which generated the event
    ScopeID sid; // The scope the event belongs to
};

std::string to_string(const CreateEventData& event);
std::string to_string(const DropEventData& event);
std::string to_string(const CreateScopeEventData& event);
std::string to_string(const DropScopeEventData& event);

/**
 * All of the data a DCP create event message will transmit in the value of the
 * message. This is the layout to be used on the wire and is in the correct
 * byte order
 */
struct CreateEventDcpData {
    explicit CreateEventDcpData(const CreateEventData& ev)
        : manifestUid(ev.manifestUid),
          sid(ev.metaData.sid),
          cid(ev.metaData.cid) {
    }
    /// The manifest uid stored in network byte order ready for sending
    ManifestUidNetworkOrder manifestUid;
    /// The scope id stored in network byte order ready for sending
    ScopeIDNetworkOrder sid;
    /// The collection id stored in network byte order ready for sending
    CollectionIDNetworkOrder cid;
    // The size is sizeof(manifestUid) + sizeof(cid) + sizeof(sid)
    // (msvc won't allow that expression)
    constexpr static size_t size{16};
};

/**
 * All of the data a DCP create event message will transmit in the value of a
 * DCP system event message (when the collection is created with a TTL). This is
 * the layout to be used on the wire and is in the correct byte order
 */
struct CreateWithMaxTtlEventDcpData {
    explicit CreateWithMaxTtlEventDcpData(const CreateEventData& ev)
        : manifestUid(ev.manifestUid),
          sid(ev.metaData.sid),
          cid(ev.metaData.cid),
          maxTtl(htonl(gsl::narrow_cast<uint32_t>(
                  ev.metaData.maxTtl.value().count()))) {
    }
    /// The manifest uid stored in network byte order ready for sending
    ManifestUidNetworkOrder manifestUid;
    /// The scope id stored in network byte order ready for sending
    ScopeIDNetworkOrder sid;
    /// The collection id stored in network byte order ready for sending
    CollectionIDNetworkOrder cid;
    /// The collection's maxTTL value (in network byte order)
    uint32_t maxTtl;
    // The size is sizeof(manifestUid) + sizeof(cid) + sizeof(sid) +
    //             sizeof(maxTTL) (msvc won't allow that expression)
    constexpr static size_t size{20};
};

/**
 * All of the data a DCP drop event message will transmit in the value of the
 * message. This is the layout to be used on the wire and is in the correct
 * byte order
 */
struct DropEventDcpData {
    explicit DropEventDcpData(const DropEventData& data)
        : manifestUid(data.manifestUid), sid(data.sid), cid(data.cid) {
    }

    /// The manifest uid stored in network byte order ready for sending
    ManifestUidNetworkOrder manifestUid;
    /// The scope id stored in network byte order ready for sending
    ScopeIDNetworkOrder sid;
    /// The collection id stored in network byte order ready for sending
    CollectionIDNetworkOrder cid;
    // The size is sizeof(manifestUid) + sizeof(cid) (msvc won't allow that
    // expression)
    constexpr static size_t size{16};
};

/**
 * All of the data a DCP create scope event message will transmit in the value
 * of the message. This is the layout to be used on the wire and is in the
 * correct byte order
 */
struct CreateScopeEventDcpData {
    explicit CreateScopeEventDcpData(const CreateScopeEventData& data)
        : manifestUid(data.manifestUid), sid(data.metaData.sid) {
    }
    /// The manifest uid stored in network byte order ready for sending
    ManifestUidNetworkOrder manifestUid;
    /// The scope id stored in network byte order ready for sending
    ScopeIDNetworkOrder sid;
    constexpr static size_t size{12};
};

/**
 * All of the data a DCP drop scope event message will transmit in the value of
 * the message. This is the layout to be used on the wire and is in the correct
 * byte order
 */
struct DropScopeEventDcpData {
    explicit DropScopeEventDcpData(const DropScopeEventData& data)
        : manifestUid(data.manifestUid), sid(data.sid) {
    }

    /// The manifest uid stored in network byte order ready for sending
    ManifestUidNetworkOrder manifestUid;
    /// The collection id stored in network byte order ready for sending
    ScopeIDNetworkOrder sid;
    constexpr static size_t size{12};
};

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
 * @param bool whether or not the item is a prepare (true if prepare)
 */
using IsDroppedEphemeralCb = std::function<bool(const DocKey&, int64_t, bool)>;

/**
 * A function for determining if a collection is visible
 */
using IsVisibleFunction =
        std::function<bool(ScopeID, std::optional<CollectionID>)>;

namespace VB {
enum class ManifestUpdateStatus { Success, Behind, EqualUidWithDifferences };
std::string to_string(ManifestUpdateStatus);

/// values required by the flusher to calculate new collection statistics
struct StatsForFlush {
    uint64_t itemCount;
    size_t diskSize;
    uint64_t highSeqno;
};

} // namespace VB

} // end namespace Collections
