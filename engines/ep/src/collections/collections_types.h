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

#include <memcached/dockey.h>
#include <memcached/types.h>
#include <platform/sized_buffer.h>
#include <gsl/gsl>

#include <unordered_map>
#include <vector>

namespace Collections {

// The reserved name of the system owned, default collection.
const char* const _DefaultCollectionIdentifier = "_default";
static cb::const_char_buffer DefaultCollectionIdentifier(
        _DefaultCollectionIdentifier);

const char* const _DefaultScopeIdentifier = "_default";
static cb::const_char_buffer DefaultScopeIdentifier(_DefaultScopeIdentifier);

// SystemEvent keys or parts which will be made into keys
const char* const SystemSeparator = ":"; // Note this never changes
const char* const CollectionEventPrefixWithSeparator = "_collection:";
const char* const ScopeEventPrefixWithSeparator = "_scope:";

// Couchstore private file name for manifest data
const char CouchstoreManifest[] = "_local/collections_manifest";

// Length of the string excluding the zero terminator (i.e. strlen)
const size_t CouchstoreManifestLen = sizeof(CouchstoreManifest) - 1;

using ManifestUid = WeaklyMonotonic<uint64_t>;

// Map used in summary stats
using Summary = std::unordered_map<CollectionID, uint64_t>;

struct ManifestUidNetworkOrder {
    ManifestUidNetworkOrder(ManifestUid uid) : uid(htonll(uid)) {
    }
    ManifestUid to_host() const {
        return ntohll(uid);
    }
    ManifestUid uid;
};

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
 */
struct CollectionMetaData {
    ScopeID sid; // The scope that the collection belongs to
    CollectionID cid; // The collection's ID
    std::string name; // The collection's name
    cb::ExpiryLimit maxTtl; // The collection's max_ttl

    bool operator==(const CollectionMetaData& other) const {
        return sid == other.sid && cid == other.cid && name == other.name &&
               maxTtl == other.maxTtl;
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
    ScopeID sid; // The scope id
    std::string name; // The scope name
};

struct DropScopeEventData {
    ManifestUid manifestUid; // The Manifest which generated the event
    ScopeID sid; // The scope the event belongs to
};

/**
 * All of the data a DCP create event message will transmit in the value of the
 * message. This is the layout to be used on the wire and is in the correct
 * byte order
 */
struct CreateEventDcpData {
    CreateEventDcpData(const CreateEventData& ev)
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
    CreateWithMaxTtlEventDcpData(const CreateEventData& ev)
        : manifestUid(ev.manifestUid),
          sid(ev.metaData.sid),
          cid(ev.metaData.cid),
          maxTtl(htonl(gsl::narrow_cast<uint32_t>(
                  ev.metaData.maxTtl.get().count()))) {
    }
    /// The manifest uid stored in network byte order ready for sending
    ManifestUidNetworkOrder manifestUid;
    /// The scope id stored in network byte order ready for sending
    ScopeIDNetworkOrder sid;
    /// The collection id stored in network byte order ready for sending
    CollectionIDNetworkOrder cid;
    /// The collection's max_ttl value (in network byte order)
    uint32_t maxTtl;
    // The size is sizeof(manifestUid) + sizeof(cid) + sizeof(sid) +
    //             sizeof(max_ttl) (msvc won't allow that expression)
    constexpr static size_t size{20};
};

/**
 * All of the data a DCP drop event message will transmit in the value of the
 * message. This is the layout to be used on the wire and is in the correct
 * byte order
 */
struct DropEventDcpData {
    DropEventDcpData(const DropEventData& data)
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
    CreateScopeEventDcpData(const CreateScopeEventData& data)
        : manifestUid(data.manifestUid), sid(data.sid) {
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
    DropScopeEventDcpData(const DropScopeEventData& data)
        : manifestUid(data.manifestUid), sid(data.sid) {
    }

    /// The manifest uid stored in network byte order ready for sending
    ManifestUidNetworkOrder manifestUid;
    /// The collection id stored in network byte order ready for sending
    ScopeIDNetworkOrder sid;
    constexpr static size_t size{12};
};

/**
 * All unknown_collection errors should be accompanied by a error context
 * value which incldues the manifest ID which was what the collection lookup
 * failed against.
 * @return std::string for use in setErrorContext
 */
std::string getUnknownCollectionErrorContext(uint64_t manifestUid);

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
 * For creation of scope SystemEvents - The SystemEventFactory
 * glues the ScopeID into the event key (so create of x doesn't
 * collide with create of y). This method yields the 'keyExtra' parameter
 *
 * @param sid The ScopeId to turn into a string
 * @return the keyExtra parameter to be passed to SystemEventFactory
 */
std::string makeScopeIdIntoString(ScopeID sid);

/**
 * For creation of collection SystemEvents - The SystemEventFactory
 * glues the CollectionID into the event key (so create of x doesn't
 * collide with create of y). This method basically reverses
 * makeCollectionIdIntoString so we can get a CollectionID from a
 * SystemEvent key
 *
 * @param key DocKey from a SystemEvent
 * @param separator the separator between the SystemEvent prefix and the
 *        CollectionID
 * @return the ID which was in the event
 */
CollectionID getCollectionIDFromKey(
        const DocKey& key,
        const char* separator = Collections::SystemSeparator);

/// Same as getCollectionIDFromKey but for events changing scopes
ScopeID getScopeIDFromKey(const DocKey& key,
                          const char* separator = Collections::SystemSeparator);

namespace VB {
/**
 * The PersistedManifest which stores a copy of the VB::Manifest, the actual
 * format of the data is defined by VB::Manifest
 */
using PersistedManifest = std::vector<uint8_t>;

class EraserContext;
} // namespace VB

/**
 * Callback function for processing against dropped collections in an ephemeral
 * vb, returns true if the key at seqno should be dropped
 */
using IsDroppedEphemeralCb = std::function<bool(const DocKey&, int64_t)>;

} // end namespace Collections

std::ostream& operator<<(std::ostream& os,
                         const Collections::VB::PersistedManifest& data);
