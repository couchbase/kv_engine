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

#include "collections/collections_types.h"
#include "item.h"

#include <fmt/ostream.h>
#include <memcached/dcp_stream_id.h>
#include <memcached/engine_common.h>
#include <memcached/engine_error.h>
#include <nlohmann/json_fwd.hpp>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

class CookieIface;
class EventuallyPersistentEngine;
class SystemEventMessage;

namespace Collections::VB {

class Manifest;
class ReadHandle;

/**
 * The VB filter object constructs from data that is yielded from DCP stream
 * request, an optional string. The string is optional because a legacy DCP
 * user will not set any string data, whilst a collection aware request can.
 * However the collection aware request may contain an empty string. For
 * reference here's what the optiona string means.
 *
 * - uninitialized: legacy stream-request. A client that can only ever receive
 *                  the default collection.
 * - empty string: A collection aware client that wants *everything* from the
 *                 epoch. For example KV replication streams.
 * - non-empty string: A collection aware client wanting to resume a stream or
 *                     request specific collections (or both). The non-empty
 *                     string is a JSON document as follows.
 *
 * A client can request individual collections or a group by defining an array
 * of collection-IDs.
 *
 * Client wants two collections:
 *   {"collections":["0", "ac1"]}}
 *
 * Client wants to resume a stream that was previously everything, client
 * specifies the highest manifest UID they've received.
 *   {"uid":"5"}
 *
 * Why not both? Client wants to resume a subset of collections.
 *    {"collections":["0", "ac1"]}, "uid":"5"}
 *
 * The class exposes methods which are for use by the ActiveStream.
 *
 * Should a Mutation/Deletion/SystemEvent be included in the DCP stream?
 *   - checkAndUpdate
 * checkAndUpdate is non-const because for SystemEvents which represent the
 * drop of a collection, they will modify the Filter to remove the collection
 * if the filter was targeting the collection.
 *
 */

class Filter {
public:
    /**
     * Construct a Collections::VB::Filter using the producer's
     * Collections::Filter and the vbucket's collections manifest.
     *
     * If the producer's filter is configured to filter collections then the
     * resulting object will filter the intersection filter:manifest
     * collections. The constructor will log when it finds it must drop a
     * collection
     *
     * If the producer's filter is effectively a passthrough
     * (allowAllCollections returns true) then so will the resulting VB filter.
     *
     * @param jsonFilter Optional string data that can contain JSON
     *        configuration info
     * @param manifest The vbucket's collection manifest.
     * @param cookie Cookie associated with the connection that is making a
     *        stream-request
     * @param engine reference to engine for checkPrivilege calls
     * @throws cb::engine_error
     */
    Filter(std::optional<std::string_view> jsonFilter,
           const ::Collections::VB::Manifest& manifest,
           CookieIface& cookie,
           const EventuallyPersistentEngine& engine);

    /**
     * Check the item and if required and maybe update the filter.
     *
     * If the item represents a collection deletion and this filter matches the
     * collection, we must update the filter so we can later see that the filter
     * is empty, and the DCP stream can choose to close.
     *
     * @param item an Item to be processed.
     * @return if the Item is allowed to be sent on the DcpStream
     */
    bool checkAndUpdate(Item& item) {
        // passthrough, everything is allowed.
        if (isPassThroughFilter()) {
            return true;
        }

        // The presence of _default is a simple check against defaultAllowed
        if (item.getKey().isInDefaultCollection() && defaultAllowed) {
            return true;
        }
        // More complex checks needed...
        return checkAndUpdateSlow(item);
    }

    /**
     * Check if the filter allows the collection
     *
     * Note: if cid is SystemEvent then we say it is allowed as we don't know
     * enough about the type/scope/collection it represents to say it is not.
     *
     * @param cid The collection-ID to check.
     * @return if the key should be allowed to be sent on the DcpStream
     */
    bool check(DocKeyView key) const {
        // passthrough, everything is allowed.
        if (isPassThroughFilter()) {
            return true;
        }

        // The presence of _default is a simple check against defaultAllowed
        if (key.isInDefaultCollection() && defaultAllowed) {
            return true;
        }
        // More complex checks needed...
        return checkSlow(key);
    }

    /**
     * @return if the filter is empty
     */
    bool empty() const;

    /**
     * Check the privilege revision for any change. If changed update our copy
     * of the revision and then evaluate the required privileges.
     * @return engine status - success/no_access/unknown_scope|collection
     */
    cb::engine_errc checkPrivileges(CookieIface& cookie,
                                    const EventuallyPersistentEngine& engine);

    /**
     * Add statistics for this filter, currently just depicts the object's state
     */
    void addStats(const AddStatFn& add_stat, CookieIface& c, Vbid vb) const;

    /// @return the size of the filter
    size_t size() const {
        return filter.size();
    }

    CollectionID front() const {
        return filter.begin()->first;
    }

    cb::mcbp::DcpStreamId getStreamId() const {
        return streamId;
    }

    // The filter maps from CollectionID to meta-data that is needed for future
    // privilege checks. This is stored on construction to avoid relocking the
    // VB::Manifest from a running DCP stream, which incurs a lock inversion
    // with the stream container lock.
    using Container = ::std::unordered_map<CollectionID, DcpFilterMeta>;
    Container::const_iterator begin() const {
        return filter.begin();
    }

    Container::const_iterator end() const {
        return filter.end();
    }

    Container::const_iterator find(CollectionID cid) const {
        return filter.find(cid);
    }

    /**
     * Method to check if the filter does not filter collections
     * @return true if the filter is a pass-through filter
     */
    bool isPassThroughFilter() const {
        return filterType == FilterType::Passthrough;
    }

    /**
     * Method to check if the filter only allows the default collection
     * @return true if this filter constructed for a non-collection aware client
     */
    bool isLegacyFilter() const {
        return filterType == FilterType::Legacy;
    }

    /**
     * Method to check if the filter represents a collection filter, one which
     * allows a subset of collections. Note a legacy filter is different, it
     * allows only the default collection and no system events. Whereas this
     * returns true is a subset of collections is permitted and their system
     * events.
     */
    bool isCollectionFilter() const {
        return filterType == FilterType::Collection;
    }

    /**
     * @return true if this filter constructed a single scope
     */
    bool isScopeFilter() const {
        return filterType == FilterType::Scope;
    }

    /**
     * Method to check if the filter permits User visible collections and denies
     * System visible collections.
     * @return true if the filter is a pass-through filter
     */
    bool isUserVisibleFilter() const {
        return filterType == FilterType::UserVisible;
    }

    /**
     * Check if the filter is suitable for an OSO snapshot given the limit.
     * Currently a !passthrough filter with a size that does not exceed the
     * limit would result in true.
     *
     * @return true if an OSO snapshot is ok for this filter
     */
    bool isOsoSuitable(size_t limit) const;

    struct CollectionSizeStats {
        /**
         * The number of items stored in the collections in the filter.
         */
        size_t itemCount;
        /**
         * The tracked size (in bytes) of the collections in the filter on disk.
         */
        size_t diskSize;
    };

    /**
     * Get aggregated collection stats for all collections in the filter. Any
     * collections which do not exist are ignored from the aggregate.
     */
    CollectionSizeStats getSizeStats(const Manifest& manifest) const;

    /**
     * @return the purgeSeqno which may of been set in the JSON object
     */
    uint64_t getRemotePurgeSeqno() const {
        return remotePurgeSeqno;
    }

    /**
     * Dump this to std::cerr
     */
    void dump() const;

    /**
     * @return a summary of this filter used in streamRequest logging
     */
    std::string summary() const;

protected:
    /**
     * Constructor helper method for parsing the JSON
     * @return first:success or unknown_collection/unknown_scope, second
     *         manifest-ID when first is != success.
     */
    [[nodiscard]] std::pair<cb::engine_errc, uint64_t> constructFromJson(
            const nlohmann::json& json,
            const Collections::VB::Manifest& manifest);

    /**
     * Private helper to examine the given collection object against the
     * manifest and add to internal container or throw an exception
     * @return true if collection is known and added
     */
    [[nodiscard]] bool addCollection(const nlohmann::json& object,
                                     const ::Collections::VB::ReadHandle& rh);

    /**
     * Private helper to examine the given scope object against the manifest and
     * add the associated collections to the internal container
     * @return true if scope is known and added
     */
    [[nodiscard]] bool addScope(const nlohmann::json& object,
                                const ::Collections::VB::ReadHandle& rh);

    /**
     * Does the filter allow the system event? I.e. a "meat,dairy" filter
     * shouldn't allow delete events for the "fruit" collection.
     *
     * May update the filter if we are filtering on scopes and the event is
     * an add or delete collection.
     *
     * @param item a SystemEventMessage to check
     * @param return true if the filter says this event should be allowed
     */
    bool checkAndUpdateSystemEvent(const Item& item);

    /// Non-inline, slow path of checkAndUpdate().
    bool checkAndUpdateSlow(Item& item);

    /// Non-inline slow path of check(key)
    bool checkSlow(DocKeyView key) const;

    /**
     * Remove the collection of the item from the filter
     *
     * @param item a SystemEventMessage to check
     * @return true if a collection was removed from this filter
     */
    bool remove(const Item& item);

    /**
     * Called Item represents a collection system event
     */
    bool processCollectionEvent(const Item& item);

    /**
     * Called Item represents a scope system event
     */
    bool processScopeEvent(const Item& item);

    /**
     * Enable the default collection at construction time
     */
    void enableDefaultCollection();

    /**
     * Disable the default collection
     */
    void disableDefaultCollection();

    /**
     * Insert the collection, will toggle defaultAllowed if found
     */
    void insertCollection(CollectionID cid, DcpFilterMeta filterData);

    Container filter;
    // For FilterType::Scope - the scope
    ScopeID scopeID;
    Visibility filteredScopeVisibility{Visibility::User};

    // use an optional so we don't use any special values to mean unset
    std::optional<uint32_t> lastCheckedPrivilegeRevision;
    cb::mcbp::DcpStreamId streamId = {};
    bool scopeIsDropped = false;
    bool defaultAllowed = false;
    uint64_t remotePurgeSeqno{0};

    enum class FilterType {
        // The "bucket stream" - everything is allowed
        Passthrough,

        // The "bucket stream" for clients that don't understand collections.
        // Only the default collection is visible
        Legacy,

        // The "bucket stream" for clients that don't have access to system
        // collections. This is a mix of Collection and Scope behaviour - it
        // filters for all non-system collections and also is dynamic in that
        // new user visible collections are added.
        UserVisible,

        // A collection stream - user requested a set of collections and only
        // those collections are allowed to pass the filter
        Collection,

        // A scope stream - user requested a single scope and only that scope is
        // allowed to pass the filter (this is dynamic as new collections in the
        // scope will grow the filter set)
        Scope
    } filterType;

    static std::string to_string(FilterType);

    friend std::ostream& operator<<(std::ostream& os, const Filter& filter);

    // keys and types used in JSON parsing
    static const char* CollectionsKey;
    static const char* ScopeKey;
    static const char* UidKey;
    static const char* StreamIdKey;
    static const char* PurgeSeqnoKey;
};

std::ostream& operator<<(std::ostream& os, const Filter& filter);

} // namespace Collections::VB

template <>
struct fmt::formatter<Collections::VB::Filter> : ostream_formatter {};
