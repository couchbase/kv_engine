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

#include "collections/collections_types.h"
#include "item.h"

#include <memcached/engine_common.h>
#include <nlohmann/json.hpp>
#include <platform/sized_buffer.h>

#include <memory>
#include <string>
#include <unordered_set>

class SystemEventMessage;

namespace Collections {
namespace VB {

class Manifest;

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
     */
    Filter(boost::optional<cb::const_char_buffer> jsonFilter,
           const ::Collections::VB::Manifest& manifest);

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
    bool checkAndUpdate(const Item& item) {
        // passthrough, everything is allowed.
        if (passthrough) {
            return true;
        }

        const auto cid = item.getKey().getCollectionID();
        // The presence of _default is a simple check against defaultAllowed
        if (cid.isDefaultCollection() && defaultAllowed) {
            return true;
        }
        // More complex checks needed...
        return checkAndUpdateSlow(cid, item);
    }

    /**
     * @return if the filter is empty
     */
    bool empty() const;

    /**
     * Add statistics for this filter, currently just depicts the object's state
     */
    void addStats(ADD_STAT add_stat,
                  const void* c,
                  const std::string& prefix,
                  Vbid vb) const;

    /**
     * Was this filter constructed for a non-collection aware client?
     */
    bool isLegacyFilter() const {
        return !systemEventsAllowed;
    }

    /// @return the size of the filter
    size_t size() const {
        return filter.size();
    }

    /// @return is this filter a passthrough (allows every collection)
    bool isPassthrough() const {
        return passthrough;
    }

    bool allowDefaultCollection() const {
        return defaultAllowed;
    }

    bool allowSystemEvents() const {
        return systemEventsAllowed;
    }

    std::string getUid() const;

    /**
     * Dump this to std::cerr
     */
    void dump() const;

protected:
    /**
     * Constructor helper method for parsing the JSON
     */
    void constructFromJson(cb::const_char_buffer json,
                           const Collections::VB::Manifest& manifest);

    /**
     * Private helper to examine the given collection object against the
     * manifest and add to internal container or throw an exception
     */
    void addCollection(const nlohmann::json& object,
                       const ::Collections::VB::Manifest& manifest);

    /**
     * Private helper to examine the given scope object against the manifest and
     * add the associated collections to the internal container
     */
    void addScope(const nlohmann::json& object,
                  const ::Collections::VB::Manifest& manifest);

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
    bool checkAndUpdateSlow(CollectionID cid, const Item& item);

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

    using Container = ::std::unordered_set<CollectionID>;
    Container filter;
    boost::optional<ScopeID> scopeID;
    bool scopeIsDropped = false;

    bool defaultAllowed = false;
    bool passthrough = false;
    bool systemEventsAllowed = false;
    boost::optional<Collections::ManifestUid> uid;

    friend std::ostream& operator<<(std::ostream& os, const Filter& filter);

    // keys and types used in JSON parsing
    static const char* CollectionsKey;
    static constexpr nlohmann::json::value_t CollectionsType =
            nlohmann::json::value_t::array;
    static const char* ScopeKey;
    static constexpr nlohmann::json::value_t ScopeType =
            nlohmann::json::value_t::string;
    static const char* UidKey;
    static constexpr nlohmann::json::value_t UidType =
            nlohmann::json::value_t::string;
};

std::ostream& operator<<(std::ostream& os, const Filter& filter);

} // end namespace VB
} // end namespace Collections
