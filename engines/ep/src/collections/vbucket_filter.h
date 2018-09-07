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

#include "item.h"

#include <memcached/dockey.h>
#include <memcached/engine_common.h>
#include <nlohmann/json.hpp>
#include <platform/sized_buffer.h>

#include <memory>
#include <string>
#include <unordered_set>

class SystemEventMessage;

namespace Collections {

class Filter;

namespace VB {

class Manifest;

/**
 * The VB filter is used to decide if keys on a DCP stream should be sent
 * or dropped.
 *
 * A filter is built from the Collections::Filter that was established when
 * the producer was opened. During the time the producer was opened and a
 * stream is requested, filtered collections may have been deleted, so the
 * ::VB::Filter becomes the intersection of the producer's filter and the
 * open collections within the manifest.
 *
 * Note: There is no locking on a VB::Filter as at the moment it is constructed
 * and then is not mutable.
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
     * Check the item and if required, update the filter.
     * If the item represents a collection deletion and this filter matches the
     * collection, we must update the filter so that no more matching items
     * would be allowed.
     *
     * @param item an Item to be processed.
     * @return if the Item is allowed to be sent on the DcpStream
     */
    bool checkAndUpdate(const Item& item) {
        // passthrough, everything is allowed.
        if (passthrough) {
            return true;
        }

        // The presence of _default is a simple check against defaultAllowed
        if (item.getKey().getCollectionID() == CollectionID::Default &&
            defaultAllowed) {
            return true;
        }
        // More complex checks needed...
        return checkAndUpdateSlow(item);
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
     * Dump this to std::cerr
     */
    void dump() const;

protected:
    /**
     * Private helper to examine the given collection object against the
     * manifest and add to internal container or throw an exception
     */
    void addCollection(const nlohmann::json& object,
                       const ::Collections::VB::Manifest& manifest);

    /**
     * Does the filter allow the system event? I.e. a "meat,dairy" filter
     * shouldn't allow delete events for the "fruit" collection.
     *
     * @param item a SystemEventMessage to check
     * @param return true if the filter says this event should be allowed
     */
    bool allowSystemEvent(const Item& item) const;

    /// Non-inline, slow path of checkAndUpdate().
    bool checkAndUpdateSlow(const Item& item);

    /**
     * Remove the collection of the item from the filter
     */
    void remove(const Item& item);

    using Container = ::std::unordered_set<CollectionID>;
    Container filter;
    bool defaultAllowed = false;
    bool passthrough = false;
    bool systemEventsAllowed = false;

    // strings used in JSON parsing
    static constexpr char const* CollectionsKey = "collections";
    static constexpr nlohmann::json::value_t CollectionsType =
            nlohmann::json::value_t::array;

    friend std::ostream& operator<<(std::ostream& os, const Filter& filter);
};

std::ostream& operator<<(std::ostream& os, const Filter& filter);

} // end namespace VB
} // end namespace Collections
