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

#include "collections/manifest.h"

#include <boost/optional/optional.hpp>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <unordered_set>

namespace Collections {

/**
 * Collections::Filter is an object which will be owned by a DcpProducer. The
 * Collections::Filter goal is to ultimately enable the DcpProducer to create
 * ActiveStream filters (Collections::VB::Filters) which do the real job of
 * dropping keys.
 *
 * A Collections::Filter can be constructed so that it is either:
 *  1. A "legacy filter" ensuring DCP streams act like they always did, in this
 *     case only items of the default collection are allowed and any collection
 *     items and SystemEvents are dropped.
 *  2. A "pass-through filter", which means nothing is filtered, allowing for
 *     example full vbucket replication to occur, i.e. every item and event of
 *     every collection is allowed.
 *  3. A "filter", only items and events of specific collections are allowed.
 *
 * DCP construction is a two-step operation, 1) create DcpProducer
 * 2) create VBucket stream(s). Between steps 1 and 2 collections could be
 * deleted, and as such creating filtered DCP is two steps. The construction of
 * the Collections::Filter performs validation that the input is sensible with
 * respect to the bucket's current collection configuration. For example, a
 * legacy filter should not be created if the bucket has removed the $default
 * collection. The same logic applies to a full filter, all collections must be
 * known at the time of creation. Of course once validation checks have passed
 * the state can change, hence the stream construction relies on the VB::Filter
 * to manage the stream inline with the data.
 *
 * When creating a Filter the JSON input specifies the Collection IDs to be
 * streamed which should match the Collection IDs from the Manifest. 0 is
 * allowed.
 *
 *   {"collections" : ["23", "61", ...]}
 *
 */
class Filter {
public:
    /// Store the uid of requested collections
    using container = std::unordered_set<CollectionID>;

    /**
     * Construct a Collections::Filter using an optional JSON document
     * and the bucket's current Manifest.
     *
     * The optional JSON document allows a client to filter a chosen set of
     * collections.
     *
     * if jsonFilter is defined and empty - then we create a passthrough.
     * if jsonFilter is defined and !empty - then we filter as requested.
     * if jsonFilter is not defined (maybe a legacy client who doesn't
     *   understand collections) then only documents with
     *   DocNamespace::DefaultCollection are allowed.
     *
     * @params jsonFilter an optional buffer as described above.
     * @params manifest pointer to the current manifest, can be null if no
     *         manifest has been set.
     * @throws invalid_argument if the JSON is invalid or contains unknown
     *         collections.
     */
    Filter(boost::optional<cb::const_char_buffer> jsonFilter,
           const Manifest* manifest);

    /**
     * Get the set of collections the filter knows about. Can be empty
     * @returns std::unordered_set of uid_t or empty for a passthrough filter
     */
    const container& getFilter() const {
        return filter;
    }

    /**
     * @returns if the filter configured so that it allows everything through?
     */
    bool isPassthrough() const {
        return passthrough;
    }

    /**
     * @returns if the filter contains the default collection
     */
    bool allowDefaultCollection() const {
        return defaultAllowed;
    }

    /**
     * @returns if the filter should allow system events
     */
    bool allowSystemEvents() const {
        return systemEventsAllowed;
    }

    /**
     * Dump this to std::cerr
     */
    void dump() const;

private:
    /**
     * Private helper to examine the given collection object against the
     * manifest and add to internal container or throw an exception
     */
    void addCollection(const nlohmann::json& object, const Manifest& manifest);

    /// A container of named collections to allow, can be empty.
    container filter;

    /// Are default collection items allowed?
    bool defaultAllowed;
    /// Should everything be allowed (i.e. ignore the vector of names)
    bool passthrough;
    /// Should system events be allowed?
    bool systemEventsAllowed;

    friend std::ostream& operator<<(std::ostream& os, const Filter& filter);
};

std::ostream& operator<<(std::ostream& os, const Filter& filter);

} // end namespace Collections
