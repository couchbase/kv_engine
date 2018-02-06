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

#include <string>
#include <vector>

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
 *  3. A "filter", only items and events of named collections are allowed.
 *
 * Because DCP construction is a two-step operation, first create DcpProducer
 * second create stream(s), and between those two steps collections could be
 * deleted, the stream cannot police the producer's filter contents as a
 * collection mis-match due to legitimate deletion cannot be distinguished from
 * junk-input. Hence the construction of the Collections::Filter performs
 * validation that the object is sensible with respect to the bucket's
 * collections configuration. For example, a legacy filter should not be created
 * if the bucket has removed the $default collection. The same logic applies to
 * a proper filter, all named collections must be known at the time of creation.
 *
 * When creating a "filter" two flavours of JSON input are valid:
 * Streaming from zero (i.e. you don't know the UID)
 *   {"collections" : ["name1", "name2", ...]}
 *
 * Streaming from non-zero seqno (you need the correct uid):
 *   {"collections" : [{"name":"name1", "uid":"xxx"},
 *                     {"name":"name2", "uid":"yyy"}, ...]}
 *
 */
class Filter {
public:
    /// a name with an optional UID
    using container =
            std::vector<std::pair<std::string, boost::optional<uid_t>>>;

    enum class Type {
        Name, // Filter is name-only
        NameUid // Filter contains name:uid entries
    };

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
     * @params jsonFilter an optional string as described above.
     * @params manifest pointer to the current manifest, can be null if no
     *         manifest has been set.
     * @throws invalid_argument if the JSON is invalid or contains unknown
     *         collections.
     */
    Filter(boost::optional<const std::string&> jsonFilter,
           const Manifest* manifest);

    /**
     * Get the list of collections the filter knows about. Can be empty
     * @returns std::vector of std::string, maybe empty for a passthrough filter
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
     * @return the Type the Filter was created from
     */
    Type getType() const {
        return type;
    }

    /**
     * @return true if the Filter uses name only entries
     */
    bool isNameFilter() const {
        return getType() == Type::Name;
    }

    /**
     * @return true if the Filter uses name only entries
     */
    bool isNameUidFilter() const {
        return getType() == Type::NameUid;
    }

    /**
     * Dump this to std::cerr
     */
    void dump() const;

private:
    /**
     * Private helper to examine the given collection name against the manifest
     * and add to internal container or throw an exception
     */
    void addCollection(const char* collection, const Manifest& manifest);

    /**
     * Private helper to examine the given collection object against the
     * manifest and add to internal container or throw an exception
     */
    void addCollection(cJSON* object, const Manifest& manifest);

    /// A container of named collections to allow, can be empty.
    container filter;

    /// Are default collection items allowed?
    bool defaultAllowed;
    /// Should everything be allowed (i.e. ignore the vector of names)
    bool passthrough;
    /// Should system events be allowed?
    bool systemEventsAllowed;

    /// The type of filter that was configured
    Type type;

    friend std::ostream& operator<<(std::ostream& os, const Filter& filter);
};

std::ostream& operator<<(std::ostream& os, const Filter& filter);

} // end namespace Collections
