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

#include "collections/filter.h"
#include "collections/collections_dockey.h"
#include "collections/collections_types.h"

#include <JSON_checker.h>
#include <cJSON.h>
#include <cJSON_utils.h>
#include <memcached/engine_error.h>
#include <platform/make_unique.h>

#include <iostream>

/**
 * Construct a Collections::Filter, optionally using a JSON document with the
 * following format specifying which collections are allowed DcpStreams created
 * from a DcpProducer who owns this.
 *
 * Format:
 *   {"collection":["collection1", "collection2"]}
 *
 * Each collection in the "collection" array must be found in the Manifest
 *
 * @param jsonFilter an optional std::string. If this is not initialised than
 *        a legacy (non-collection) DcpProducer is being opened.
 * @param manifest the Manifest (bucket manifest) to check the filter is valid
 *        which can be null in the case when the bucket has not been told the
 *        manifest.
 * @throws invalid_argument for input errors (with detailed message)
 */
Collections::Filter::Filter(boost::optional<const std::string&> jsonFilter,
                            const Manifest* manifest)
    : defaultAllowed(false), passthrough(false), systemEventsAllowed(true) {
    // If the jsonFilter is not initialised we are building a filter for a
    // legacy DCP stream, one which could only ever support $default
    if (!jsonFilter.is_initialized()) {
        // 1. If there's no manifest, we'll allow the construction, $default may
        //    or may not exist, streamRequest will re-check.
        // 2. If manifest is specified then we can check for $default.
        if (!manifest || manifest->doesDefaultCollectionExist()) {
            defaultAllowed = true;

            // This filter is for a 'legacy' producer which will not know about
            // system events.
            systemEventsAllowed = false;
            return;
        } else {
            throw cb::engine_error(cb::engine_errc::unknown_collection,
                                   "Filter::Filter no $default");
        }
    }

    auto json = jsonFilter.get();

    // The filter is the empty string. Create this Filter to be:
    // 1. passthrough - all collections allowed
    // 2. defaultAllowed -  all $default items allowed
    // 3. systemEventsAllowed - the client will be informed of system events
    //    this is already set by the initializer list.
    if (json.empty()) {
        passthrough = true;
        defaultAllowed = true;
        return;
    }

    if (!checkUTF8JSON(reinterpret_cast<const unsigned char*>(json.data()),
                       json.size())) {
        throw cb::engine_error(cb::engine_errc::invalid_arguments,
                               "Filter::Filter input not valid jsonFilter:" +
                                       jsonFilter.get());
    }

    unique_cJSON_ptr cjson(cJSON_Parse(json.c_str()));
    if (!cjson) {
        throw cb::engine_error(cb::engine_errc::invalid_arguments,
                               "Filter::Filter cJSON cannot parse jsonFilter:" +
                                       jsonFilter.get());
    }

    // Now before processing the JSON we must have a manifest. We cannot create
    // a filtered producer without a manifest.
    if (!manifest) {
        throw cb::engine_error(cb::engine_errc::no_collections_manifest,
                               "Filter::Filter no manifest");
    }

    // @todo null check manifest to go past this point. Will be done along with
    // an appropriate error-code

    auto jsonCollections = cJSON_GetObjectItem(cjson.get(), "collections");
    if (!jsonCollections || jsonCollections->type != cJSON_Array) {
        throw cb::engine_error(
                cb::engine_errc::invalid_arguments,
                "Filter::Filter cannot find collections:" +
                        (!jsonCollections
                                 ? "nullptr"
                                 : std::to_string(jsonCollections->type)) +
                        ", jsonFilter:" + jsonFilter.get());
    } else {
        for (int ii = 0; ii < cJSON_GetArraySize(jsonCollections); ii++) {
            auto collection = cJSON_GetArrayItem(jsonCollections, ii);
            if (!collection || collection->type != cJSON_String) {
                throw cb::engine_error(
                        cb::engine_errc::invalid_arguments,
                        "Filter::Filter cannot find "
                        "valid collection for index:" +
                                std::to_string(ii) + ", collection:" +
                                (!collection
                                         ? "nullptr"
                                         : std::to_string(collection->type)) +
                                ", jsonFilter:" + jsonFilter.get());
            } else {
                // Can throw..
                addCollection(collection->valuestring, *manifest);
            }
        }
    }
}

void Collections::Filter::addCollection(const char* collection,
                                        const Manifest& manifest) {
    // Is this the default collection?
    if (DefaultCollectionIdentifier == collection) {
        if (manifest.doesDefaultCollectionExist()) {
            defaultAllowed = true;
        } else {
            throw cb::engine_error(cb::engine_errc::unknown_collection,
                                   "Filter::addCollection no $default");
        }
    } else {
        if (manifest.find(collection) != manifest.end()) {
            filter.push_back(collection);
        } else {
            throw cb::engine_error(cb::engine_errc::unknown_collection,
                                   "Filter::addCollection unknown collection:" +
                                           std::string(collection));
        }
    }
}

void Collections::Filter::dump() const {
    std::cerr << *this << std::endl;
}

std::ostream& Collections::operator<<(std::ostream& os,
                                      const Collections::Filter& filter) {
    os << "Collections::Filter"
       << ": passthrough:" << filter.passthrough
       << ", defaultAllowed:" << filter.defaultAllowed
       << ", systemEventsAllowed:" << filter.systemEventsAllowed
       << ", filter.size:" << filter.filter.size() << std::endl;
    for (const auto& entry : filter.filter) {
        os << entry << std::endl;
    }
    return os;
}
