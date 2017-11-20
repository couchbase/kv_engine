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
#include <platform/make_unique.h>

#include <iostream>

/**
 * Create a filter from a JSON object with the following format:
 *
 *   {"collection":["collection1", "collection2"]}
 *
 * Each element of the "collection" array must be collections known to the
 * specified Manifest.
 *
 * @param jsonFilter a buffer containing an optional JSON object to initialise
 *        from
 * @param manifest the Manifest (bucket manifest) to check the filter is valid
 * @throws invalid_argument for input errors (with detailed message)
 */
Collections::Filter::Filter(boost::optional<const std::string&> jsonFilter,
                            const Manifest& manifest)
    : defaultAllowed(false), passthrough(false), systemEventsAllowed(true) {
    // If no filter is specified at all, then create a default collection only
    // filter. This filter means the default collection is streamed
    if (!jsonFilter.is_initialized()) {
        // If $default is a collection, then let's filter it
        if (manifest.doesDefaultCollectionExist()) {
            defaultAllowed = true;

            // This filter is for a 'legacy' default only user, so they should
            // never see system events.
            systemEventsAllowed = false;
            return;
        } else {
            throw std::invalid_argument(
                    "Filter::Filter default collection does not exist");
        }
    }

    auto json = jsonFilter.get();

    // An empty filter is specified so all documents are available
    if (json.empty()) {
        passthrough = true;
        defaultAllowed = true;
        return;
    }

    if (!checkUTF8JSON(reinterpret_cast<const unsigned char*>(json.data()),
                       json.size())) {
        throw std::invalid_argument(
                "Filter::Filter input not valid jsonFilter:" +
                jsonFilter.get());
    }

    unique_cJSON_ptr cjson(cJSON_Parse(json.c_str()));
    if (!cjson) {
        throw std::invalid_argument(
                "Filter::Filter cJSON cannot parse jsonFilter:" +
                jsonFilter.get());
    }

    auto jsonCollections = cJSON_GetObjectItem(cjson.get(), "collections");
    if (!jsonCollections || jsonCollections->type != cJSON_Array) {
        throw std::invalid_argument(
                "Filter::Filter cannot find collections:" +
                (!jsonCollections ? "nullptr"
                                  : std::to_string(jsonCollections->type)) +
                ", jsonFilter:" + jsonFilter.get());
    } else {
        for (int ii = 0; ii < cJSON_GetArraySize(jsonCollections); ii++) {
            auto collection = cJSON_GetArrayItem(jsonCollections, ii);
            if (!collection || collection->type != cJSON_String) {
                throw std::invalid_argument(
                        "Filter::Filter cannot find "
                        "valid collection for index:" +
                        std::to_string(ii) + ", collection:" +
                        (!collection ? "nullptr"
                                     : std::to_string(collection->type)) +
                        ", jsonFilter:" + jsonFilter.get());
            } else {
                // Can throw..
                addCollection(collection->valuestring, manifest);
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
            throw std::invalid_argument(
                    "Filter::Filter: $default is not a known collection");
        }
    } else {
        if (manifest.find(collection) != manifest.end()) {
            filter.push_back(collection);
        } else {
            throw std::invalid_argument("Filter::Filter: collection:" +
                                        std::string(collection) +
                                        " is not a known collection");
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
