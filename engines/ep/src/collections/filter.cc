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
#include <memcached/engine_error.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <memory>

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
Collections::Filter::Filter(boost::optional<cb::const_char_buffer> jsonFilter,
                            const Manifest* manifest)
    : defaultAllowed(false),
      passthrough(false),
      systemEventsAllowed(true),
      type(Type::NoFilter) {
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
    if (json.size() == 0) {
        passthrough = true;
        defaultAllowed = true;
        return;
    }

    if (!checkUTF8JSON(reinterpret_cast<const unsigned char*>(json.data()),
                       json.size())) {
        throw cb::engine_error(cb::engine_errc::invalid_arguments,
                               "Filter::Filter input not valid jsonFilter:" +
                                       cb::to_string(json));
    }

    nlohmann::json parsed;
    try {
        parsed = nlohmann::json::parse(json);
    } catch (const nlohmann::json::exception& e) {
        throw cb::engine_error(cb::engine_errc::invalid_arguments,
                               "Filter::Filter cannot parse jsonFilter:" +
                                       cb::to_string(json) +
                                       " json::exception:" + e.what());
    }

    // Now before processing the JSON we must have a manifest. We cannot create
    // a filtered producer without a manifest.
    if (!manifest) {
        throw cb::engine_error(cb::engine_errc::no_collections_manifest,
                               "Filter::Filter no manifest");
    }

    bool nameFound = false, uidFound = false;
    try {
        auto jsonCollections = parsed.at("collections");
        if (!jsonCollections.is_array()) {
            throw cb::engine_error(
                    cb::engine_errc::invalid_arguments,
                    "Filter::Filter collections is not an array, jsonFilter:" +
                            cb::to_string(json));
        } else {
            for (const auto& entry : jsonCollections) {
                if (entry.is_string()) {
                    // Can throw..
                    addCollection(entry.get<std::string>(), *manifest);
                    nameFound = true;
                } else if (entry.is_object()) {
                    addCollection(entry, *manifest);
                    uidFound = true;
                } else {
                    throw cb::engine_error(cb::engine_errc::invalid_arguments,
                                           "Filter::Filter found invalid array "
                                           "entry jsonFilter:" +
                                                   cb::to_string(json));
                }
            }
        }
    } catch (const nlohmann::json::exception& e) {
        throw cb::engine_error(
                cb::engine_errc::invalid_arguments,
                "Filter::Filter label 'collections' is not found, jsonFilter:" +
                        cb::to_string(json) + " json::exception:" + e.what());
    }

    // Validate that the input hasn't mixed and matched name/uid vs name
    // require one or the other.
    if (uidFound && nameFound) {
        throw cb::engine_error(
                cb::engine_errc::invalid_arguments,
                "Filter::Filter mixed name/uid not allowed jsonFilter:" +
                        cb::to_string(json));
    }
    type = uidFound ? Type::NameUid : Type::Name;
}

void Collections::Filter::addCollection(const std::string& collection,
                                        const Manifest& manifest) {
    // Is this the default collection?
    if (DefaultCollectionIdentifier == collection.c_str()) {
        if (manifest.doesDefaultCollectionExist()) {
            defaultAllowed = true;
        } else {
            throw cb::engine_error(cb::engine_errc::unknown_collection,
                                   "Filter::addCollection no $default");
        }
    } else {
        auto itr = manifest.find(collection.c_str());
        if (itr != manifest.end()) {
            filter.push_back({collection, {}});
        } else {
            throw cb::engine_error(
                    cb::engine_errc::unknown_collection,
                    "Filter::addCollection unknown collection:" + collection);
        }
    }
}

void Collections::Filter::addCollection(const nlohmann::json& object,
                                        const Manifest& manifest) {
    try {
        auto name = object.at("name");
        auto uid = object.at("uid");

        if (!name.is_string() || !uid.is_string()) {
            throw cb::engine_error(
                    cb::engine_errc::invalid_arguments,
                    "Filter::Filter invalid collection entry:" + object.dump());
        }

        auto entry = manifest.find(
                Identifier{{name.get<std::string>()},
                           makeUid(uid.get<std::string>().c_str())});

        if (entry == manifest.end()) {
            throw cb::engine_error(
                    cb::engine_errc::unknown_collection,
                    "Filter::Filter: cannot add unknown collection:" +
                            object.dump());
        } else {
            filter.push_back(
                    {cb::to_string(entry->getName()), {entry->getUid()}});
        }
    } catch (const nlohmann::json::exception& e) {
        throw cb::engine_error(
                cb::engine_errc::invalid_arguments,
                "Filter::Filter invalid collection entry no name/uid entry:" +
                        object.dump() + " json::exception:" + e.what());
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
       << ", systemEventsAllowed:" << filter.systemEventsAllowed;
    switch (filter.getType()) {
    case Collections::Filter::Type::NoFilter:
        os << ", type:no-filter";
        break;
    case Collections::Filter::Type::Name:
        os << ", type:name";
        break;
    case Collections::Filter::Type::NameUid:
        os << ", type:name-uid";
        break;
    }
    os << ", filter.size:" << filter.filter.size() << std::endl;
    for (const auto& entry : filter.filter) {
        os << entry.first;
        if (entry.second.is_initialized()) {
            os << ":" << entry.second.get();
        }
        os << std::endl;
    }
    return os;
}
