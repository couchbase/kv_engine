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

#include "collections/vbucket_filter.h"
#include "collections/vbucket_manifest.h"
#include "bucket_logger.h"
#include "dcp/response.h"
#include "statwriter.h"

#include <JSON_checker.h>
#include <json_utilities.h>
#include <nlohmann/json.hpp>
#include <platform/checked_snprintf.h>
#include <memory>

Collections::VB::Filter::Filter(
        boost::optional<cb::const_char_buffer> jsonFilter,
        const Collections::VB::Manifest& manifest) {
    // If the jsonFilter is not initialised we are building a filter for a
    // legacy DCP stream, one which could only ever support _default
    if (!jsonFilter.is_initialized()) {
        // Ask the manifest object if the default collection exists?
        if (manifest.lock().doesDefaultCollectionExist()) {
            defaultAllowed = true;
            return;
        } else {
            throw cb::engine_error(cb::engine_errc::unknown_collection,
                                   "Filter::Filter cannot make filter - no "
                                   "_default collection");
        }
    }

    auto json = jsonFilter.get();

    // If the filter is initialised, collections are enabled - system events are
    // allowed
    systemEventsAllowed = true;

    // If the filter is empty, then everything is allowed on DCP
    if (json.size() == 0) {
        passthrough = true;
        defaultAllowed = true;
        return;
    }

    // Filter is non-zero, it should contain JSON filter data, so let's parse it
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

    auto jsonCollections = cb::getJsonObject(
            parsed, CollectionsKey, CollectionsType, "Filter::Filter");

    for (const auto& entry : jsonCollections) {
        cb::throwIfWrongType(std::string(CollectionsKey),
                             entry,
                             nlohmann::json::value_t::string);
        addCollection(entry, manifest);
    }
}

void Collections::VB::Filter::addCollection(
        const nlohmann::json& object,
        const Collections::VB::Manifest& manifest) {
    // Require that the requested collection exists in the manifest.
    // DCP cannot filter an unknown collection.
    auto uid = makeCollectionID(object.get<std::string>());
    {
        auto rh = manifest.lock();
        if (!rh.isCollectionOpen(uid)) {
            // Error time
            throw cb::engine_error(
                    cb::engine_errc::unknown_collection,
                    "Filter::Filter unknown collection:" + uid.to_string());
        }
    }

    if (uid.isDefaultCollection()) {
        defaultAllowed = true;
    } else {
        this->filter.insert(uid);
    }
}

bool Collections::VB::Filter::checkAndUpdateSlow(const Item& item) {
    bool allowed = false;

    if (item.getKey().getCollectionID() == DocNamespace::System) {
        allowed = allowSystemEvent(item);

        if (item.isDeleted()) {
            remove(item);
        }
    } else {
        allowed = filter.count(item.getKey().getCollectionID());
    }

    return allowed;
}

void Collections::VB::Filter::remove(const Item& item) {
    if (passthrough) {
        return;
    }

    CollectionID collection =
            VB::Manifest::getCollectionIDFromKey(item.getKey());
    if (collection == CollectionID::Default) {
        defaultAllowed = false;
    } else {
        filter.erase(collection);
    }
}

bool Collections::VB::Filter::empty() const {
    return filter.empty() && !defaultAllowed;
}

bool Collections::VB::Filter::allowSystemEvent(const Item& item) const {
    if (!systemEventsAllowed) {
        return false;
    }
    switch (SystemEvent(item.getFlags())) {
    case SystemEvent::Collection: {
        CollectionID collection =
                VB::Manifest::getCollectionIDFromKey(item.getKey());
        if ((collection == CollectionID::Default && defaultAllowed) ||
            passthrough) {
            return true;
        } else {
            // These events are sent only if they relate to a collection in the
            // filter
            return filter.count(collection) > 0;
        }
    }
    case SystemEvent::DeleteCollectionHard: {
        return false;
    }
    default: {
        throw std::invalid_argument(
                "VB::Filter::allowSystemEvent:: event unknown:" +
                std::to_string(int(item.getFlags())));
    }
    }
}

void Collections::VB::Filter::addStats(ADD_STAT add_stat,
                                       const void* c,
                                       const std::string& prefix,
                                       Vbid vb) const {
    try {
        const int bsize = 1024;
        char buffer[bsize];
        checked_snprintf(buffer,
                         bsize,
                         "%s:filter_%d_passthrough",
                         prefix.c_str(),
                         vb.get());
        add_casted_stat(buffer, passthrough, add_stat, c);

        checked_snprintf(buffer,
                         bsize,
                         "%s:filter_%d_default_allowed",
                         prefix.c_str(),
                         vb.get());
        add_casted_stat(buffer, defaultAllowed, add_stat, c);

        checked_snprintf(buffer,
                         bsize,
                         "%s:filter_%d_system_allowed",
                         prefix.c_str(),
                         vb.get());
        add_casted_stat(buffer, systemEventsAllowed, add_stat, c);

        checked_snprintf(
                buffer, bsize, "%s:filter_%d_size", prefix.c_str(), vb.get());
        add_casted_stat(buffer, filter.size(), add_stat, c);
    } catch (std::exception& error) {
        EP_LOG_WARN(
                "Collections::VB::Filter::addStats: {}:{}"
                " exception.what:{}",
                prefix,
                vb,
                error.what());
    }
}

void Collections::VB::Filter::dump() const {
    std::cerr << *this << std::endl;
}

std::ostream& Collections::VB::operator<<(
        std::ostream& os, const Collections::VB::Filter& filter) {
    os << "VBucket::Filter"
       << ": defaultAllowed:" << filter.defaultAllowed
       << ", passthrough:" << filter.passthrough
       << ", systemEventsAllowed:" << filter.systemEventsAllowed;

    os << ", filter.size:" << filter.filter.size() << std::endl;
    for (auto& cid : filter.filter) {
        os << std::hex << cid << "," << std::endl;
    }
    return os;
}
