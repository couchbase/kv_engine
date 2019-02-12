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

namespace Collections {

static constexpr nlohmann::json::value_t CollectionsType =
        nlohmann::json::value_t::array;
static constexpr nlohmann::json::value_t ScopeType =
        nlohmann::json::value_t::string;
static constexpr nlohmann::json::value_t UidType =
        nlohmann::json::value_t::string;
static constexpr nlohmann::json::value_t StreamIdType =
        nlohmann::json::value_t::number_unsigned;

namespace VB {

Filter::Filter(boost::optional<cb::const_char_buffer> jsonFilter,
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

    // If the filter is initialised that means collections are enabled so system
    // events are allowed
    systemEventsAllowed = true;

    // Assume passthrough/defaultAllowed constructFromJson will correct
    passthrough = true;
    defaultAllowed = true;
    if (json.empty()) {
        return;
    } else {
        try {
            constructFromJson(json, manifest);
        } catch (const std::invalid_argument& e) {
            // json utilities may throw invalid_argument
            throw cb::engine_error(cb::engine_errc::invalid_arguments,
                                   e.what());
        }
    }
}

void Filter::constructFromJson(cb::const_char_buffer json,
                               const Collections::VB::Manifest& manifest) {
    nlohmann::json parsed;
    try {
        parsed = nlohmann::json::parse(json);
    } catch (const nlohmann::json::exception& e) {
        throw cb::engine_error(
                cb::engine_errc::invalid_arguments,
                "Filter::constructFromJson cannot parse jsonFilter:" +
                        cb::to_string(json) + " json::exception:" + e.what());
    }

    const auto streamIdObject = parsed.find(StreamIdKey);
    if (streamIdObject != parsed.end()) {
        auto sid = cb::getJsonObject(
                parsed, StreamIdKey, StreamIdType, "Filter::constructFromJson");
        streamId = cb::mcbp::DcpStreamId(sid.get<uint16_t>());
        if (streamId == cb::mcbp::DcpStreamId(0)) {
            throw cb::engine_error(cb::engine_errc::dcp_streamid_invalid,
                                   "Filter::constructFromJson illegal sid:0:" +
                                           cb::to_string(json));
        }
    }

    const auto uidObject = parsed.find(UidKey);
    // Check if a uid is specified and parse it
    if (uidObject != parsed.end()) {
        auto jsonUid = cb::getJsonObject(
                parsed, UidKey, UidType, "Filter::constructFromJson");
        uid = makeUid(jsonUid.get<std::string>());

        // Critical - if the client has a uid ahead of the vbucket, tempfail
        // we expect ns_server to update us to the latest manifest.
        auto vbUid = manifest.lock().getManifestUid();
        if (*uid > vbUid) {
            throw cb::engine_error(
                    cb::engine_errc::collections_manifest_is_ahead,
                    "Filter::constructFromJson client is ahead client:uid:" +
                            std::to_string(*uid) +
                            ", vb:uid:" + std::to_string(vbUid));
        }
    }

    const auto scopesObject = parsed.find(ScopeKey);
    const auto collectionsObject = parsed.find(CollectionsKey);
    if (scopesObject != parsed.end()) {
        if (collectionsObject != parsed.end()) {
            throw cb::engine_error(cb::engine_errc::invalid_arguments,
                                   "Filter::constructFromJson cannot specify "
                                   "both scope and collections");
        }
        passthrough = false;
        defaultAllowed = false;
        auto scope = cb::getJsonObject(
                parsed, ScopeKey, ScopeType, "Filter::constructFromJson");
        addScope(scope, manifest);
    } else {
        if (collectionsObject != parsed.end()) {
            passthrough = false;
            defaultAllowed = false;
            auto jsonCollections =
                    cb::getJsonObject(parsed,
                                      CollectionsKey,
                                      CollectionsType,
                                      "Filter::constructFromJson");

            for (const auto& entry : jsonCollections) {
                cb::throwIfWrongType(std::string(CollectionsKey),
                                     entry,
                                     nlohmann::json::value_t::string);
                addCollection(entry, manifest);
            }
        } else if (uidObject == parsed.end()) {
            // The input JSON must of contained at least a UID, scope, or
            // collections
            //  * {} is valid JSON but is invalid for this class
            //  * {uid:4} is OK - client wants everything (non-zero start)
            //  * {collections:[...]} - is OK - client wants some collections
            //  from epoch
            //  * {uid:4, collections:[...]} - is OK
            //  * {sid:4} - is OK
            //  * {uid:4, sid:4} - is OK
            if (collectionsObject == parsed.end() &&
                uidObject == parsed.end()) {
                throw cb::engine_error(cb::engine_errc::invalid_arguments,
                                       "Filter::constructFromJson no uid or "
                                       "collections found");
            }
        }
    }
}

void Filter::addCollection(const nlohmann::json& object,
                           const Collections::VB::Manifest& manifest) {
    // Require that the requested collection exists in the manifest.
    // DCP cannot filter an unknown collection.
    auto cid = makeCollectionID(object.get<std::string>());
    {
        auto rh = manifest.lock();
        if (!rh.exists(cid)) {
            // Error time
            throw cb::engine_error(cb::engine_errc::unknown_collection,
                                   "Filter::addCollection unknown collection:" +
                                           cid.to_string());
        }
    }

    if (cid.isDefaultCollection()) {
        defaultAllowed = true;
    } else {
        this->filter.insert(cid);
    }
}

void Filter::addScope(const nlohmann::json& object,
                      const Collections::VB::Manifest& manifest) {
    // Require that the requested scope exists in the manifest.
    // DCP cannot filter an unknown scope.
    auto sid = makeScopeID(object.get<std::string>());
    boost::optional<std::vector<CollectionID>> collections =
            manifest.lock().getCollectionsForScope(sid);

    if (!collections) {
        // Error time - the scope does not exist
        throw cb::engine_error(
                cb::engine_errc::unknown_scope,
                "Filter::addScope unknown scope:" + sid.to_string());
    }

    scopeID = sid;
    for (const auto& cid : *collections) {
        if (cid.isDefaultCollection()) {
            defaultAllowed = true;
        } else {
            this->filter.insert(cid);
        }
    }
}

bool Filter::checkAndUpdateSlow(CollectionID cid, const Item& item) {
    bool allowed = false;
    if (cid == CollectionID::System) {
        allowed = checkAndUpdateSystemEvent(item);
    } else {
        allowed = filter.count(cid);
    }

    return allowed;
}

bool Filter::remove(const Item& item) {
    if (passthrough) {
        return false;
    }

    CollectionID collection = getCollectionIDFromKey(item.getKey());
    if (collection == CollectionID::Default && defaultAllowed) {
        defaultAllowed = false;
        return true;
    } else {
        return filter.erase(collection);
    }
}

bool Filter::empty() const {
    if (scopeID) {
        return scopeIsDropped;
    }

    return (filter.empty() && !defaultAllowed);
}

bool Filter::checkAndUpdateSystemEvent(const Item& item) {
    switch (SystemEvent(item.getFlags())) {
    case SystemEvent::Collection:
        return processCollectionEvent(item);
    case SystemEvent::Scope:
        return processScopeEvent(item);
    default:
        throw std::invalid_argument(
                "Filter::checkAndUpdateSystemEvent:: event unknown:" +
                std::to_string(int(item.getFlags())));
    }
}

bool Filter::processCollectionEvent(const Item& item) {
    bool deleted = false;
    if (item.isDeleted()) {
        // Save the return value (indicating if something has actually been
        // deleted) so that we can return it if we want to send this event
        deleted = remove(item);
    }

    if (!systemEventsAllowed) {
        // Legacy filters do not support system events
        return false;
    }

    CollectionID cid;
    ManifestUid manifestUid = 0;
    boost::optional<ScopeID> sid;
    if (!item.isDeleted()) {
        auto dcpData = VB::Manifest::getCreateEventData(
                {item.getData(), item.getNBytes()});
        manifestUid = dcpData.manifestUid;
        sid = dcpData.metaData.sid;
        cid = dcpData.metaData.cid;
    } else {
        auto dcpData = VB::Manifest::getDropEventData(
                {item.getData(), item.getNBytes()});
        manifestUid = dcpData.manifestUid;
        sid = dcpData.sid;
        cid = dcpData.cid;
    }

    // Only consider this if it is an event the client hasn't observed
    if (!uid || (manifestUid > *uid)) {
        if (passthrough || deleted ||
            (cid.isDefaultCollection() && defaultAllowed)) {
            return true;
        }

        // If scopeID is initialized then we are filtering on a scope
        if (sid && scopeID && (sid == scopeID)) {
            if (item.isDeleted()) {
                // The item is a drop collection from the filtered scope. The
                // ::filter std::set should not store this collection, but it
                // should be included in a DCP stream which cares for the scope.
                // Return true and take no further actions.
                return true;
            } else {
                // update the filter set as this collection is in our scope
                filter.insert(cid);
            }
        }

        // When filtered allow only if there is a match
        return filter.count(cid) > 0;
    }
    return false;
}

bool Filter::processScopeEvent(const Item& item) {
    if (!systemEventsAllowed) {
        // Legacy filters do not support system events
        return false;
    }

    // scope filter we check if event matches our scope
    // passthrough we check if the event manifest-id is greater than the clients
    if (scopeID || passthrough) {
        ScopeID sid = 0;
        ManifestUid manifestUid = 0;

        if (!item.isDeleted()) {
            auto dcpData = VB::Manifest::getCreateScopeEventData(
                    {item.getData(), item.getNBytes()});
            manifestUid = dcpData.manifestUid;
            sid = dcpData.sid;
        } else {
            auto dcpData = VB::Manifest::getDropScopeEventData(
                    {item.getData(), item.getNBytes()});
            manifestUid = dcpData.manifestUid;
            sid = dcpData.sid;

            if (sid == scopeID) {
                // Scope dropped - ::empty must now return true
                scopeIsDropped = true;
            }
        }

        // Only consider this if it is an event the client hasn't observed
        if (!uid || (manifestUid > *uid)) {
            return sid == scopeID || passthrough;
        }
    }

    return false;
}

void Filter::addStats(const AddStatFn& add_stat,
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

        if (scopeID) {
            checked_snprintf(buffer,
                             bsize,
                             "%s:filter_%d_scope_id",
                             prefix.c_str(),
                             vb.get());
            add_casted_stat(buffer, *scopeID, add_stat, c);
            checked_snprintf(buffer,
                             bsize,
                             "%s:filter_%d_scope_dropped",
                             prefix.c_str(),
                             vb.get());
            add_casted_stat(buffer, scopeIsDropped, add_stat, c);
        }

        checked_snprintf(
                buffer, bsize, "%s:filter_%d_uid", prefix.c_str(), vb.get());
        add_casted_stat(buffer, getUid(), add_stat, c);

        checked_snprintf(
                buffer, bsize, "%s:filter_%d_sid", prefix.c_str(), vb.get());
        add_casted_stat(buffer, streamId.to_string(), add_stat, c);

        checked_snprintf(
                buffer, bsize, "%s:filter_%d_size", prefix.c_str(), vb.get());
        add_casted_stat(buffer, filter.size(), add_stat, c);
    } catch (std::exception& error) {
        EP_LOG_WARN(
                "Filter::addStats: {}:{}"
                " exception.what:{}",
                prefix,
                vb,
                error.what());
    }
}

std::string Filter::getUid() const {
    if (uid) {
        return std::to_string(*uid);
    }
    return "none";
}

void Filter::dump() const {
    std::cerr << *this << std::endl;
}

// To use the keys in json::find, they need to be statically allocated
const char* Filter::CollectionsKey = "collections";
const char* Filter::ScopeKey = "scope";
const char* Filter::UidKey = "uid";
const char* Filter::StreamIdKey = "sid";

std::ostream& operator<<(std::ostream& os, const Filter& filter) {
    os << "VBucket::Filter"
       << ": defaultAllowed:" << filter.defaultAllowed
       << ", passthrough:" << filter.passthrough
       << ", systemEventsAllowed:" << filter.systemEventsAllowed
       << ", scopeIsDropped:" << filter.scopeIsDropped;
    if (filter.uid) {
        os << ", uid:" << *filter.uid;
    }

    os << ", sid:" << filter.streamId;
    os << ", filter.size:" << filter.filter.size() << std::endl;
    for (auto& cid : filter.filter) {
        os << "filter:entry:0x" << std::hex << cid << std::endl;
    }
    return os;
}

} // end namespace VB
} // end namespace Collections
