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

#include "collections/vbucket_filter.h"

#include "bucket_logger.h"
#include "collections/collections_constants.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/response.h"
#include "ep_engine.h"

#include <json_utilities.h>
#include <nlohmann/json.hpp>
#include <platform/checked_snprintf.h>
#include <statistics/cbstat_collector.h>
#include <memory>

namespace Collections::VB {

Filter::Filter(std::optional<std::string_view> jsonFilter,
               const Collections::VB::Manifest& manifest,
               gsl::not_null<const void*> cookie,
               const EventuallyPersistentEngine& engine) {
    cb::engine_errc status = cb::engine_errc::success;
    uint64_t manifestUid{0};
    // If the jsonFilter is not initialised we are building a filter for a
    // legacy DCP stream, one which could only ever support _default
    if (!jsonFilter.has_value()) {
        // Ask the manifest object if the default collection exists?
        auto rh = manifest.lock();
        if (rh.doesDefaultCollectionExist()) {
            enableDefaultCollection();
        } else {
            status = cb::engine_errc::unknown_collection;
            manifestUid = rh.getManifestUid();
        }
    } else {
        auto jsonString = jsonFilter.value();

        // If the filter is initialised that means collections are enabled so
        // system events are allowed
        systemEventsAllowed = true;

        // Assume passthrough constructFromJson will correct if we reach it
        passthrough = true;
        if (!jsonString.empty()) {
            // assume default, constructFromJson will correct based on the JSON
            enableDefaultCollection();

            nlohmann::json json;
            try {
                json = nlohmann::json::parse(jsonString);
            } catch (const nlohmann::json::exception& e) {
                throw cb::engine_error(
                        cb::engine_errc::invalid_arguments,
                        "Filter::Filter cannot parse nlohmann::json::parse "
                        "exception:" +
                                std::string{e.what()} +
                                " json:" + std::string{jsonString});
            }

            try {
                std::tie(status, manifestUid) =
                        constructFromJson(json, manifest);
            } catch (const std::invalid_argument& e) {
                // json utilities may throw invalid_argument, catch and change
                // to cb::engine_error so we return 'EINVAL'
                throw cb::engine_error(cb::engine_errc::invalid_arguments,
                                       e.what());
            }
        }
    }

    if (status == cb::engine_errc::success) {
        // Now use checkPrivileges to check if the user has the required access
        status = checkPrivileges(cookie, engine);
    }

    switch (status) {
    case cb::engine_errc::success:
        break;
    case cb::engine_errc::unknown_scope:
    case cb::engine_errc::unknown_collection:
        engine.setUnknownCollectionErrorContext(cookie, manifestUid);
        [[fallthrough]];
    default:
        throw cb::engine_error(
                status,
                "Filter::Filter failed status:" + cb::to_string(status));
    }
}

std::pair<cb::engine_errc, uint64_t> Filter::constructFromJson(
        const nlohmann::json& json, const Collections::VB::Manifest& manifest) {
    auto rh = manifest.lock();
    const auto streamIdObject = json.find(StreamIdKey);
    if (streamIdObject != json.end()) {
        auto sid = cb::getJsonObject(
                json, StreamIdKey, StreamIdType, "Filter::constructFromJson");
        streamId = cb::mcbp::DcpStreamId(sid.get<uint16_t>());
        if (streamId == cb::mcbp::DcpStreamId(0)) {
            throw cb::engine_error(cb::engine_errc::dcp_streamid_invalid,
                                   "Filter::constructFromJson illegal sid:0");
        }
    }

    // A uid by itself ok, we no longer do anything with it, but we must not
    // fail the user for this.
    const auto uidObject = json.find(UidKey);

    const auto scopesObject = json.find(ScopeKey);
    const auto collectionsObject = json.find(CollectionsKey);
    if (scopesObject != json.end()) {
        if (collectionsObject != json.end()) {
            throw cb::engine_error(cb::engine_errc::invalid_arguments,
                                   "Filter::constructFromJson cannot specify "
                                   "both scope and collections");
        }
        passthrough = false;
        disableDefaultCollection();
        auto scope = cb::getJsonObject(
                json, ScopeKey, ScopeType, "Filter::constructFromJson");
        if (!addScope(scope, rh)) {
            return {cb::engine_errc::unknown_scope, rh.getManifestUid()};
        }
    }

    if (collectionsObject != json.end()) {
        if (scopesObject != json.end()) {
            throw cb::engine_error(cb::engine_errc::invalid_arguments,
                                   "Filter::constructFromJson cannot specify "
                                   "both scope and collections");
        }
        passthrough = false;
        disableDefaultCollection();
        auto jsonCollections = cb::getJsonObject(json,
                                                 CollectionsKey,
                                                 CollectionsType,
                                                 "Filter::constructFromJson");

        for (const auto& entry : jsonCollections) {
            cb::throwIfWrongType(std::string(CollectionsKey),
                                 entry,
                                 nlohmann::json::value_t::string);
            if (!addCollection(entry, rh)) {
                return {cb::engine_errc::unknown_collection,
                        rh.getManifestUid()};
            }
        }
    }

    // The input JSON must of contained at least a sid, uid, scope, or
    // collections key
    if (uidObject == json.end() && collectionsObject == json.end() &&
        scopesObject == json.end() && streamIdObject == json.end()) {
        throw cb::engine_error(
                cb::engine_errc::invalid_arguments,
                "Filter::constructFromJson no sid, uid, scope or "
                "collections found");
    }
    return {cb::engine_errc::success, rh.getManifestUid()};
}

bool Filter::addCollection(const nlohmann::json& object,
                           const Collections::VB::ReadHandle& rh) {
    // Require that the requested collection exists in the manifest.
    // DCP cannot filter an unknown collection.
    auto cid = makeCollectionID(object.get<std::string>());
    ScopeID sid;
    auto scope = rh.getScopeID(cid);
    if (!scope) {
        // Error time
        return false;
    }
    sid = *scope;

    insertCollection(cid, sid);
    return true;
}

bool Filter::addScope(const nlohmann::json& object,
                      const Collections::VB::ReadHandle& rh) {
    // Require that the requested scope exists in the manifest.
    // DCP cannot filter an unknown scope.
    auto sid = makeScopeID(object.get<std::string>());

    auto collectionVector = rh.getCollectionsForScope(sid);

    if (!collectionVector) {
        // Error time - the scope does not exist
        return false;
    }

    scopeID = sid;
    for (CollectionID cid : collectionVector.value()) {
        insertCollection(cid, sid);
    }
    return true;
}

void Filter::insertCollection(CollectionID cid, ScopeID sid) {
    this->filter.insert({cid, sid});
    if (cid.isDefaultCollection()) {
        defaultAllowed = true;
    }
}

bool Filter::checkAndUpdateSlow(Item& item) {
    bool allowed = false;
    if (item.getKey().isInSystemCollection()) {
        item.decompressValue();
        allowed = checkAndUpdateSystemEvent(item);
    } else {
        allowed = filter.count(item.getKey().getCollectionID());
    }

    return allowed;
}

bool Filter::checkSlow(DocKey key) const {
    bool allowed = false;
    if (key.isInSystemCollection() && systemEventsAllowed) {
        // For a collection filter, we could figure out from the entire DocKey
        // however we will just defer the decision to the more comprehensive
        // checkAndUpdate
        allowed = true;
    } else {
        allowed = filter.count(key.getCollectionID());
    }

    return allowed;
}

bool Filter::remove(const Item& item) {
    if (passthrough) {
        return false;
    }

    CollectionID collection = getCollectionIDFromKey(item.getKey());
    if (collection == CollectionID::Default && defaultAllowed) {
        disableDefaultCollection();
        return true;
    } else {
        return filter.erase(collection);
    }
}

bool Filter::empty() const {
    // Passthrough filters are never empty
    if (passthrough) {
        return false;
    }

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
    std::optional<ScopeID> sid;
    if (!item.isDeleted()) {
        auto dcpData = VB::Manifest::getCreateEventData(
                {item.getData(), item.getNBytes()});
        sid = dcpData.metaData.sid;
        cid = dcpData.metaData.cid;
    } else {
        auto dcpData = VB::Manifest::getDropEventData(
                {item.getData(), item.getNBytes()});
        sid = dcpData.sid;
        cid = dcpData.cid;
    }

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
            filter.insert({cid, *sid});
        }
    }

    // When filtered allow only if there is a match
    return filter.count(cid) > 0;
}

bool Filter::processScopeEvent(const Item& item) {
    if (!systemEventsAllowed) {
        // Legacy filters do not support system events
        return false;
    }

    // scope filter we check if event matches our scope
    if (scopeID || passthrough) {
        ScopeID sid = 0;

        if (!item.isDeleted()) {
            auto dcpData = VB::Manifest::getCreateScopeEventData(
                    {item.getData(), item.getNBytes()});
            sid = dcpData.metaData.sid;
        } else {
            auto dcpData = VB::Manifest::getDropScopeEventData(
                    {item.getData(), item.getNBytes()});
            sid = dcpData.sid;

            if (sid == scopeID) {
                // Scope dropped - ::empty must now return true
                scopeIsDropped = true;
            }
        }

        return sid == scopeID || passthrough;
    }

    return false;
}

void Filter::enableDefaultCollection() {
    defaultAllowed = true;
    // For simpler client usage, insert in the set
    filter.insert({CollectionID::Default, ScopeID::Default});
}

void Filter::disableDefaultCollection() {
    defaultAllowed = false;
    filter.erase(CollectionID::Default);
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
            add_casted_stat(buffer, scopeID->to_string(), add_stat, c);
            checked_snprintf(buffer,
                             bsize,
                             "%s:filter_%d_scope_dropped",
                             prefix.c_str(),
                             vb.get());
            add_casted_stat(buffer, scopeIsDropped, add_stat, c);
        }

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

cb::engine_errc Filter::checkPrivileges(
        gsl::not_null<const void*> cookie,
        const EventuallyPersistentEngine& engine) {
    const auto rev = engine.getPrivilegeRevision(cookie);
    if (!lastCheckedPrivilegeRevision ||
        lastCheckedPrivilegeRevision.value() != rev) {
        lastCheckedPrivilegeRevision = rev;
        if (passthrough) {
            // Must have access to the bucket
            return engine.testPrivilege(
                    cookie, cb::rbac::Privilege::DcpStream, {}, {});
        } else if (scopeID) {
            // Must have access to at least the scope
            return engine.testPrivilege(
                    cookie, cb::rbac::Privilege::DcpStream, scopeID, {});
        } else {
            // Must have access to the collections
            bool unknownCollection = false;
            bool accessError = false;

            // Check all collections
            for (const auto& c : filter) {
                const auto status =
                        engine.testPrivilege(cookie,
                                             cb::rbac::Privilege::DcpStream,
                                             c.second,
                                             c.first);
                switch (status) {
                case cb::engine_errc::success:
                    continue;
                case cb::engine_errc::unknown_collection:
                    unknownCollection = true;
                    break;
                case cb::engine_errc::no_access:
                    accessError = true;
                    break;
                default:
                    throw std::logic_error(
                            "Filter::checkPrivileges: unexpected error:" +
                            to_string(status));
                }
            }
            // Ordering here is important - 1 unknown collection in a sea of
            // success/no_access dominates and shall be what is seen
            if (unknownCollection) {
                return cb::engine_errc::unknown_collection;
            } else if (accessError) {
                return cb::engine_errc::no_access;
            }
        }
    }

    return cb::engine_errc::success;
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
    if (filter.scopeID) {
        os << ", scopeID:" << filter.scopeID.value();
    }
    if (filter.lastCheckedPrivilegeRevision) {
        os << ", lastCheckedPrivilegeRevision: "
           << filter.lastCheckedPrivilegeRevision.value();
    }

    os << ", sid:" << filter.streamId;
    os << ", filter.size:" << filter.filter.size() << std::endl;
    for (const auto& c : filter.filter) {
        os << "filter:entry:0x" << std::hex << c.first << std::endl;
    }
    return os;
}

} // namespace Collections::VB
