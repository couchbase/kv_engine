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
#include "trace_helpers.h"

#include <memcached/rbac/privileges.h>
#include <memcached/tracer.h>
#include <nlohmann/json.hpp>
#include <platform/checked_snprintf.h>
#include <platform/scope_timer.h>
#include <statistics/cbstat_collector.h>
#include <utilities/json_utilities.h>
#include <memory>

namespace Collections::VB {

Filter::Filter(std::optional<std::string_view> jsonFilter,
               const Collections::VB::Manifest& manifest,
               CookieIface& cookie,
               const EventuallyPersistentEngine& engine) {
    using cb::tracing::Code;
    ScopeTimer1<TracerStopwatch> timer(cookie, Code::StreamFilterCreate, true);
    cb::engine_errc status = cb::engine_errc::success;
    uint64_t manifestUid{0};
    // If the jsonFilter is not initialised we are building a filter for a
    // legacy DCP stream, one which could only ever support _default
    if (!jsonFilter.has_value()) {
        filterType = FilterType::Legacy;
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

        // Assume passthrough constructFromJson will correct if we reach it
        filterType = FilterType::Passthrough;

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

        // Passthrough must have bucket level SystemCollectionLookup, else it
        // has to become a UserVisible filter
        if (isPassThroughFilter() &&
            engine.testPrivilege(cookie,
                                 cb::rbac::Privilege::SystemCollectionLookup,
                                 {},
                                 {}) != cb::engine_errc::success) {
            // Generate a filter from all User visible collections
            auto rh = manifest.lock();

            // Firstly, enable the default collection if it exists and wasn't
            // already done by the earlier code blocks
            if (rh.doesDefaultCollectionExist() && !defaultAllowed) {
                enableDefaultCollection();
            }

            for (const auto& [cid, entry] : rh) {
                if (cid.isDefaultCollection()) {
                    continue; // handled by enableDefaultCollection
                }
                auto filterData = rh.getMetaForDcpFilter(cid);
                Expects(filterData);
                if (filterData->collectionVisibility == Visibility::User) {
                    insertCollection(cid, *filterData);
                }
            }

            filterType = FilterType::UserVisible;
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
        filterType = FilterType::Scope;
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
        filterType = FilterType::Collection;
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
    CollectionID cid{object.get<std::string>()};
    auto filterData = rh.getMetaForDcpFilter(cid);
    if (!filterData) {
        // Error time
        return false;
    }

    insertCollection(cid, *filterData);
    return true;
}

bool Filter::addScope(const nlohmann::json& object,
                      const Collections::VB::ReadHandle& rh) {
    // Require that the requested scope exists in the manifest.
    // DCP cannot filter an unknown scope.
    ScopeID sid{object.get<std::string>()};

    auto collectionVector = rh.getCollectionsForScope(sid);

    if (!collectionVector) {
        // Error time - the scope does not exist
        return false;
    }

    // set the scopeID member so we know we're tracking a complete scope (which
    // means the collection set can change as we see new collections pass-by)
    scopeID = sid;
    // set the scope visibility so we know if we're tracking a system scope
    filteredScopeVisibility = rh.getScopeVisibility(sid);
    for (CollectionID cid : collectionVector.value()) {
        auto filterData = rh.getMetaForDcpFilter(cid);
        if (!filterData) {
            // Error time
            return false;
        }
        insertCollection(cid, *filterData);
    }
    return true;
}

void Filter::insertCollection(CollectionID cid, DcpFilterMeta filterData) {
    auto [itr, emplaced] = this->filter.try_emplace(cid, filterData);
    Expects(emplaced);
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
    if (key.isInSystemCollection() && !isLegacyFilter()) {
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
    if (isPassThroughFilter()) {
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
    // Passthrough filters are never empty and neither are system filters, even
    // if the filter set is empty, a new collection could be created and update
    // the set of valid collections.
    if (isPassThroughFilter() || isUserVisibleFilter()) {
        return false;
    }

    if (isScopeFilter()) {
        return scopeIsDropped;
    }

    return (filter.empty() && !defaultAllowed);
}

bool Filter::checkAndUpdateSystemEvent(const Item& item) {
    switch (SystemEvent(item.getFlags())) {
    case SystemEvent::Collection:
    case SystemEvent::ModifyCollection:
        return processCollectionEvent(item);
    case SystemEvent::Scope:
        return processScopeEvent(item);
    }
    throw std::invalid_argument(
            fmt::format("Filter::checkAndUpdateSystemEvent:: Item with unknown "
                        "event/flag:{}",
                        item));
}

bool Filter::processCollectionEvent(const Item& item) {
    bool deleted = false;
    if (item.isDeleted()) {
        // Save the return value (indicating if something has actually been
        // deleted) so that we can return it if we want to send this event
        deleted = remove(item);
    }

    if (isLegacyFilter()) {
        // Legacy filters do not support system events
        return false;
    }

    if (isPassThroughFilter()) {
        return true;
    }

    CollectionID cid;
    Visibility collectionVisibility{Visibility::User};
    ScopeID sid;
    if (!item.isDeleted()) {
        auto dcpData = VB::Manifest::getCreateEventData(item);
        sid = dcpData.metaData.sid;
        cid = dcpData.metaData.cid;
        collectionVisibility = Collections::getCollectionVisibility(
                dcpData.metaData.name, cid);
    } else {
        auto dcpData = VB::Manifest::getDropEventData(
                {item.getData(), item.getNBytes()});
        sid = dcpData.sid;
        cid = dcpData.cid;
        collectionVisibility = dcpData.isSystemCollection ? Visibility::System
                                                          : Visibility::User;
    }

    if (deleted || (cid.isDefaultCollection() && defaultAllowed)) {
        return true;
    }

    if ((isScopeFilter() && sid == scopeID) ||
        (isUserVisibleFilter() && collectionVisibility == Visibility::User)) {
        if (item.isDeleted()) {
            // The item is a drop collection from the filtered scope. The
            // ::filter std::set should not store this collection, but it
            // should be included in a DCP stream which cares for the scope.
            // Return true and take no further actions.
            // Or this is a System filter - the drop is Visibility::User
            return true;
        } else {
            // update the filter set as this collection is in our scope or
            // this is a system filter and the filter set stores all user
            // collections
            filter.insert({cid, DcpFilterMeta{collectionVisibility, sid}});
        }
    }

    // When filtered allow only if there is a match
    return filter.count(cid) > 0;
}

bool Filter::processScopeEvent(const Item& item) {
    switch (filterType) {
    case FilterType::Passthrough:
        return true;
    case FilterType::Legacy:
    case FilterType::Collection:
        // Legacy filters do not support system events
        // Collection filter does not show scope events
        return false;
    case FilterType::Scope:
    case FilterType::UserVisible:
        break;
    }
    // scope filter we check if event matches our scope or if the scope has
    // appropriate visibility for System
    ScopeID sid;
    Visibility scopeVisibility{Visibility::User};

    if (!item.isDeleted()) {
        auto dcpData = VB::Manifest::getCreateScopeEventData(
                {item.getData(), item.getNBytes()});
        sid = dcpData.metaData.sid;
        scopeVisibility =
                Collections::getScopeVisibility(dcpData.metaData.name, sid);
    } else {
        auto dcpData = VB::Manifest::getDropScopeEventData(
                {item.getData(), item.getNBytes()});
        sid = dcpData.sid;
        scopeVisibility =
                dcpData.isSystemScope ? Visibility::System : Visibility::User;

        if (sid == scopeID) {
            // Scope dropped - ::empty must now return true
            scopeIsDropped = true;
        }
    }

    if (isScopeFilter()) {
        return sid == scopeID;
    }

    return scopeVisibility == Visibility::User;
}

void Filter::enableDefaultCollection() {
    defaultAllowed = true;
    // For simpler client usage, insert the default collection into the set.
    // This collection is not system, resides in the default scope (and that
    // scope is not system either)
    filter.insert({CollectionID::Default,
                   DcpFilterMeta{Visibility::User, ScopeID::Default}});
}

void Filter::disableDefaultCollection() {
    defaultAllowed = false;
    filter.erase(CollectionID::Default);
}

void Filter::addStats(const AddStatFn& add_stat,
                      CookieIface& c,
                      Vbid vb) const {
    try {
        add_casted_stat("type", to_string(filterType), add_stat, c);

        // Skip adding the rest of the state as they only apply for !passthrough
        if (isPassThroughFilter()) {
            return;
        }

        add_casted_stat("default_allowed", defaultAllowed, add_stat, c);

        if (isScopeFilter()) {
            add_casted_stat("scope_id", scopeID.to_string(), add_stat, c);
            add_casted_stat("scope_dropped", scopeIsDropped, add_stat, c);
        }

        add_casted_stat("sid", streamId.to_string(), add_stat, c);
        add_casted_stat("size", filter.size(), add_stat, c);

        std::string cids;
        for (const auto& pair : filter) {
            cids += pair.first.to_string() + ",";
        }
        if (!cids.empty()) {
            cids.pop_back();
        }
        add_casted_stat("cids", cids, add_stat, c);
    } catch (std::exception& error) {
        EP_LOG_WARN(
                "Filter::addStats: {}"
                " exception.what:{}",
                vb,
                error.what());
    }
}

cb::engine_errc Filter::checkPrivileges(
        CookieIface& cookie, const EventuallyPersistentEngine& engine) {
    const auto rev = cookie.getPrivilegeContextRevision();

    if (lastCheckedPrivilegeRevision && *lastCheckedPrivilegeRevision == rev) {
        // Nothing changed since last check
        return cb::engine_errc::success;
    }

    lastCheckedPrivilegeRevision = rev;
    if (isPassThroughFilter()) {
        // Passthrough require access to the system collections
        if (cookie.testPrivilege(
                          cb::rbac::Privilege::SystemCollectionLookup, {}, {})
                    .failed()) {
            return cb::engine_errc::no_access;
        }
        // Must have access to the bucket
        return engine.testPrivilege(
                cookie, cb::rbac::Privilege::DcpStream, {}, {});
    }

    if (isUserVisibleFilter()) {
        // Must have access to the bucket
        return engine.testPrivilege(
                cookie, cb::rbac::Privilege::DcpStream, {}, {});
    }

    if (isScopeFilter()) {
        // Must have access to at least the scope
        return engine.testScopeAccess(
                cookie,
                cb::rbac::Privilege::SystemCollectionLookup,
                cb::rbac::Privilege::DcpStream,
                scopeID,
                filteredScopeVisibility);
    }

    // Must have access to the collections
    bool unknownCollection = false;
    bool accessError = false;

    // Check all collections
    for (const auto& [cid, meta] : filter) {
        const auto status = engine.testCollectionAccess(
                cookie,
                cb::rbac::Privilege::SystemCollectionLookup,
                cb::rbac::Privilege::DcpStream,
                cid,
                meta.scopeId,
                meta.collectionVisibility);
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
            throw std::logic_error(fmt::format(
                    "Filter::checkPrivileges: unexpected error: {}", status));
        }
    }

    // Ordering here is important - 1 unknown collection in a sea of
    // success/no_access dominates and shall be what is seen
    if (unknownCollection) {
        return cb::engine_errc::unknown_collection;
    }

    if (accessError) {
        return cb::engine_errc::no_access;
    }

    return cb::engine_errc::success;
}

Filter::CollectionSizeStats Filter::getSizeStats(
        const Manifest& manifest) const {
    size_t colItemCount = 0, colDiskSize = 0;
    // For each collection, obtain the item count and disk size
    for (auto [cid, sid] : filter) {
        (void)sid;
        const auto stats = manifest.lock(cid);
        if (!stats.valid()) {
            // The collection has been dropped, so we don't increment the stats.
            continue;
        }
        colItemCount += stats.getItemCount();
        colDiskSize += stats.getDiskSize();
    }
    return {colItemCount, colDiskSize};
}

bool Filter::isOsoSuitable(size_t limit) const {
    // Note: passthrough means all collections match, we could probably do OSO
    // for passthrough, setting the key range to match every collection, i.e.
    // namespace \00 to \01 and \08 to \ff, but for now just consider the
    // explicitly filtered case and when the filter size is acceptable for the
    // given limit.
    return (isCollectionFilter() || isScopeFilter() || isLegacyFilter()) &&
           filter.size() <= limit;
}

void Filter::dump() const {
    std::cerr << *this << std::endl;
}

std::string Filter::summary() const {
    return fmt::format(
            "filter:{{{}, size:{}}}", to_string(filterType), filter.size());
}

std::string Filter::to_string(FilterType type) {
    switch (type) {
    case FilterType::Passthrough:
        return "passthrough";
    case FilterType::Legacy:
        return "legacy";
    case FilterType::UserVisible:
        return "uservisible";
    case FilterType::Collection:
        return "collection";
    case FilterType::Scope:
        return "scope";
    }
    folly::assume_unreachable();
}

// To use the keys in json::find, they need to be statically allocated
const char* Filter::CollectionsKey = "collections";
const char* Filter::ScopeKey = "scope";
const char* Filter::UidKey = "uid";
const char* Filter::StreamIdKey = "sid";

std::ostream& operator<<(std::ostream& os, const Filter& filter) {
    os << "VBucket::Filter"
       << ": defaultAllowed:" << filter.defaultAllowed
       << ", type:" << Filter::to_string(filter.filterType)
       << ", scopeIsDropped:" << filter.scopeIsDropped;
    if (filter.isScopeFilter()) {
        os << ", scopeID:" << filter.scopeID;
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
