/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <fmt/format.h>
#include <folly/CancellationToken.h>
#include <memcached/collections.h>
#include <memcached/engine.h>
#include <memcached/range_scan_optional_configuration.h>
#include <nlohmann/json.hpp>

cb::engine_errc EngineIface::get_stats(CookieIface& cookie,
                                       std::string_view key,
                                       std::string_view value,
                                       const AddStatFn& add_stat) {
    return get_stats(cookie, key, value, add_stat, []() { return false; });
}

cb::EngineErrorGetCollectionIDResult EngineIface::get_collection_id(
        CookieIface& cookie, std::string_view path) {
    return cb::EngineErrorGetCollectionIDResult{cb::engine_errc::not_supported};
}

cb::EngineErrorGetScopeIDResult EngineIface::get_scope_id(
        CookieIface& cookie, std::string_view path) {
    return cb::EngineErrorGetScopeIDResult{cb::engine_errc::not_supported};
}

cb::EngineErrorGetCollectionMetaResult EngineIface::get_collection_meta(
        CookieIface& cookie, CollectionID cid, std::optional<Vbid> vbid) const {
    return cb::EngineErrorGetCollectionMetaResult(
            cb::engine_errc::not_supported);
}

std::pair<cb::engine_errc, cb::rangescan::Id> EngineIface::createRangeScan(
        CookieIface& cookie, const cb::rangescan::CreateParameters& params) {
    return {cb::engine_errc::not_supported, {}};
}

cb::engine_errc EngineIface::continueRangeScan(
        CookieIface& cookie, const cb::rangescan::ContinueParameters& params) {
    return cb::engine_errc::not_supported;
}

cb::engine_errc EngineIface::cancelRangeScan(CookieIface& cookie,
                                             Vbid vbid,
                                             cb::rangescan::Id uuid) {
    return cb::engine_errc::not_supported;
}

std::pair<cb::engine_errc, nlohmann::json>
EngineIface::getFusionStorageSnapshot(Vbid vbid,
                                      std::string_view snapshotUuid,
                                      std::time_t validity) {
    return {cb::engine_errc::not_supported, {}};
}

cb::engine_errc EngineIface::releaseFusionStorageSnapshot(
        Vbid vbid, std::string_view snapshotUuid) {
    return cb::engine_errc::not_supported;
}

cb::engine_errc EngineIface::pause(folly::CancellationToken cancellationToken) {
    return cb::engine_errc::not_supported;
}

cb::EngineErrorItemPair EngineIface::get_replica(CookieIface&,
                                                 const DocKeyView&,
                                                 Vbid,
                                                 DocStateFilter) {
    return cb::makeEngineErrorItemPair(cb::engine_errc::not_supported);
}

cb::EngineErrorItemPair EngineIface::get_random_document(CookieIface& cookie,
                                                         CollectionID cid) {
    return cb::makeEngineErrorItemPair(cb::engine_errc::not_supported);
}

std::string to_string(const TrafficControlMode mode) {
    switch (mode) {
    case TrafficControlMode::Enabled:
        return "enabled";
    case TrafficControlMode::Disabled:
        return "disabled";
    }
    return fmt::format("TrafficControlMode::Invalid({})", int(mode));
}

std::ostream& operator<<(std::ostream& os, const TrafficControlMode& mode) {
    os << to_string(mode);
    return os;
}
