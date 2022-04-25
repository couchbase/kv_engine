/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <memcached/collections.h>
#include <memcached/engine.h>
#include <memcached/range_scan_optional_configuration.h>

cb::EngineErrorGetCollectionIDResult EngineIface::get_collection_id(
        const CookieIface& cookie, std::string_view path) {
    return cb::EngineErrorGetCollectionIDResult{cb::engine_errc::not_supported};
}

cb::EngineErrorGetScopeIDResult EngineIface::get_scope_id(
        const CookieIface& cookie, std::string_view path) {
    return cb::EngineErrorGetScopeIDResult{cb::engine_errc::not_supported};
}

cb::EngineErrorGetScopeIDResult EngineIface::get_scope_id(
        const CookieIface& cookie,
        CollectionID cid,
        std::optional<Vbid> vbid) const {
    return cb::EngineErrorGetScopeIDResult(cb::engine_errc::not_supported);
}

std::pair<cb::engine_errc, cb::rangescan::Id> EngineIface::createRangeScan(
        const CookieIface& cookie,
        Vbid vbid,
        CollectionID cid,
        cb::rangescan::KeyView start,
        cb::rangescan::KeyView end,
        cb::rangescan::KeyOnly keyOnly,
        std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
        std::optional<cb::rangescan::SamplingConfiguration> samplingConfig) {
    return {cb::engine_errc::not_supported, {}};
}

cb::engine_errc EngineIface::continueRangeScan(
        const CookieIface& cookie,
        Vbid vbid,
        cb::rangescan::Id uuid,
        size_t itemLimit,
        std::chrono::milliseconds timeLimit) {
    return cb::engine_errc::not_supported;
}

cb::engine_errc EngineIface::cancelRangeScan(const CookieIface& cookie,
                                             Vbid vbid,
                                             cb::rangescan::Id uuid) {
    return cb::engine_errc::not_supported;
}