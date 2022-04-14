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
