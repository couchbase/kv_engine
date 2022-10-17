/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "executors.h"

#include "engine_wrapper.h"
#include "utilities.h"
#include <memcached/protocol_binary.h>

void handle_executor_status(Cookie& cookie, cb::engine_errc status) {
    using cb::engine_errc;
    auto& connection = cookie.getConnection();

    const auto mapped = connection.remapErrorCode(status);
    switch (mapped) {
    case engine_errc::throttled:
    case engine_errc::would_block:
        cookie.setEwouldblock(true);
        break;

    case engine_errc::disconnect:
        connection.shutdown();
        break;

    case engine_errc::rollback:
        throw std::logic_error(
                "handle_executor_status: should not be called for "
                "rollback");
        break;

    case engine_errc::success:
    case engine_errc::no_such_key:
    case engine_errc::key_already_exists:
    case engine_errc::no_memory:
    case engine_errc::not_stored:
    case engine_errc::invalid_arguments:
    case engine_errc::not_supported:
    case engine_errc::too_big:
    case engine_errc::no_access:
    case engine_errc::not_my_vbucket:
    case engine_errc::temporary_failure:
    case engine_errc::out_of_range:
    case engine_errc::no_bucket:
    case engine_errc::too_busy:
    case engine_errc::authentication_stale:
    case engine_errc::delta_badval:
    case engine_errc::locked:
    case engine_errc::locked_tmpfail:
    case engine_errc::unknown_collection:
    case engine_errc::predicate_failed:
    case engine_errc::cannot_apply_collections_manifest:
    case engine_errc::unknown_scope:
    case engine_errc::durability_impossible:
    case engine_errc::sync_write_in_progress:
    case engine_errc::sync_write_ambiguous:
    case engine_errc::dcp_streamid_invalid:
    case engine_errc::durability_invalid_level:
    case engine_errc::sync_write_re_commit_in_progress:
    case engine_errc::sync_write_pending:
    case engine_errc::failed:
    case engine_errc::stream_not_found:
    case engine_errc::opaque_no_match:
    case engine_errc::scope_size_limit_exceeded:
    case engine_errc::range_scan_cancelled:
    case engine_errc::range_scan_more:
    case engine_errc::range_scan_complete:
    case engine_errc::vbuuid_not_equal:
    case engine_errc::too_many_connections:
    case engine_errc::cancelled:
    case engine_errc::bucket_paused:
        cookie.sendResponse(mapped);
        break;
    }
}
