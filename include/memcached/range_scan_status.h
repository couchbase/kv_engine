/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <memcached/engine_error.h>

#pragma once

namespace cb::rangescan {

enum class HandlingStatus {
    // The background task sends the status to the connection which requested
    // the range-scan-continue
    TaskSends,
    // The frontend executor sends the status to the connection which requested
    // the range-scan-continue
    ExecutorSends
};

/**
 * Function determines how range-scan-continue responds to certain status codes.
 * @return the HandlingStatus for the given engine_errc
 */
static HandlingStatus getContinueHandlingStatus(cb::engine_errc status) {
    switch (status) {
    case cb::engine_errc::range_scan_more:
    case cb::engine_errc::range_scan_complete:
        // These status codes have successfully terminated the continue and
        // have been sent to the client by RangeScanDataHandler::handleStatus;
        // nothing more to do on front-end here.
        return HandlingStatus::TaskSends;
    case cb::engine_errc::not_my_vbucket:
    case cb::engine_errc::unknown_collection:
    case cb::engine_errc::range_scan_cancelled:
    case cb::engine_errc::failed:
    case cb::engine_errc::would_block:
    case cb::engine_errc::no_such_key:
    case cb::engine_errc::throttled:
        // These status codes either start or block the continue from happening
        // or these status codes have terminated a continue but have not been
        // sent. The executor will process these, e.g. ensuring correct nmvb
        // responses.
        return HandlingStatus::ExecutorSends;
    // The following are not expected status codes to be handled by range-scan
    // continue (except success which is handled outside of this function)
    case cb::engine_errc::success:
    case cb::engine_errc::key_already_exists:
    case cb::engine_errc::no_memory:
    case cb::engine_errc::not_stored:
    case cb::engine_errc::invalid_arguments:
    case cb::engine_errc::not_supported:
    case cb::engine_errc::too_big:
    case cb::engine_errc::too_many_connections:
    case cb::engine_errc::disconnect:
    case cb::engine_errc::no_access:
    case cb::engine_errc::temporary_failure:
    case cb::engine_errc::out_of_range:
    case cb::engine_errc::rollback:
    case cb::engine_errc::no_bucket:
    case cb::engine_errc::too_busy:
    case cb::engine_errc::authentication_stale:
    case cb::engine_errc::delta_badval:
    case cb::engine_errc::locked:
    case cb::engine_errc::locked_tmpfail:
    case cb::engine_errc::predicate_failed:
    case cb::engine_errc::cannot_apply_collections_manifest:
    case cb::engine_errc::unknown_scope:
    case cb::engine_errc::durability_impossible:
    case cb::engine_errc::sync_write_in_progress:
    case cb::engine_errc::sync_write_ambiguous:
    case cb::engine_errc::dcp_streamid_invalid:
    case cb::engine_errc::durability_invalid_level:
    case cb::engine_errc::sync_write_re_commit_in_progress:
    case cb::engine_errc::sync_write_pending:
    case cb::engine_errc::stream_not_found:
    case cb::engine_errc::opaque_no_match:
    case cb::engine_errc::scope_size_limit_exceeded:
    case cb::engine_errc::vbuuid_not_equal:
    case cb::engine_errc::bucket_paused:
    case cb::engine_errc::cancelled:
        throw std::runtime_error(
                "cb::rangescan::getHandlingStatus unexpected status:" +
                to_string(status));
    }
    throw std::runtime_error(
            "cb::rangescan::getHandlingStatus illegal value status:" +
            std::to_string(int(status)));
}
} // namespace cb::rangescan
