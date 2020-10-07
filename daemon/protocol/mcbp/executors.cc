/*
 *     Copyright 2020 Couchbase, Inc.
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
#include "engine_wrapper.h"
#include "utilities.h"
#include <memcached/protocol_binary.h>

void handle_executor_status(Cookie& cookie, cb::engine_errc status) {
    using cb::engine_errc;
    auto& connection = cookie.getConnection();

    const auto mapped = connection.remapErrorCode(status);
    switch (mapped) {
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
    case engine_errc::collections_manifest_is_ahead:
    case engine_errc::unknown_scope:
    case engine_errc::durability_impossible:
    case engine_errc::sync_write_in_progress:
    case engine_errc::sync_write_ambiguous:
    case engine_errc::dcp_streamid_invalid:
    case engine_errc::durability_invalid_level:
    case engine_errc::sync_write_re_commit_in_progress:
    case engine_errc::sync_write_pending:
    case engine_errc::failed:
        cookie.sendResponse(mapped);
        break;
    }
}
