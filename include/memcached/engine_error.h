/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <nlohmann/json_fwd.hpp>
#include <iosfwd>
#include <system_error>

namespace cb {
/**
 * The Error enum contains all the various error codes the engines
 * may return. They may be used together with the cb::engine_category()
 * in std::system_error exceptions (where you can fetch the code, and the
 * textual description for the given error).
 */
enum class engine_errc {
    /// The command executed successfully
    success,

    /// The key does not exist
    no_such_key,

    /// The key already exists
    key_already_exists,

    /// Could not allocate memory
    no_memory,

    /// The item was not stored
    not_stored,

    /// Invalid arguments
    invalid_arguments,

    /// The engine does not support this
    not_supported,

    /// This would cause the engine to block
    would_block,

    /// The data is too big for the engine
    too_big,

    /// Tell the server to disconnect this client
    disconnect,

    /// Access control violations
    no_access,

    /// This vbucket doesn't belong to me
    not_my_vbucket,

    /// Temporary failure, please try again later
    temporary_failure,

    /// Value outside legal range
    out_of_range,

    /// Roll back to a previous version
    rollback,

    /// The connection isn't bound to an engine
    no_bucket,

    /// Can't serve the request... busy
    too_busy,

    /// Auth data stale
    authentication_stale,

    /// The value stored in the document is incompatible with the requested
    /// operation.
    delta_badval,

    /// The requested resource is locked
    locked,

    /// The requested resource is locked, but the engine used to
    /// report it as a tmpfail (we need to be able to separate this
    /// in the core so that old clients can get TMPFAIL back instead
    /// of EEXISTS
    locked_tmpfail,

    /// The request has no collection or an unknown collection.
    unknown_collection,

    /// A command with a predicate has been failed because of the predicate
    predicate_failed,

    /// The collections manifest passed validation but could not be applied
    cannot_apply_collections_manifest,

    /// The request has no scope or an unknown scope
    unknown_scope,

    /// The durability level can't be satisfied
    durability_impossible,

    /// The requested key has already a synchronous write in progress
    sync_write_in_progress,

    /// The SyncWrite request has not completed in the specified time
    /// and has ambiguous result - it may Succeed or Fail; but the final
    /// value is not yet known
    sync_write_ambiguous,

    /// A DCP method was invoked and the stream-ID is invalid. Could be that
    /// an ID was provided when the feature is disabled, or no ID and the
    /// feature is enabled.
    dcp_streamid_invalid,

    /// The durability level is invalid (e.g. persist on Ephemeral)
    durability_invalid_level,

    /// The SyncWrite is being re-committed after a change in active node.
    sync_write_re_commit_in_progress,

    /// The SyncWrite is pending, effectively a special case would_block used
    /// internally by ep-engine
    sync_write_pending,

    /// Stream not found for DCP message
    stream_not_found,

    /// Opaque in message did not match stream's
    opaque_no_match,

    /// RangeScan was cancelled
    range_scan_cancelled,

    /// RangeScan has more data available
    range_scan_more,

    /// RangeScan has completed, successfully, no more data.
    range_scan_complete,

    /// RangeScan create failed due to vb-uuid mismatch
    vbuuid_not_equal,

    /// Too many connections
    too_many_connections,

    /// Throttled for some reason
    throttled,

    /// Bucket is currently paused and operation is not possible.
    bucket_paused,

    /// Operation was cancelled before completion and had no effect.
    cancelled,

    /// The requested resource is not locked
    not_locked,

    /// CAS value used is invalid
    cas_value_invalid,

    /// Generic failue
    failed
};

/**
 * Get the error category object used to map from numeric values to
 * a textual representation of the error code.
 *
 * @return The one and only instance of the error object
 */
const std::error_category& engine_error_category() noexcept;

class engine_error : public std::system_error {
public:
    engine_error(engine_errc ev, const std::string& what_arg)
        : system_error(
                  static_cast<int>(ev), engine_error_category(), what_arg) {
    }

    engine_error(engine_errc ev, const char* what_arg)
        : system_error(
                  static_cast<int>(ev), engine_error_category(), what_arg) {
    }

    engine_errc engine_code() const {
        return static_cast<engine_errc>(code().value());
    }
};

static inline std::error_condition make_error_condition(engine_errc e) {
    return {static_cast<int>(e), engine_error_category()};
}

std::string to_string(engine_errc ev);

// GoogleTest printing function.
void PrintTo(engine_errc ev, ::std::ostream* os);
// For checkeqfn
std::ostream& operator<<(std::ostream& os, cb::engine_errc ec);
inline auto format_as(engine_errc ec) {
    return to_string(ec);
}

template <typename BasicJsonType>
void to_json(BasicJsonType& j, engine_errc ec) {
    j = format_as(ec);
}
} // namespace cb

namespace std {

template <>
struct is_error_condition_enum<cb::engine_errc> : public true_type { };

}
