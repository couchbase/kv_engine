/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <iosfwd>
#include <system_error>

namespace cb {
/**
 * The Error enum contains all of the various error codes the engines
 * may return. They may be used together with the cb::engine_category()
 * in std::system_error exceptions (where you can fetch the code, and the
 * textual description for the given error).
 */
enum class engine_errc {
    /** The command executed successfully */
    success = 0x00,
    /** The key does not exists */
    no_such_key = 0x01,
    /** The key already exists */
    key_already_exists = 0x02,
    /** Could not allocate memory */
    no_memory = 0x03,
    /** The item was not stored */
    not_stored = 0x04,
    /** Invalid arguments */
    invalid_arguments = 0x05,
    /** The engine does not support this */
    not_supported = 0x06,
    /** This would cause the engine to block */
    would_block = 0x07,
    /** The data is too big for the engine */
    too_big = 0x08,
    /** Tell the server to disconnect this client */
    disconnect = 0x0a,
    /** Access control violations */
    no_access = 0x0b,
    /** < This vbucket doesn't belong to me */
    not_my_vbucket = 0x0c,
    /** Temporary failure, please try again later */
    temporary_failure = 0x0d,
    /** Value outside legal range */
    out_of_range = 0x0e,
    /** Roll back to a previous version */
    rollback = 0x0f,
    /** The connection isn't bound to an engine */
    no_bucket = 0x10,
    /** Can't serve the request.. busy */
    too_busy = 0x11,
    /** Auth data stale */
    authentication_stale = 0x12,
    /**
     * The value stored in the document is incompatible with the
     * requested operation.
     */
    delta_badval = 0x13,
    /**
     * The requested resource is locked
     */
    locked = 0x14,
    /**
     * The requested resource is locked, but the engine used to
     * report it as a tmpfail (we need to be able to separate this
     * in the core so that old clients can get TMPFAIL back instead
     * of EEXISTS
     */
    locked_tmpfail = 0x15,

    /**
     * The request has no collection or an unknown collection.
     */
    unknown_collection = 0x16,

    /**
     * A command with a predicate has been failed because of the predicate
     */
    predicate_failed = 0x17,

    /**
     * The collections manifest passed validation but could not be applied
     */
    cannot_apply_collections_manifest = 0x19,

    /**
     * The request has no scope or an unknown scope
     */
    unknown_scope = 0x1b,

    /// The durability level can't be satisfied
    durability_impossible = 0x1c,
    /// The requested key has already a synchronous write in progress
    sync_write_in_progress = 0x1d,
    /// The SyncWrite request has not completed in the specified time
    /// and has ambiguous result - it may Succeed or Fail; but the final
    /// value is not yet known
    sync_write_ambiguous = 0x1e,

    /// A DCP method was invoked and the stream-ID is invalid. Could be that
    /// an ID was provided when the feature is disabled, or no ID and the
    /// feature is enabled.
    dcp_streamid_invalid = 0x1f,

    /// The durability level is invalid (e.g. persist on Ephemeral)
    durability_invalid_level = 0x20,

    /// The SyncWrite is being re-committed after a change in active node.
    sync_write_re_commit_in_progress = 0x21,

    /// The SyncWrite is pending, effectively a special case would_block used
    /// internally by ep-engine
    sync_write_pending = 0x22,

    /** Stream not found for DCP message */
    stream_not_found = 0x23,

    /** Opaque in message did not match stream's */
    opaque_no_match = 0x24,

    /// Too much data in the scope
    scope_size_limit_exceeded = 0x25,

    /// RangeScan was cancelled
    range_scan_cancelled = 0x26,

    /// RangeScan has more data available
    range_scan_more = 0x27,

    /// RangeScan has completed, successfully, no more data.
    range_scan_complete = 0x28,

    /// RangeScan create failed due to vb-uuid mismatch
    vbuuid_not_equal = 0x29,

    /// Too many connections
    too_many_connections = 0x2a,

    /// Throttled for some reason
    throttled = 0x2b,

    /// Bucket is currently paused and operation is not possible.
    bucket_paused = 0x2c,

    /// Operation was cancelled before completion and had no effect.
    cancelled = 0x2d,

    /** Generic failue. */
    failed = 0xff
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
        : system_error(int(ev), engine_error_category(), what_arg) {}

    engine_error(engine_errc ev, const char* what_arg)
        : system_error(int(ev), engine_error_category(), what_arg) {}
};

static inline std::error_condition make_error_condition(engine_errc e) {
    return std::error_condition(int(e), engine_error_category());
}

std::string to_string(engine_errc ev);

// GoogleTest printing function.
void PrintTo(engine_errc ev, ::std::ostream* os);
// For checkeqfn
std::ostream& operator<<(std::ostream& os, cb::engine_errc ec);
} // namespace cb


namespace std {

template <>
struct is_error_condition_enum<cb::engine_errc> : public true_type { };

}
