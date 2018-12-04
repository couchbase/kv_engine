/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#pragma once

#include <memcached/engine_utilities_visibility.h>
#include <platform/platform.h>

#include <iosfwd>
#include <system_error>

#ifdef _MSC_VER
// We need to add the right __declspec magic to the system types
// to avoid msvc to spit out warnings
class ENGINE_UTILITIES_PUBLIC_API std::system_error;
#endif

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
     * The request cannot complete until a collections manifest has been set
     */
    no_collections_manifest = 0x18,

    /**
     * The collections manifest passed validation but could not be applied to a
     * vbucket
     */
    cannot_apply_collections_manifest = 0x19,

    /**
     * The client is from the future, i.e. they have a collections manifest
     * which is ahead of the vbuckets.
     */
    collections_manifest_is_ahead = 0x1a,

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

    /** Generic failue. */
    failed = 0xff
};

/**
 * Get the error category object used to map from numeric values to
 * a textual representation of the error code.
 *
 * @return The one and only instance of the error object
 */
ENGINE_UTILITIES_PUBLIC_API
const std::error_category& engine_error_category() NOEXCEPT;

class ENGINE_UTILITIES_PUBLIC_API engine_error : public std::system_error {
public:
    engine_error(engine_errc ev, const std::string& what_arg)
        : system_error(int(ev), engine_error_category(), what_arg) {}

    engine_error(engine_errc ev, const char* what_arg)
        : system_error(int(ev), engine_error_category(), what_arg) {}
};

static inline std::error_condition make_error_condition(engine_errc e) {
    return std::error_condition(int(e), engine_error_category());
}

ENGINE_UTILITIES_PUBLIC_API
std::string to_string(engine_errc ev);

// GoogleTest printing function.
ENGINE_UTILITIES_PUBLIC_API
void PrintTo(engine_errc ev, ::std::ostream* os);

} // namespace cb

// For checkeqfn
ENGINE_UTILITIES_PUBLIC_API
std::ostream& operator<<(std::ostream& os, cb::engine_errc ec);

// For backwards compatibility with the old memcached source code we need
// to keep the old constants around
typedef enum {
    ENGINE_SUCCESS = int(cb::engine_errc::success),
    ENGINE_KEY_ENOENT = int(cb::engine_errc::no_such_key),
    ENGINE_KEY_EEXISTS = int(cb::engine_errc::key_already_exists),
    ENGINE_ENOMEM = int(cb::engine_errc::no_memory),
    ENGINE_NOT_STORED = int(cb::engine_errc::not_stored),
    ENGINE_EINVAL = int(cb::engine_errc::invalid_arguments),
    ENGINE_ENOTSUP = int(cb::engine_errc::not_supported),
    ENGINE_EWOULDBLOCK = int(cb::engine_errc::would_block),
    ENGINE_E2BIG = int(cb::engine_errc::too_big),
    ENGINE_DISCONNECT = int(cb::engine_errc::disconnect),
    ENGINE_EACCESS = int(cb::engine_errc::no_access),
    ENGINE_NOT_MY_VBUCKET = int(cb::engine_errc::not_my_vbucket),
    ENGINE_TMPFAIL = int(cb::engine_errc::temporary_failure),
    ENGINE_ERANGE = int(cb::engine_errc::out_of_range),
    ENGINE_ROLLBACK = int(cb::engine_errc::rollback),
    ENGINE_NO_BUCKET = int(cb::engine_errc::no_bucket),
    ENGINE_EBUSY = int(cb::engine_errc::too_busy),
    ENGINE_AUTH_STALE = int(cb::engine_errc::authentication_stale),
    ENGINE_DELTA_BADVAL = int(cb::engine_errc::delta_badval),
    ENGINE_LOCKED = int(cb::engine_errc::locked),
    ENGINE_LOCKED_TMPFAIL = int(cb::engine_errc::locked_tmpfail),
    ENGINE_UNKNOWN_COLLECTION = int(cb::engine_errc::unknown_collection),
    ENGINE_COLLECTIONS_MANIFEST_IS_AHEAD =
            int(cb::engine_errc::collections_manifest_is_ahead),
    ENGINE_FAILED = int(cb::engine_errc::failed),
    ENGINE_PREDICATE_FAILED = int(cb::engine_errc::predicate_failed),
    ENGINE_DURABILITY_IMPOSSIBLE = int(cb::engine_errc::durability_impossible),
    ENGINE_SYNC_WRITE_IN_PROGRESS =
            int(cb::engine_errc::sync_write_in_progress),
    ENGINE_SYNC_WRITE_AMBIGUOUS = int(cb::engine_errc::sync_write_ambiguous),
    ENGINE_DCP_STREAMID_INVALID = int(cb::engine_errc::dcp_streamid_invalid)
} ENGINE_ERROR_CODE;

namespace std {

template <>
struct is_error_condition_enum<cb::engine_errc> : public true_type { };

}

namespace cb {
// Backward compatibility - convert ENGINE_ERROR_CODE to engine_errc.
ENGINE_UTILITIES_PUBLIC_API
cb::engine_errc to_engine_errc(ENGINE_ERROR_CODE eec);
}
