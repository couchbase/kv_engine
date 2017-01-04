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
    /** The engine want more data if the frontend have more data available. */
    want_more = 0x09,
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

}

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
    ENGINE_WANT_MORE = int(cb::engine_errc::want_more),
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
    ENGINE_FAILED = int(cb::engine_errc::failed)
} ENGINE_ERROR_CODE;


namespace std {

template <>
struct is_error_condition_enum<cb::engine_errc> : public true_type { };

}
