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
#include <memcached/engine_error.h>

#include <iostream>
#include <map>
#include <string>

/**
 * engine_error_category provides the mapping from the engine error codes to
 * a textual mapping and is used together with the std::system_error.
 */
class engine_category : public std::error_category {
public:
    const char* name() const NOEXCEPT override {
        return "engine error codes";
    }

    std::string message(int code) const override {
        return to_string(cb::engine_errc(code));
    }

    std::error_condition default_error_condition(int code) const NOEXCEPT override {
        return std::error_condition(code, *this);
    }
};

const std::error_category& cb::engine_error_category() NOEXCEPT {
    static engine_category category_instance;
    return category_instance;
}

std::string cb::to_string(cb::engine_errc code) {
    switch (code) {
    case cb::engine_errc::success:
        return "success";
    case cb::engine_errc::no_such_key:
        return "no such key";
    case cb::engine_errc::key_already_exists:
        return "key already exists";
    case cb::engine_errc::no_memory:
        return "no memory";
    case cb::engine_errc::not_stored:
        return "not stored";
    case cb::engine_errc::invalid_arguments:
        return "invalid arguments";
    case cb::engine_errc::not_supported:
        return "not supported";
    case cb::engine_errc::would_block:
        return "would block";
    case cb::engine_errc::too_big:
        return "too big";
    case cb::engine_errc::disconnect:
        return "disconnect";
    case cb::engine_errc::no_access:
        return "no access";
    case cb::engine_errc::not_my_vbucket:
        return "not my vbucket";
    case cb::engine_errc::temporary_failure:
        return "temporary failure";
    case cb::engine_errc::out_of_range:
        return "out of range";
    case cb::engine_errc::rollback:
        return "rollback";
    case cb::engine_errc::no_bucket:
        return "no bucket";
    case cb::engine_errc::too_busy:
        return "too busy";
    case cb::engine_errc::authentication_stale:
        return "authentication stale";
    case cb::engine_errc::delta_badval:
        return "delta bad value";
    case cb::engine_errc::locked:
        return "resource is locked";
    case cb::engine_errc::locked_tmpfail:
        return "resource is locked; tmpfail";
    case cb::engine_errc::failed:
        return "generic failure";
    case cb::engine_errc::unknown_collection:
        return "unknown collection";
    case cb::engine_errc::predicate_failed:
        return "predicate_failed";
    case cb::engine_errc::no_collections_manifest:
        return "no_collections_manifest";
    case cb::engine_errc::cannot_apply_collections_manifest:
        return "cannot_apply_collections_manifest";
    case cb::engine_errc::collections_manifest_is_ahead:
        return "collections_manifest_is_ahead";
    case cb::engine_errc::unknown_scope:
        return "unknown scope";
    case engine_errc::durability_impossible:
        return "durability impossible";
    case engine_errc::sync_write_in_progress:
        return "synchronous write in progress";
    case engine_errc::sync_write_ambiguous:
        return "synchronous write ambiguous";
    case engine_errc::dcp_streamid_invalid:
        return "dcp_streamid_invalid";
    };
    throw std::invalid_argument(
        "engine_error_category::message: code does not represent a "
            "legal error code: " + std::to_string(int(code)));
}

void cb::PrintTo(cb::engine_errc ev, ::std::ostream* os) {
    *os << cb::to_string(ev);
}

std::ostream& operator<<(std::ostream& os, cb::engine_errc ec) {
    cb::PrintTo(ec, &os);
    return os;
}

cb::engine_errc cb::to_engine_errc(ENGINE_ERROR_CODE eec) {
    switch (eec) {
    case ENGINE_SUCCESS:
        return cb::engine_errc::success;
    case ENGINE_KEY_ENOENT:
        return cb::engine_errc::no_such_key;
    case ENGINE_KEY_EEXISTS:
        return cb::engine_errc::key_already_exists;
    case ENGINE_ENOMEM:
        return cb::engine_errc::no_memory;
    case ENGINE_NOT_STORED:
        return cb::engine_errc::not_stored;
    case ENGINE_EINVAL:
        return cb::engine_errc::invalid_arguments;
    case ENGINE_ENOTSUP:
        return cb::engine_errc::not_supported;
    case ENGINE_EWOULDBLOCK:
        return cb::engine_errc::would_block;
    case ENGINE_E2BIG:
        return cb::engine_errc::too_big;
    case ENGINE_DISCONNECT:
        return cb::engine_errc::disconnect;
    case ENGINE_EACCESS:
        return cb::engine_errc::no_access;
    case ENGINE_NOT_MY_VBUCKET:
        return cb::engine_errc::not_my_vbucket;
    case ENGINE_TMPFAIL:
        return cb::engine_errc::temporary_failure;
    case ENGINE_ERANGE:
        return cb::engine_errc::out_of_range;
    case ENGINE_ROLLBACK:
        return cb::engine_errc::rollback;
    case ENGINE_NO_BUCKET:
        return cb::engine_errc::no_bucket;
    case ENGINE_EBUSY:
        return cb::engine_errc::too_busy;
    case ENGINE_AUTH_STALE:
        return cb::engine_errc::authentication_stale;
    case ENGINE_DELTA_BADVAL:
        return cb::engine_errc::delta_badval;
    case ENGINE_LOCKED:
        return cb::engine_errc::locked;
    case ENGINE_LOCKED_TMPFAIL:
        return cb::engine_errc::locked_tmpfail;
    case ENGINE_UNKNOWN_COLLECTION:
        return cb::engine_errc::unknown_collection;
    case ENGINE_COLLECTIONS_MANIFEST_IS_AHEAD:
        return cb::engine_errc::collections_manifest_is_ahead;
    case ENGINE_FAILED:
        return cb::engine_errc::failed;
    case ENGINE_PREDICATE_FAILED:
        return cb::engine_errc::predicate_failed;
    case ENGINE_DURABILITY_IMPOSSIBLE:
        return cb::engine_errc::durability_impossible;
    case ENGINE_SYNC_WRITE_IN_PROGRESS:
        return cb::engine_errc::sync_write_in_progress;
    case ENGINE_SYNC_WRITE_AMBIGUOUS:
        return cb::engine_errc::sync_write_ambiguous;
    case ENGINE_DCP_STREAMID_INVALID:
        return cb::engine_errc::dcp_streamid_invalid;
    }
    throw std::invalid_argument(
            "cb::to_engine_errc: invalid ENGINE_ERROR_CODE " +
            std::to_string(eec));
}
