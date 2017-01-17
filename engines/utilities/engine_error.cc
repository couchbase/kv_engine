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
    case cb::engine_errc::want_more:
        return "want more";
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
    };

    throw std::invalid_argument(
        "engine_error_category::message: code does not represent a "
            "legal error code: " + std::to_string(int(code)));
}
