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

using error_map = std::map<const cb::engine_errc, std::string>;
static const error_map mapping = {
        {cb::engine_errc::success, "success"},
        {cb::engine_errc::no_such_key, "no such key"},
        {cb::engine_errc::key_already_exists, "key already exists"},
        {cb::engine_errc::no_memory, "no memory"},
        {cb::engine_errc::not_stored, "not stored"},
        {cb::engine_errc::invalid_arguments, "invalid arguments"},
        {cb::engine_errc::not_supported, "not supported"},
        {cb::engine_errc::would_block, "would block"},
        {cb::engine_errc::too_big, "too big"},
        {cb::engine_errc::want_more, "want more"},
        {cb::engine_errc::disconnect, "disconnect"},
        {cb::engine_errc::no_access, "no access"},
        {cb::engine_errc::not_my_vbucket, "not my vbucket"},
        {cb::engine_errc::temporary_failure, "temporary failure"},
        {cb::engine_errc::out_of_range, "out of range"},
        {cb::engine_errc::rollback, "rollback"},
        {cb::engine_errc::no_bucket, "no bucket"},
        {cb::engine_errc::too_busy, "too busy"},
        {cb::engine_errc::authentication_stale, "authentication stale"},
        {cb::engine_errc::delta_badval, "delta bad value"},
        {cb::engine_errc::locked, "resource is locked"},
        {cb::engine_errc::locked_tmpfail, "resource is locked; tmpfail"},
        {cb::engine_errc::failed, "generic failure"}};

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
        const auto iter = mapping.find(cb::engine_errc(code));
        if (iter == mapping.cend()) {
            throw std::invalid_argument(
                    "engine_error_category::message: code does not represent a "
                    "legal error code: " +
                    std::to_string(code));
        } else {
            return iter->second;
        }
    }

    std::error_condition default_error_condition(int code) const NOEXCEPT override {
        return std::error_condition(code, *this);
    }
};

const std::error_category& cb::engine_error_category() NOEXCEPT {
    static engine_category category_instance;
    return category_instance;
}
