/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <cbsasl/error.h>
#include <mcbp/protocol/status.h>
#include <memcached/engine_error.h>
#include <memcached/rbac.h>
#include <nlohmann/json.hpp>
#include <platform/sized_buffer.h>
#include <utility>

class AuthProvider {
public:
    virtual ~AuthProvider() = default;

    /**
     * Process the provided authentication request
     *
     * @param request the JSON description of the authentication data
     * @return a pair where the first entry is the status code of the operation
     *                and the second parameter is the payload to return back
     *                to the client
     * @throws nlohmann::json::exception if the input data cannot be parsed
     *         std::runtime_error if the input data is missing required fields
     *         std::bad_alloc for memory allocation problems
     *         std::bad_function if no password validator is set
     */
    std::pair<cb::mcbp::Status, std::string> process(
            const std::string& request);

protected:
    /**
     * Callback called during password validation (used by PLAIN auth)
     */
    virtual std::pair<cb::sasl::Error, nlohmann::json> validatePassword(
            const std::string& username, const std::string& password) = 0;

    std::pair<cb::mcbp::Status, std::string> start(
            const std::string& mechanism, const std::string& challenge);

    std::pair<cb::mcbp::Status, std::string> plain_auth(
            cb::const_char_buffer input);
};
