/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <cbsasl/error.h>
#include <memcached/engine_error.h>
#include <memcached/rbac.h>
#include <nlohmann/json_fwd.hpp>
#include <utility>

namespace cb::mcbp {
class Request;
enum class Status : uint16_t;
} // namespace cb::mcbp

class AuthProvider {
public:
    virtual ~AuthProvider() = default;

    std::pair<cb::mcbp::Status, std::string> process(
            const cb::mcbp::Request& req);

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
    std::pair<cb::mcbp::Status, std::string> processAuthnRequest(
            const std::string& request);

    std::pair<cb::mcbp::Status, std::string> processAuthzRequest(
            const std::string& user);

protected:
    std::pair<cb::mcbp::Status, std::string> processAuthnRequest(
            cb::const_byte_buffer value) {
        return processAuthnRequest(std::string{
                reinterpret_cast<const char*>(value.data()), value.size()});
    }
    std::pair<cb::mcbp::Status, std::string> processAuthzRequest(
            cb::const_byte_buffer user) {
        return processAuthzRequest(std::string{
                reinterpret_cast<const char*>(user.data()), user.size()});
    }

    /**
     * Callback called during password validation (used by PLAIN auth)
     */
    virtual std::pair<cb::sasl::Error, nlohmann::json> validatePassword(
            const std::string& username, const std::string& password) = 0;

    virtual std::pair<cb::sasl::Error, nlohmann::json> getUserEntry(
            const std::string& username) = 0;

    std::pair<cb::mcbp::Status, std::string> start(const std::string& mechanism,
                                                   const std::string& challenge,
                                                   bool authOnly);

    std::pair<cb::mcbp::Status, std::string> plain_auth(std::string_view input,
                                                        bool authOnly);
};
