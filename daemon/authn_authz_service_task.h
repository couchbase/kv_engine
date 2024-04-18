/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <gsl/gsl-lite.hpp>
#include <chrono>
#include <string>

namespace cb::mcbp {
enum class Status : uint16_t;
} // namespace cb::mcbp

/**
 * The AuthnAuthzServiceTask is an abstract class for all of the tasks
 * which use the authentication / authorization service.
 *
 * This allows for a common API for injecting messages to the external
 * authentication/authorization service.
 */
class AuthnAuthzServiceTask {
public:
    using steady_clock = std::chrono::steady_clock;
    using time_point = steady_clock::time_point;

    virtual ~AuthnAuthzServiceTask() = default;

    /**
     * The external auth service received a response from the provider
     *
     * @param status The status code from the auth provider
     * @param payload The payload from the auth provider (depends on the
     *                request type and the status)
     */
    virtual void externalResponse(cb::mcbp::Status status,
                                  std::string_view payload) = 0;

    void recordStartTime() {
        Expects(startTime.time_since_epoch().count() == 0);
        startTime = steady_clock::now();
    }

    [[nodiscard]] time_point getStartTime() const {
        return startTime;
    }

protected:
    time_point startTime{};
};
