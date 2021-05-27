/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "steppable_command_context.h"

#include <cbsasl/error.h>
#include <daemon/cookie.h>

/**
 * Base abstract class used handle SASL AUTH and SASL STEP.
 */
class SaslAuthCommandContext : public SteppableCommandContext {
public:
    // The authentication phase starts off in the Initial state where
    // we create a task and pass off to our executor before we wait
    // for it to complete. Once it completes we'll handle the
    // result in handleSaslAuthTaskResult, which also send the
    // response back to the client before we enter the "Done"
    // state which just returns and terminates the state
    // machinery.
    enum class State { Initial, HandleSaslAuthTaskResult, Done };

    explicit SaslAuthCommandContext(Cookie& cookie);

protected:
    cb::engine_errc step() override;

    /// Verify the input and start SASL authentication. If everything
    /// is OK it should set the state to HandleSaslAuthTaskResult
    virtual cb::engine_errc initial() = 0;
    /// Called by the state machine and the underlying implementation
    /// should call doHandleSaslAuthTaskResult with the appropriate
    /// error code and status and set the state to Done.
    virtual cb::engine_errc handleSaslAuthTaskResult() = 0;

    /**
     * Perform the correct action for a SASL authentication (build
     * response messages and perform the appropriate audit / logging)
     *
     * @param error The error code returned from SASL
     * @param payload The payload SASL provided that we should send to
     *                the client
     */
    cb::engine_errc doHandleSaslAuthTaskResult(cb::sasl::Error error,
                                               std::string_view payload);

    const cb::mcbp::Request& request;
    const std::string mechanism;
    const std::string challenge;
    State state;

private:
    cb::engine_errc tryHandleSaslOk(std::string_view payload);
    cb::engine_errc authContinue(std::string_view challenge);
    cb::engine_errc authBadParameters();
    cb::engine_errc authFailure(cb::sasl::Error error);
};
