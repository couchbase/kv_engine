/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "steppable_command_context.h"
#include <mcbp/protocol/datatype.h>
#include <expected>
#include <functional>
#include <string>

/**
 * SingleStateCommandContext is used to implement commands which just
 * use a single state method which may or may not block.
 *
 * The handler function provided to the class contains the implementation
 * of the command, and SingleStateCommandContext deals with all return
 * codes (including would_block and call the handler at a later time
 * once the cookie has been notified).
 *
 * The handler should return the success payload (which may be empty)
 * when the execution of the command is complete; SingleStateCommandContext
 * will then send a "success" message back to the client with that value
 * as the payload. On failure the handler should return the error via
 * std::unexpected.
 */
class SingleStateCommandContext : public SteppableCommandContext {
public:
    using Handler =
            std::function<std::expected<std::string, cb::engine_errc>(Cookie&)>;

    /**
     * Create a SingleStateCommandContext with a handler that returns a
     * payload on success or an engine error code on failure.
     *
     * @param cookie The client cookie associated with the command
     * @param handler Callback returning std::expected with success payload
     * string or cb::engine_errc
     * @param successDatatype Datatype of the payload sent on success
     */
    SingleStateCommandContext(
            Cookie& cookie,
            Handler handler,
            cb::mcbp::Datatype successDatatype = cb::mcbp::Datatype::Raw);

    /**
     * Adapt a plain cb::engine_errc result (no success payload) to the
     * SingleStateCommandContext handler contract.
     */
    static std::expected<std::string, cb::engine_errc> noPayload(
            cb::engine_errc status) {
        if (status == cb::engine_errc::success) {
            return std::string{};
        }
        return std::unexpected(status);
    }

protected:
    cb::engine_errc step() override;
    const Handler handler;
    const cb::mcbp::Datatype successDatatype;
};
