/*
 *     Copyright 2026-Present Couchbase, Inc.
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
#include <functional>

/**
 * NoSuccessResponseCommandContext is used to implement commands which just
 * use a single state method which may or may not block and do not respond
 * for success (most DCP messages...)
 *
 * The handler should return status and drive will process if an error occurs
 */
class NoSuccessResponseCommandContext : public SteppableCommandContext {
public:
    explicit NoSuccessResponseCommandContext(
            Cookie& cookie, std::function<cb::engine_errc(Cookie&)> handler)
        : SteppableCommandContext(cookie), handler(std::move(handler)) {
    }

protected:
    cb::engine_errc step() override {
        // called from drive, which will respond on error.
        return handler(cookie);
    }
    const std::function<cb::engine_errc(Cookie&)> handler;
};
