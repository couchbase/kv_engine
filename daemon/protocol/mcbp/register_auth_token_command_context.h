/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once
#include "background_thread_command_context.h"
#include <daemon/decode_token_task.h>

/**
 * The RegisterAuthTokenCommandContext is a command context task that is used to
 * execute the RegisterAuthToken command.
 */
class RegisterAuthTokenCommandContext : public SteppableCommandContext {
public:
    enum class State : uint8_t { RemoveId, AddId, WaitForDecode, Done };

    explicit RegisterAuthTokenCommandContext(Cookie& cookie);

protected:
    cb::engine_errc step() override;

    cb::engine_errc remove_id();
    cb::engine_errc add_id();

    std::shared_ptr<DecodeTokenTask> decodeTokenTask;
    State state;
    uint16_t id = 0;
    std::string token;
};
