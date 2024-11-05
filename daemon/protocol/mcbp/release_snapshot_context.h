/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "steppable_command_context.h"

/**
 * Implementation of the "ReleaseSnapshot" command.
 *
 * The command takes the UUID for the snapshot in the key and removes
 * the on-disk knowledge of the snapshot. Given that we don't want
 * file IO from the worker threads it'll schedule a task to run
 * in the thread pool to perform the actual IO before being rescheduled
 * to send the reply back to the client.
 */
class ReleaseSnapshotContext : public SteppableCommandContext {
public:
    enum class State : uint8_t { Initialize, Done };

    explicit ReleaseSnapshotContext(Cookie& cookie);

protected:
    cb::engine_errc step() override;
    cb::engine_errc initialize();

    const std::string uuid;
    State state = State::Initialize;
};
