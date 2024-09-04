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
 * The SetActiveEncryptionKeysContext is a state machine used by the memcached
 * core to implement the "SetActiveEncryptionKeys" operation
 */
class SetActiveEncryptionKeysContext : public SteppableCommandContext {
public:
    enum class State {
        /// In the schedule task state the task is scheduled on the
        /// executor pool and continues to the "Done" state
        ScheduleTask,
        /// In the done state the result of the operation is sent to the client
        Done
    };

    explicit SetActiveEncryptionKeysContext(Cookie& cookie);

protected:
    // Execute the operation when running on the executor
    void execute();

    cb::engine_errc step() override;
    cb::engine_errc scheduleTask();
    cb::engine_errc done() const;
    const nlohmann::json json;
    const std::string entity;
    cb::engine_errc status = cb::engine_errc::invalid_arguments;
    State state = State::ScheduleTask;
};
