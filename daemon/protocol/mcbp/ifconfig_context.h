/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "steppable_command_context.h"

namespace cb::mcbp {
enum class Status : uint16_t;
}

/**
 * The IfconfigCommandContext is a state machine used by the memcached
 * core to implement the "ifconfig" operation(s)
 */
class IfconfigCommandContext : public SteppableCommandContext {
public:
    // The internal states. Look at the function headers below to
    // for the functions with the same name to figure out what each
    // state does
    enum class State { scheduleTask, Done };

    explicit IfconfigCommandContext(Cookie& cookie);

protected:
    cb::engine_errc step() override;

    cb::engine_errc scheduleTask();
    cb::engine_errc done();

    State state = State::scheduleTask;

    cb::mcbp::Status status;
    std::string payload;
};