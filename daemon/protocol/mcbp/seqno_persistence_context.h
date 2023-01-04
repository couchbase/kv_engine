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

#include <mcbp/protocol/status.h>

/**
 * SeqnoPersistenceCommandContext is a steppable command context
 * used for implementing SeqnoPersistence command
 */
class SeqnoPersistenceCommandContext : public SteppableCommandContext {
public:
    explicit SeqnoPersistenceCommandContext(Cookie& cookie);

protected:
    cb::engine_errc step() override;
    const uint64_t seqno;
    enum class State { Wait, Done };
    State state = State::Wait;
};
