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
#include <memcached/dockey_view.h>
#include <memcached/vbucket.h>
#include <deque>

/**
 * ObserveCommandContext is used to implement the observe command
 */
class ObserveCommandContext : public SteppableCommandContext {
public:
    enum class State { Initialize, Observe, Done };
    explicit ObserveCommandContext(Cookie& cookie);

protected:
    cb::engine_errc step() override;
    cb::engine_errc initialize();
    cb::engine_errc observe();

    /// The current state
    State state = State::Initialize;
    std::stringstream output;
    uint64_t persist_time_hint;
    std::deque<std::pair<Vbid, DocKeyView>> keys;
};
