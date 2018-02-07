/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once

#include "steppable_command_context.h"

#include <daemon/memcached.h>
#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/request.h>

/**
 * FlushCommandContext is responsible for handling the
 * flush command. It may block and offload the configure
 * event to a different thread and be notified later on.
 */
class FlushCommandContext : public SteppableCommandContext {
public:
    enum class State {
        /// The command context starts off in the Flushing state
        /// which triggers the flush in the underlying engine (which
        /// may offload the flush to a different thread).
        Flushing,
        /// Send the response back to the client
        Done
    };

    explicit FlushCommandContext(Cookie& cookie)
        : SteppableCommandContext(cookie), state(State::Flushing) {
        LOG_NOTICE(&connection, "%u: flush b:%s", connection.getId(),
                   connection.getBucket().name);
    }

protected:
    ENGINE_ERROR_CODE step() override {
        auto ret = ENGINE_SUCCESS;
        do {
            switch (state) {
            case State::Flushing:
                ret = flushing();
                break;
            case State::Done:
                done();
                return ENGINE_SUCCESS;
            }
        } while (ret == ENGINE_SUCCESS);

        return ret;
    }

    ENGINE_ERROR_CODE flushing();
    void done();

private:
    State state;
};
