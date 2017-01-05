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

#include <platform/compress.h>
#include <daemon/unique_item_ptr.h>
#include <include/memcached/protocol_binary.h>
#include "daemon/memcached.h"
#include "steppable_command_context.h"

/**
 * The UnlockCommandContext is a state machine used by the memcached
 * core to implement the Unlock Key operation
 */
class UnlockCommandContext : public SteppableCommandContext {
public:
    // The internal states. Look at the function headers below to
    // for the functions with the same name to figure out what each
    // state does
    enum class State : uint8_t {
        Initialize,
        Unlock,
        Done
    };

    UnlockCommandContext(McbpConnection& c,
                            protocol_binary_request_no_extras* req)
        : SteppableCommandContext(c),
          key(req->bytes + sizeof(req->bytes),
              ntohs(req->message.header.request.keylen),
              DocNamespace::DefaultCollection),
          vbucket(ntohs(req->message.header.request.vbucket)),
          cas(ntohll(req->message.header.request.cas)),
          state(State::Initialize) {
    }

protected:
    /**
     * Keep running the state machine.
     *
     * @return A standard engine error code (if SUCCESS we've changed the
     *         the connections state to one of the appropriate states (send
     *         data, or start processing the next command)
     */
    ENGINE_ERROR_CODE step() override;

    /**
     * This is the initial state of the Unlock operation. It may log the
     * operation (and it would be the place where you would add a phosphor
     * trace if you wanted to trace get requests
     *
     * @return ENGINE_SUCCESS (always)
     */
    ENGINE_ERROR_CODE initialize();

    /**
     * Unlock the document (this is the state the statemachine would
     * be "stuck" in if the underlying engine returns EWOULDBLOCK
     *
     * @return The return value of the engine interface unlock method
     */
     ENGINE_ERROR_CODE unlock();

private:
    const DocKey key;
    const uint16_t vbucket;
    const uint64_t cas;

    State state;
};
