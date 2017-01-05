/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include <memcached/engine.h>
#include <memcached/protocol_binary.h>

#include <daemon/unique_item_ptr.h>

/**
 * The MutationCommandContext is a state machine used by the memcached
 * core to implement ADD, SET and REPLACE.
 */
class MutationCommandContext : public SteppableCommandContext {
public:
    /**
     * The internal state machine used to implement the mutation commands
     * Add, Set, Replace
     */
    enum class State : uint8_t {
        // Validate the input data
        ValidateInput,
        // Allocate the destination object
        AllocateNewItem,
        // Store the new document
        StoreItem,
        // Send the response back to the client
        SendResponse,
        // We're all done :)
        Done
    };

    MutationCommandContext(McbpConnection& c,
                           protocol_binary_request_set* req,
                           const ENGINE_STORE_OPERATION op_);

protected:
    ENGINE_ERROR_CODE step() override;

    /**
     * Validate the input data provided by the client, and update the
     * datatype for the document if the client isn't datatype aware.
     *
     * @return ENGINE_SUCCESS if we want to proceed to the next state
     */
    ENGINE_ERROR_CODE validateInput();

    /**
     * Allocate memory for the object and initialize it with the value
     * provided by the client

     * @return ENGINE_SUCCESS if we want to proceed to the next state
     */
    ENGINE_ERROR_CODE allocateNewItem();

    /**
     * Store the newly created document in the engine
     *
     * @return ENGINE_SUCCESS if we want to proceed to the next state
     */
    ENGINE_ERROR_CODE storeItem();

    /**
     * Send the response back to the client (or progress to the next
     * phase for quiet ops).
     *
     * @return ENGINE_SUCCESS if we want to proceed to the next state
     */
    ENGINE_ERROR_CODE sendResponse();

private:
    const ENGINE_STORE_OPERATION operation;
    const DocKey key;
    const cb::const_char_buffer value;
    const uint16_t vbucket;
    const uint64_t input_cas;
    const rel_time_t expiration;
    const uint32_t flags;

    // The datatype for the document to be created. We trust datatype aware
    // clients to correctly initialize this.
    protocol_binary_datatype_t datatype;

    // The current state
    State state;

    // The newly created document
    cb::unique_item_ptr newitem;
};
