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

#include <memcached/protocol_binary.h>
#include <memcached/engine.h>
#include <daemon/memcached.h>

/**
 * The RemoveCommandContext is a state machine used by the memcached
 * core to implement DELETE.
 */
class RemoveCommandContext : public SteppableCommandContext {
public:
    /**
     * The internal state machine used to implement the DELETE
     */
    enum class State : uint8_t {
        // Look up the item to delete
        GetItem,
        // Rebuild the xattr segment we want to preserve
        RebuildXattr,
        // Allocate a new deleted object
        AllocateDeletedItem,
        // Store the deleted document (tombstone)
        StoreItem,
        // The document did not have any xattrs do an "old style" remove
        RemoveItem,
        // Release all allocated resources. The reason we've got a separate
        // state for this and not using the destructor for this is that
        // we try to store the newly created document with a CAS operation
        // and we might have a race with another client.
        Reset,
        // Send the response back to the client if requested
        SendResponse,
        // We're all done :)
        Done
    };

    RemoveCommandContext(Cookie& cookie, const cb::mcbp::Request& req)
        : SteppableCommandContext(cookie),
          key(cookie.getRequestKey()),
          vbucket(req.getVBucket()),
          input_cas(req.getCas()),
          state(State::GetItem),
          mutation_descr{} {
    }

protected:
    ENGINE_ERROR_CODE step() override;

    /**
     * Fetch the existing item from the underlying engine. This must exist
     * for the operation to success.
     *
     * @return ENGINE_SUCCESS if we want to continue the state machine
     */
    ENGINE_ERROR_CODE getItem();

    /**
     * Rebuild the XATTR segment to keep with the deleted document
     *
     * According to the spec everything except the system xattrs
     * should be stripped off.
     *
     * @return
     */
    ENGINE_ERROR_CODE rebuildXattr();

    /**
     * Allocate the item used to represent the deleted object. (note that
     * the object may disappear at any point in time when we release it.
     * The underlying engine is not obligated to keep it around)
     *
     * In the future we'll extend this code to preserve the XATTRs from
     * the original document
     *
     * @return ENGINE_SUCCESS if we want to continue the state machine
     */
    ENGINE_ERROR_CODE allocateDeletedItem();

    /**
     * Store the deleted document in the underlying engine.
     *
     * @return ENGINE_SUCCESS if we want to continue the state machine
     */
    ENGINE_ERROR_CODE storeItem();

    /**
     * Try to remove the document with the "old style" remove (the
     * document didn't have any xattrs we wanted to preserve)
     *
     * @return ENGINE_SUCCESS if we want to continue the state machine
     */
    ENGINE_ERROR_CODE removeItem();

    /**
     * Create and send the response packet back to the client (include
     * sequence number and vbucket uuid if the client asked for it)
     *
     * @return ENGINE_SUCCESS if we want to continue the state machine
     */
    ENGINE_ERROR_CODE sendResponse();

    /**
     * Release all allocated resources
     *
     * @return ENGINE_SUCCESS if we want to continue the state machine
     */
    ENGINE_ERROR_CODE reset();

private:
    const DocKey key;
    const uint16_t vbucket;
    const uint64_t input_cas;

    // The current state we're operating in, and where we'll resume
    // after returned from an EWOULDBLOCK
    State state;

    // Pointer to the deleted object to store
    cb::unique_item_ptr deleted;

    // Pointer to the current value stored in the engine
    cb::unique_item_ptr existing;

    // The metadata for the existing item
    item_info existing_info;

    // The mutation descriptor for the mutation
    mutation_descr_t mutation_descr;

    // The xattr segment to keep around
    cb::char_buffer xattr;

    // Backing store for the modified xattrs
    std::unique_ptr<char[]> xattr_buffer;
};
