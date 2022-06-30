/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "steppable_command_context.h"

#include <daemon/cookie.h>
#include <daemon/memcached.h>
#include <memcached/dockey.h>
#include <memcached/engine.h>
#include <memcached/protocol_binary.h>

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
          vbucket(req.getVBucket()),
          input_cas(req.getCas()),
          state(State::GetItem),
          mutation_descr{} {
    }

protected:
    cb::engine_errc step() override;

    /**
     * Fetch the existing item from the underlying engine. This must exist
     * for the operation to success.
     *
     * @return cb::engine_errc::success if we want to continue the state machine
     */
    cb::engine_errc getItem();

    /**
     * Rebuild the XATTR segment to keep with the deleted document
     *
     * According to the spec everything except the system xattrs
     * should be stripped off.
     *
     * @return
     */
    cb::engine_errc rebuildXattr();

    /**
     * Allocate the item used to represent the deleted object. (note that
     * the object may disappear at any point in time when we release it.
     * The underlying engine is not obligated to keep it around)
     *
     * In the future we'll extend this code to preserve the XATTRs from
     * the original document
     *
     * @return cb::engine_errc::success if we want to continue the state machine
     */
    cb::engine_errc allocateDeletedItem();

    /**
     * Store the deleted document in the underlying engine.
     *
     * @return cb::engine_errc::success if we want to continue the state machine
     */
    cb::engine_errc storeItem();

    /**
     * Try to remove the document with the "old style" remove (the
     * document didn't have any xattrs we wanted to preserve)
     *
     * @return cb::engine_errc::success if we want to continue the state machine
     */
    cb::engine_errc removeItem();

    /**
     * Create and send the response packet back to the client (include
     * sequence number and vbucket uuid if the client asked for it)
     *
     * @return cb::engine_errc::success if we want to continue the state machine
     */
    cb::engine_errc sendResponse();

    /**
     * Release all allocated resources
     *
     * @return cb::engine_errc::success if we want to continue the state machine
     */
    cb::engine_errc reset();

private:
    const Vbid vbucket;
    const uint64_t input_cas;

    // The current state we're operating in, and where we'll resume
    // after returned from an EWOULDBLOCK
    State state;

    // Pointer to the deleted object to store
    cb::unique_item_ptr deleted;

    // Pointer to the current value stored in the engine
    cb::unique_item_ptr existing;

    // The metadata for the existing item
    uint64_t existing_cas;
    uint8_t existing_datatype;

    // The mutation descriptor for the mutation
    mutation_descr_t mutation_descr;

    // The xattr segment to keep around
    std::string_view xattr;

    // Backing store for the modified xattrs
    std::unique_ptr<char[]> xattr_buffer;
};
