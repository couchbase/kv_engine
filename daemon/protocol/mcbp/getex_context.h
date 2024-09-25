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
#include <daemon/cookie.h>
#include <daemon/stats.h>
#include <mcbp/protocol/header.h>
#include <memcached/engine.h>

class ItemDissector;

/**
 * The GetExCommandContext is a state machine used by the memcached
 * core to implement the GetEx[Replica] operation
 */
class GetExCommandContext : public SteppableCommandContext {
public:
    // The internal states. Look at the function headers below to
    // for the functions with the same name to figure out what each
    // state does
    enum class State { GetItem, MaybeStripXattrs, SendResponse, Done };

    explicit GetExCommandContext(Cookie& cookie);

protected:
    /**
     * Keep running the state machine.
     *
     * @return A standard engine error code (if SUCCESS we've changed the
     *         the connections state to one of the appropriate states (send
     *         data, or start processing the next command)
     */
    cb::engine_errc step() override;

    /**
     * Try to lookup the named item in the underlying engine. Given that
     * the engine may block we would return cb::engine_errc::would_block in
     * these cases (that could in theory happen multiple times etc).
     *
     * If the document is found we may move to the State::InflateItem if we
     * have to inflate the item before we can send it to the client (that
     * would happen if the document is compressed and the client can't handle
     * that (or it contains xattrs which we need to strip off).
     *
     * If the object isn't compressed (or it doesn't contain any xattrs and
     * the client won't freak out if we send compressed data) we'll progress
     * into the State::SendResponse state.
     *
     * @return cb::engine_errc::would_block if the underlying engine needs to
     * block cb::engine_errc::success if we want to continue to run the state
     * diagram a standard engine error code if something goes wrong
     */
    cb::engine_errc getItem();

    void maybeStripXattrs();

    /**
     * Craft up the response message and send it to the client. Given that
     * the command context object lives until we start the next command
     * we don't need to copy the data into temporary buffers, but can point
     * directly into the actual item (or the temporary allocated inflated
     * buffer).
     */
    void sendResponse();

    /// The VBucket where the document should be located in
    const Vbid vbucket;
    /// The item (in the case we don't need to inflate it)
    cb::unique_item_ptr item;

    /// The current state in the state machine
    State state = State::GetItem;

    protocol_binary_datatype_t datatype = PROTOCOL_BINARY_RAW_BYTES;
    std::optional<std::string> value_copy;
    uint32_t flags = 0;
};
