/*
 *     Copyright 2017-Present Couchbase, Inc.
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
#include <memcached/dockey.h>
#include <memcached/engine.h>
#include <memcached/protocol_binary.h>

class ItemDissector;

/**
 * The GetLockedCommandContext is a state machine used by the memcached
 * core to implement the Get Locked operation
 */
class GetLockedCommandContext : public SteppableCommandContext {
public:
    // The internal states. Look at the function headers below to
    // for the functions with the same name to figure out what each
    // state does
    enum class State : uint8_t { GetAndLockItem, SendResponse, Done };

    explicit GetLockedCommandContext(Cookie& cookie);

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
    cb::engine_errc getAndLockItem();

    /**
     * Craft up the response message and send it to the client. Given that
     * the command context object lives until we start the next command
     * we don't need to copy the data into temporary buffers, but can point
     * directly into the actual item (or the temporary allocated inflated
     * buffer).
     *
     * @return cb::engine_errc::disconnect or cb::engine_errc::success
     */
    cb::engine_errc sendResponse();

private:
    /**
     * Pick out the lock timeout from the input message. This is an optional
     * field, and if not present it should be passed as 0 to the underlying
     * engine which would use the buckets default. (it is refactored as a
     * separate member function to make the code easier to read ;-)
     *
     * @param req the input message
     * @return The lock timeout value.
     */
    static uint32_t get_exptime(const cb::mcbp::Request& request);

    /// The VBucket where the document should be located in
    const Vbid vbucket;
    /// The timeout value for the lock
    const uint32_t lock_timeout;
    /// The actual item (looked up in getItem, and valid in sendResponse)
    std::unique_ptr<ItemDissector> item_dissector;
    /// The current state in the state machine
    State state;
};
