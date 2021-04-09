/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <daemon/memcached.h>
#include <memcached/dockey.h>
#include <memcached/engine.h>
#include <memcached/protocol_binary.h>
#include <platform/compress.h>

/**
 * The GatCommandContext is a state machine used by the memcached
 * core to implement the "get and touch" operation(s)
 */
class GatCommandContext : public SteppableCommandContext {
public:
    // The internal states. Look at the function headers below to
    // for the functions with the same name to figure out what each
    // state does
    enum class State : uint8_t {
        GetAndTouchItem,
        NoSuchItem,
        InflateItem,
        SendResponse,
        Done
    };

    explicit GatCommandContext(Cookie& cookie);

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
     * Try to lookup (and change the expiration time) the named item in the
     * underlying engine. Given that the engine may block we would return
     * cb::engine_errc::would_block in these cases (that could in theory happen
     * multiple times etc).
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
    cb::engine_errc getAndTouchItem();

    /**
     * Handle the case where the item isn't found. If the client don't want
     * to be notified about misses we'd just update the stats. Otherwise
     * we'll craft up the response messages and insert them into the pipe.
     *
     * The next state would be State::Done :-)
     *
     * @return cb::engine_errc::success if we want to continue to run the state
     * diagram a standard engine error code if something goes wrong
     */
    cb::engine_errc noSuchItem();

    /**
     * Inflate the document before progressing to State::SendResponse
     *
     * @return cb::engine_errc::failed if inflate failed
     *         cb::engine_errc::no_memory if we're out of memory
     *         cb::engine_errc::success to go to the next state
     */
    cb::engine_errc inflateItem();

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
    uint32_t getExptime(Cookie& cookie);

    const Vbid vbucket;
    const uint32_t exptime;

    cb::unique_item_ptr it;
    item_info info;

    std::string_view payload;
    cb::compression::Buffer buffer;
    State state;
};
