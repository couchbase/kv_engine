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
    ENGINE_ERROR_CODE step() override;

    /**
     * Try to lookup (and change the expiration time) the named item in the
     * underlying engine. Given that the engine may block we would return
     * ENGINE_EWOULDBLOCK in these cases (that could in theory happen multiple
     * times etc).
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
     * @return ENGINE_EWOULDBLOCK if the underlying engine needs to block
     *         ENGINE_SUCCESS if we want to continue to run the state diagram
     *         a standard engine error code if something goes wrong
     */
    ENGINE_ERROR_CODE getAndTouchItem();

    /**
     * Handle the case where the item isn't found. If the client don't want
     * to be notified about misses we'd just update the stats. Otherwise
     * we'll craft up the response messages and insert them into the pipe.
     *
     * The next state would be State::Done :-)
     *
     * @return ENGINE_SUCCESS if we want to continue to run the state diagram
     *         a standard engine error code if something goes wrong
     */
    ENGINE_ERROR_CODE noSuchItem();

    /**
     * Inflate the document before progressing to State::SendResponse
     *
     * @return ENGINE_FAILED if inflate failed
     *         ENGINE_ENOMEM if we're out of memory
     *         ENGINE_SUCCESS to go to the next state
     */
    ENGINE_ERROR_CODE inflateItem();

    /**
     * Craft up the response message and send it to the client. Given that
     * the command context object lives until we start the next command
     * we don't need to copy the data into temporary buffers, but can point
     * directly into the actual item (or the temporary allocated inflated
     * buffer).
     *
     * @return ENGINE_DISCONNECT or ENGINE_SUCCESS
     */
    ENGINE_ERROR_CODE sendResponse();

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
