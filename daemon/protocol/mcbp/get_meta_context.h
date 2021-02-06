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

#include <memcached/dockey.h>
#include "steppable_command_context.h"

/**
 * The GetMetaCommandContext is a state machine used by the memcached
 * core to implement the Get_Meta operation
 */
class GetMetaCommandContext : public SteppableCommandContext {
public:
    // The internal states. Look at the function headers below to
    // for the functions with the same name to figure out what each
    // state does
    enum class State : uint8_t { GetItemMeta, NoSuchItem, SendResponse, Done };

    explicit GetMetaCommandContext(Cookie& cookie);

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
     * Try to lookup the named item metadata in the underlying engine. Given
     * that the engine may block (in case Full Eviction is enabled) we would
     * return cb::engine_errc::would_block in these cases (that could in theory
     * happen multiple times etc).
     *
     * If the item is found we move to the State::SendResponse state. We move
     * to the State::NoSuchItem otherwise.
     *
     * @return cb::engine_errc::would_block if the underlying engine needs to
     * block cb::engine_errc::success if we want to continue to run the state
     * diagram a standard engine error code if something goes wrong
     */
    cb::engine_errc getItemMeta();

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
     * Make the response message and send it to the client.
     *
     * @return cb::engine_errc::success
     */
    cb::engine_errc sendResponse();

private:
    const Vbid vbucket;

    State state;

    item_info info;
    bool fetchDatatype;
};
