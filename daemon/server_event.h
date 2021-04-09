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

#include <string>

class Connection;

/**
 * The server event is a base class for events the server may inject
 * into a connection and have the server execute as part of running
 * the state machinery.
 */
class ServerEvent {
public:
    virtual ~ServerEvent() = default;

    /**
     * Get a textual description of the event type (used for debugging
     * purposes)
     */
    virtual std::string getDescription() const = 0;

    /**
     * Execute the server event
     *
     * @param connection the connection object the server event is
     *                    bound to (to allow the event to inject
     *                    messages into the connection object.
     * @return true if we're done executing the event, false if
     *              execute should be called at a later time.
     */
    virtual bool execute(Connection& connection) = 0;
};
