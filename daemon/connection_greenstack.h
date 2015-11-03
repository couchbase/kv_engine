/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "connection.h"

/**
 * The state of the connection.
 */
enum class ConnectionState : uint8_t {
    /**
     * Right after the connection is established we're in the established
     * state. At this point we're waiting for the client to greet us
     * with the HELLO command.
     */
    ESTABLISHED,
    /**
     * After receiving HELLO we're in the OPEN state. At this point we
     * accept all commands.
     */
    OPEN,
    /**
     * After a successful SASL AUTH we're in the authenticated state. At
     * this point we accept all commands
     */
    AUTHENTICATED
};

const char* to_string(const ConnectionState& connectionState);

class GreenstackConnection : public Connection {
public:
    GreenstackConnection(SOCKET sfd, event_base* b) = delete;

    GreenstackConnection(SOCKET sfd, event_base* b,
                         const struct listening_port& ifc);

    virtual cJSON* toJSON() const override;

    virtual const Protocol getProtocol() const override;


    /**
     * Get the state of the connection.
     *
     * This state is currently only used by the state machinery in Greenstack.
     * At some point we should refactor the conneciton object to use a union
     * for Greenstack and MemcachedBinaryProtocol (or a sub class)
     */
    const ConnectionState& getConnectionState() const {
        return connectionState;
    }

    /**
     * Set the state of the connection.
     *
     * This state is currently only used by the state machinery in Greenstack.
     * At some point we should refactor the conneciton object to use a union
     * for Greenstack and MemcachedBinaryProtocol (or a sub class)
     */
    void setConnectionState(const ConnectionState& connectionState) {
        GreenstackConnection::connectionState = connectionState;
    }

    virtual void runEventLoop(short) override;

protected:

    /** The state of the connection (used by greenstack only) */
    ConnectionState connectionState;
};
