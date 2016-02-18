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

#include <cJSON_utils.h>
#include <memory>

/**
 * The ListenConnection class is used by the "server sockets" in memcached.
 * It is not really a connection object, but a server socket that is used to
 * accept new clients. It could have been called ServerConnection, but I think
 * it would be misleading given that it is not used in a server to server
 * communication which the name would imply.
 */
class ListenConnection : public Connection {
public:
    ListenConnection() = delete;

    ListenConnection(SOCKET sfd,
                     event_base* b,
                     in_port_t port,
                     sa_family_t fam,
                     const struct interface &interf);

    virtual ~ListenConnection();

    ListenConnection(const ListenConnection&) = delete;

    virtual const Protocol getProtocol() const override;

    void enable();

    void disable();

    virtual void runEventLoop(short) override;

    bool isManagement() const {
        return management;
    }

    /**
     * Get the details for this connection to put in the portnumber
     * file so that the test framework may pick up the port numbers
     */
    unique_cJSON_ptr getDetails();

protected:
    bool registered_in_libevent;
    const sa_family_t family;
    const int backlog;
    const bool ssl;
    const bool management;
    const Protocol protocol;

    struct EventDeleter {
        void operator()(struct event* ev) {
            if (ev != nullptr) {
                event_free(ev);
            }
        }
    };

    std::unique_ptr<struct event, EventDeleter> ev;
};
