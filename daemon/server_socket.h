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
 * The ServerSocket represents the socket used to accept new clients.
 */
class ServerSocket {
public:
    ServerSocket() = delete;
    ServerSocket(const ServerSocket&) = delete;

    /**
     * Create a new instance
     *
     * @param sfd The socket to operate on
     * @param b The event base to use (the caller owns the event base)
     * @param port The port we're listening to
     * @param fam The address family for the port (IPv4/6)
     * @param interf The interface object containing properties to use (backlog,
     *               ssl, management etc)
     */
    ServerSocket(SOCKET sfd,
                 event_base* b,
                 in_port_t port,
                 sa_family_t fam,
                 const NetworkInterface& interf);

    ~ServerSocket();

    /**
     * Get the name of the socket in a human readable form (used for logging)
     */
    const std::string& getSockname() {
        return sockname;
    }

    /**
     * Get the socket (used for logging)
     */
    SOCKET getSocket() {
        return sfd;
    }

    void enable();

    void disable();

    void acceptNewClient();

    /**
     * Get the details for this connection to put in the portnumber
     * file so that the test framework may pick up the port numbers
     */
    unique_cJSON_ptr getDetails();

protected:
    /// The socket object to accept clients from
    const SOCKET sfd;

    /// The port number we're listening on
    in_port_t listen_port;

    /// The address family of this server socket (IPv4 / IPv6)
    const sa_family_t family;

    /// The sockets name (used for debug)
    const std::string sockname;

    /// The backlog to specify to bind
    const int backlog;

    /// Is this an SSL connection or not
    const bool ssl;

    /// Is this socket supposed to be used for management traffic?
    const bool management;

    struct EventDeleter {
        void operator()(struct event* ev) {
            if (ev != nullptr) {
                event_free(ev);
            }
        }
    };

    /// Are we currently registered in libevent or not
    bool registered_in_libevent = {false};

    /// The libevent object we're using
    std::unique_ptr<struct event, EventDeleter> ev;
};
