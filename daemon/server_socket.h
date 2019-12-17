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

#include <nlohmann/json_fwd.hpp>
#include <memory>

class ListeningPort;
class NetworkInterface;

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
     * @param interf The interface object containing properties to use
     */
    ServerSocket(SOCKET sfd,
                 event_base* b,
                 std::shared_ptr<ListeningPort> interf);

    ~ServerSocket();

    /**
     * Get the socket (used for logging)
     */
    SOCKET getSocket() {
        return sfd;
    }

    void acceptNewClient();

    const ListeningPort& getInterfaceDescription() const {
        return *interface;
    }

    /// Update the interface description to use the provided SSL info
    void updateSSL(const std::string& key, const std::string& cert);

    /**
     * Get the details for this connection to put in the portnumber
     * file so that the test framework may pick up the port numbers
     */
    nlohmann::json toJson() const;

protected:
    /// The socket object to accept clients from
    const SOCKET sfd;

    std::shared_ptr<ListeningPort> interface;

    /// The sockets name (used for debug)
    const std::string sockname;

    /// The backlog to specify to bind
    const int backlog = 1024;

    struct EventDeleter {
        void operator()(struct event* e);
    };

    /// The libevent object we're using
    std::unique_ptr<struct event, EventDeleter> ev;
};
