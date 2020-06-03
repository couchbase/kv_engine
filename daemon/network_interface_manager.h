/*
 *     Copyright 2020 Couchbase, Inc.
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

#include <event2/util.h>
#include <libevent/utilities.h>
#include <platform/socket.h>
#include <array>
#include <atomic>

/**
 * The NetworkInterfaceManager will eventually be responsible for adding /
 * removing network interfaces and keep control of all of the network
 * interfaces memcached currently expose.
 *
 * Right now it is just a copy of what used to be the old dispatcher
 * logic
 */
class NetworkInterfaceManager {
public:
    /**
     * Create a new instance and bind it to a given event base (the same
     * base as all of the listening sockets use)
     */
    explicit NetworkInterfaceManager(event_base* base);

    /**
     * Signal the network interface from any other thread (by sending
     * a message over the notification pipe)
     */
    void signal();

protected:
    static void event_handler(evutil_socket_t, short, void*);

    std::array<SOCKET, 2> pipe = {{INVALID_SOCKET, INVALID_SOCKET}};
    cb::libevent::unique_event_ptr event;
    std::atomic_bool check_listen_conn{};
};

/// The one and only instance of the network interface manager.
extern std::unique_ptr<NetworkInterfaceManager> networkInterfaceManager;
