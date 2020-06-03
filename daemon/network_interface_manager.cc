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
#include "network_interface_manager.h"
#include "log_macros.h"
#include "memcached.h"
#include <platform/strerror.h>

std::unique_ptr<NetworkInterfaceManager> networkInterfaceManager;

NetworkInterfaceManager::NetworkInterfaceManager(event_base* base) {
    if (!create_nonblocking_socketpair(pipe)) {
        FATAL_ERROR(EXIT_FAILURE, "Unable to create notification pipe");
    }

    event.reset(event_new(base,
                          pipe[0],
                          EV_READ | EV_PERSIST,
                          NetworkInterfaceManager::event_handler,
                          this));

    if (!event) {
        FATAL_ERROR(EXIT_FAILURE, "Failed to allocate network manager event");
    }

    if (event_add(event.get(), nullptr) == -1) {
        FATAL_ERROR(EXIT_FAILURE, "Can't setup network manager event");
    }
}

void NetworkInterfaceManager::signal() {
    check_listen_conn = true;
    if (pipe[1] != INVALID_SOCKET) {
        if (cb::net::send(pipe[1], "", 1, 0) != 1 &&
            !cb::net::is_blocking(cb::net::get_socket_error())) {
            LOG_WARNING("Failed to notify network manager: {}",
                        cb_strerror(cb::net::get_socket_error()));
        }
    }
}
