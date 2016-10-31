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
#include "config.h"
#include "memcached.h"
#include "runtime.h"

#include <exception>
#include <utilities/protocol2text.h>
#include <platform/strerror.h>


GreenstackConnection::GreenstackConnection(SOCKET sfd, event_base* b,
                                           const ListeningPort& ifc)
    : Connection(sfd, b, ifc),
      connectionState(ConnectionState::ESTABLISHED) {
    if (ifc.protocol != Protocol::Greenstack) {
        throw std::logic_error("Incorrect object for Greenstack");
    }
}

const Protocol GreenstackConnection::getProtocol() const {
    return Protocol::Greenstack;
}

cJSON* GreenstackConnection::toJSON() const {
    cJSON* obj = Connection::toJSON();
    if (obj != nullptr) {
        cJSON_AddStringToObject(obj, "connection_state",
                                to_string(connectionState));
    }

    return obj;
}

void GreenstackConnection::runEventLoop(short) {
    throw std::runtime_error("Not implemented");
}
