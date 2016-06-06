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
#include "statemachine_mcbp.h"

#include <exception>
#include <utilities/protocol2text.h>
#include <platform/strerror.h>
#include <string>
#include <memory>

ListenConnection::ListenConnection(SOCKET sfd,
                                   event_base* b,
                                   in_port_t port,
                                   sa_family_t fam,
                                   const interface& interf)
    : Connection(sfd, b),
      registered_in_libevent(false),
      family(fam),
      backlog(interf.backlog),
      ssl(!interf.ssl.cert.empty()),
      management(interf.management),
      protocol(interf.protocol),
      ev(event_new(b, sfd, EV_READ | EV_PERSIST, listen_event_handler,
                   reinterpret_cast<void*>(this))) {

    if (ev.get() == nullptr) {
        throw std::bad_alloc();
    }

    parent_port = port;
    resolveConnectionName(true);
    setAuthContext(auth_create(NULL, NULL, NULL));
    // Listen connections should not be associated with a bucket
    setBucketIndex(-1);
    enable();
}

ListenConnection::~ListenConnection() {
    disable();

}

const Protocol ListenConnection::getProtocol() const {
    // @todo we need a new version of this
    return Protocol::Memcached;
}

void ListenConnection::enable() {
    if (!registered_in_libevent) {
        if (management || is_server_initialized()) {
            LOG_NOTICE(this, "%u Listen on %s", getId(), getSockname().c_str());
            if (listen(getSocketDescriptor(), backlog) == SOCKET_ERROR) {
                LOG_WARNING(this, "%u: Failed to listen on %s: %s",
                            getId(), getSockname().c_str(), strerror(errno));
            }
        }

        if (event_add(ev.get(), NULL) == -1) {
            log_system_error(EXTENSION_LOG_WARNING,
                             NULL,
                             "Failed to add connection to libevent: %s");
        } else {
            registered_in_libevent = true;
        }
    }
}

void ListenConnection::disable() {
    if (registered_in_libevent) {
        if (getSocketDescriptor() != INVALID_SOCKET) {
            /*
             * Try to reduce the backlog length so that clients
             * may get ECONNREFUSED instead of blocking. Note that the
             * backlog parameter is a hint, so the actual value being
             * used may be higher than what we try to set it.
             */
            if (listen(getSocketDescriptor(), 1) == SOCKET_ERROR) {
                LOG_WARNING(this, "%u: Failed to set backlog to 1 on %s: %s",
                            getId(), getSockname().c_str(), strerror(errno));
            }
        }
        if (event_del(ev.get()) == -1) {
            log_system_error(EXTENSION_LOG_WARNING,
                             NULL,
                             "Failed to remove connection to libevent: %s");
        } else {
            registered_in_libevent = false;
        }
    }
}

void ListenConnection::runEventLoop(short) {
    try {
        do {
            LOG_DEBUG(this, "%u - Running task: (conn_listening)", getId());
        } while (conn_listening(this));
    } catch (std::invalid_argument& e) {
        LOG_WARNING(this,
                    "%d: exception occurred while accepting clients: %s",
                    getId(), e.what());
    }
}

unique_cJSON_ptr ListenConnection::getDetails() {
    unique_cJSON_ptr ret(cJSON_CreateObject());
    cJSON* obj = ret.get();

    if (ssl) {
        cJSON_AddTrueToObject(obj, "ssl");
    } else {
        cJSON_AddFalseToObject(obj, "ssl");
    }

    cJSON_AddStringToObject(obj, "protocol", to_string(protocol));
    if (family == AF_INET) {
        cJSON_AddStringToObject(obj, "family", "AF_INET");
    } else {
        cJSON_AddStringToObject(obj, "family", "AF_INET6");
    }

    cJSON_AddNumberToObject(obj, "port", parent_port);
    if (management) {
        cJSON_AddTrueToObject(obj, "management");
    } else {
        cJSON_AddFalseToObject(obj, "management");
    }

    return ret;
}
