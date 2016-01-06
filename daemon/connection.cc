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

const char* to_string(const Connection::Priority& priority) {
    switch (priority) {
    case Connection::Priority::High:
        return "High";
    case Connection::Priority::Medium:
        return "Medium";
    case Connection::Priority::Low:
        return "Low";
    }
    throw std::invalid_argument("No such priority: " +
                                std::to_string(int(priority)));
}

static cbsasl_conn_t* create_new_cbsasl_server_t() {
    cbsasl_conn_t *conn;
    if (cbsasl_server_new("memcached", // service
                          nullptr, // Server DQDN
                          nullptr, // user realm
                          nullptr, // iplocalport
                          nullptr, // ipremoteport
                          nullptr, // callbacks
                          0, // flags
                          &conn) != CBSASL_OK) {
        throw std::bad_alloc();
    }
    return conn;
}

Connection::Connection(SOCKET sfd, event_base* b)
    : socketDescriptor(sfd),
      base(b),
      sasl_conn(create_new_cbsasl_server_t()),
      admin(false),
      authenticated(false),
      username("unknown"),
      nodelay(false),
      refcount(0),
      engine_storage(nullptr),
      next(nullptr),
      thread(nullptr),
      parent_port(0),
      auth_context(nullptr),
      bucketIndex(0),
      bucketEngine(nullptr),
      peername("unknown"),
      sockname("unknown"),
      priority(Priority::Medium) {
    MEMCACHED_CONN_CREATE(this);
}

Connection::Connection(SOCKET sock,
                       event_base* b,
                       const struct listening_port& interface)
    : Connection(sock, b)
{
    parent_port = interface.port;
    resolveConnectionName(false);
    setAuthContext(auth_create(NULL, peername.c_str(), sockname.c_str()));
    setTcpNoDelay(interface.tcp_nodelay);
}

Connection::~Connection() {
    MEMCACHED_CONN_DESTROY(this);
    auth_destroy(auth_context);
    if (socketDescriptor != INVALID_SOCKET) {
        LOG_INFO(this, "%u - Closing socket descriptor", getId());
        safe_close(socketDescriptor);
    }
}

/**
 * Convert a sockaddr_storage to a textual string (no name lookup).
 *
 * @param addr the sockaddr_storage received from getsockname or
 *             getpeername
 * @param addr_len the current length used by the sockaddr_storage
 * @return a textual string representing the connection. or NULL
 *         if an error occurs (caller takes ownership of the buffer and
 *         must call free)
 */
static std::string sockaddr_to_string(const struct sockaddr_storage* addr,
                                      socklen_t addr_len) {
    char host[50];
    char port[50];

    int err = getnameinfo(reinterpret_cast<const struct sockaddr*>(addr),
                          addr_len,
                          host, sizeof(host),
                          port, sizeof(port),
                          NI_NUMERICHOST | NI_NUMERICSERV);
    if (err != 0) {
        LOG_WARNING(NULL, "getnameinfo failed with error %d", err);
        return NULL;
    }

    if (addr->ss_family == AF_INET6) {
        return "[" + std::string(host) + "]:" + std::string(port);
    } else {
        return std::string(host) + ":" + std::string(port);
    }
}

void Connection::resolveConnectionName(bool listening) {
    int err;
    try {
        if (listening) {
            peername = "*";
        } else {
            struct sockaddr_storage peer;
            socklen_t peer_len = sizeof(peer);
            if ((err = getpeername(socketDescriptor,
                                   reinterpret_cast<struct sockaddr*>(&peer),
                                   &peer_len)) != 0) {
                LOG_WARNING(NULL, "getpeername for socket %d with error %d",
                            socketDescriptor, err);
            } else {
                peername = sockaddr_to_string(&peer, peer_len);
            }
        }

        struct sockaddr_storage sock;
        socklen_t sock_len = sizeof(sock);
        if ((err = getsockname(socketDescriptor,
                               reinterpret_cast<struct sockaddr*>(&sock),
                               &sock_len)) != 0) {
            LOG_WARNING(NULL, "getsockname for socket %d with error %d",
                        socketDescriptor, err);
        } else {
            sockname = sockaddr_to_string(&sock, sock_len);
        }
    } catch (std::bad_alloc& e) {
        LOG_WARNING(NULL,
                    "Connection::resolveConnectionName: failed to allocate memory: %s",
                    e.what());
    }
}

bool Connection::setTcpNoDelay(bool enable) {
    int flags = enable ? 1 : 0;

#if defined(WIN32)
    char* flags_ptr = reinterpret_cast<char*>(&flags);
#else
    void* flags_ptr = reinterpret_cast<void*>(&flags);
#endif
    int error = setsockopt(socketDescriptor, IPPROTO_TCP, TCP_NODELAY,
                           flags_ptr,
                           sizeof(flags));

    if (error != 0) {
        std::string errmsg = cb_strerror(GetLastNetworkError());
        LOG_WARNING(this, "setsockopt(TCP_NODELAY): %s",
                    errmsg.c_str());
        nodelay = false;
        return false;
    } else {
        nodelay = enable;
    }

    return true;
}

/* cJSON uses double for all numbers, so only has 53 bits of precision.
 * Therefore encode 64bit integers as string.
 */
static cJSON* json_create_uintptr(uintptr_t value) {
    char buffer[32];
    if (snprintf(buffer, sizeof(buffer),
                 "0x%" PRIxPTR, value) >= int(sizeof(buffer))) {
        return cJSON_CreateString("<too long>");
    } else {
        return cJSON_CreateString(buffer);
    }
}

static void json_add_uintptr_to_object(cJSON* obj, const char* name,
                                       uintptr_t value) {
    cJSON_AddItemToObject(obj, name, json_create_uintptr(value));
}

static void json_add_bool_to_object(cJSON* obj, const char* name, bool value) {
    if (value) {
        cJSON_AddTrueToObject(obj, name);
    } else {
        cJSON_AddFalseToObject(obj, name);
    }
}

const char* to_string(const Protocol& protocol) {
    if (protocol == Protocol::Memcached) {
        return "memcached";
    } else if (protocol == Protocol::Greenstack) {
        return "greenstack";
    } else {
        return "unknown";
    }
}

const char* to_string(const ConnectionState& connectionState) {
    switch (connectionState) {
    case ConnectionState::ESTABLISHED:
        return "established";
    case ConnectionState::OPEN:
        return "open";
    case ConnectionState::AUTHENTICATED:
        return "authenticated";
    }

    throw std::logic_error(
        "Unknown connection state: " + std::to_string(int(connectionState)));
}

cJSON* Connection::toJSON() const {
    cJSON* obj = cJSON_CreateObject();
    json_add_uintptr_to_object(obj, "connection", (uintptr_t)this);
    if (socketDescriptor == INVALID_SOCKET) {
        cJSON_AddStringToObject(obj, "socket", "disconnected");
    } else {
        cJSON_AddNumberToObject(obj, "socket", (double)socketDescriptor);
        cJSON_AddStringToObject(obj, "protocol", to_string(getProtocol()));
        cJSON_AddStringToObject(obj, "peername", getPeername().c_str());
        cJSON_AddStringToObject(obj, "sockname", getSockname().c_str());
        cJSON_AddNumberToObject(obj, "parent_port", parent_port);
        cJSON_AddNumberToObject(obj, "bucket_index", bucketIndex);
        json_add_bool_to_object(obj, "admin", isAdmin());
        if (authenticated) {
            cJSON_AddStringToObject(obj, "username", username.c_str());
        }
        if (sasl_conn != NULL) {
            json_add_uintptr_to_object(obj, "sasl_conn",
                                       (uintptr_t)sasl_conn.get());
        }
        json_add_bool_to_object(obj, "nodelay", nodelay);
        cJSON_AddNumberToObject(obj, "refcount", refcount);

        cJSON* features = cJSON_CreateObject();
        json_add_bool_to_object(features, "datatype",
                                isSupportsDatatype());
        json_add_bool_to_object(features, "mutation_extras",
                                isSupportsMutationExtras());

        cJSON_AddItemToObject(obj, "features", features);

        json_add_uintptr_to_object(obj, "engine_storage",
                                   (uintptr_t)engine_storage);
        json_add_uintptr_to_object(obj, "next", (uintptr_t)next);
        json_add_uintptr_to_object(obj, "thread", (uintptr_t)thread.load(
            std::memory_order::memory_order_relaxed));
        cJSON_AddStringToObject(obj, "priority", to_string(priority));
    }
    return obj;
}

std::string Connection::getDescription() const {
    std::string descr("[ " + getPeername() + " - " + getSockname());
    if (isAdmin()) {
        descr += " (Admin)";
    }
    descr += " ]";
    return descr;
}
