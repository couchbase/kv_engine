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

Connection::Connection()
    : all_next(nullptr),
      all_prev(nullptr),
      sfd(INVALID_SOCKET),
      max_reqs_per_event(settings.default_reqs_per_event),
      nevents(0),
      sasl_conn(nullptr),
      state(conn_immediate_close),
      substate(bin_no_state),
      protocol(Protocol::Memcached),
      admin(false),
      registered_in_libevent(false),
      ev_flags(0),
      currentEvent(0),
      write_and_go(nullptr),
      write_and_free(nullptr),
      ritem(nullptr),
      rlbytes(0),
      item(nullptr),
      iov(nullptr),
      iovsize(0),
      iovused(0),
      msglist(nullptr),
      msgsize(0),
      msgused(0),
      msgcurr(0),
      msgbytes(0),
      request_addr_size(0),
      hdrsize(0),
      noreply(false),
      nodelay(false),
      refcount(0),
      supports_datatype(false),
      supports_mutation_extras(false),
      engine_storage(nullptr),
      start(0),
      cas(0),
      cmd(PROTOCOL_BINARY_CMD_INVALID),
      opaque(0),
      keylen(0),
      list_state(0),
      next(nullptr),
      thread(nullptr),
      aiostat(ENGINE_SUCCESS),
      ewouldblock(false),
      tap_iterator(nullptr),
      parent_port(0),
      dcp(0),
      cmd_context(nullptr),
      auth_context(nullptr),
      peername("unknown"),
      sockname("unknown") {
    MEMCACHED_CONN_CREATE(this);

    memset(&event, 0, sizeof(event));
    memset(&request_addr, 0, sizeof(request_addr));
    memset(&read, 0, sizeof(read));
    memset(&write, 0, sizeof(write));
    memset(&dynamic_buffer, 0, sizeof(dynamic_buffer));
    memset(&ssl, 0, sizeof(ssl));
    memset(&bucket, 0, sizeof(bucket));
    memset(&binary_header, 0, sizeof(binary_header));

    state = conn_immediate_close;
    sfd = INVALID_SOCKET;
    resetBufferSize();
}

Connection::~Connection() {
    MEMCACHED_CONN_DESTROY(this);
    auth_destroy(auth_context);
    cbsasl_dispose(&sasl_conn);
    free(read.buf);
    free(write.buf);
    free(iov);
    free(msglist);

    cb_assert(reservedItems.empty());
    for (auto* ptr : temp_alloc) {
        free(ptr);
    }
}

void Connection::resetBufferSize() {
    bool ret = true;

    if (iovsize != IOV_LIST_INITIAL) {
        void* mem = malloc(sizeof(struct iovec) * IOV_LIST_INITIAL);
        auto* ptr = reinterpret_cast<struct iovec*>(mem);
        if (ptr != NULL) {
            free(iov);
            iov = ptr;
            iovsize = IOV_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    if (msgsize != MSG_LIST_INITIAL) {
        void* mem = malloc(sizeof(struct msghdr) * MSG_LIST_INITIAL);
        auto* ptr = reinterpret_cast<struct msghdr*>(mem);
        if (ptr != NULL) {
            free(msglist);
            msglist = ptr;
            msgsize = MSG_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    if (!ret) {
        free(msglist);
        free(iov);
        std::bad_alloc ex;
        throw ex;
    }
}

void Connection::setState(STATE_FUNC next_state) {
    if (next_state != state) {
        /*
         * The connections in the "tap thread" behaves differently than
         * normal connections because they operate in a full duplex mode.
         * New messages may appear from both sides, so we can't block on
         * read from the nework / engine
         */
        if (tap_iterator != NULL || dcp) {
            if (state == conn_waiting) {
                setCurrentEvent(EV_WRITE);
                state = conn_ship_log;
            }
        }

        if (settings.verbose > 2 || state == conn_closing
            || state == conn_setup_tap_stream) {
            settings.extensions.logger->log(EXTENSION_LOG_DETAIL, this,
                                            "%d: going from %s to %s\n",
                                            sfd, state_text(state),
                                            state_text(next_state));
        }

        if (next_state == conn_write || next_state == conn_mwrite) {
            if (start != 0) {
                collect_timings(this);
                start = 0;
            }
            MEMCACHED_PROCESS_COMMAND_END(sfd, write.buf, write.bytes);
        }

        state = next_state;
    }
}

void Connection::runStateMachinery() {
    do {
        if (settings.verbose) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, this,
                                            "%d - Running task: (%s)", sfd,
                                            state_text(state));
        }
    } while (state(this));
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

    int err = getnameinfo(reinterpret_cast<const struct sockaddr*>(addr), addr_len,
                          host, sizeof(host),
                          port, sizeof(port),
                          NI_NUMERICHOST | NI_NUMERICSERV);
    if (err != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "getnameinfo failed with error %d",
                                        err);
        return NULL;
    }

    return std::string(host) + ":" + std::string(port);
}

void Connection::resolveConnectionName() {
    int err;
    struct sockaddr_storage peer;
    socklen_t peer_len = sizeof(peer);

    if ((err = getpeername(sfd, reinterpret_cast<struct sockaddr*>(&peer),
                           &peer_len)) != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "getpeername for socket %d with error %d",
                                        sfd, err);
        return;
    }

    struct sockaddr_storage sock;
    socklen_t sock_len = sizeof(sock);
    if ((err = getsockname(sfd, reinterpret_cast<struct sockaddr*>(&sock),
                           &sock_len)) != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "getsock for socket %d with error %d",
                                        sfd, err);
        return;
    }

    peername = sockaddr_to_string(&peer, peer_len);
    sockname = sockaddr_to_string(&sock, sock_len);
}

bool Connection::unregisterEvent() {
    cb_assert(registered_in_libevent);
    cb_assert(sfd != INVALID_SOCKET);

    if (event_del(&event) == -1) {
        log_system_error(EXTENSION_LOG_WARNING,
                         NULL,
                         "Failed to remove connection to libevent: %s");
        return false;
    }

    registered_in_libevent = false;
    return true;
}

bool Connection::registerEvent() {
    cb_assert(!registered_in_libevent);
    cb_assert(sfd != INVALID_SOCKET);

    if (event_add(&event, NULL) == -1) {
        log_system_error(EXTENSION_LOG_WARNING,
                         NULL,
                         "Failed to add connection to libevent: %s");
        return false;
    }

    registered_in_libevent = true;

    return true;
}

bool Connection::updateEvent(const short new_flags) {
    struct event_base* base = event.ev_base;

    if (ssl.isEnabled() && ssl.isConnected() && (new_flags & EV_READ)) {
        /*
         * If we want more data and we have SSL, that data might be inside
         * SSL's internal buffers rather than inside the socket buffer. In
         * that case signal an EV_READ event without actually polling the
         * socket.
         */
        char dummy;
        /* SSL_pending() will not work here despite the name */
        int rv = ssl.peek(&dummy, 1);
        if (rv > 0) {
            /* signal a call to the handler */
            event_active(&event, EV_READ, 0);
            return true;
        }
    }

    if (ev_flags == new_flags) {
        // There is no change in the event flags!
        return true;
    }

    if (settings.verbose > 1) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, NULL,
                                        "Updated event for %d to read=%s, write=%s\n",
                                        sfd,
                                        (new_flags & EV_READ ? "yes" : "no"),
                                        (new_flags & EV_WRITE ? "yes" : "no"));
    }

    if (!unregisterEvent()) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, this,
                                        "Failed to remove connection from "
                                            "event notification library. Shutting "
                                            "down connection [%s - %s]",
                                        getPeername().c_str(),
                                        getSockname().c_str());
        return false;
    }

    event_set(&event, sfd, new_flags, event_handler,
              reinterpret_cast<void*>(this));
    event_base_set(base, &event);
    ev_flags = new_flags;

    if (!registerEvent()) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, this,
                                        "Failed to add connection to the "
                                            "event notification library. Shutting "
                                            "down connection [%s - %s]",
                                        getPeername().c_str(),
                                        getSockname().c_str());
        return false;
    }

    return true;
}

bool Connection::initializeEvent(struct event_base* base) {
    short event_flags = (EV_READ | EV_PERSIST);
    event_set(&event, sfd, event_flags, event_handler,
              reinterpret_cast<void*>(this));
    event_base_set(base, &event);
    ev_flags = event_flags;

    return registerEvent();
}

bool Connection::setTcpNoDelay(bool enable) {
    int flags = enable ? 1 : 0;

#if defined(WIN32)
    char* flags_ptr = reinterpret_cast<char*>(&flags);
#else
    void* flags_ptr = reinterpret_cast<void*>(&flags);
#endif
    int error = setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, flags_ptr,
                           sizeof(flags));

    if (error != 0) {
        std::string errmsg = cb_strerror(GetLastNetworkError());
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, this,
                                        "setsockopt(TCP_NODELAY): %s",
                                        errmsg.c_str());
        nodelay = false;
        return false;
    } else {
        nodelay = enable;
    }

    return true;
}


static const char* substate_text(enum bin_substates state) {
    switch (state) {
    case bin_no_state: return "bin_no_state";
    case bin_reading_packet: return "bin_reading_packet";
    default:
        return "illegal";
    }
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

cJSON* Connection::toJSON() const {
    cJSON* obj = cJSON_CreateObject();
    json_add_uintptr_to_object(obj, "connection", (uintptr_t) this);
    if (sfd == INVALID_SOCKET) {
        cJSON_AddStringToObject(obj, "socket", "disconnected");
    } else {
        cJSON_AddNumberToObject(obj, "socket", (double) sfd);
        cJSON_AddStringToObject(obj, "protocol", to_string(getProtocol()));
        cJSON_AddStringToObject(obj, "peername", getPeername().c_str());
        cJSON_AddStringToObject(obj, "sockname", getSockname().c_str());
        cJSON_AddNumberToObject(obj, "max_reqs_per_event",
                                max_reqs_per_event);
        cJSON_AddNumberToObject(obj, "nevents", nevents);
        json_add_bool_to_object(obj, "admin", isAdmin());
        if (sasl_conn != NULL) {
            json_add_uintptr_to_object(obj, "sasl_conn",
                                       (uintptr_t) sasl_conn);
        }
        {
            cJSON* state = cJSON_CreateArray();
            cJSON_AddItemToArray(state,
                                 cJSON_CreateString(state_text(getState())));
            cJSON_AddItemToArray(state,
                                 cJSON_CreateString(substate_text(substate)));
            cJSON_AddItemToObject(obj, "state", state);
        }
        json_add_bool_to_object(obj, "registered_in_libevent",
                                isRegisteredInLibevent());
        json_add_uintptr_to_object(obj, "ev_flags",
                                   (uintptr_t) getEventFlags());
        json_add_uintptr_to_object(obj, "which", (uintptr_t) getCurrentEvent());
        {
            cJSON* readobj = cJSON_CreateObject();
            json_add_uintptr_to_object(readobj, "buf", (uintptr_t) read.buf);
            json_add_uintptr_to_object(readobj, "curr", (uintptr_t) read.curr);
            cJSON_AddNumberToObject(readobj, "size", read.size);
            cJSON_AddNumberToObject(readobj, "bytes", read.bytes);

            cJSON_AddItemToObject(obj, "read", readobj);
        }
        {
            cJSON* writeobj = cJSON_CreateObject();
            json_add_uintptr_to_object(writeobj, "buf", (uintptr_t) write.buf);
            json_add_uintptr_to_object(writeobj, "curr",
                                       (uintptr_t) write.curr);
            cJSON_AddNumberToObject(writeobj, "size", write.size);
            cJSON_AddNumberToObject(writeobj, "bytes", write.bytes);

            cJSON_AddItemToObject(obj, "write", writeobj);
        }
        json_add_uintptr_to_object(obj, "write_and_go",
                                   (uintptr_t) write_and_go);
        json_add_uintptr_to_object(obj, "write_and_free",
                                   (uintptr_t) write_and_free);
        json_add_uintptr_to_object(obj, "ritem", (uintptr_t) ritem);
        cJSON_AddNumberToObject(obj, "rlbytes", rlbytes);
        json_add_uintptr_to_object(obj, "item", (uintptr_t) item);
        {
            cJSON* iov = cJSON_CreateObject();
            json_add_uintptr_to_object(iov, "ptr", (uintptr_t) iov);
            cJSON_AddNumberToObject(iov, "size", iovsize);
            cJSON_AddNumberToObject(iov, "used", iovused);

            cJSON_AddItemToObject(obj, "iov", iov);
        }
        {
            cJSON* msg = cJSON_CreateObject();
            json_add_uintptr_to_object(msg, "list", (uintptr_t) msglist);
            cJSON_AddNumberToObject(msg, "size", msgsize);
            cJSON_AddNumberToObject(msg, "used", msgused);
            cJSON_AddNumberToObject(msg, "curr", msgcurr);
            cJSON_AddNumberToObject(msg, "bytes", msgbytes);

            cJSON_AddItemToObject(obj, "msglist", msg);
        }
        {
            cJSON* ilist = cJSON_CreateObject();
            cJSON_AddNumberToObject(ilist, "size", reservedItems.size());
            cJSON_AddItemToObject(obj, "itemlist", ilist);
        }
        {
            cJSON* talloc = cJSON_CreateObject();
            cJSON_AddNumberToObject(talloc, "size", temp_alloc.size());
            cJSON_AddItemToObject(obj, "temp_alloc_list", talloc);
        }
        json_add_bool_to_object(obj, "noreply", noreply);
        json_add_bool_to_object(obj, "nodelay", nodelay);
        cJSON_AddNumberToObject(obj, "refcount", refcount);
        {
            cJSON* features = cJSON_CreateObject();
            json_add_bool_to_object(features, "datatype",
                                    supports_datatype);
            json_add_bool_to_object(features, "mutation_extras",
                                    supports_mutation_extras);

            cJSON_AddItemToObject(obj, "features", features);
        }
        {
            cJSON* dy_buf = cJSON_CreateObject();
            json_add_uintptr_to_object(dy_buf, "buffer",
                                       (uintptr_t) dynamic_buffer.buffer);
            cJSON_AddNumberToObject(dy_buf, "size",
                                    (double) dynamic_buffer.size);
            cJSON_AddNumberToObject(dy_buf, "offset",
                                    (double) dynamic_buffer.offset);

            cJSON_AddItemToObject(obj, "dynamic_buffer", dy_buf);
        }
        json_add_uintptr_to_object(obj, "engine_storage",
                                   (uintptr_t) engine_storage);
        /* @todo we should decode the binary header */
        json_add_uintptr_to_object(obj, "cas", cas);
        {
            cJSON* cmdarr = cJSON_CreateArray();
            cJSON_AddItemToArray(cmdarr, json_create_uintptr(cmd));
            const char* cmd_name = memcached_opcode_2_text(cmd);
            if (cmd_name == NULL) {
                cmd_name = "";
            }
            cJSON_AddItemToArray(cmdarr, cJSON_CreateString(cmd_name));

            cJSON_AddItemToObject(obj, "cmd", cmdarr);
        }
        json_add_uintptr_to_object(obj, "opaque", opaque);
        cJSON_AddNumberToObject(obj, "keylen", keylen);
        cJSON_AddNumberToObject(obj, "list_state", list_state);
        json_add_uintptr_to_object(obj, "next", (uintptr_t) next);
        json_add_uintptr_to_object(obj, "thread", (uintptr_t) thread);
        cJSON_AddNumberToObject(obj, "aiostat", aiostat);
        json_add_bool_to_object(obj, "ewouldblock", ewouldblock);
        json_add_uintptr_to_object(obj, "tap_iterator",
                                   (uintptr_t) tap_iterator);
        cJSON_AddNumberToObject(obj, "parent_port", parent_port);
        json_add_bool_to_object(obj, "dcp", dcp);

        {
            cJSON* sslobj = cJSON_CreateObject();
            json_add_bool_to_object(sslobj, "enabled", ssl.isEnabled());
            if (ssl.isEnabled()) {
                json_add_bool_to_object(sslobj, "connected", ssl.isConnected());
            }

            cJSON_AddItemToObject(obj, "ssl", sslobj);
        }
    }
    return obj;
}

SslContext::~SslContext() {
    if (enabled) {
        disable();
    }
}

bool SslContext::enable(const std::string& cert, const std::string& pkey) {
    ctx = SSL_CTX_new(SSLv23_server_method());

    /* MB-12359 - Disable SSLv2 & SSLv3 due to POODLE */
    SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);

    /* @todo don't read files, but use in-memory-copies */
    if (!SSL_CTX_use_certificate_chain_file(ctx, cert.c_str()) ||
        !SSL_CTX_use_PrivateKey_file(ctx, pkey.c_str(), SSL_FILETYPE_PEM)) {
        return false;
    }

    set_ssl_ctx_cipher_list(ctx);

    enabled = true;
    error = false;
    client = NULL;

    try {
        in.buffer.resize(settings.bio_drain_buffer_sz);
        out.buffer.resize(settings.bio_drain_buffer_sz);
    } catch (std::bad_alloc) {
        return false;
    }

    BIO_new_bio_pair(&application, in.buffer.size(),
                     &network, out.buffer.size());

    client = SSL_new(ctx);
    SSL_set_bio(client, application, application);

    return true;
}

void SslContext::disable() {
    if (network != nullptr) {
        BIO_free_all(network);
    }
    if (client != nullptr) {
        SSL_free(client);
    }
    error = false;
    if (ctx != nullptr) {
        SSL_CTX_free(ctx);
    }
    enabled = false;
}

void SslContext::drainBioRecvPipe(SOCKET sfd) {
    int n;
    bool stop = false;

    do {
        if (in.current < in.total) {
            n = BIO_write(network, in.buffer.data() + in.current,
                          int(in.total - in.current));
            if (n > 0) {
                in.current += n;
                if (in.current == in.total) {
                    in.current = in.total = 0;
                }
            } else {
                /* Our input BIO is full, no need to grab more data from
                 * the network at this time..
                 */
                return;
            }
        }

        if (in.total < in.buffer.size()) {
            n = recv(sfd, in.buffer.data() + in.total,
                     in.buffer.size() - in.total, 0);
            if (n > 0) {
                in.total += n;
            } else {
                stop = true;
                if (n == 0) {
                    error = true; /* read end shutdown */
                } else {
                    if (!is_blocking(GetLastNetworkError())) {
                        error = true;
                    }
                }
            }
        }
    } while (!stop);
}

void SslContext::drainBioSendPipe(SOCKET sfd) {
    int n;
    bool stop = false;

    do {
        if (out.current < out.total) {
            n = send(sfd, out.buffer.data() + out.current,
                     out.total - out.current, 0);
            if (n > 0) {
                out.current += n;
                if (out.current == out.total) {
                    out.current = out.total = 0;
                }
            } else {
                if (n == -1) {
                    if (!is_blocking(GetLastNetworkError())) {
                        error = true;
                    }
                }
                return;
            }
        }

        if (out.total == 0) {
            n = BIO_read(network, out.buffer.data(), int(out.buffer.size()));
            if (n > 0) {
                out.total = n;
            } else {
                stop = true;
            }
        }
    } while (!stop);
}

void SslContext::dumpCipherList(SOCKET sfd) {
    settings.extensions.logger->log(EXTENSION_LOG_DEBUG, NULL,
                                    "%d: Using SSL ciphers:", sfd);
    int ii = 0;
    const char* cipher;
    while ((cipher = SSL_get_cipher_list(client, ii++)) != NULL) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, NULL,
                                        "%d    %s", sfd, cipher);
    }
}