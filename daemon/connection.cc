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
#include <exception>

Connection::Connection()
        : all_next(nullptr),
          all_prev(nullptr),
          sfd(INVALID_SOCKET),
          protocol(Protocol::Memcached),
          peername(nullptr), // refactor to string
          sockname(nullptr), // refactor to string
          max_reqs_per_event(settings.default_reqs_per_event),
          nevents(0),
          admin(false),
          sasl_conn(nullptr),
          state(conn_immediate_close),
          substate(bin_no_state),
          registered_in_libevent(false),
          ev_flags(0),
          which(0),
          write_and_go(nullptr),
          write_and_free(nullptr),
          ritem(nullptr),
          rlbytes(0),
          item(nullptr),
          sbytes(0),
          iov(nullptr), iovsize(0), iovused(0),
          msglist(nullptr), msgsize(0), msgused(0), msgcurr(0), msgbytes(0),
          ilist(nullptr), isize(0), icurr(0),
          ileft(0), // refactor to std::vector
          temp_alloc_list(nullptr), temp_alloc_size(0), temp_alloc_curr(0),
          temp_alloc_left(0), // refactor to std::vector
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
          cmd_context(nullptr), cmd_context_dtor(nullptr),
          auth_context(nullptr)
{
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
    auth_destroy(auth_context);
    free(peername);
    free(sockname);
    cbsasl_dispose(&sasl_conn);
    free(read.buf);
    free(write.buf);
    free(ilist);
    free(temp_alloc_list);
    free(iov);
    free(msglist);
}

void Connection::resetBufferSize() {
    bool ret = true;

    /* itemlist only needed for TAP / DCP connections, so we just free when the
     * connection is reset.
     */
    free(ilist);
    ilist = NULL;
    isize = 0;

    if (temp_alloc_size != TEMP_ALLOC_LIST_INITIAL) {
        char **ptr = reinterpret_cast<char**>
        (malloc(sizeof(char *) * TEMP_ALLOC_LIST_INITIAL));
        if (ptr != NULL) {
            free(temp_alloc_list);
            temp_alloc_list = ptr;
            temp_alloc_size = TEMP_ALLOC_LIST_INITIAL;
            temp_alloc_curr = temp_alloc_list;
        } else {
            ret = false;
        }
    }

    if (iovsize != IOV_LIST_INITIAL) {
        auto *ptr = reinterpret_cast<struct iovec*>
        (malloc(sizeof(struct iovec) * IOV_LIST_INITIAL));
        if (ptr != NULL) {
            free(iov);
            iov = ptr;
            iovsize = IOV_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    if (msgsize != MSG_LIST_INITIAL) {
        auto* ptr = reinterpret_cast<struct msghdr*>
        (malloc(sizeof(struct msghdr) * MSG_LIST_INITIAL));
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
        free(temp_alloc_list);
        std::bad_alloc ex;
        throw ex;
    }
}