/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "ssl_impl.h"
#include "testapp.h"

#include <folly/portability/GTest.h>
#include <platform/platform_thread.h>
#include <platform/socket.h>

static SSL_CTX *ssl_ctx = nullptr;
static SSL *ssl = nullptr;
static BIO *bio = nullptr;
static BIO *ssl_bio_r = nullptr;
static BIO *ssl_bio_w = nullptr;

SOCKET create_connect_ssl_socket(in_port_t port) {
    SOCKET sfd;
    if (ssl_port == in_port_t(-1)) {
        throw std::runtime_error(
                "create_connect_ssl_socket: Can't connect to ssl_port == -1");
    }

    std::tie(sfd, ssl_ctx, bio) =
            cb::net::new_ssl_socket("", port, AF_INET, {});

    if (sfd == INVALID_SOCKET) {
        ADD_FAILURE() << "Failed to connect over ssl to 127.0.0.1:" << port;
        return INVALID_SOCKET;
    }

    // SSL "trickery". To ensure we have full control over send/receive of data.
    // Switch out the BIO_ssl_connect BIO for a plain memory BIO
    // Now send/receive is done under our control. byte by byte, large chunks
    // etc...
    BIO_get_ssl(bio, &ssl);
    EXPECT_EQ(nullptr, ssl_bio_r);
    ssl_bio_r = BIO_new(BIO_s_mem());

    EXPECT_EQ(nullptr, ssl_bio_w);
    ssl_bio_w = BIO_new(BIO_s_mem());

    // Note: previous BIOs attached to 'bio' freed as a result of this call.
    SSL_set_bio(ssl, ssl_bio_r, ssl_bio_w);

    return sfd;
}

void destroy_ssl_socket() {
    BIO_free_all(bio);
    bio = nullptr;
    ssl_bio_r = nullptr;
    ssl_bio_w = nullptr;

    SSL_CTX_free(ssl_ctx);
    ssl_ctx = nullptr;
    if (sock_ssl != INVALID_SOCKET) {
        cb::net::closesocket(sock_ssl);
        sock_ssl = INVALID_SOCKET;
    }
}

ssize_t phase_send_ssl(const void *buf, size_t len) {
    ssize_t rv = 0, send_rv = 0;

    long send_len = 0;
    char* send_buf = nullptr;
    /* push the data through SSL into the BIO */
    rv = (ssize_t)SSL_write(ssl, (const char*)buf, (int)len);
    send_len = BIO_get_mem_data(ssl_bio_w, &send_buf);

    send_rv = socket_send(sock_ssl, send_buf, send_len);

    if (send_rv > 0) {
        EXPECT_EQ(send_len, send_rv);
        (void)BIO_reset(ssl_bio_w);
    } else {
        /* flag failure to user */
        rv = send_rv;
    }

    return rv;
}

ssize_t phase_recv_ssl(void* buf, size_t len) {
    ssize_t rv;

    /* can we read some data? */
    while ((rv = SSL_peek(ssl, buf, (int)len)) == -1) {
        /* nope, keep feeding SSL until we can */
        rv = socket_recv(sock_ssl, reinterpret_cast<char*>(buf), len);

        if (rv > 0) {
            /* write into the BIO what came off the network */
            BIO_write(ssl_bio_r, buf, rv);
        } else if (rv == 0) {
            return rv; /* peer closed */
        }
    }

    /* now pull the data out and return */
    return SSL_read(ssl, buf, (int)len);
}
