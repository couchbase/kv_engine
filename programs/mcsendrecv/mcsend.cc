/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

/**
 * mcsend is a program that connects to a upstream server and dump the
 * database and send the output messages to standard out, which means you
 * could easily do:
 *
 * ./mcsend | ssh my-other-server mcrecv
 */

#include "config.h"

#include <memcached/protocol_binary.h>
#include <memcached/openssl.h>

#include <getopt.h>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "programs/utilities.h"
#include "common.h"

static bool tap_dump(BIO* source, BIO* dest, const char* name, bool verbose) {
    uint16_t keylen(strlen(name));
    protocol_binary_request_tap_connect request;
    memset(request.bytes, 0, sizeof(request.bytes));

    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_CONNECT;
    request.message.header.request.extlen = 4;
    request.message.header.request.keylen = htons(keylen);
    request.message.header.request.bodylen = htonl(4 + keylen);

    uint32_t flags = 0;
    flags |= TAP_CONNECT_FLAG_DUMP;
    flags |= TAP_CONNECT_TAP_FIX_FLAG_BYTEORDER;

    request.message.body.flags = htonl(flags);

    ensure_send(source, request.bytes, sizeof(request.bytes));
    ensure_send(source, name, keylen);

    return tap_cp(source, dest, verbose);
}

int main(int argc, char** argv) {
    int cmd;
    const char* port = "11210";
    const char* host = "localhost";
    const char* user = NULL;
    const char* pass = NULL;
    const char* name = "mcsend";
    bool verbose = false;
    int secure = 0;
    char* ptr;
    bool tcp_nodelay = false;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "Th:p:u:P:sn:v?")) != EOF) {
        switch (cmd) {
        case 'T' :
            tcp_nodelay = true;
            break;
        case 'h' :
            host = optarg;
            ptr = strchr(optarg, ':');
            if (ptr != NULL) {
                *ptr = '\0';
                port = ptr + 1;
            }
            break;
        case 'p':
            port = optarg;
            break;
        case 'u' :
            user = optarg;
            break;
        case 'P':
            pass = optarg;
            break;
        case 's':
            secure = 1;
            break;
        case 'n':
            name = optarg;
            break;
        case 'v':
            verbose = true;
            break;
        default:
            fprintf(stderr,
                    "Usage mcsend [options]\n\n"
                        "\t[-h host[:port]] - Connect to the named host\n"
                        "\t[-p port]        - Connect to the specified port\n"
                        "\t[-u user]        - Username for authentication\n"
                        "\t[-P pass]        - Password for authentication\n"
                        "\t[-s]             - Use SSL\n"
                        "\t[-T]             - Use TCP NODELAY\n"
                        "\t[-v]             - Print progress information\n"
                        "\t[-n name]        - Name to use for the TAP stream\n"
                        "\n"
                        "mcsend is a program that may be used to generate a "
                        "stream of TAP messages\nfrom a bucket on a given"
                        " server.\n\n"
                        "You may use mcsend together with mcrecv to move the"
                        " content of a bucket\nto another bucket with:\n"
                        "\n"
                        "   $ mcsend -u bucket1 -P pass | mcrecv -u bucket2"
                        " -P pass\n\n");
            return 1;
        }
    }

    BIO* dest = BIO_new_fp(stdout, 0);
    if (dest == nullptr) {
        std::cerr << "Failed to create output stream" << std::endl;
        return EXIT_FAILURE;
    }

    SSL_CTX* ctx;
    BIO* source;
    if (create_ssl_connection(&ctx, &source, host, port,
                              user, pass, secure) != 0) {
        return 1;
    }

    if (tcp_nodelay && !enable_tcp_nodelay(source)) {
        return 1;
    }

    int exitcode;
    if (tap_dump(source, dest, name, verbose)) {
        exitcode = EXIT_SUCCESS;
    } else {
        exitcode = EXIT_FAILURE;
    }

    BIO_free_all(source);
    BIO_free_all(dest);
    if (secure) {
        SSL_CTX_free(ctx);
    }

    return exitcode;
}
