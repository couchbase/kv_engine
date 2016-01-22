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
 * mcrecv is a program that connects to a Couchbase server and feeds the
 * packets read off stdin into the couchbase server, which means you
 * could easily do:
 *
 *     mcsend | ssh my-other-server mcrecv
 *
 * Or to copy data between two buckets:
 *
 *     mcsend -u source | ./mcrecv -u destination
 *
 */

#include "config.h"

#include <memcached/openssl.h>

#include <getopt.h>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "programs/utilities.h"
#include "common.h"

int main(int argc, char** argv) {
    int cmd;
    const char* port = "11210";
    const char* host = "localhost";
    const char* user = NULL;
    const char* pass = NULL;
    bool verbose = false;
    int secure = 0;
    char* ptr;
    bool tcp_nodelay = false;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "Th:p:u:P:sv?")) != EOF) {
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
        case 'v':
            verbose = true;
            break;
        default:
            fprintf(stderr,
                    "Usage mcrecv [options]\n\n"
                        "\t[-h host[:port]] - Connect to the named host\n"
                        "\t[-p port]        - Connect to the specified port\n"
                        "\t[-u user]        - Username for authentication\n"
                        "\t[-P pass]        - Password for authentication\n"
                        "\t[-s]             - Use SSL\n"
                        "\t[-T]             - Use TCP NODELAY\n"
                        "\t[-v]             - Print progress information\n"
                        "\n"
                        "mcrecv is a program that may be used to apply a "
                        "recorded stream of TAP messages\nto a server.\n\n"
                        "You may use mcrecv together with mcsend to move the"
                        " content of a bucket\nto another bucket with:\n"
                        "\n"
                        "   $ mcsend -u bucket1 -P pass | mcrecv -u bucket2"
                        " -P pass\n\n");
            return EXIT_FAILURE;
        }
    }

    SSL_CTX* ctx;
    BIO* dest;
    if (create_ssl_connection(&ctx, &dest, host, port,
                              user, pass, secure) != 0) {
        return 1;
    }

    if (tcp_nodelay && !enable_tcp_nodelay(dest)) {
        return 1;
    }

    BIO* source = BIO_new_fp(stdin, 0);
    if (source == nullptr) {
        std::cerr << "Failed to create input stream" << std::endl;
        return EXIT_FAILURE;
    }

    int exitcode = tap_cp(source, dest, verbose) ? EXIT_SUCCESS : EXIT_FAILURE;

    BIO_free_all(dest);
    BIO_free_all(source);
    if (secure) {
        SSL_CTX_free(ctx);
    }

    return exitcode;
}
