/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

/* mcset - test program to perform setting of a KV pair on the server
 *         by using the binary protocol or the ASCII protocol.
 *         It starts by setting the requested size, then loops and
 *         increase the size by one byte until it fails to store the
 *         object. This program was developed to track down MB-14288
 *         in moxi, and may be useful at a later time.
 */
#include "config.h"

#include <memcached/protocol_binary.h>
#include <memcached/openssl.h>
#include <memcached/util.h>
#include <platform/platform.h>
#include <memcached/util.h>

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>
#include <ctype.h>
#include <utilities/protocol2text.h>

#include "programs/utilities.h"

static int set_ascii(BIO *bio, const char *key, size_t size) {
    char line[1024];
    int len = snprintf(line, sizeof(line), "set %s 0 0 %lu\r\n",
                       key, (unsigned long)size);
    ensure_send(bio, &line, len);
    char *value = malloc(size);
    if (value) {
        ensure_send(bio, value, (int)size);
        free(value);
    } else {
        for (size_t ii = 0; ii < size; ++ii) {
            ensure_send(bio, key, 1);
        }
    }
    ensure_send(bio, "\r\n", 2);
    memset(line, 0, sizeof(line));
    int ii = -1;
    do {
        ++ii;
        ensure_recv(bio, line + ii, 1);
    } while (line[ii] != '\n');

    while (ii >= 0 && isspace(line[ii])) {
        line[ii] = '\0';
        --ii;
    }

    if (strcasecmp(line, "stored") == 0) {
        fprintf(stdout, "Stored %s with %lu bytes\n", key, (unsigned long)size);
        return EXIT_SUCCESS;
    }

    fprintf(stderr, "FAILURE: Received %s", line);

    return EXIT_FAILURE;
}

static int set_binary(BIO *bio, const char *key, size_t size) {
    protocol_binary_request_set request = {
        .message.header.request.magic = PROTOCOL_BINARY_REQ,
        .message.header.request.opcode = PROTOCOL_BINARY_CMD_SET,
        .message.header.request.extlen = 8,
        .message.header.request.keylen = htons((uint16_t)strlen(key)),
        .message.header.request.bodylen = htonl(8 + strlen(key) + size),
        .message.body.flags = 0,
        .message.body.expiration = 0
    };
    ensure_send(bio, &request, (int)sizeof(request.bytes));
    ensure_send(bio, key, strlen(key));
    char *value = malloc(size);
    if (value) {
        ensure_send(bio, value, (int)size);
        free(value);
    } else {
        for (size_t ii = 0; ii < size; ++ii) {
            ensure_send(bio, key, 1);
        }
    }

    protocol_binary_response_no_extras response;
    ensure_recv(bio, &response, sizeof(response.bytes));
    char *buffer = NULL;
    const uint32_t bodylen = ntohl(response.message.header.response.bodylen);
    if (bodylen > 0) {
        if ((buffer = malloc(bodylen + 1)) == NULL) {
            fprintf(stderr, "Failed to allocate memory\n");
            exit(EXIT_FAILURE);
        }
        buffer[bodylen] = '\0';
    }
    ensure_recv(bio, buffer, bodylen);

    if (response.message.header.response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        fprintf(stdout, "Stored %s with %lu bytes\n", key, (unsigned long)size);
    } else {
        uint16_t err = ntohs(response.message.header.response.status);
        fprintf(stderr, "Error from server: ");
        fprintf(stderr, "%s\n",  memcached_status_2_text(err));
        if (buffer != NULL) {
            fprintf(stderr, " (response payload: %s)\n", buffer);
        }
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

int main(int argc, char** argv) {
    int cmd;
    const char *port = "11210";
    const char *host = "localhost";
    const char *user = NULL;
    const char *pass = NULL;
    int secure = 0;
    char *ptr;
    SSL_CTX* ctx;
    BIO* bio;
    bool tcp_nodelay = false;
    bool ascii = false;
    size_t size = 0;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "Th:p:u:b:P:sAS:")) != EOF) {
        switch (cmd) {
        case 'A':
            ascii = true;
            break;
        case 'S':
            /* getopt() guarantees optarg is non-null on a successful call, however
               Clang Static Analyzer is not aware of this. */
            cb_assert(optarg != NULL);
            size = atoi(optarg);
            break;
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
        case 'b' :
        case 'u' :
            /* Currently -u and -b are synonymous - only allow the user to
             * specify one. */
            if (user == NULL) {
                user = optarg;
            } else {
                fprintf(stderr, "Error: cannot specify both -u (user) and -b (bucket).\n");
                return 1;
            }
            break;
        case 'P':
            pass = optarg;
            break;
        case 's':
            secure = 1;
            break;
        default:
            fprintf(stderr,
                    "Usage: mcset [-h host[:port]] [-p port] [-b bucket] [-u user] [-P pass] [-s] [-T] [-A] [-S size] key\n"
                    "\n"
                    "  -h hostname[:port]  Host (and optional port number) to store key to\n"
                    "  -p port             Port number\n"
                    "  -u username         Username (currently synonymous with -b)\n"
                    "  -b bucket           Bucket name\n"
                    "  -P password         Password (if bucket is password-protected)\n"
                    "  -s                  Connect to node securely (using SSL)\n"
                    "  -T                  Request TCP_NODELAY from the server\n"
                    "  -A                  Use ASCII protocol (will not work if -u/-b/-P is used)\n"
                    "  -S size             The size to start setting (and then grow with 1b until it fails\n"
                    "  key                 The name of the key to set\n");
            return 1;
        }
    }

    if (optind == argc) {
        fprintf(stderr, "You need to specify a key\n");
        return EXIT_FAILURE;
    }

    if (create_ssl_connection(&ctx, &bio, host, port, user, pass, secure) != 0) {
        return EXIT_FAILURE;
    }

    if (tcp_nodelay && !enable_tcp_nodelay(bio)) {
        return EXIT_FAILURE;
    }

    int ret = EXIT_FAILURE;
    do {
        if (ascii) {
            ret = set_ascii(bio, argv[optind], size);
        } else {
            ret = set_binary(bio, argv[optind], size);
        }
        ++size;
    } while (ret == EXIT_SUCCESS);

    BIO_free_all(bio);
    if (secure) {
        SSL_CTX_free(ctx);
    }

    return ret;
}
