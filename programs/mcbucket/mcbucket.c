/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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
 * mcbucket is a small standalone program you may use to:
 *     * Create new buckets
 *     * Delete buckets
 *     * list buckets
 *
 * Note that you cannot use this program to create/delete buckets on
 * the individual nodes on a running Couchbase cluster, because it
 * talks directly to the memcached nodes and bypasses ns_server which
 * is responsible for defining the buckets in the cluster. You may
 * however use this program if you want to test out resource usage,
 * how many buckets you may create etc.
 */
#include "config.h"

#include <memcached/protocol_binary.h>
#include <memcached/openssl.h>
#include <platform/cb_malloc.h>
#include <platform/platform.h>

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>

#include <memcached/util.h>
#include <utilities/protocol2text.h>
#include "programs/utilities.h"

static int usage() {
    fprintf(stderr,
            "Usage: \n"
            "\tmcbucket [-h host[:port]] [-p port] [-u user] [-P pass] [-s] create name type config='config'\n"
            "\tmcbucket [-h host[:port]] [-p port] [-u user] [-P pass] [-s] delete name\n"
            "\tmcbucket [-h host[:port]] [-p port] [-u user] [-P pass] [-s] list'\n");
    return EXIT_FAILURE;
}

typedef int (*handler_t)(BIO *bio, int argc, char **argv);

/**
 * Read the response packet from a create/delete/list bucket
 * request and print an error message and the payload of the packet
 * (in case they differ from the status code)
 *
 * @param bio the connection to the server
 * @return 0 on success, 1 on failure
 */
static int read_response(BIO *bio) {
    protocol_binary_response_no_extras response;

    ensure_recv(bio, &response, sizeof(response.bytes));

    uint16_t status = htons(response.message.header.response.status);
    uint32_t valuelen = ntohl(response.message.header.response.bodylen);

    char *payload;
    if (valuelen == 0) {
        payload = 0;
    } else {
        payload = cb_malloc(valuelen + 1);
        if (payload == NULL) {
            fprintf(stderr, "Failed to allocate memory for response\n");
            exit(EXIT_FAILURE);
        }
        payload[valuelen] = '\0';
        ensure_recv(bio, payload, valuelen);
    }

    if (status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        fprintf(stderr, "Error from server: %s\n",
                memcached_status_2_text(status));
    }

    if (payload != NULL) {
        if (strcmp(payload, memcached_status_2_text(status)) != 0) {
            fprintf(stdout, "%s\n", payload);
        }
        cb_free(payload);
    }

    return (status == PROTOCOL_BINARY_RESPONSE_SUCCESS) ? 0 : 1;
}

/**
 * Create a bucket
 *
 * @param argc argument count
 * @param argv argument vector
 * @return 0 on success, errorcode otherwise
 */
static int create_bucket(BIO *bio, int argc, char **argv) {
    if (argc - optind < 3) {
        return usage();
    }

    union {
        char buffer[1024];
        protocol_binary_request_create_bucket req;
    } request;

    memset(&request.buffer, 0, sizeof(request.buffer));

    request.req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.req.message.header.request.opcode = PROTOCOL_BINARY_CMD_CREATE_BUCKET;

    size_t offset = sizeof(request.req.bytes);
    size_t len = strlen(argv[optind]);
    memcpy(request.buffer + offset, argv[optind++], len);
    offset += len;

    request.req.message.header.request.keylen = htons((uint16_t)(offset -
                                                                 sizeof(request.req.bytes)));

    /* Add the type */
    len = strlen(argv[optind]);
    memcpy(request.buffer + offset, argv[optind++], len);
    offset += len + 1;

    /* Add the config */
    len = strlen(argv[optind]);
    memcpy(request.buffer + offset, argv[optind], len);
    offset += len;
    request.req.message.header.request.bodylen =
        htonl((uint32_t)(offset - sizeof(request.req.bytes)));

    ensure_send(bio, &request.buffer, (int)offset);
    return read_response(bio);
}

static int delete_bucket(BIO *bio, int argc, char **argv) {
    if (argc - optind < 1) {
        return usage();
    }

    union {
        char buffer[1024];
        protocol_binary_request_create_bucket req;
    } request;

    memset(&request.buffer, 0, sizeof(request.buffer));

    request.req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.req.message.header.request.opcode = PROTOCOL_BINARY_CMD_DELETE_BUCKET;

    size_t offset = sizeof(request.req.bytes);
    size_t len = strlen(argv[optind]);
    memcpy(request.buffer + offset, argv[optind++], len);
    offset += len;

    request.req.message.header.request.keylen = htons((uint16_t)(offset -
                                                                 sizeof(request.req.bytes)));
    request.req.message.header.request.bodylen =
        htonl((uint32_t)(offset -sizeof(request.req.bytes)));

    ensure_send(bio, &request.buffer, (int)offset);
    return read_response(bio);
}

static int list_bucket(BIO *bio, int argc, char **argv) {
    if (argc - optind != 0) {
        return usage();
    }

    protocol_binary_request_create_bucket req;

    memset(&req.bytes, 0, sizeof(req.bytes));

    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_LIST_BUCKETS;
    ensure_send(bio, &req.bytes, sizeof(req.bytes));

    return read_response(bio);
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
    int result = EXIT_FAILURE;
    handler_t handler = NULL;

    struct {
        const char *name;
        handler_t handler;
    } handlers[] = {
        { "create", create_bucket },
        { "delete", delete_bucket },
        { "list", list_bucket },
        { NULL, NULL }
    };

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "h:p:u:P:s")) != EOF) {
        switch (cmd) {
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
        default:
            return usage();
        }
    }

    if (argv[optind] == NULL) {
        return usage();
    }

    int ii = 0;
    while (handlers[ii].name != NULL) {
        if (strcasecmp(handlers[ii].name, argv[optind]) == 0) {
            handler = handlers[ii].handler;
            break;
        }
        ++ii;
    }

    if (handler == NULL) {
        return usage();
    }

    if (create_ssl_connection(&ctx, &bio, host, port, user, pass, secure) != 0) {
        return 1;
    }

    ++optind;
    result = handler(bio, argc, argv);

    BIO_free_all(bio);
    if (secure) {
        SSL_CTX_free(ctx);
    }

    return result;
}
