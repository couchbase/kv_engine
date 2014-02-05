/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <memcached/protocol_binary.h>
#include <memcached/openssl.h>
#include <platform/platform.h>

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>

#include "utilities.h"

/**
 * Request all features from the server and dump the available ones
 * @param sock socket connected to the server
 * @param key the name of the stat to receive (NULL == ALL)
 */
static void request_hello(BIO *bio, const char *key)
{
    protocol_binary_request_hello req;
    protocol_binary_response_hello res;
    char buffer[1024];
    const char useragent[] = "mchello v1.0";
    uint16_t nkey = (uint16_t)strlen(useragent);
    uint16_t features[MEMCACHED_TOTAL_HELLO_FEATURES];
    uint32_t total = nkey + MEMCACHED_TOTAL_HELLO_FEATURES * 2;
    int ii;

    memset(&req, 0, sizeof(req));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_HELLO;
    req.message.header.request.bodylen = htonl(total);
    req.message.header.request.keylen = htons(nkey);
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;

    ensure_send(bio, &req, sizeof(req));
    ensure_send(bio, useragent, nkey);

    for (ii = 0; ii < MEMCACHED_TOTAL_HELLO_FEATURES; ++ii) {
        features[ii] = htons(MEMCACHED_FIRST_HELLO_FEATURE + ii);
    }
    ensure_send(bio, &features, MEMCACHED_TOTAL_HELLO_FEATURES * 2);
    ensure_recv(bio, &res, sizeof(res.bytes));
    total = ntohl(res.message.header.response.bodylen);
    ensure_recv(bio, buffer, total);

    if (res.message.header.response.status != 0) {
        fprintf(stderr, "Got return value: %d\n",
                ntohs(res.message.header.response.status));
        return;
    }

    memcpy(features, buffer, total);
    total /= 2;

    fprintf(stdout, "Features:\n");
    for (ii = 0; ii < total; ++ii) {
        fprintf(stderr, "\t- %s\n",
                protocol_feature_2_text(ntohs(features[ii])));
    }
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
            fprintf(stderr,
                    "Usage mchello [-h host[:port]] [-p port] [-u user] [-p pass] [-s]\n");
            return 1;
        }
    }

    if (create_ssl_connection(&ctx, &bio, host, port, user, pass, secure) != 0) {
        return 1;
    }

    request_hello(bio, NULL);

    BIO_free_all(bio);
    if (secure) {
        SSL_CTX_free(ctx);
    }

    return EXIT_SUCCESS;
}
