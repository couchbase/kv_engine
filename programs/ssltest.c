/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <memcached/protocol_binary.h>
#include <memcached/openssl.h>
#include <platform/platform.h>

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "utilities.h"


static void *get_response(BIO *bio, protocol_binary_response_no_extras *res) {
    uint32_t vallen;

    ensure_recv(bio, res, sizeof(*res));
    vallen = ntohl(res->message.header.response.bodylen);

    if (vallen == 0) {
        return NULL;
    } else {
        void *buffer = malloc(vallen);
        assert(buffer != NULL);
        ensure_recv(bio, buffer, vallen);
        return buffer;
    }
}

static void store(BIO *bio,
                  const void *key,
                  uint16_t nkey,
                  const void *bytes,
                  uint32_t nbytes)
{
    protocol_binary_request_set request;
    protocol_binary_response_no_extras response;
    void *payload;

    memset(&request, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_SET;
    request.message.header.request.keylen = htons((uint16_t)nkey);
    request.message.header.request.extlen = 8;
    request.message.header.request.bodylen = htonl(nkey + 8 + nbytes);

    ensure_send(bio, &request, sizeof(request.bytes));
    ensure_send(bio, key, nkey);
    ensure_send(bio, bytes, nbytes);
    if (BIO_flush(bio) != 1) {
        fprintf(stderr, "Failed to flush bio instance\n");
        exit(EXIT_FAILURE);
    }

    payload = get_response(bio, &response);
    free(payload);
    assert(ntohs(response.message.header.response.status) == PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

static char * fetch(BIO *bio,
                  const void *key,
                  uint16_t nkey)
{
    protocol_binary_request_get request;
    protocol_binary_response_no_extras response;
    char *payload;

    memset(&request, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET;
    request.message.header.request.keylen = htons((uint16_t)nkey);
    request.message.header.request.bodylen = htonl(nkey);

    ensure_send(bio, &request, sizeof(request.bytes));
    ensure_send(bio, key, nkey);
    if (BIO_flush(bio) != 1) {
        fprintf(stderr, "Failed to flush bio instance\n");
        exit(EXIT_FAILURE);
    }

    payload = get_response(bio, &response);
    assert(ntohs(response.message.header.response.status) == PROTOCOL_BINARY_RESPONSE_SUCCESS);
    assert(payload != NULL);
    return payload;

}


static void do_ssl_test(BIO *bio)
{
    uint32_t datalen = 512*1024;
    void *data = malloc(datalen);
    char *rcv = NULL;
    const char *key = "Hello World";
    int ii;

    for (ii = 0; ii < 1024; ++ii) {
        fprintf(stdout, "%u\n", ii);
        store(bio, key, (uint16_t)strlen(key), data, datalen);
        rcv = fetch(bio, key, (uint16_t)strlen(key));
        assert(memcmp(data, rcv + 4, datalen) == 0);
        free(rcv);
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
                    "Usage ssltest [-h host[:port]] [-p port] [-u user] [-p pass] [-s]\n");
            return 1;
        }
    }

    if (create_ssl_connection(&ctx, &bio, host, port, user, pass, secure) != 0) {
        return 1;
    }

    do_ssl_test(bio);

    BIO_free_all(bio);
    if (secure) {
        SSL_CTX_free(ctx);
    }

    return EXIT_SUCCESS;
}
