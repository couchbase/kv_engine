/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <memcached/protocol_binary.h>

#include "utilities.h"

void ensure_send(BIO *bio, const void *data, int nbytes) {
    int total = 0;

    while (total < nbytes) {
        int nw = BIO_write(bio, (const char*)data + total, nbytes - total);
        if (nw < 0) {
            if (BIO_should_retry(bio) == 0) {
                fprintf(stderr, "Failed to write data\n");
                exit(EXIT_FAILURE);
            }
        } else {
            total += nw;
        }
    }
}

void ensure_recv(BIO *bio, void *data, int nbytes) {
    int total = 0;

    while (total < nbytes) {
        int nr = BIO_read(bio, (char*)data + total, nbytes - total);
        if (nr < 0) {
            if (BIO_should_retry(bio) == 0) {
                fprintf(stderr, "Failed to read data\n");
                exit(EXIT_FAILURE);
            }
        } else {
            total += nr;
        }
    }
}

int do_sasl_auth(BIO *bio, const char *user, const char *pass)
{
    /*
     * For now just shortcut the SASL phase by requesting a "PLAIN"
     * sasl authentication.
     */
    protocol_binary_response_status status;
    protocol_binary_request_stats request;
    protocol_binary_response_no_extras response;
    uint32_t vallen;
    char *buffer = NULL;

    size_t ulen = strlen(user) + 1;
    size_t plen = pass ? strlen(pass) + 1 : 1;
    size_t tlen = ulen + plen + 1;

    memset(&request, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_SASL_AUTH;
    request.message.header.request.keylen = htons(5);
    request.message.header.request.bodylen = htonl(5 + (uint32_t)tlen);

    ensure_send(bio, &request, sizeof(request));
    ensure_send(bio, "PLAIN", 5);
    ensure_send(bio, "", 1);
    ensure_send(bio, user, ulen);
    if (pass) {
        ensure_send(bio, pass, plen);
    } else {
        ensure_send(bio, "", 1);
    }

    ensure_recv(bio, &response, sizeof(response.bytes));
    vallen = ntohl(response.message.header.response.bodylen);
    buffer = NULL;

    if (vallen != 0) {
        buffer = malloc(vallen);
        ensure_recv(bio, buffer, vallen);
    }

    status = ntohs(response.message.header.response.status);

    if (status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        fprintf(stderr, "Failed to authenticate to the server\n");
        return -1;
    }

    free(buffer);
    return 0;
}

void initialize_openssl(void) {
    CRYPTO_malloc_init();
    SSL_library_init();
    SSL_load_error_strings();
    ERR_load_BIO_strings();
    OpenSSL_add_all_algorithms();
}

int create_ssl_connection(SSL_CTX** ctx,
                          BIO** bio,
                          const char* host,
                          const char* port,
                          const char* user,
                          const char* pass,
                          int secure)
{
    char uri[1024];

    initialize_openssl();
    if ((*ctx = SSL_CTX_new(SSLv23_client_method())) == NULL) {
        fprintf(stderr, "Failed to create openssl client contex\n");
        return 1;
    }

    snprintf(uri, sizeof(uri), "%s:%s", host, port);

    if (secure) {
        /* Create a secure connection */
        *bio = BIO_new_ssl_connect(*ctx);
        if (*bio == NULL) {
            fprintf(stderr, "Error creating openssl BIO object!\n");
            ERR_print_errors_fp(stderr);
            SSL_CTX_free(*ctx);
            return 1;
        }

        BIO_set_conn_hostname(*bio, uri);
    } else {
        /* create an unsecure */
        *bio = BIO_new_connect(uri);
        if (*bio == NULL) {
            fprintf(stderr, "Error creating openssl BIO object!\n");
            ERR_print_errors_fp(stderr);
            return 1;
        }
    }

    if (BIO_do_connect(*bio) <= 0) {
        char error[65535];
        fprintf(stderr, "Failed to connect to %s\n", uri);
        ERR_error_string_n(ERR_get_error(), error, 65535);
        printf("Error: %s\n", error);
        ERR_print_errors(*bio);
        BIO_free_all(*bio);
        if (secure) {
            SSL_CTX_free(*ctx);
        }
        return 1;
    }

    /* we're connected */
    if (secure && BIO_do_handshake(*bio) <= 0) {
        fprintf(stderr, "Failed to do SSL handshake!");
        BIO_free_all(*bio);
        SSL_CTX_free(*ctx);
        return 1;
    }

    /* Do we need to do SASL auth? */
    if (user && do_sasl_auth(*bio, user, pass) == -1) {
        BIO_free_all(*bio);
        SSL_CTX_free(*ctx);
        return 1;
    }

    return 0;
}
