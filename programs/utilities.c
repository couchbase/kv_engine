/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <memcached/protocol_binary.h>
#include <openssl/conf.h>
#include <openssl/engine.h>

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
    ensure_send(bio, user, (int)ulen);
    if (pass) {
        ensure_send(bio, pass, (int)plen);
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


bool enable_tcp_nodelay(BIO *bio)
{
    protocol_binary_request_hello request;
    uint16_t feature = htons(PROTOCOL_BINARY_FEATURE_TCPNODELAY);
    const char useragent[] = "mctools";
    memset(&request, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_HELLO;
    request.message.header.request.keylen = htons(sizeof(useragent) - 1);
    request.message.header.request.bodylen = htonl(2 + sizeof(useragent) - 1);

    ensure_send(bio, &request, sizeof(request));
    ensure_send(bio, &useragent, sizeof(useragent) - 1);
    ensure_send(bio, &feature, sizeof(feature));

    protocol_binary_response_hello response;
    ensure_recv(bio, &response, sizeof(response.bytes));

    uint32_t vallen = ntohl(response.message.header.response.bodylen);
    char *buffer = NULL;
    if (vallen != 0) {
        buffer = malloc(vallen);
        cb_assert(buffer != NULL);
        ensure_recv(bio, buffer, vallen);
    }

    uint16_t status = ntohs(response.message.header.response.status);
    bool ret = true;

    if (status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        fprintf(stderr, "Failed to enable TCP NODELAY: %s\n", buffer);
        ret = false;
    }

    free(buffer);
    return ret;
}

void initialize_openssl(void) {
    CRYPTO_malloc_init();
    SSL_library_init();
    SSL_load_error_strings();
    ERR_load_BIO_strings();
    OpenSSL_add_all_algorithms();
}

void shutdown_openssl() {
    // Global OpenSSL cleanup:
    CRYPTO_set_locking_callback(NULL);
    CRYPTO_set_id_callback(NULL);
    ENGINE_cleanup();
    CONF_modules_unload(1);
    ERR_free_strings();
    EVP_cleanup();
    CRYPTO_cleanup_all_ex_data();

    // per-thread cleanup:
    ERR_remove_state(0);

    // Newer versions of openssl (1.0.2a) have a the function
    // SSL_COMP_free_compression_methods() to perform this;
    // however we arn't that new...
    sk_SSL_COMP_free(SSL_COMP_get_compression_methods());
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
