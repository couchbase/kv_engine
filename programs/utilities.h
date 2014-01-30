/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once
#include <memcached/openssl.h>

#ifdef __cplusplus
extern "C" {
#endif

    void ensure_send(BIO *bio, const void *data, int nbytes);
    void ensure_recv(BIO *bio, void *data, int nbytes);
    int do_sasl_auth(BIO *bio, const char *user, const char *pass);
    void initialize_openssl(void);

    int create_ssl_connection(SSL_CTX** ctx,
                              BIO** bio,
                              const char* host,
                              const char* port,
                              const char* user,
                              const char* pass,
                              int secure);



#ifdef __cplusplus
}
#endif
