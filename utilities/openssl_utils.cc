/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "openssl_utils.h"

#include <openssl/conf.h>
#include <openssl/engine.h>
#include <openssl/ssl.h>

void initialize_openssl() {
    OPENSSL_init_ssl(0, nullptr);
    SSL_load_error_strings();
    ERR_load_BIO_strings();
    OpenSSL_add_all_algorithms();
}

void shutdown_openssl() {
    // Global OpenSSL cleanup:
    ENGINE_cleanup();
    CONF_modules_unload(1);
    ERR_free_strings();
    EVP_cleanup();
    CRYPTO_cleanup_all_ex_data();
}
