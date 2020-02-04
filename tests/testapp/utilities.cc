/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include <openssl/conf.h>
#include <openssl/engine.h>

#include "utilities.h"

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
