/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "config.h"
#include "memcached_openssl.h"

#include <memcached/openssl.h>
#include <openssl/engine.h>
#include <openssl/conf.h>

#if OPENSSL_VERSION_NUMBER < 0x10100000L

#include <mutex>
#include <vector>

static std::vector<std::mutex> openssl_lock_cs;

static unsigned long get_thread_id() {
    return (unsigned long)cb_thread_self();
}

static void openssl_locking_callback(int mode, int type, const char *, int)
{
    if (mode & CRYPTO_LOCK) {
        openssl_lock_cs[type].lock();
    } else {
        openssl_lock_cs[type].unlock();
    }
}
#endif

void initialize_openssl() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    CRYPTO_malloc_init();
    SSL_library_init();
#else
    OPENSSL_init_ssl(0, NULL);
#endif

    SSL_load_error_strings();
    ERR_load_BIO_strings();
    OpenSSL_add_all_algorithms();

#if OPENSSL_VERSION_NUMBER < 0x10100000L
    openssl_lock_cs = std::vector<std::mutex>(CRYPTO_num_locks());

    CRYPTO_set_id_callback(get_thread_id);
    CRYPTO_set_locking_callback(openssl_locking_callback);
#endif
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

#if OPENSSL_VERSION_NUMBER < 0x10100000L
    // per-thread cleanup:
    ERR_remove_state(0);

    // Newer versions of openssl (1.0.2a) have a the function
    // SSL_COMP_free_compression_methods() to perform this;
    // however we arn't that new...
    sk_SSL_COMP_free(SSL_COMP_get_compression_methods());

#endif
}
