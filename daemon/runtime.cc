/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "config.h"
#include "runtime.h"
#include "settings.h"

#include <atomic>
#include <string>
#include <mutex>

#include <memcached/openssl.h>


static std::atomic<bool> server_initialized;

bool is_server_initialized(void) {
    return server_initialized.load(std::memory_order_acquire);
}

void set_server_initialized(bool enable) {
    server_initialized.store(enable, std::memory_order_release);
}

static std::string ssl_cipher_list;
static std::mutex ssl_cipher_list_mutex;

void set_ssl_cipher_list(const char *list) {
    std::lock_guard<std::mutex> lock(ssl_cipher_list_mutex);
    if (list == NULL) {
        ssl_cipher_list.resize(0);
    } else {
        ssl_cipher_list.assign(list);
    }
}

void set_ssl_ctx_cipher_list(SSL_CTX *ctx) {
    std::lock_guard<std::mutex> lock(ssl_cipher_list_mutex);
    if (ssl_cipher_list.length()) {
        if (SSL_CTX_set_cipher_list(ctx, ssl_cipher_list.c_str()) == 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "Failed to select any of the "
                                            "requested ciphers (%s)",
                                            ssl_cipher_list.c_str());
        }
    }
}
