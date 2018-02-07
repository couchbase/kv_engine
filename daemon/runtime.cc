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
#include "memcached.h"
#include "settings.h"
#include "ssl_utils.h"

#include <atomic>
#include <string>
#include <mutex>

#include <memcached/openssl.h>


static std::string ssl_cipher_list;
static std::mutex ssl_cipher_list_mutex;

void set_ssl_cipher_list(const std::string& list) {
    std::lock_guard<std::mutex> lock(ssl_cipher_list_mutex);
    if (list.empty()) {
        ssl_cipher_list.resize(0);
    } else {
        ssl_cipher_list.assign(list);
    }
}

void set_ssl_ctx_cipher_list(SSL_CTX *ctx) {
    std::lock_guard<std::mutex> lock(ssl_cipher_list_mutex);
    if (ssl_cipher_list.length()) {
        if (SSL_CTX_set_cipher_list(ctx, ssl_cipher_list.c_str()) == 0) {
            LOG_WARNING(
                    "Failed to select any of the "
                    "requested ciphers ({})",
                    ssl_cipher_list);
        }
    }
}

static std::atomic_long ssl_protocol_mask;

void set_ssl_protocol_mask(const std::string& mask) {
    try {
        ssl_protocol_mask.store(decode_ssl_protocol(mask),
                                std::memory_order_release);
        if (!mask.empty()) {
            LOG_INFO("Setting SSL minimum protocol to: {}", mask);
        }
    } catch (const std::invalid_argument& e) {
        LOG_WARNING("Invalid SSL protocol specified: {}", mask);

    } catch (...) {
        LOG_ERROR("An error occured while decoding the SSL protocol: {}", mask);
    }
}

void set_ssl_ctx_protocol_mask(SSL_CTX* ctx) {
    SSL_CTX_set_options(ctx, ssl_protocol_mask.load(std::memory_order_acquire));
}

static std::atomic<Audit*> auditHandle { nullptr };

void set_audit_handle(Audit* handle) {
    auditHandle.store(handle);
}

Audit* get_audit_handle(void) {
    return auditHandle.load(std::memory_order_relaxed);
}

static const bool unit_tests{getenv("MEMCACHED_UNIT_TESTS") != NULL};

static std::atomic_bool default_bucket_enabled;
bool is_default_bucket_enabled() {
    if (unit_tests) {
        if (getenv("MEMCACHED_UNIT_TESTS_NO_DEFAULT_BUCKET")) {
            return false;
        }
    }
    return default_bucket_enabled.load(std::memory_order_relaxed);
}

void set_default_bucket_enabled(bool enabled) {
    default_bucket_enabled.store(enabled, std::memory_order_relaxed);
}
