/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "ssl_utils.h"

#include "listening_port.h"
#include "settings.h"
#include <folly/Synchronized.h>
#include <openssl/ssl.h>
#include <algorithm>
#include <cctype>
#include <stdexcept>

long decode_ssl_protocol(const std::string& protocol) {
    // MB-12359 - Disable SSLv2 & SSLv3 due to POODLE
    // MB-41757 - Disable renegotiation
    long disallow = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_RENEGOTIATION;

    std::string minimum(protocol);
    std::transform(minimum.begin(), minimum.end(), minimum.begin(), tolower);

    if (minimum.empty() || minimum == "tlsv1") {
        // nothing
    } else if (minimum == "tlsv1.1" || minimum == "tlsv1_1") {
        disallow |= SSL_OP_NO_TLSv1;
    } else if (minimum == "tlsv1.2" || minimum == "tlsv1_2") {
        disallow |= SSL_OP_NO_TLSv1_1 | SSL_OP_NO_TLSv1;
    } else if (minimum == "tlsv1.3" || minimum == "tlsv1_3") {
        disallow |= SSL_OP_NO_TLSv1_2 | SSL_OP_NO_TLSv1_1 | SSL_OP_NO_TLSv1;
    } else {
        throw std::invalid_argument("Unknown protocol: " + minimum);
    }

    return disallow;
}

void set_ssl_ctx_ciphers(SSL_CTX* ctx,
                         const std::string& list,
                         const std::string& suites) {
    if (list.empty()) {
        auto options = SSL_CTX_get_options(ctx);
        SSL_CTX_set_options(ctx,
                            options | SSL_OP_NO_TLSv1_2 | SSL_OP_NO_TLSv1_1 |
                                    SSL_OP_NO_TLSv1);
        SSL_CTX_set_cipher_list(ctx, "");
    } else if (SSL_CTX_set_cipher_list(ctx, list.c_str()) == 0) {
        throw std::runtime_error(
                "Failed to select any of the requested TLS < 1.3 ciphers (" +
                list + ")");
    }

    if (suites.empty()) {
        auto options = SSL_CTX_get_options(ctx);
        SSL_CTX_set_options(ctx, options | SSL_OP_NO_TLSv1_3);
        SSL_CTX_set_ciphersuites(ctx, "");
    } else if (SSL_CTX_set_ciphersuites(ctx, suites.c_str()) == 0) {
        throw std::runtime_error(
                "Failed to select any of the requested TLS > 1.2 ciphers (" +
                suites + ")");
    }
}

struct ssl_ctx_st_deleter {
    void operator()(ssl_ctx_st* ctx) {
        SSL_CTX_free(ctx);
    }
};
using uniqueSslCtxPtr = std::unique_ptr<ssl_ctx_st, ssl_ctx_st_deleter>;
folly::Synchronized<std::map<std::string, uniqueSslCtxPtr>> interfaceCache;

static uniqueSslCtxPtr createCacheEntry(const ListeningPort& ifc) {
    auto& settings = Settings::instance();
    uniqueSslCtxPtr ret{SSL_CTX_new(SSLv23_server_method())};
    auto* server_ctx = ret.get();
    SSL_CTX_set_dh_auto(server_ctx, 1);
    SSL_CTX_set_options(server_ctx, settings.getSslProtocolMask());
    SSL_CTX_set_mode(server_ctx,
                     SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER |
                             SSL_MODE_ENABLE_PARTIAL_WRITE);

    if (!SSL_CTX_use_certificate_chain_file(server_ctx, ifc.sslCert.c_str()) ||
        !SSL_CTX_use_PrivateKey_file(
                server_ctx, ifc.sslKey.c_str(), SSL_FILETYPE_PEM)) {
        throw std::runtime_error("Failed to enable ssl!");
    }
    set_ssl_ctx_ciphers(server_ctx,
                        settings.getSslCipherList(),
                        settings.getSslCipherSuites());
    int ssl_flags = 0;
    switch (settings.getClientCertMode()) {
    case cb::x509::Mode::Mandatory:
        ssl_flags |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
        // FALLTHROUGH
    case cb::x509::Mode::Enabled: {
        ssl_flags |= SSL_VERIFY_PEER;
        auto* certNames = SSL_load_client_CA_file(ifc.sslCert.c_str());
        if (certNames == nullptr) {
            std::string msg = "Failed to read SSL cert " + ifc.sslCert;
            throw std::runtime_error(msg);
        }
        SSL_CTX_set_client_CA_list(server_ctx, certNames);
        SSL_CTX_load_verify_locations(server_ctx, ifc.sslCert.c_str(), nullptr);
        SSL_CTX_set_verify(server_ctx, ssl_flags, nullptr);
        break;
    }
    case cb::x509::Mode::Disabled:
        break;
    }

    return ret;
}

void ssl_st_deleter::operator()(struct ssl_st* st) {
    SSL_free(st);
}

uniqueSslPtr createSslStructure(const ListeningPort& ifc) {
    std::string name = ifc.host + "_" + std::to_string(ifc.family) + "_" +
                       std::to_string(ifc.port);

    // fast path, we have an entry
    auto rlock = interfaceCache.rlock();
    auto iter = rlock->find(name);
    if (iter != rlock->end()) {
        return uniqueSslPtr{SSL_new(iter->second.get())};
    }
    rlock.unlock();

    // Slow path, we need to create cache entry
    auto ctx = createCacheEntry(ifc);
    auto wlock = interfaceCache.wlock();
    // verify that it isn't there...
    iter = wlock->find(name);
    if (iter != wlock->end()) {
        // race condition, use the one from the cache!
        return uniqueSslPtr{SSL_new(iter->second.get())};
    }

    auto ret = uniqueSslPtr{SSL_new(ctx.get())};
    wlock->emplace(std::make_pair<std::string, uniqueSslCtxPtr>(
            std::move(name), std::move(ctx)));
    return ret;
}

void invalidateSslCache() {
    interfaceCache.wlock()->clear();
}
