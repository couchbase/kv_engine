/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ssl_utils.h"

#include <fmt/format.h>
#include <openssl/ssl.h>
#include <stdexcept>

long decode_ssl_protocol(std::string_view protocol) {
    // MB-41757 - Disable renegotiation
    long disallow = SSL_OP_NO_SSL_MASK | SSL_OP_NO_RENEGOTIATION;

    if (protocol.empty() || protocol == "TLS 1.2") {
        disallow &= ~(SSL_OP_NO_TLSv1_2 | SSL_OP_NO_TLSv1_3);
    } else if (protocol == "TLS 1.3") {
        disallow &= ~SSL_OP_NO_TLSv1_3;
    } else {
        throw std::invalid_argument(
                fmt::format("Unknown protocol: {}", protocol));
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

void ssl_st_deleter::operator()(struct ssl_st* st) {
    SSL_free(st);
}
