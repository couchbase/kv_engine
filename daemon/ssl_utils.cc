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

uint64_t decode_ssl_protocol(std::string_view protocol) {
    // MB-41757 - Disable renegotiation
    uint64_t disallow = SSL_OP_NO_SSL_MASK | SSL_OP_NO_RENEGOTIATION;

    // MB-57002: Disable issuing session tickets to client for session
    // resumption, as (a) KV-Engine doesn't fully enable them, (b) neither
    // do the vast majority of our SDKs (only .NET on Windows using Schannel),
    // and (c) they are potentially security risks before TLSv1.3.
    // If we wanted to reduce the cost of a TLS client connecting to the same
    // server a subsequent time we could look into re-enabling this, but needs
    // explicit support on most of our SDKs. See MB for lots more detail.
    disallow |= SSL_OP_NO_TICKET;

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
