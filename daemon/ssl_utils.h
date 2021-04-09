/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memory>
#include <string>

struct ssl_ctx_st;
typedef struct ssl_ctx_st SSL_CTX;

long decode_ssl_protocol(const std::string& protocol);

/**
 * Update the SSL_CTX with the provided SSL ciphers
 *
 * @param ctx the context to update
 * @param list the list of ciphers to use for TLS < 1.3
 * @param suites the ciphersuites to use for TLS >= 1.3
 * @throws std::runtime_error if we fail to set the provided ciphers
 */
void set_ssl_ctx_ciphers(SSL_CTX* ctx,
                         const std::string& list,
                         const std::string& suites);

struct ssl_st;
struct ssl_st_deleter {
    void operator()(ssl_st* st);
};
class ListeningPort;
using uniqueSslPtr = std::unique_ptr<ssl_st, ssl_st_deleter>;
/**
 * Create an SSL structure to use for the provided port
 *
 * @param port The port description (containing the certificates etc)
 * @return the SSL structure to use
 * @throws std::runtime error  if we fail to read the certificates etc
 *         std::bad_alloc for memory allocation failures
 */
uniqueSslPtr createSslStructure(const ListeningPort& port);

/// Invalidate the cache we've got of SSL_CTX objects in use
void invalidateSslCache();
