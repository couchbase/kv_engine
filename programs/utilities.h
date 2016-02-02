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
#pragma once
#include <memcached/openssl.h>

#ifdef __cplusplus
#include <cstdint>
#include <vector>

/**
 * Send a command to the server connected via the bio
 *
 * This is the preferred way to send commands to the memcached server
 * from our command line tools as this method creates a packet dump to
 * stderr if the environment variable COUCHBASE_PACKET_DUMP is set to
 * a non-null value.
 *
 * @param bio the connection used to send the data
 * @param buffer the buffer containing the _entire_ MCBP encoded packet.
 */
void sendCommand(BIO* bio, const std::vector<uint8_t>& buffer);

/**
 * read a single MCBP-encoded packet from the server
 *
 * This is the preferred way to read responses from the memcached server
 * from our command line tools as this method creates a packet dump to
 * stderr if the environment variable COUCHBASE_PACKET_DUMP is set to
 * a non-null value.
 *
 * @param bio the connection used to read the data from
 * @param buffer the buffer containing the _entire_ MCBP encoded packet.
 *               (the buffer will be resized to fit the entire response)
 */
void readResponse(BIO* bio, std::vector<uint8_t>& buffer);

extern "C" {
#endif

    void ensure_send(BIO *bio, const void *data, int nbytes);
    void ensure_recv(BIO *bio, void *data, int nbytes);
    int do_sasl_auth(BIO *bio, const char *user, const char *pass);
    void initialize_openssl(void);

    // Frees all resources allocated by initialize_openssl().
    void shutdown_openssl(void);

    int create_ssl_connection(SSL_CTX** ctx,
                              BIO** bio,
                              const char* host,
                              const char* port,
                              const char* user,
                              const char* pass,
                              int secure);

    bool enable_tcp_nodelay(BIO *bio);

#ifdef __cplusplus
}
#endif
