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
#include "config.h"
#include <cbsasl/client.h>
#include <mcbp/mcbp.h>
#include <memcached/protocol_binary.h>
#include <openssl/conf.h>
#include <openssl/engine.h>
#include <gsl/gsl>
#include <iostream>
#include <vector>
#include "utilities/protocol2text.h"

#include "utilities.h"

static const bool packet_dump = getenv("COUCHBASE_PACKET_DUMP") != nullptr;

void ensure_send(BIO* bio, const void* data, int nbytes) {
    int total = 0;

    while (total < nbytes) {
        int nw = BIO_write(bio, (const char*)data + total, nbytes - total);
        if (nw <= 0) {
            if (BIO_should_retry(bio) == 0) {
                fprintf(stderr,
                        "Failed to write data, BIO_write returned %d\n",
                        nw);
                exit(EXIT_FAILURE);
            }
        } else {
            total += nw;
        }
    }
}

void ensure_recv(BIO* bio, void* data, int nbytes) {
    int total = 0;

    while (total < nbytes) {
        int nr = BIO_read(bio, (char*)data + total, nbytes - total);
        if (nr <= 0) {
            if (BIO_should_retry(bio) == 0) {
                fprintf(stderr,
                        "Failed to read data, BIO_read returned %d\n",
                        nr);
                exit(EXIT_FAILURE);
            }
        } else {
            total += nr;
        }
    }
}

static void sendCommand(BIO* bio,
                        uint8_t opcode,
                        const std::string& key,
                        const std::string& value) {
    protocol_binary_request_no_extras req;
    memset(req.bytes, 0, sizeof(req.bytes));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = opcode;
    req.message.header.request.keylen =
            htons(gsl::narrow<uint16_t>(key.length()));
    req.message.header.request.bodylen =
            htonl(gsl::narrow<uint32_t>(key.length() + value.length()));

    std::vector<uint8_t> buffer(
        sizeof(req.bytes) + key.length() + value.length());
    memcpy(buffer.data(), req.bytes, sizeof(req.bytes));
    memcpy(buffer.data() + sizeof(req.bytes), key.data(), key.length());
    memcpy(buffer.data() + sizeof(req.bytes) + key.length(), value.data(),
           value.length());
    sendCommand(bio, buffer);
}

void sendCommand(BIO* bio, const std::vector<uint8_t>& buffer) {
    ensure_send(bio, buffer.data(), gsl::narrow<int>(buffer.size()));
    if (packet_dump) {
        cb::mcbp::dump(buffer.data(), std::cerr);
    }
}

void readResponse(BIO* bio, std::vector<uint8_t>& buffer) {
    protocol_binary_response_no_extras res;
    ensure_recv(bio, res.bytes, sizeof(res.bytes));
    uint32_t bodylen = res.message.header.response.getBodylen();
    buffer.resize(sizeof(res.bytes) + bodylen);
    memcpy(buffer.data(), res.bytes, sizeof(res.bytes));
    ensure_recv(bio, buffer.data() + sizeof(res.bytes), bodylen);
    if (packet_dump) {
        cb::mcbp::dump(buffer.data(), std::cerr);
    }
    auto* r = reinterpret_cast<protocol_binary_response_header*>(buffer.data());
    r->response.status = ntohs(r->response.status);
}

static std::string sasl_listmech(BIO* bio) {
    sendCommand(bio, PROTOCOL_BINARY_CMD_SASL_LIST_MECHS, "", "");

    std::vector<uint8_t> response;
    readResponse(bio, response);
    auto* r = reinterpret_cast<protocol_binary_response_header*>(response.data());
    if (r->response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        auto value = r->response.getValue();
        return std::string{reinterpret_cast<const char*>(value.data()),
                           value.size()};
    }

    std::stringstream ss;

    auto status = (protocol_binary_response_status)r->response.status;
    ss << "SASL LIST MECHS failed: " << memcached_status_2_text(status);
    throw std::runtime_error(ss.str());
}

int do_sasl_auth(BIO* bio, const char* user, const char* pass) {
    std::string mecs;
    try {
        mecs = sasl_listmech(bio);
    } catch (const std::runtime_error& error) {
        std::cerr << error.what() << std::endl;
        return -1;
    }

    cb::sasl::client::ClientContext client(
            [user]() -> std::string { return user; },
            [pass]() -> std::string { return pass; },
            mecs);

    auto client_data = client.start();
    if (client_data.first != cb::sasl::Error::OK) {
        std::cerr << "Failed to start sasl client: "
                  << to_string(client_data.first) << std::endl;
        return -1;
    }

    sendCommand(
            bio,
            PROTOCOL_BINARY_CMD_SASL_AUTH,
            client.getName(),
            std::string(client_data.second.data(), client_data.second.size()));

    std::vector<uint8_t> response;
    readResponse(bio, response);

    auto* rsp = reinterpret_cast<protocol_binary_response_header*>(response.data());

    while (rsp->response.status ==
           PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE) {
        size_t datalen = rsp->response.getBodylen() -
                         rsp->response.getKeylen() - rsp->response.extlen;

        size_t dataoffset = sizeof(rsp->bytes) + rsp->response.getKeylen() +
                            rsp->response.extlen;

        client_data = client.step(
                {reinterpret_cast<char*>(rsp->bytes + dataoffset), datalen});

        if (client_data.first != cb::sasl::Error::OK &&
            client_data.first != cb::sasl::Error::CONTINUE) {
            std::cerr << "Authentication failed: "
                      << to_string(client_data.first) << std::endl;
            return -1;
        }

        sendCommand(bio,
                    PROTOCOL_BINARY_CMD_SASL_STEP,
                    client.getName(),
                    std::string(client_data.second.data(),
                                client_data.second.size()));

        readResponse(bio, response);
        rsp = reinterpret_cast<protocol_binary_response_header*>(response.data());
    }

    if (rsp->response.status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        auto status = (protocol_binary_response_status)rsp->response.status;
        std::cerr << "Authentication failure: "
                  << memcached_status_2_text(status)
                  << std::endl;
        return -1;
    }

    return 0;
}

void initialize_openssl(void) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    CRYPTO_malloc_init();
    SSL_library_init();
#else
    OPENSSL_init_ssl(0, NULL);
#endif
    SSL_load_error_strings();
    ERR_load_BIO_strings();
    OpenSSL_add_all_algorithms();
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
