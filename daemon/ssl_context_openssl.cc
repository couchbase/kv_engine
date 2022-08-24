/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include "ssl_context.h"

#include "memcached.h"
#include "runtime.h"
#include "settings.h"
#include "ssl_utils.h"

#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <platform/socket.h>
#include <platform/strerror.h>
#include <utilities/logtags.h>

SslContext::~SslContext() {
    if (enabled) {
        disable();
    }
}

int SslContext::accept() {
    return SSL_accept(client);
}

int SslContext::getError(int errormask) const {
    return SSL_get_error(client, errormask);
}

int SslContext::read(void* buf, int num) {
    return SSL_read(client, buf, num);
}

int SslContext::write(const void* buf, int num) {
    return SSL_write(client, buf, num);
}

bool SslContext::bio_pending() {
    return BIO_ctrl_pending(network);
}

bool SslContext::havePendingInputData() {
    if (isEnabled()) {
        // Move any data in the memory buffer over to the ssl pipe
        drainInputSocketBuf();
        return SSL_pending(client) > 0;
    }
    return false;
}

bool SslContext::enable(const std::string& cert, const std::string& pkey) {
    const auto& settings = Settings::instance();
    ctx = SSL_CTX_new(SSLv23_server_method());
    SSL_CTX_set_dh_auto(ctx, 1);
    SSL_CTX_set_options(
            ctx, settings.getSslProtocolMask() | SSL_OP_NO_RENEGOTIATION);
    SSL_CTX_set_mode(ctx,
                     SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER |
                             SSL_MODE_ENABLE_PARTIAL_WRITE);

    /* @todo don't read files, but use in-memory-copies */
    if (!SSL_CTX_use_certificate_chain_file(ctx, cert.c_str()) ||
        !SSL_CTX_use_PrivateKey_file(ctx, pkey.c_str(), SSL_FILETYPE_PEM)) {
        LOG_WARNING("Failed to use SSL cert {} and pkey {}",
                    cb::UserDataView(cert),
                    cb::UserDataView(pkey));
        return false;
    }

    try {
        set_ssl_ctx_ciphers(ctx,
                            settings.getSslCipherList(),
                            settings.getSslCipherSuites());
    } catch (const std::runtime_error& error) {
        LOG_WARNING("{}", error.what());
        return false;
    }

    int ssl_flags = 0;
    switch (settings.getClientCertMode()) {
    case cb::x509::Mode::Mandatory:
        ssl_flags |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
    // FALLTHROUGH
    case cb::x509::Mode::Enabled: {
        ssl_flags |= SSL_VERIFY_PEER;
        STACK_OF(X509_NAME)* certNames = SSL_load_client_CA_file(cert.c_str());
        if (certNames == NULL) {
            LOG_WARNING("Failed to read SSL cert {}", cb::UserDataView(cert));
            return false;
        }
        SSL_CTX_set_client_CA_list(ctx, certNames);
        SSL_CTX_load_verify_locations(ctx, cert.c_str(), nullptr);
        SSL_CTX_set_verify(ctx, ssl_flags, nullptr);
        break;
    }
    case cb::x509::Mode::Disabled:
        break;
    }

    enabled = true;
    error = false;
    client = nullptr;

    try {
        inputPipe.ensureCapacity(settings.getBioDrainBufferSize());
        outputPipe.ensureCapacity(settings.getBioDrainBufferSize());
    } catch (const std::bad_alloc&) {
        return false;
    }

    BIO_new_bio_pair(&application,
                     Settings::instance().getBioDrainBufferSize(),
                     &network,
                     Settings::instance().getBioDrainBufferSize());

    client = SSL_new(ctx);
    SSL_set_bio(client, application, application);

    return true;
}

std::pair<cb::x509::Status, std::string> SslContext::getCertUserName() {
    cb::openssl::unique_x509_ptr cert(SSL_get_peer_certificate(client));
    return Settings::instance().lookupUser(cert.get());
}

void SslContext::disable() {
    if (network != nullptr) {
        BIO_free_all(network);
    }
    if (client != nullptr) {
        SSL_free(client);
    }
    error = false;
    if (ctx != nullptr) {
        SSL_CTX_free(ctx);
    }
    enabled = false;
}

bool SslContext::drainInputSocketBuf() {
    if (!inputPipe.empty()) {
        auto* bio = network;
        auto n =
                inputPipe.consume([bio](cb::const_byte_buffer data) -> ssize_t {
                    return BIO_write(
                            bio, data.data(), gsl::narrow<int>(data.size()));
                });

        if (n > 0) {
            // We did move some data
            return true;
        }
    }

    return false;
}

void SslContext::drainBioRecvPipe(SOCKET sfd) {
    bool stop;

    do {
        // Try to move data from our internal buffer to the SSL pipe
        stop = !drainInputSocketBuf();

        // If there is room in the input pipe (the internal buffer for read)
        // try to read out as much as possible from the socket
        if (!inputPipe.full()) {
            auto n = inputPipe.produce([sfd](cb::byte_buffer data) -> ssize_t {
                return cb::net::recv(sfd,
                                     reinterpret_cast<char*>(data.data()),
                                     data.size(),
                                     0);
            });
            if (n > 0) {
                totalRecv += n;
                // We did receive some data... move it into the BIO
                stop = false;
            } else {
                if (n == 0) {
                    error = true; /* read end shutdown */
                } else {
                    if (!cb::net::is_blocking(cb::net::get_socket_error())) {
                        error = true;
                    }
                }
            }
        }

        // As long as we moved some data (from the socket to our internal buffer
        // or from our internal buffer to the BIO) we should keep on moving
        // data.
    } while (!stop);

    // At this time there is:
    //   * either no more data to receive (everything is moved into the
    //     BIO object
    //   * The BIO object is full and the input pipe is:
    //       * full - there may be more data on the network
    //       * not full - there is no more data to read from the network
}

void SslContext::drainBioSendPipe(SOCKET sfd) {
    bool stop;

    do {
        stop = true;
        // Try to move data from our internal buffer to the socket
        if (!outputPipe.empty()) {
            auto n = outputPipe.consume(
                    [sfd](cb::const_byte_buffer data) -> ssize_t {
                        return cb::net::send(
                                sfd,
                                reinterpret_cast<const char*>(data.data()),
                                data.size(),
                                0);
                    });

            if (n > 0) {
                totalSend += n;
                // We did move some data
                stop = false;
            } else {
                if (n == -1) {
                    auto err = cb::net::get_socket_error();
                    if (!cb::net::is_blocking(err)) {
                        LOG_WARNING(
                                "Failed to write, and not due to blocking: {}",
                                cb_strerror(err));
                        error = true;
                    }
                }
                return;
            }
        }

        if (!outputPipe.full()) {
            auto* bio = network;
            auto n = outputPipe.produce([bio](cb::byte_buffer data) -> ssize_t {
                return BIO_read(
                        bio, data.data(), gsl::narrow<int>(data.size()));
            });

            if (n > 0) {
                // We did move data
                stop = false;
            }
        }
        // As long as we moved some data (from the internal buffer to the
        // socket or from the BIO to our internal buffer) we should keep on
        // moving data.
    } while (!stop);

    // At this time there is:
    //   * There is no more data to send
    //   * The socket buffer is full
}

void SslContext::dumpCipherList(uint32_t id) const {
    nlohmann::json array;

    int ii = 0;
    const char* cipher;
    while ((cipher = SSL_get_cipher_list(client, ii++)) != nullptr) {
        array.push_back(cipher);
    }

    LOG_DEBUG("{}: Using SSL ciphers: {}", id, array.dump());
}

nlohmann::json SslContext::toJSON() const {
    nlohmann::json obj;

    obj["enabled"] = enabled;
    if (enabled) {
        obj["connected"] = connected;
        obj["error"] = error;
        obj["total_recv"] = totalRecv;
        obj["total_send"] = totalSend;
    }

    return obj;
}

const char* SslContext::getCurrentCipherName() const {
    return SSL_get_cipher_name(client);
}
