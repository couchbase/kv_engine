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

bool SslContext::havePendingInputData() {
    if (isEnabled()) {
        // Move any data in the memory buffer over to the ssl pipe
        drainInputSocketBuf();
        return SSL_pending(client) > 0;
    }
    return false;
}

bool SslContext::enable(const std::string& cert, const std::string& pkey) {
    ctx = SSL_CTX_new(SSLv23_server_method());
    set_ssl_ctx_protocol_mask(ctx);

    /* @todo don't read files, but use in-memory-copies */
    if (!SSL_CTX_use_certificate_chain_file(ctx, cert.c_str()) ||
        !SSL_CTX_use_PrivateKey_file(ctx, pkey.c_str(), SSL_FILETYPE_PEM)) {
        LOG_WARNING("Failed to use SSL cert {} and pkey {}",
                    cb::logtags::tagUserData(cert),
                    cb::logtags::tagUserData(pkey));
        return false;
    }

    set_ssl_ctx_cipher_list(ctx);
    int ssl_flags = 0;
    switch (settings.getClientCertMode()) {
    case cb::x509::Mode::Mandatory:
        ssl_flags |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
    // FALLTHROUGH
    case cb::x509::Mode::Enabled: {
        ssl_flags |= SSL_VERIFY_PEER;
        STACK_OF(X509_NAME)* certNames = SSL_load_client_CA_file(cert.c_str());
        if (certNames == NULL) {
            LOG_WARNING("Failed to read SSL cert {}",
                        cb::logtags::tagUserData(cert));
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
    client = NULL;

    try {
        inputPipe.ensureCapacity(settings.getBioDrainBufferSize());
        outputPipe.ensureCapacity(settings.getBioDrainBufferSize());
    } catch (std::bad_alloc) {
        return false;
    }

    BIO_new_bio_pair(&application,
                     settings.getBioDrainBufferSize(),
                     &network,
                     settings.getBioDrainBufferSize());

    client = SSL_new(ctx);
    SSL_set_bio(client, application, application);

    return true;
}

std::pair<cb::x509::Status, std::string> SslContext::getCertUserName() {
    cb::openssl::unique_x509_ptr cert(SSL_get_peer_certificate(client));
    return settings.lookupUser(cert.get());
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
        auto n = inputPipe.consume(
            [bio](cb::const_byte_buffer data) -> ssize_t {
                return BIO_write(bio, data.data(), data.size());
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
                return ::recv(sfd,
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
                    if (!is_blocking(GetLastNetworkError())) {
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
                        return ::send(sfd,
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
                    if (!is_blocking(GetLastNetworkError())) {
                        log_socket_error(
                                EXTENSION_LOG_WARNING,
                                this,
                                "Failed to write, and not due to blocking: %s");
                        error = true;
                    }
                }
                return;
            }
        }

        if (!outputPipe.full()) {
            auto* bio = network;
            auto n = outputPipe.produce([bio](cb::byte_buffer data) -> ssize_t {
                return BIO_read(bio, data.data(), data.size());
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
    unique_cJSON_ptr array(cJSON_CreateArray());

    int ii = 0;
    const char* cipher;
    while ((cipher = SSL_get_cipher_list(client, ii++)) != nullptr) {
        cJSON_AddItemToArray(array.get(), cJSON_CreateString(cipher));
    }

    LOG_DEBUG("{}: Using SSL ciphers: {}", id, to_string(array, false));
}

cJSON* SslContext::toJSON() const {
    cJSON* obj = cJSON_CreateObject();
    cJSON_AddBoolToObject(obj, "enabled", enabled);
    if (enabled) {
        cJSON_AddBoolToObject(obj, "connected", connected);
        cJSON_AddBoolToObject(obj, "error", error);
        cJSON_AddNumberToObject(obj, "total_recv", totalRecv);
        cJSON_AddNumberToObject(obj, "total_send", totalSend);
    }

    return obj;
}
