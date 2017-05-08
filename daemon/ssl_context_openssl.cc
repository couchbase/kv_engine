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

int SslContext::peek(void* buf, int num) {
    return SSL_peek(client, buf, num);
}

bool SslContext::enable(const std::string& cert, const std::string& pkey) {
    ctx = SSL_CTX_new(SSLv23_server_method());
    set_ssl_ctx_protocol_mask(ctx);

    /* @todo don't read files, but use in-memory-copies */
    if (!SSL_CTX_use_certificate_chain_file(ctx, cert.c_str()) ||
        !SSL_CTX_use_PrivateKey_file(ctx, pkey.c_str(), SSL_FILETYPE_PEM)) {
        LOG_WARNING(nullptr,
                    "Failed to use SSL cert %s and pkey %s",
                    cert.c_str(),
                    pkey.c_str());
        return false;
    }

    set_ssl_ctx_cipher_list(ctx);
    int ssl_flags = 0;
    switch (settings.getClientCertAuth()) {
    case ClientCertAuth::Mode::Mandatory:
        ssl_flags |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
    // FALLTHROUGH
    case ClientCertAuth::Mode::Enabled: {
        ssl_flags |= SSL_VERIFY_PEER;
        STACK_OF(X509_NAME)* certNames = SSL_load_client_CA_file(cert.c_str());
        if (certNames == NULL) {
            LOG_WARNING(nullptr, "Failed to read SSL cert %s", cert.c_str());
            return false;
        }
        SSL_CTX_set_client_CA_list(ctx, certNames);
        SSL_CTX_load_verify_locations(ctx, cert.c_str(), nullptr);
        SSL_CTX_set_verify(ctx, ssl_flags, nullptr);
        break;
    }
    case ClientCertAuth::Mode::Disabled:
        break;
    }

    enabled = true;
    error = false;
    client = NULL;

    try {
        in.buffer.resize(settings.getBioDrainBufferSize());
        out.buffer.resize(settings.getBioDrainBufferSize());
    } catch (std::bad_alloc) {
        return false;
    }

    BIO_new_bio_pair(
            &application, in.buffer.size(), &network, out.buffer.size());

    client = SSL_new(ctx);
    SSL_set_bio(client, application, application);

    return true;
}

SslCertResult SslContext::getCertUserName() {
    ClientCertUser::Status status = ClientCertUser::Status::Success;
    std::string result;

    auto certUser = settings.getCertUserCopy();
    if (certUser) {
        cb::openssl::unique_x509_ptr cert(SSL_get_peer_certificate(client));
        if (cert) {
            return certUser->getUser(cert.get());
        } else {
            result = "certificate not presented by client";
            status = ClientCertUser::Status::NotPresent;
        }
    }

    return std::make_pair(status, result);
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

void SslContext::drainBioRecvPipe(SOCKET sfd) {
    int n;
    bool stop = false;

    do {
        if (in.current < in.total) {
            n = BIO_write(network,
                          in.buffer.data() + in.current,
                          int(in.total - in.current));
            if (n > 0) {
                in.current += n;
                if (in.current == in.total) {
                    in.current = in.total = 0;
                }
            } else {
                /* Our input BIO is full, no need to grab more data from
                 * the network at this time..
                 */
                return;
            }
        }

        if (in.total < in.buffer.size()) {
            n = recv(sfd,
                     in.buffer.data() + in.total,
                     in.buffer.size() - in.total,
                     0);
            if (n > 0) {
                in.total += n;
                totalRecv += n;
            } else {
                stop = true;
                if (n == 0) {
                    error = true; /* read end shutdown */
                } else {
                    if (!is_blocking(GetLastNetworkError())) {
                        error = true;
                    }
                }
            }
        }
    } while (!stop);
}

void SslContext::drainBioSendPipe(SOCKET sfd) {
    int n;
    bool stop = false;

    do {
        if (out.current < out.total) {
            n = send(sfd,
                     out.buffer.data() + out.current,
                     out.total - out.current,
                     0);
            if (n > 0) {
                out.current += n;
                if (out.current == out.total) {
                    out.current = out.total = 0;
                }
                totalSend += n;
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

        if (out.total == 0) {
            n = BIO_read(network, out.buffer.data(), int(out.buffer.size()));
            if (n > 0) {
                out.total = n;
            } else {
                stop = true;
            }
        }
    } while (!stop);
}

void SslContext::dumpCipherList(uint32_t id) const {
    LOG_DEBUG(NULL, "%u: Using SSL ciphers:", id);
    int ii = 0;
    const char* cipher;
    while ((cipher = SSL_get_cipher_list(client, ii++)) != NULL) {
        LOG_DEBUG(NULL, "%u    %s", id, cipher);
    }
}

cJSON* SslContext::toJSON() const {
    cJSON* obj = cJSON_CreateObject();
    cJSON_AddBoolToObject(obj, "enabled", enabled);
    if (enabled) {
        cJSON_AddBoolToObject(obj, "connected", connected);
        cJSON_AddBoolToObject(obj, "error", error);
        cJSON_AddNumberToObject(obj, "total_recv", totalRecv);
        cJSON_AddNumberToObject(obj, "total_send", totalSend);
        cJSON_AddNumberToObject(obj, "input_buff_total", in.total);
        cJSON_AddNumberToObject(obj, "input_buff_current", in.current);
        cJSON_AddNumberToObject(obj, "output_buff_total", out.total);
        cJSON_AddNumberToObject(obj, "output_buff_current", out.current);
    }

    return obj;
}
