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
#pragma once

#include "config.h"

#include "sslcert.h"

#include <cJSON.h>
#include <memcached/openssl.h>
#include <platform/pipe.h>
#include <cstdint>
#include <vector>

/**
 * The SslContext class is a holder class for all of the ssl-related
 * information used by the connection object.
 */
class SslContext {
public:
    ~SslContext();

    /**
     * Is ssl enabled for this connection or not?
     */
    bool isEnabled() const {
        return enabled;
    }

    /**
     * Is the client fully connected over ssl or not?
     */
    bool isConnected() const {
        return connected;
    }

    /**
     * Set the status of the connected flag
     */
    void setConnected() {
        connected = true;
    }

    /**
     * Is there an error on the SSL stream?
     */
    bool hasError() const {
        return error;
    }

    /**
     * Enable SSL for this connection.
     *
     * @param cert the certificate file to use
     * @param pkey the private key file to use
     * @return true if success, false if we failed to enable SSL
     */
    bool enable(const std::string& cert, const std::string& pkey);

    /**
     * Disable SSL for this connection
     */
    void disable();

    /**
     * Try to fill the SSL stream with as much data from the network
     * as possible.
     *
     * @param sfd the socket to read data from
     */
    void drainBioRecvPipe(SOCKET sfd);

    /**
     * Try to drain the SSL stream with as much data as possible and
     * send it over the network.
     *
     * @param sfd the socket to write data to
     */
    void drainBioSendPipe(SOCKET sfd);

    bool moreInputAvailable() const {
        return !inputPipe.empty();
    }

    bool morePendingOutput() const {
        return !outputPipe.empty();
    }

    /**
     * Dump the list of available ciphers to the log
     * @param id the connection id. Its only used in the
     *            log messages.
     */
    void dumpCipherList(uint32_t id) const;

    int accept();

    int getError(int errormask) const;

    int read(void* buf, int num);

    int write(const void* buf, int num);

    bool havePendingInputData();

    std::pair<ClientCertUser::Status, std::string> getCertUserName();
    /**
     * Get a JSON description of this object.. caller must call cJSON_Delete()
     */
    cJSON* toJSON() const;

protected:
    bool drainInputSocketBuf();

    bool enabled = false;
    bool connected = false;
    bool error = false;
    BIO* application = nullptr;
    BIO* network = nullptr;
    SSL_CTX* ctx = nullptr;
    SSL* client = nullptr;

    // The pipe used to buffer data between the socket and the SSL library
    // (data being read)
    cb::Pipe inputPipe;
    // The pipe used to buffer data between the SSL library and the socket
    // (data being written)
    cb::Pipe outputPipe;

    // Total number of bytes received on the network
    size_t totalRecv = 0;
    // Total number of bytes sent to the network
    size_t totalSend = 0;
};
