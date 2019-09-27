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

#include "client_cert_config.h"

#include <memcached/openssl.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/pipe.h>
#include <platform/socket.h>
#include <cstdint>
#include <vector>

/**
 * The SslContext class is a holder class for all of the ssl-related
 * information used by the connection object.
 *
 * As described in the architecture documentation, memcached use a "small"
 * number of worker threads to server all of the clients. This means that
 * the working threads can't block trying to send and receive data by using
 * the "standard" BIO objects provided with OpenSSL.
 *
 * We _could_ have implemented our own BIO object which took care for this
 * for ourself, but instead we (read I) decided to just use the standard
 * BIO object and perform read/write operations from them into a socket
 * due to the fact that it looked easier at the time (with the short deadline
 * in order to add the support for OpenSSL).
 *
 * This gives us the following implementation:
 *
 * Call SSL_write with the data we want to send. OpenSSL tries to put as
 * much as possible of the data we want to send into it's send BIO and
 * when it is full it returns the number of bytes sent. At this time the
 * memcached core tries to call drainBioSendPipe which loops trying to
 * read the buffered data in the BIO, and tries to send it over the socket.
 * The function returns when the BIO is empty or the socket buffer is full
 * and memcached may try another SSL_write.
 *
 * The read path works the same way by reading from the socket before feeding
 * the data into the BIO before calling SSL_read.
 *
 * It would most likely be more efficient to refactor our code to implement
 * our own BIO object instead.
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
     * @return true if any progress was made
     */
    bool drainBioRecvPipe(SOCKET sfd);

    /**
     * Try to drain the SSL stream with as much data as possible and
     * send it over the network.
     *
     * @param sfd the socket to write data to
     * @return true if any progress was made
     */
    bool drainBioSendPipe(SOCKET sfd);

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

    /**
     * Accept the next connection.
     *
     * Check hasError() before looking at the return code (hasError is for
     * the underlying socket error)
     *
     * @param sfd The socket used for recv/send in the case we
     *            need more data
     * @return 1 success
     *         0 clean shutdown
     *        <0 an error
     */
    int accept(SOCKET sfd);

    int getError(int errormask) const;

    /**
     * Read from the SSL pipe
     *
     * Check hasError() before looking at the return code (hasError is for
     * the underlying socket error)
     *
     * @param sfd the underlying socket
     * @param buf where to store the data
     * @param num the amount of bytes to read
     * @return > 0 - the number of bytes read
     *         <= 0 - an error occurred. Check with getError
     */
    int read(SOCKET sfd, void* buf, int num);

    /**
     * Write data to the SSL pipe
     *
     * Check hasError() before looking at the return code (hasError is for
     * the underlying socket error)
     *
     * @param sfd The underlying socket
     * @param buf The data to send
     * @param num The number of bytes ot send
     * @return > 0 - The number of bytes sent
     *           0 - Connection closed
     *         < 0 - An error occurred. Check with getError
     */
    int write(SOCKET sfd, const void* buf, int num);

    bool havePendingInputData();

    std::pair<cb::x509::Status, std::string> getCertUserName();
    /**
     * Get a JSON description of this object
     */
    nlohmann::json toJSON() const;

    /// Get the name of the cipher in use
    const char* getCurrentCipherName() const;

protected:
    enum class DrainProgress { Error, Progress, NoProgress };

    /**
     * Try to drain the SSL output buffer and fill its input buffer
     *
     * @param sock the socket to use for transfer
     * @return true if no error occurred
     */
    DrainProgress drainBios(SOCKET sock);

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
