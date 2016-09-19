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

#include <string>
#include <include/cbsasl/cbsasl.h>
#include <vector>

#ifdef WIN32
#error "This file should not be included on windows"
#endif

/**
 * This is a class that integrates to saslauthd. All communciation to
 * saslauthd is potentially very slow, so you should NOT be using this
 * in the frontend worker threads...
 *
 * Saslauthd speaks a really simple protocol:
 *
 * Client sends:
 * [uint16]username[uint16]passwd[uint16]service[uint16]realm
 *
 * Server responds with:
 * OK[potentially more data]
 * NO[potentially more data]
 * And the server close the connection
 */
class Saslauthd {
public:
    /**
     * Create a new instance of the Saslauth object.
     *
     * @param socketfile the name of the file in the filesystem to communicate
     *                   to the real saslauth daemon
     *
     * @throws
     *    std::runtime_error if the path to the socket file is too long
     *    std::system_error if there is a socket problem
     */
    Saslauthd(const std::string& socketfile);

    Saslauthd() = delete;

    Saslauthd(const Saslauthd&) = delete;

    ~Saslauthd();

    /**
     * Check the username and password combination.
     *
     * @param username
     * @param passwd
     * @return CBSASL_OK for success
     *         CBSASL_PWERR for failures
     *         CBSASL_FAIL for unexpected answers
     * @throws std::system_error for socket errors
     *         std::logic_error if saslauth replies with more than 1k data
     */
    cbsasl_error_t check(const std::string& username,
                         const std::string& passwd);

protected:
    /**
     * Encode the string the way saslauthd expects it (2 byte length in
     * network byte order followed by the string)
     *
     * @param data where to store the data
     * @param msg The string to append
     */
    void encode_and_append_string(std::vector<uint8_t>& data,
                                  const std::string& msg);

    /**
     * Send the (entire) request over the socket.
     *
     * @param data all of the data to send
     * @throws std::system_error if we fail to send data to the socket
     */
    void sendRequest(const std::vector<uint8_t>& data);

    /**
     * Read the response from the socket. saslauth will close the
     * connection when it is done transmitting the result. The format
     * for the response message is:
     *
     * [length]text
     *
     * length is two bytes in network byte order
     *
     * @return The entire message returned from saslauthd
     * @throws std::system_error if a socket error occurs
     */
    std::string readResponse();

    /**
     * Fill the provided buffer with data received from the network
     *
     * @param bytes the buffer to fill
     * @throws std::system_error if a socket error occurs
     *         std::runtime_error if the other end close the socket
     *
     */
    void fillBufferFromNetwork(std::vector<uint8_t>& bytes);
    /**
     * The socket used in the communication with saslauthd
     */
    int sock;
};
