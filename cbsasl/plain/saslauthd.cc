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
#include "config.h"
#include "saslauthd.h"

#include <cstring>
#include <system_error>

#ifdef WIN32
#error "This file should not be included on windows"
#endif

Saslauthd::Saslauthd(const std::string& socketfile)
    : sock(-1) {

    struct sockaddr_un un = {0};
    un.sun_family = AF_UNIX;
    if (socketfile.size() + 1 >= sizeof(un.sun_path)) {
        throw std::runtime_error("Saslauthd::Saslauthd(): CBAUTH_SOCKPATH does"
                                     " not fit in sockaddr_un");
    }
    strcpy(un.sun_path, socketfile.c_str());

    if ((sock = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        throw std::system_error(errno, std::system_category(),
                                "Saslauthd::Saslauthd(): Failed to create "
                                    "socket");
    }

    if (connect(sock, reinterpret_cast<struct sockaddr*>(&un),
                socklen_t(sizeof(un))) == -1) {
        std::string msg{"Saslauthd::Saslauthd(): Failed to connect to "};
        msg.append(socketfile);
        throw std::system_error(errno, std::system_category(), msg.c_str());
    }
}

Saslauthd::~Saslauthd() {
    if (sock != -1) {
        // There is not much we can do if we fail to close the socket
        (void)close(sock);
    }
}

cbsasl_error_t Saslauthd::check(const std::string& username,
                                const std::string& passwd) {
    std::vector<uint8_t> request;
    encode_and_append_string(request, username);
    encode_and_append_string(request, passwd);
    encode_and_append_string(request, "couchbase");
    encode_and_append_string(request, "");

    sendRequest(request);

    const auto response = readResponse();
    if (response.find("OK") == 0) {
        return CBSASL_OK;
    } else if (response.find("NO") == 0) {
        return CBSASL_PWERR;
    } else {
        return CBSASL_FAIL;
    }
}

void Saslauthd::sendRequest(const std::vector<uint8_t>& data) {
    size_t offset = 0;
    do {
        auto bw = send(sock, data.data() + offset, data.size() - offset, 0);
        if (bw == -1) {
            throw std::system_error(errno, std::system_category(),
                                    "Saslauthd::send((): Failed to send "
                                        "data to saslauthd");
        }
        offset += size_t(bw);
    } while (offset < data.size());
}

void Saslauthd::encode_and_append_string(std::vector<uint8_t>& data,
                                         const std::string& msg) {
    auto offset = data.size();
    data.resize(offset + 2 + msg.size());
    short len = htons(static_cast<short>(msg.size()));
    memcpy(data.data() + offset, &len, 2);
    memcpy(data.data() + offset + 2, msg.data(), msg.size());
}

std::string Saslauthd::readResponse() {
    std::vector<uint8_t> response(2);
    fillBufferFromNetwork(response);
    uint16_t len;
    memcpy(&len, response.data(), sizeof(len));
    len = ntohs(len);
    response.resize(len);
    fillBufferFromNetwork(response);

    return std::string{reinterpret_cast<char*>(response.data()), len};
}

void Saslauthd::fillBufferFromNetwork(std::vector<uint8_t>& bytes) {
    size_t offset = 0;
    do {
        auto nr = recv(sock, bytes.data() + offset,
                       bytes.size() - offset, 0);
        if (nr == -1) {
            throw std::system_error(errno, std::system_category(),
                                    "Saslauthd::readResponse(): Failed to "
                                        "receive data from saslauthd");
        } else if (nr == 0) {
            throw std::underflow_error("Saslauthd::fillBufferFromNetwork(): The other"
                                           "end closed the connection");
        } else {
            offset += size_t(nr);
            if (offset == bytes.size()) {
                return;
            }
        }
    } while (true);
}
