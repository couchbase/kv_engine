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
#include "saslauthd_mock.h"

#include <system_error>
#include <platform/dirutils.h>

#ifdef WIN32
#error "This file should not be included on windows"
#endif

SaslauthdMock::SaslauthdMock()
    : sock(-1) {
    if ((sock = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        throw std::system_error(errno, std::system_category(),
                                "SaslauthdMock::SaslauthdMock(): Failed to "
                                    "create socket");
    }

    sockfile.assign("/tmp/saslauthdmock." + std::to_string(cb_getpid()));

    struct sockaddr_un un = {0};
    un.sun_family = AF_UNIX;
    strcpy(un.sun_path, sockfile.c_str());

    std::remove(sockfile.c_str());
    if (bind(sock, reinterpret_cast<struct sockaddr*>(&un), sizeof(un)) == -1) {
        std::string msg{"SaslauthdMock::SaslauthdMock(): Failed to "
                            "bind socket " + sockfile};
        throw std::system_error(errno, std::system_category(), msg);
    }

    if (listen(sock, 10) == -1) {
        throw std::system_error(errno, std::system_category(),
                                "SaslauthdMock::SaslauthdMock(): Failed to "
                                    "listen to socket");
    }
}

SaslauthdMock::~SaslauthdMock() {
    if (sock != -1) {
        close(sock);
    }
    if (!sockfile.empty()) {
        std::remove(sockfile.c_str());
    }
}

static void fill(int client, std::vector<uint8_t>& data) {
    size_t offset = 0;
    do {
        auto nr = ::recv(client, data.data() + offset,
                         data.size() - offset, 0);
        if (nr == -1) {
            throw std::system_error(errno, std::system_category(),
                                    "fill(): Failed to receive data from"
                                        " client");
        } else if (nr == 0) {
            throw std::system_error(errno, std::system_category(),
                                    "fill(): Failed to receive data from "
                                        "client (client hung up)");
        } else {
            offset += size_t(nr);
        }
    } while (offset < data.size());
}

static std::string readString(int client) {
    std::vector<uint8_t> data(2);
    fill(client, data);
    short len;
    memcpy(&len, data.data(), 2);
    len = ntohs(len);
    if (len == 0) {
        return std::string{""};
    }
    data.resize(len);
    fill(client, data);

    return std::string{reinterpret_cast<const char*>(data.data()), data.size()};
}

void SaslauthdMock::processOne() {
    struct sockaddr_storage addr;
    socklen_t addr_len = sizeof(addr);

    int client = accept(sock, reinterpret_cast<struct sockaddr*>(&addr),
                        &addr_len);

    if (client == -1) {
        throw std::system_error(errno, std::system_category(),
                                "SaslauthdMock::processOne(): Failed to "
                                    "accept client");
    }

    // the protocol is:
    // [ulen]user[plen]passwd[slen]service[rlen]realm
    std::string username = readString(client);
    std::string passwd = readString(client);
    std::string service = readString(client);
    std::string realm = readString(client);

    if (service != "couchbase") {
        std::string response{"NO invalid service"};
        send(client, response.data(), response.size(), 0);
        close(client);
    } else if (!realm.empty()) {
        std::string response{"NO unknown realm"};
        send(client, response.data(), response.size(), 0);
        close(client);
    } else if (username == "superman" && passwd == "<3LoisLane<3") {
        std::string response{"OK welcome \"_admin\""};
        send(client, response.data(), response.size(), 0);
        close(client);
    } else {
        std::string response{"NO I don't like you"};
        send(client, response.data(), response.size(), 0);
        close(client);
    }
}
