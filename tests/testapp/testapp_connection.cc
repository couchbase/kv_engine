/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "testapp_connection.h"

#include "testapp_greenstack_connection.h"
#include "testapp_mcbp_connection.h"
#include "testapp_binprot.h"

#include <cbsasl/cbsasl.h>
#include <extensions/protocol/testapp_extension.h>
#include <gtest/gtest.h>
#include <iostream>
#include <libgreenstack/Greenstack.h>
#include <memcached/protocol_binary.h>
#include <platform/strerror.h>
#include <sstream>
#include <stdexcept>
#include <string>

/////////////////////////////////////////////////////////////////////////
// Implementation of the ConnectionMap class
/////////////////////////////////////////////////////////////////////////
MemcachedConnection& ConnectionMap::getConnection(const Protocol& protocol,
                                                  bool ssl,
                                                  sa_family_t family,
                                                  in_port_t port) {
    for (auto* conn : connections) {
        if (conn->getProtocol() == protocol && conn->isSsl() == ssl &&
            conn->getFamily() == family &&
            (port == 0 || conn->getPort() == port)) {
            return *conn;
        }
    }

    throw std::runtime_error("No connection matching the request");
}

bool ConnectionMap::contains(const Protocol& protocol, bool ssl,
                             sa_family_t family) {
    try {
        (void)getConnection(protocol, ssl, family, 0);
        return true;
    } catch (std::runtime_error) {
        return false;
    }
}

void ConnectionMap::initialize(cJSON* ports) {
    invalidate();
    cJSON* array = cJSON_GetObjectItem(ports, "ports");
    if (array == nullptr) {
        char* json = cJSON_PrintUnformatted(ports);
        std::string msg("ports not found in portnumber file: ");
        msg.append(json);
        cJSON_Free(json);
        throw std::runtime_error(msg);
    }

    auto numEntries = cJSON_GetArraySize(array);
    sa_family_t family;
    for (int ii = 0; ii < numEntries; ++ii) {
        auto obj = cJSON_GetArrayItem(array, ii);
        auto fam = cJSON_GetObjectItem(obj, "family");
        if (strcmp(fam->valuestring, "AF_INET") == 0) {
            family = AF_INET;
        } else if (strcmp(fam->valuestring, "AF_INET6") == 0) {
            family = AF_INET6;
        } else {
            char* json = cJSON_PrintUnformatted(obj);
            std::string msg("Unsupported network family: ");
            msg.append(json);
            cJSON_Free(json);
            throw std::runtime_error(msg);
        }

        auto ssl = cJSON_GetObjectItem(obj, "ssl");
        if (ssl == nullptr) {
            char* json = cJSON_PrintUnformatted(obj);
            std::string msg("ssl missing for entry: ");
            msg.append(json);
            cJSON_Free(json);
            throw std::runtime_error(msg);
        }

        auto port = cJSON_GetObjectItem(obj, "port");
        if (port == nullptr) {
            char* json = cJSON_PrintUnformatted(obj);
            std::string msg("port missing for entry: ");
            msg.append(json);
            cJSON_Free(json);
            throw std::runtime_error(msg);
        }

        auto protocol = cJSON_GetObjectItem(obj, "protocol");
        if (protocol == nullptr) {
            char* json = cJSON_PrintUnformatted(obj);
            std::string msg("protocol missing for entry: ");
            msg.append(json);
            cJSON_Free(json);
            throw std::runtime_error(msg);
        }

        auto portval = static_cast<in_port_t>(port->valueint);
        bool useSsl = ssl->type == cJSON_True ? true : false;

#ifdef WIN32
        if (useSsl) {
            // Looks like OpenSSL isn't fully safe for windows when
            // it comes to the BIO_set_fd (the socket is a 32 bit integer
            // on unix, but a SOCKET on windows is a HANDLE and could
            // overflow.. Just disable the ssl code for now as it isn't
            // being used yet (will be used as part of the Greenstack
            // tests)
            continue;
        }
#endif

        MemcachedConnection* connection;
        if (strcmp(protocol->valuestring, "greenstack") == 0) {
            // Enable when we get greenstack support
            connection = new MemcachedGreenstackConnection(portval,
                                                           family,
                                                           useSsl);
        } else {
            connection = new MemcachedBinprotConnection(portval, family,
                                                        useSsl);
        }
        connections.push_back(connection);
    }
}

void ConnectionMap::invalidate() {
    for (auto c : connections) {
        delete c;
    }
    connections.resize(0);
}

/////////////////////////////////////////////////////////////////////////
// Implementation of the MemcachedConnection class
/////////////////////////////////////////////////////////////////////////
MemcachedConnection::MemcachedConnection(in_port_t port, sa_family_t family,
                                         bool ssl, const Protocol& protocol)
    : port(port),
      family(family),
      ssl(ssl),
      protocol(protocol),
      context(nullptr),
      bio(nullptr),
      sock(INVALID_SOCKET),
      synchronous(false) {
    connect();
}

MemcachedConnection::~MemcachedConnection() {
    close();
}

void MemcachedConnection::reconnect() {
    close();
    connect();
}

void MemcachedConnection::close() {
    if (ssl) {
        // the socket is closed by the underlying BIO stuctures
        if (bio != nullptr) {
            BIO_free_all(bio);
            bio = nullptr;
        }
        if (context != nullptr) {
            SSL_CTX_free(context);
            context = nullptr;
        }
    } else {
        if (sock != INVALID_SOCKET) {
            ::closesocket(sock);
            sock = INVALID_SOCKET;
        }
    }
}

SOCKET new_socket(in_port_t port, sa_family_t family) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = family;

    int error;
    struct addrinfo* ai;
    if (family == AF_INET) {
        error = getaddrinfo("127.0.0.1", std::to_string(port).c_str(), &hints,
                            &ai);
    } else {
        error = getaddrinfo("::1", std::to_string(port).c_str(), &hints, &ai);
    }

    if (error != 0) {
        std::string msg("Failed to resolve address");
        msg.append(std::to_string(error));
        throw std::runtime_error(msg);
    }

    for (struct addrinfo* next = ai; next; next = next->ai_next) {
        SOCKET sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (sfd != INVALID_SOCKET) {
            if (connect(sfd, ai->ai_addr, ai->ai_addrlen) != SOCKET_ERROR) {
                freeaddrinfo(ai);
                return sfd;
            }
            closesocket(sfd);
        }
    }

    freeaddrinfo(ai);
    return INVALID_SOCKET;
}

void MemcachedConnection::connect() {
    sock = new_socket(port, family);
    if (sock == INVALID_SOCKET) {
        std::string msg("Failed to connect to: ");
        if (family == AF_INET) {
            msg.append("127.0.0.1:");
        } else {
            msg.append("[::1]:");
        }
        msg.append(std::to_string(port));
        throw std::runtime_error(msg);
    }

    /* we're connected */
    if (ssl) {
        if ((context = SSL_CTX_new(SSLv23_client_method())) == NULL) {
            BIO_free_all(bio);
            throw std::runtime_error("Failed to create openssl client contex");
        }

        /* Ensure read/write operations only return after the
         * handshake and successful completion.
         */
        SSL_CTX_set_mode(context, SSL_MODE_AUTO_RETRY);

        bio = BIO_new_ssl(context, 1);
        BIO_push(bio, BIO_new_socket(sock, 0));

        if (BIO_do_handshake(bio) <= 0) {
            BIO_free_all(bio);
            SSL_CTX_free(context);
            bio = nullptr;
            context = nullptr;
            throw std::runtime_error("Failed to do SSL handshake!");
        }
    }
}

void MemcachedConnection::sendFrameSsl(const Frame& frame) {
    const char* data = reinterpret_cast<const char*>(frame.payload.data());
    Frame::size_type nbytes = frame.payload.size();
    Frame::size_type offset = 0;

    while (offset < nbytes) {
        int nw = BIO_write(bio, data + offset, nbytes - offset);
        if (nw < 0) {
            if (BIO_should_retry(bio) == 0) {
                throw std::runtime_error("Failed to write data");
            }
        } else {
            offset += nw;
        }
    }
}

void MemcachedConnection::sendFramePlain(const Frame& frame) {
    const char* data = reinterpret_cast<const char*>(frame.payload.data());
    Frame::size_type nbytes = frame.payload.size();
    Frame::size_type offset = 0;

    while (offset < nbytes) {
        auto nw = send(sock, data + offset, nbytes - offset, 0);
        if (nw < 0) {
            std::string msg("Failed to send data: ");
            msg.append(cb_strerror());
            throw std::runtime_error(msg);
        } else {
            offset += nw;
        }
    }
}

void MemcachedConnection::readSsl(Frame& frame, size_t bytes) {
    Frame::size_type offset = frame.payload.size();
    frame.payload.resize(bytes + offset);
    char* data = reinterpret_cast<char*>(frame.payload.data()) + offset;

    size_t total = 0;

    while (total < bytes) {
        int nr = BIO_read(bio, data + total, bytes - total);
        if (nr < 0) {
            if (BIO_should_retry(bio) == 0) {
                throw std::runtime_error("Failed to read data");
            }
        } else {
            total += nr;
        }
    }
}

void MemcachedConnection::readPlain(Frame& frame, size_t bytes) {
    Frame::size_type offset = frame.payload.size();
    frame.payload.resize(bytes + offset);
    char* data = reinterpret_cast<char*>(frame.payload.data()) + offset;

    size_t total = 0;

    while (total < bytes) {
        auto nr = recv(sock, data + total, bytes - total, 0);
        if (nr < 0) {
            std::string msg("Failed to read data: ");
            msg.append(cb_strerror());
            throw std::runtime_error(msg);
        } else {
            total += nr;
        }
    }
}

void MemcachedConnection::sendFrame(const Frame& frame) {
    if (ssl) {
        sendFrameSsl(frame);
    } else {
        sendFramePlain(frame);
    }
}

void MemcachedConnection::read(Frame& frame, size_t bytes) {
    if (ssl) {
        readSsl(frame, bytes);
    } else {
        readPlain(frame, bytes);
    }
}
