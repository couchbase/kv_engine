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
#pragma once

#include "config.h"

#include <cstdlib>
#include <vector>
#include <string>
#include <memcached/openssl.h>
#include <cJSON.h>

enum class Protocol : uint8_t {
    Memcached,
    Greenstack
};

/**
 * The Frame class is used to represent all of the data included in the
 * protocol unit going over the wire. For the memcached binary protocol
 * this is either the full request or response as defined in
 * memcached/protocol_binary.h, and for greenstack this is the greenstack
 * frame as defined in libreenstack/Frame.h
 */
class Frame {
public:
    void reset() {
        payload.resize(0);
    }
    std::vector<uint8_t> payload;
    typedef std::vector<uint8_t>::size_type size_type;
};

/**
 * The MemcachedConnection class is an abstract class representing a
 * connection to memcached. The concrete implementations of the class
 * implements the Memcached binary protocol and Greenstack.
 */
class MemcachedConnection {
public:
    MemcachedConnection() = delete;

    MemcachedConnection(const MemcachedConnection&) = delete;

    virtual ~MemcachedConnection();

    in_port_t getPort() const {
        return port;
    }

    sa_family_t getFamily() const {
        return family;
    }

    bool isSsl() const {
        return ssl;
    }

    const Protocol& getProtocol() const {
        return protocol;
    }

    /**
     * Perform a SASL authentication to memcached
     *
     * @param username the username to use in authentication
     * @param password the password to use in authentication
     * @param mech the SASL mech to use
     */
    virtual void authenticate(const std::string& username,
                              const std::string& password,
                              const std::string& mech) = 0;

    /**
     * Sent the given frame over this connection
     * @throws std::runtime_error if an error occurs
     */
    virtual void sendFrame(const Frame& frame);

    virtual void recvFrame(Frame& frame) = 0;

    void reconnect();

protected:
    MemcachedConnection(in_port_t port, sa_family_t family, bool ssl,
                        const Protocol& protocol);

    void close();
    void connect();

    /**
     * Extend the frame with the requested number of bytes
     * and read them off the network.
     *
     * @throws runtime_error if an error occurs
     */
    void read(Frame& frame, size_t bytes);

    in_port_t port;
    sa_family_t family;
    bool ssl;
    Protocol protocol;
    SSL_CTX* context;
    BIO* bio;
};

class MemcachedBinprotConnection : public MemcachedConnection {
public:
    MemcachedBinprotConnection(in_port_t port, sa_family_t family, bool ssl)
        : MemcachedConnection(port, family, ssl, Protocol::Memcached) { }


    virtual void authenticate(const std::string& username,
                              const std::string& password,
                              const std::string& mech);

    virtual void recvFrame(Frame& frame);
};

class MemcachedGreenstackConnection : public MemcachedConnection {
public:
    MemcachedGreenstackConnection(in_port_t port, sa_family_t family, bool ssl)
        : MemcachedConnection(port, family, ssl, Protocol::Greenstack) { }

    virtual void authenticate(const std::string& username,
                              const std::string& password,
                              const std::string& mech);

    virtual void recvFrame(Frame& frame);
};

class ConnectionMap {
public:
    /**
     * Initialize the connection map with connections matching the ports
     * opened from Memcached
     */
    void initialize(cJSON* ports);

    /**
     * Invalidate all of the connections
     */
    void invalidate();

    /**
     * Get a connection object matching the given attributes
     *
     * @param protocol The requested protocol (Greenstack / Memcached)
     * @param ssl If ssl should be enabled or not
     * @param family the network family (IPv4 / IPv6)
     * @param port (optional) The specific port number to use..
     * @return A connection object to use
     * @throws std::runtime_error if the request can't be served
     */
    MemcachedConnection& getConnection(const Protocol& protocol,
                                       bool ssl,
                                       sa_family_t family = AF_INET,
                                       in_port_t port = 0);

    /**
     * Do we have a connection matching the requested attributes
     */
    bool contains(const Protocol& protocol, bool ssl, sa_family_t family);

private:
    std::vector<MemcachedConnection*> connections;
};
