/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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

#include <platform/socket.h>
#include <atomic>
#include <memory>
#include <string>
#include <utility>

/**
 * A class representing the properties used by Listening port.
 *
 * This class differs from the "interface" class that it represents
 * an actual port memcached have open. It contains some dynamic and
 * some fixed properties
 */
class ListeningPort {
public:
    ListeningPort(in_port_t port, std::string host, bool tcp_nodelay);

    /**
     * The actual port number being used by this connection. Please note
     * that you cannot configure the system to use the same port, but different
     * hostnames.
     */
    const in_port_t port;

    /** The current number of connections connected to this port */
    int curr_conns;

    /** The maximum number of connections allowed for this port */
    int maxconns;

    /** The hostname this port is bound to ("*" means all interfaces) */
    const std::string host;

    /** Is IPv6 enabled for this port */
    bool ipv6;
    /** Is IPv4 enabled for this port */
    bool ipv4;
    /** Should TCP_NODELAY be enabled or not */
    bool tcp_nodelay;

    /// SSL related properties for the port
    struct Ssl {
        Ssl(std::string key, std::string cert)
            : key(std::move(key)), cert(std::move(cert)) {
        }
        /// The name of the file containing the SSL key
        std::string key;
        /// The name of the file containing the certificate
        std::string cert;
    };

    /// Get the SSL settings for the port, or empty if SSL isn't configured
    std::shared_ptr<Ssl> getSslSettings() const;

    /// Set the SSL settings for the port. Both key and cert must be
    /// set to a non-empty value otherwise SSL will be disabled.
    void setSslSettings(const std::string& key, const std::string& cert);

protected:
    /// The shared pointer used to configure SSL to allow for dynamic
    /// reconfiguration of SSL parameters (as part of node-to-node encryption
    /// we may want to lock down all ports to be over SSL)
    std::shared_ptr<Ssl> ssl;
};
