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
#include <string>
#include <utility>

/**
 * A class representing the properties used by Listening port.
 *
 * This class differs from the "interface" class that it represents
 * an actual port memcached have open. This class is used through
 * shared pointers and we create a new copy every time it change
 */
class ListeningPort {
public:
    ListeningPort(std::string tag,
                  std::string host,
                  in_port_t port,
                  sa_family_t family,
                  bool system,
                  std::string key,
                  std::string cert)
        : tag(std::move(tag)),
          host(std::move(host)),
          port(port),
          family(family),
          system(system),
          sslKey(std::move(key)),
          sslCert(std::move(cert)) {
    }

    /// The tag provided by the user to identify the port. It is possible
    /// to use ephemeral ports in the system, and if we want to change
    /// such ports at runtime the system needs a way to find the correct
    /// entry to change (the value of the tag _should_ be unique within the
    /// interface descriptions, but the server does not try to validate that
    /// it is unique (the behaviour is undefined if the same tag is used for
    /// multiple interfaces).
    const std::string tag;

    /// The hostname this port is bound to ("*" means all interfaces)
    const std::string host;

    /**
     * The actual port number being used by this connection. Please note
     * that you cannot configure the system to use the same port, but different
     * hostnames.
     */
    const in_port_t port;

    /// Is this AF_INET or AF_INET6
    const sa_family_t family;

    /// Is this an interface used for system traffic
    const bool system;

    // SSL related properties for the port. Both key and certificate must
    // be set

    /// The name of the file containing the SSL key
    const std::string sslKey;
    /// The name of the file containing the certificate
    const std::string sslCert;

    bool isSslPort() const {
        return !sslKey.empty() && !sslCert.empty();
    }

    /// Set to false once the interface is being shut down
    std::atomic_bool valid{true};
};
