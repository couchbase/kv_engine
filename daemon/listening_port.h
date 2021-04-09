/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <platform/socket.h>
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

    ListeningPort(const ListeningPort& other) = default;

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
};
