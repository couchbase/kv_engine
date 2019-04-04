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

#include <nlohmann/json_fwd.hpp>
#include <platform/socket.h>

/**
 * The NetworkInterface class is an in-memory representation of the
 * attributes one may specify for a network interface. See
 * docs/memcached.json.adoc for a description of the various properties
 */
class NetworkInterface {
public:
    /// Should a protocol be enabled, and if so how?
    enum class Protocol {
        Off, /// Do not enable protocol.
        Optional, /// Protocol should be enabled, failure is non-fatal.
        Required, /// Protocol must be enabled; failure is fatal.
    };

    NetworkInterface() = default;
    explicit NetworkInterface(const nlohmann::json& json);

    bool operator==(const NetworkInterface& o) const {
        return host == o.host && ssl.key == o.ssl.key &&
               ssl.cert == o.ssl.cert && port == o.port && ipv6 == o.ipv6 &&
               ipv4 == o.ipv4 && tag == o.tag;
    }

    bool operator!=(const NetworkInterface& o) const {
        return !(*this == o);
    }

    std::string tag;
    std::string host;
    struct {
        std::string key;
        std::string cert;
    } ssl;
    in_port_t port = 11211;
    Protocol ipv6 = Protocol::Optional;
    Protocol ipv4 = Protocol::Optional;
};

std::string to_string(const NetworkInterface::Protocol& proto);
