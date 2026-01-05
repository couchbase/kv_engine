/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <nlohmann/json_fwd.hpp>
#include <platform/socket.h>

/**
 * The NetworkInterface class is an in-memory representation of the
 * attributes one may specify for a network interface.
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

    bool operator==(const NetworkInterface&) const = default;

    std::string tag;
    std::string host;
    in_port_t port = 11211;
    Protocol ipv6 = Protocol::Optional;
    Protocol ipv4 = Protocol::Optional;
    bool tls = false;
    bool system = false;
};

void to_json(nlohmann::json& json, const NetworkInterface& iface);
void from_json(const nlohmann::json& json, NetworkInterface& iface);
