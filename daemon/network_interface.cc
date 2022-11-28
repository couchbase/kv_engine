/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "network_interface.h"

#include <fmt/format.h>
#include <nlohmann/json.hpp>

static std::string proto2str(NetworkInterface::Protocol proto) {
    switch (proto) {
    case NetworkInterface::Protocol::Off:
        return "off";
    case NetworkInterface::Protocol::Optional:
        return "optional";
    case NetworkInterface::Protocol::Required:
        return "required";
    }
    throw std::invalid_argument("proto2str: Invalid protocol");
}

static NetworkInterface::Protocol str2proto(std::string_view value) {
    if (value == "required") {
        return NetworkInterface::Protocol::Required;
    }
    if (value == "optional") {
        return NetworkInterface::Protocol::Optional;
    }
    if (value == "off") {
        return NetworkInterface::Protocol::Off;
    }

    throw std::invalid_argument(
            fmt::format(R"(str2proto unrecognized string value "{}")", value));
}

void to_json(nlohmann::json& json, const NetworkInterface& iface) {
    json = {
            {"system", iface.system},
            {"tls", iface.tls},
            {"tag", iface.tag},
            {"host", iface.host},
            {"port", iface.port},
            {"ipv4", proto2str(iface.ipv4)},
            {"ipv6", proto2str(iface.ipv6)},
    };
}

void from_json(const nlohmann::json& json, NetworkInterface& iface) {
    iface.system = json.value("system", iface.system);
    iface.tls = json.value("tls", iface.tls);
    iface.tag = json.value("tag", iface.tag);
    iface.host = json.value("host", iface.host);
    iface.port = json.value("port", iface.port);
    iface.ipv4 = str2proto(json.value("ipv4", proto2str(iface.ipv4)));
    iface.ipv6 = str2proto(json.value("ipv6", proto2str(iface.ipv6)));
}
