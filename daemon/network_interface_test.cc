/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "network_interface.h"
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

/// Allow an "empty" JSON blob -> all default values
TEST(NetworkInterface, AllDefault) {
    auto iface = nlohmann::json::parse("{}").get<NetworkInterface>();
    EXPECT_TRUE(iface.tag.empty());
    EXPECT_TRUE(iface.host.empty());
    EXPECT_FALSE(iface.tls);
    EXPECT_EQ(11211, iface.port);
    EXPECT_EQ(NetworkInterface::Protocol::Optional, iface.ipv6);
    EXPECT_EQ(NetworkInterface::Protocol::Optional, iface.ipv4);
    EXPECT_FALSE(iface.system);
}

/// Test that we can dump to JSON, and it should be the same
/// when we parse it back
TEST(NetworkInterface, ToFromJson) {
    NetworkInterface iface;
    iface.host = "*";
    iface.port = 666;
    iface.tag = "tag";
    iface.tls = true;
    iface.system = true;
    iface.ipv4 = NetworkInterface::Protocol::Required;
    iface.ipv6 = NetworkInterface::Protocol::Off;

    const auto json = nlohmann::json(iface);
    EXPECT_EQ("*", json["host"].get<std::string>());
    EXPECT_EQ(666, json["port"].get<in_port_t>());
    EXPECT_EQ("tag", json["tag"].get<std::string>());
    EXPECT_EQ(true, json["tls"].get<bool>());
    EXPECT_EQ(true, json["system"].get<bool>());
    EXPECT_EQ("required", json["ipv4"].get<std::string>());
    EXPECT_EQ("off", json["ipv6"].get<std::string>());

    auto parsed = json.get<NetworkInterface>();
    EXPECT_EQ(iface, parsed);
}