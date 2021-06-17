/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "json_validator_test.h"
#include "network_interface_description.h"

class NetworkInterfaceDescriptionTest : public JsonValidatorTest {
public:
    NetworkInterfaceDescriptionTest()
        : JsonValidatorTest({{"host", "*"},
                             {"port", 11210U},
                             {"family", "inet"},
                             {"system", false},
                             {"type", "mcbp"},
                             {"tls", true},
                             {"tag", "oh yeah"}}) {
    }

protected:
    void expectFail(const nlohmann::json& json) override {
        EXPECT_THROW(NetworkInterfaceDescription descr(json),
                     std::invalid_argument)
                << json.dump();
    }

    void expectSuccess(const nlohmann::json& json) override {
        try {
            NetworkInterfaceDescription descr(json);
        } catch (const std::exception& e) {
            FAIL() << e.what() << " " << json.dump();
        }
    }
};

/// Start off by testing that we can parse a legal configuration and
/// that it returns the correct values
TEST_F(NetworkInterfaceDescriptionTest, Legal) {
    NetworkInterfaceDescription descr(legalSpec);
    EXPECT_EQ("*", descr.getHost());
    EXPECT_EQ(11210, descr.getPort());
    EXPECT_EQ(AF_INET, descr.getFamily());
    EXPECT_EQ("0.0.0.0", descr.getHostname());
    EXPECT_EQ("oh yeah", descr.getTag());
    EXPECT_EQ(NetworkInterfaceDescription::Type::Mcbp, descr.getType());
    EXPECT_FALSE(descr.isSystem());
    EXPECT_TRUE(descr.isTls());
    legalSpec["family"] = "inet6";
    NetworkInterfaceDescription ipv6(legalSpec);
    EXPECT_EQ(AF_INET6, ipv6.getFamily());
    EXPECT_EQ("::", ipv6.getHostname());
}
TEST_F(NetworkInterfaceDescriptionTest, Host) {
    acceptString("host");
    // Host is mandatory
    legalSpec.erase(legalSpec.find("host"));
    expectFail(legalSpec);
}
TEST_F(NetworkInterfaceDescriptionTest, Port) {
    acceptIntegers("port", 0, std::numeric_limits<in_port_t>::max());
    // port is mandatory
    legalSpec.erase(legalSpec.find("port"));
    expectFail(legalSpec);
}
TEST_F(NetworkInterfaceDescriptionTest, Family) {
    acceptString("family", std::vector<std::string>{{"inet"}, {"inet6"}});
    // family is mandatory
    legalSpec.erase(legalSpec.find("family"));
    expectFail(legalSpec);
}
TEST_F(NetworkInterfaceDescriptionTest, System) {
    acceptBoolean("system");
    // system is optional and defaults to off
    legalSpec.erase(legalSpec.find("system"));
    expectSuccess(legalSpec);
    NetworkInterfaceDescription d(legalSpec);
    EXPECT_FALSE(d.isSystem());
}
TEST_F(NetworkInterfaceDescriptionTest, Type) {
    acceptString("type", std::vector<std::string>{{"mcbp"}, {"prometheus"}});
    // type is mandatory
    legalSpec.erase(legalSpec.find("type"));
    expectFail(legalSpec);
}
TEST_F(NetworkInterfaceDescriptionTest, Tls) {
    acceptBoolean("tls");
    // tls is optional and defaults to off
    legalSpec.erase(legalSpec.find("tls"));
    expectSuccess(legalSpec);
    NetworkInterfaceDescription d(legalSpec);
    EXPECT_FALSE(d.isTls());
}
TEST_F(NetworkInterfaceDescriptionTest, Tag) {
    acceptString("tag");
    // tag is optional
    legalSpec.erase(legalSpec.find("tag"));
    expectSuccess(legalSpec);
}
TEST_F(NetworkInterfaceDescriptionTest, UnknownKeys) {
    legalSpec["unknown"] = "foo";
    expectFail(legalSpec);
}
