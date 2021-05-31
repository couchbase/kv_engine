/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "network_interface_description.h"

#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

class NetworkInterfaceDescriptionTest : public ::testing::Test {
public:
    NetworkInterfaceDescriptionTest()
        : spec({{"host", "*"},
                {"port", 11210U},
                {"family", "inet"},
                {"system", false},
                {"type", "mcbp"},
                {"tls", true},
                {"tag", "oh yeah"}}) {
    }

protected:
    nlohmann::json spec;

    void expectFail(const nlohmann::json& json) {
        EXPECT_THROW(NetworkInterfaceDescription descr(json),
                     std::invalid_argument)
                << json.dump();
    }

    void expectSuccess(const nlohmann::json& json) {
        try {
            NetworkInterfaceDescription descr(json);
        } catch (const std::exception& e) {
            FAIL() << e.what() << " " << json.dump();
        }
    }

    /**
     * Verify that we only accept strings
     *
     * @param tag The tag to check
     * @param legalValues If provided we should only allow these strings
     */
    void acceptString(const std::string& tag,
                      std::vector<std::string> legalValues = {}) {
        // Boolean values should not be accepted
        nlohmann::json json = spec;
        json[tag] = true;
        expectFail(json);

        json[tag] = false;
        expectFail(json);

        // Numbers should not be accepted
        json[tag] = 5;
        expectFail(json);

        json[tag] = 5.0;
        expectFail(json);

        // An array should not be accepted
        json[tag] = nlohmann::json::array();
        expectFail(json);

        // An object should not be accepted
        json[tag] = nlohmann::json::object();
        expectFail(json);

        // we should accept the provided values, but fail on others
        if (legalValues.empty()) {
            json[tag] = "this should be accepted";
            expectSuccess(json);
        } else {
            for (const auto& value : legalValues) {
                EXPECT_NE("this should not be accepted", value)
                        << "Can't use the text as a legal value";
                json[tag] = value;
                expectSuccess(json);
            }
            json[tag] = "this should not be accepted";
            expectFail(json);
        }
    }

    /// Verify that we only accept boolean values
    void acceptBoolean(const std::string& tag) {
        // String values should not be accepted
        nlohmann::json json = spec;
        json[tag] = "foo";
        expectFail(json);

        // Numbers should not be accepted
        json[tag] = 5;
        expectFail(json);

        json[tag] = 5.0;
        expectFail(json);

        // An array should not be accepted
        json[tag] = nlohmann::json::array();
        expectFail(json);

        // An object should not be accepted
        json[tag] = nlohmann::json::object();
        expectFail(json);
    }

    /// Verify that we only allow positive integers representing an in_port_t
    void acceptInPortT(const std::string& tag) {
        // Boolean values should not be accepted
        nlohmann::json json = spec;
        json[tag] = true;
        expectFail(json);

        json[tag] = false;
        expectFail(json);

        // String values should not be accepted
        json[tag] = "foo";
        expectFail(json);

        // An array should not be accepted
        json[tag] = nlohmann::json::array();
        expectFail(json);

        // An object should not be accepted
        json[tag] = nlohmann::json::object();
        expectFail(json);

        json[tag] = 0.5;
        expectFail(json);

        json[tag] = -1;
        expectFail(json);

        json[tag] = uint64_t(std::numeric_limits<in_port_t>::max()) + 1;
        expectFail(json);

        json[tag] = 0;
        expectSuccess(json);

        json[tag] = 1234;
        expectSuccess(json);
    }
};

/// Start off by testing that we can parse a legal configuration and
/// that it returns the correct values
TEST_F(NetworkInterfaceDescriptionTest, Legal) {
    NetworkInterfaceDescription descr(spec);
    EXPECT_EQ("*", descr.getHost());
    EXPECT_EQ(11210, descr.getPort());
    EXPECT_EQ(AF_INET, descr.getFamily());
    EXPECT_EQ("0.0.0.0", descr.getHostname());
    EXPECT_EQ("oh yeah", descr.getTag());
    EXPECT_EQ(NetworkInterfaceDescription::Type::Mcbp, descr.getType());
    EXPECT_FALSE(descr.isSystem());
    EXPECT_TRUE(descr.isTls());
    spec["family"] = "inet6";
    NetworkInterfaceDescription ipv6(spec);
    EXPECT_EQ(AF_INET6, ipv6.getFamily());
    EXPECT_EQ("::", ipv6.getHostname());
}
TEST_F(NetworkInterfaceDescriptionTest, Host) {
    acceptString("host");
    // Host is mandatory
    spec.erase(spec.find("host"));
    expectFail(spec);
}
TEST_F(NetworkInterfaceDescriptionTest, Port) {
    acceptInPortT("port");
    // port is mandatory
    spec.erase(spec.find("port"));
    expectFail(spec);
}
TEST_F(NetworkInterfaceDescriptionTest, Family) {
    acceptString("family", std::vector<std::string>{{"inet"}, {"inet6"}});
    // family is mandatory
    spec.erase(spec.find("family"));
    expectFail(spec);
}
TEST_F(NetworkInterfaceDescriptionTest, System) {
    acceptBoolean("system");
    // system is optional and defaults to off
    spec.erase(spec.find("system"));
    expectSuccess(spec);
    NetworkInterfaceDescription d(spec);
    EXPECT_FALSE(d.isSystem());
}
TEST_F(NetworkInterfaceDescriptionTest, Type) {
    acceptString("type", std::vector<std::string>{{"mcbp"}, {"prometheus"}});
    // type is mandatory
    spec.erase(spec.find("type"));
    expectFail(spec);
}
TEST_F(NetworkInterfaceDescriptionTest, Tls) {
    acceptBoolean("tls");
    // tls is optional and defaults to off
    spec.erase(spec.find("tls"));
    expectSuccess(spec);
    NetworkInterfaceDescription d(spec);
    EXPECT_FALSE(d.isTls());
}
TEST_F(NetworkInterfaceDescriptionTest, Tag) {
    acceptString("tag");
    // tag is optional
    spec.erase(spec.find("tag"));
    expectSuccess(spec);
}
TEST_F(NetworkInterfaceDescriptionTest, UnknownKeys) {
    spec["unknown"] = "foo";
    expectFail(spec);
}
