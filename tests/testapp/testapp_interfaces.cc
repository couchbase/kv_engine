/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp.h"
#include "testapp_client_test.h"
#include <platform/base64.h>

/*
 * This test batch verifies that the interface array in the server may
 * be dynamically changed.
 */
class InterfacesTest : public TestappClientTest {
public:
    /// Simulate the the server created the bootstrap interfaces
    static void SetUpTestCase() {
        token = 0xdeadbeef;
        memcached_cfg = generate_config();
        start_memcached_server();
        if (HasFailure()) {
            std::cerr << "Error in InterfacesTest::SetUpTestCase, terminating "
                         "process"
                      << std::endl;

            exit(EXIT_FAILURE);
        } else {
            CreateTestBucket();
        }
    }

protected:
    void TearDown() override {
        remove(memcached_cfg["portnumber_file"].get<std::string>().c_str());
    }

    nlohmann::json getInterfaces(MemcachedConnection& c) {
        const auto cmd =
                BinprotGenericCommand{cb::mcbp::ClientOpcode::Ifconfig, "list"};
        auto rsp = c.execute(cmd);
        if (!rsp.isSuccess()) {
            throw ConnectionError("Failed to list interfaces", rsp);
        }
        auto json = rsp.getDataJson();
        if (!json.is_array()) {
            throw std::runtime_error(
                    "getInterfaces(): The returned object should be an array");
        }
        return json;
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         InterfacesTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

/// Test that all of the operations fail for the "normal" user
TEST_P(InterfacesTest, NoAccessTest) {
    auto& conn = getConnection();
    try {
        getInterfaces(conn);
        FAIL() << "Normal users should not be able to get interface list";
    } catch (const ConnectionError& e) {
        ASSERT_TRUE(e.isAccessDenied());
    }

    // We should not be able to define an interface
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Ifconfig);
    cmd.setKey("define");
    nlohmann::json descr = {{"host", "127.0.0.1"},
                            {"port", 0},
                            {"family", "inet"},
                            {"system", false},
                            {"type", "mcbp"},
                            {"tag", "fail"}};
    cmd.setValue(descr.dump());
    auto rsp = conn.execute(cmd);
    ASSERT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus())
            << "Normal users should not be able to define an interface";

    // And we should not be able to delete one
    rsp = conn.execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Ifconfig,
                                  "delete",
                                  "b1ba0893-930c-450a-a1a0-45ce88e25611"});
    ASSERT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus())
            << "Normal users should not be able to delete an interface";
}

/// Test that we can use ifconfig to list all of the defined interfaces
TEST_P(InterfacesTest, ListInterfaces) {
    auto& conn = getAdminConnection();
    auto json = getInterfaces(conn);
    auto [ipv4, ipv6] = cb::net::getIpAddresses(false);
    int total = 2; // prometheus and the ipv4 interface
    (void)ipv4;
    if (!ipv6.empty()) {
        ++total;
    }

    // We should have the bootstrap interface and the prometheus one
    ASSERT_EQ(total, json.size()) << json.dump(2);
    ASSERT_EQ("mcbp", json.front()["type"]);
    if (total == 3) {
        ASSERT_EQ("mcbp", json[1]["type"]);
    }
    ASSERT_EQ("prometheus", json.back()["type"]);
}

TEST_P(InterfacesTest, Prometheus) {
    // We should only allow a single one. Defining another one should fail
    auto& conn = getAdminConnection();
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Ifconfig);
    cmd.setKey("define");
    nlohmann::json descr = {{"host", "127.0.0.1"},
                            {"port", 0},
                            {"family", "inet"},
                            {"type", "prometheus"}};
    cmd.setValue(descr.dump());
    auto rsp = conn.execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());

    auto interfaces = getInterfaces(conn);
    std::string uuid = interfaces.back()["uuid"];
    ASSERT_FALSE(uuid.empty()) << "Failed to locate the uuid for prometheus";
    rsp = conn.execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Ifconfig, "delete", uuid});
    ASSERT_TRUE(rsp.isSuccess()) << "Should be allowed to delete prometheus";

    // But now it should be gone and we shouldnt be able to delete it again
    rsp = conn.execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Ifconfig, "delete", uuid});
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus())
            << "The interface should be deleted";
    // And no longer part of list interfaces
    interfaces = getInterfaces(conn);
    auto [ipv4, ipv6] = cb::net::getIpAddresses(false);
    int total = 1;
    (void)ipv4;
    if (!ipv6.empty()) {
        ++total;
    }

    ASSERT_EQ(total, interfaces.size());
    ASSERT_EQ("mcbp", interfaces.back()["type"]);

    rsp = conn.execute(cmd);
    EXPECT_TRUE(rsp.isSuccess());
    const auto interf = rsp.getDataJson();
    EXPECT_TRUE(interf["errors"].is_array());
    EXPECT_TRUE(interf["errors"].empty());
    EXPECT_TRUE(interf["ports"].is_array());
    EXPECT_EQ(1, interf["ports"].size());
    const auto& config = interf["ports"][0];
    EXPECT_EQ("inet", config["family"]);
    EXPECT_EQ("127.0.0.1", config["host"]);
    EXPECT_TRUE(config["port"].is_number());
    EXPECT_LT(0, config["port"]);
    EXPECT_EQ("prometheus", config["type"]);
    EXPECT_NE(uuid, config["uuid"]);
    uuid = config["uuid"];

    interfaces = getInterfaces(conn);
    ASSERT_EQ(uuid, interfaces.back()["uuid"])
            << "list interfaces should return the newly created interface";
}

TEST_P(InterfacesTest, Mcbp) {
    auto& conn = getAdminConnection();

    // I should not be able to connect to the same host/port I'm already
    // connected to
    auto interfaces = getInterfaces(conn);
    ASSERT_EQ("mcbp", interfaces.front()["type"]);
    ASSERT_EQ("127.0.0.1", interfaces.front()["host"]);
    ASSERT_NE(0, interfaces.front()["port"]);

    nlohmann::json descr = {{"host", "127.0.0.1"},
                            {"port", interfaces.front()["port"]},
                            {"family", "inet"},
                            {"system", false},
                            {"type", "mcbp"}};

    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Ifconfig);
    cmd.setKey("define");
    cmd.setValue(descr.dump());
    auto rsp = conn.execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());

    // But I should be allowed to bind to the ANY interface on the same
    // port
    descr["host"] = "0.0.0.0";
    cmd.setValue(descr.dump());
    rsp = conn.execute(cmd);
    ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                 << rsp.getDataString();
    auto json = rsp.getDataJson();
    auto uuid = json["ports"][0]["uuid"].get<std::string>();
    descr["port"] = json["ports"][0]["port"];
    cmd.setValue(descr.dump());
    bool found = false;

    auto findUuid = [&uuid, &found](MemcachedConnection& conn) {
        auto rsp = conn.execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::Ifconfig, "list"});
        ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                     << rsp.getDataString();
        ASSERT_FALSE(rsp.getDataString().empty());
        auto json = rsp.getDataJson();
        ASSERT_TRUE(json.is_array());
        found = false;
        for (auto& e : json) {
            if (e["uuid"].get<std::string>() == uuid) {
                found = true;
                break;
            }
        }
    };

    // It should now be part of the list command
    findUuid(conn);
    ASSERT_TRUE(found) << "Did not find the interface with uuid: " << uuid
                       << std::endl
                       << json.dump(2);

    // It should not be possible to define it again
    rsp = conn.execute(cmd);
    ASSERT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());

    // And not if I try to use the wild hard either
    descr["host"] = "*";
    cmd.setValue(descr.dump());
    rsp = conn.execute(cmd);
    ASSERT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());

    // but we should be allowed to bind explicitly to all of the other
    // IP addresses
    auto [ipv4, ipv6] = cb::net::getIpAddresses(true);
    (void)ipv6;
    ASSERT_FALSE(ipv4.empty());
    for (auto host : ipv4) {
        descr["host"] = host;
        cmd.setValue(descr.dump());
        rsp = conn.execute(cmd);
        ASSERT_TRUE(rsp.isSuccess());
    }

    interfaces = getInterfaces(conn);
    for (auto interface : interfaces) {
        if (interface["type"] == "mcbp" && interface["tag"] != "bootstrap") {
            rsp = conn.execute(
                    BinprotGenericCommand{cb::mcbp::ClientOpcode::Ifconfig,
                                          "delete",
                                          interface["uuid"]});
            ASSERT_TRUE(rsp.isSuccess())
                    << to_string(rsp.getStatus()) << std::endl
                    << rsp.getDataString();
        }
    }

    // Test if we cannot resolve the hostname
    descr["host"] = "This-name-should-not-resolve";
    cmd.setValue(descr.dump());
    rsp = conn.execute(cmd);
    ASSERT_EQ(cb::mcbp::Status::Einternal, rsp.getStatus())
            << rsp.getDataString();

    // We should be back to how it looked initially
    InterfacesTest_ListInterfaces_Test();
}

TEST_P(InterfacesTest, TlsProperties) {
    auto& conn = getAdminConnection();

    nlohmann::json tls_properties = {
            {"private key", SOURCE_ROOT "/tests/cert/testapp.pem"},
            {"certificate chain", SOURCE_ROOT "/tests/cert/testapp.cert"},
            {"minimum version", "TLS 1"},
            {"cipher list",
             {{"TLS 1.2", "HIGH"},
              {"TLS 1.3",
               "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_"
               "AES_"
               "128_GCM_SHA256:TLS_AES_128_CCM_8_SHA256:TLS_AES_128_CCM_"
               "SHA256"}}},
            {"cipher order", true},
            {"client cert auth", "disabled"}};
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Ifconfig);
    cmd.setKey("tls");
    cmd.setValue(tls_properties.dump());
    auto rsp = conn.execute(cmd);
    ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                 << rsp.getDataString();
}

TEST_P(InterfacesTest, TlsPropertiesEncryptedCert) {
    auto& conn = getAdminConnection();

    nlohmann::json tls_properties = {
            {"private key", SOURCE_ROOT "/tests/cert/encrypted-testapp.pem"},
            {"certificate chain", SOURCE_ROOT "/tests/cert/testapp.cert"},
            {"minimum version", "TLS 1"},
            {"cipher list",
             {{"TLS 1.2", "HIGH"},
              {"TLS 1.3",
               "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_"
               "AES_"
               "128_GCM_SHA256:TLS_AES_128_CCM_8_SHA256:TLS_AES_128_CCM_"
               "SHA256"}}},
            {"cipher order", true},
            {"client cert auth", "disabled"},
            {"password", cb::base64::encode("This is the passphrase", false)}};

    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Ifconfig);
    cmd.setKey("tls");
    cmd.setValue(tls_properties.dump());

    const auto rsp = conn.execute(cmd);
    ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                 << rsp.getDataString();
}
