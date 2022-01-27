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
#include <platform/dirutils.h>

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
        }
        rebuildAdminConnection();
        CreateTestBucket();
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

    BinprotResponse reconfigure(const nlohmann::json& config) {
        write_config_to_file(config.dump());
        return adminConnection->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::ConfigReload, {}, {}});
    }

    void test_mb47707(bool whitelist_localhost_interface);
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
    auto json = getInterfaces(*adminConnection);
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
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Ifconfig);
    cmd.setKey("define");
    nlohmann::json descr = {{"host", "127.0.0.1"},
                            {"port", 0},
                            {"family", "inet"},
                            {"type", "prometheus"}};
    cmd.setValue(descr.dump());
    auto rsp = adminConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());

    auto interfaces = getInterfaces(*adminConnection);
    std::string uuid = interfaces.back()["uuid"];
    ASSERT_FALSE(uuid.empty()) << "Failed to locate the uuid for prometheus";
    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Ifconfig, "delete", uuid});
    ASSERT_TRUE(rsp.isSuccess()) << "Should be allowed to delete prometheus";

    // But now it should be gone and we shouldnt be able to delete it again
    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Ifconfig, "delete", uuid});
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus())
            << "The interface should be deleted";
    // And no longer part of list interfaces
    interfaces = getInterfaces(*adminConnection);
    auto [ipv4, ipv6] = cb::net::getIpAddresses(false);
    int total = 1;
    (void)ipv4;
    if (!ipv6.empty()) {
        ++total;
    }

    ASSERT_EQ(total, interfaces.size());
    ASSERT_EQ("mcbp", interfaces.back()["type"]);

    rsp = adminConnection->execute(cmd);
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

    interfaces = getInterfaces(*adminConnection);
    ASSERT_EQ(uuid, interfaces.back()["uuid"])
            << "list interfaces should return the newly created interface";
}

TEST_P(InterfacesTest, Mcbp) {
    // I should not be able to define to the same host/port I'm already
    // connected to
    auto interfaces = getInterfaces(*adminConnection);
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
    auto rsp = adminConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());

    // But I should be allowed to bind to the ANY interface on the same
    // port
    descr["host"] = "0.0.0.0";
    cmd.setValue(descr.dump());
    rsp = adminConnection->execute(cmd);
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
    findUuid(*adminConnection);
    ASSERT_TRUE(found) << "Did not find the interface with uuid: " << uuid
                       << std::endl
                       << json.dump(2);

    // It should not be possible to define it again
    rsp = adminConnection->execute(cmd);
    ASSERT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());

    // And not if I try to use the wild hard either
    descr["host"] = "*";
    cmd.setValue(descr.dump());
    rsp = adminConnection->execute(cmd);
    ASSERT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());

    // but we should be allowed to bind explicitly to all of the other
    // IP addresses
    auto [ipv4, ipv6] = cb::net::getIpAddresses(true);
    (void)ipv6;
    ASSERT_FALSE(ipv4.empty());
    for (auto host : ipv4) {
        descr["host"] = host;
        cmd.setValue(descr.dump());
        rsp = adminConnection->execute(cmd);
        ASSERT_TRUE(rsp.isSuccess());
    }

    interfaces = getInterfaces(*adminConnection);
    for (auto interface : interfaces) {
        if (interface["type"] == "mcbp" && interface["tag"] != "bootstrap") {
            rsp = adminConnection->execute(
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
    rsp = adminConnection->execute(cmd);
    ASSERT_EQ(cb::mcbp::Status::Einternal, rsp.getStatus())
            << rsp.getDataString();

    // We should be back to how it looked initially
    InterfacesTest_ListInterfaces_Test();
}

TEST_P(InterfacesTest, TlsProperties) {
    nlohmann::json tls_properties = {
            {"private key", OBJECT_ROOT "/tests/cert/root/ca_root.key"},
            {"certificate chain", OBJECT_ROOT "/tests/cert/root/ca_root.cert"},
            {"CA file", OBJECT_ROOT "/tests/cert/root/ca_root.cert"},
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
    const auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Ifconfig, "tls", tls_properties.dump()});
    ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                 << rsp.getDataString();
}

TEST_P(InterfacesTest, TlsPropertiesEncryptedKey) {
    nlohmann::json tls_properties = {
            {"private key",
             OBJECT_ROOT "/tests/cert/root/ca_root_encrypted.key"},
            {"certificate chain", OBJECT_ROOT "/tests/cert/root/ca_root.cert"},
            {"CA file", OBJECT_ROOT "/tests/cert/root/ca_root.cert"},
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

    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Ifconfig, "tls", tls_properties.dump()});
    ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                 << rsp.getDataString();

    // Verify that we don't return the passphrase
    rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Ifconfig, "tls"});
    ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                 << rsp.getDataString();
    auto json = rsp.getDataJson();
    EXPECT_EQ("set", json["password"]) << json.dump(2);
}

TEST_P(InterfacesTest, TlsPropertiesEncryptedCertInvalidPassphrase) {
    nlohmann::json tls_properties = {
            {"private key",
             OBJECT_ROOT "/tests/cert/root/ca_root_encrypted.key"},
            {"certificate chain", OBJECT_ROOT "/tests/cert/root/ca_root.cert"},
            {"CA file", OBJECT_ROOT "/tests/cert/root/ca_root.cert"},
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
            {"password",
             cb::base64::encode("This is the wrong passphrase", false)}};

    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Ifconfig, "tls", tls_properties.dump()});
    ASSERT_FALSE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                  << rsp.getDataString();
}

/// ns_server revoked the commitment to implement MB-46863 for 7.1,
/// but wanted to keep the interfaces in memcached.json for now.
/// To work around their limitations we added back backwards compatibility.
/// Verify that we fail if the address in use
TEST_P(InterfacesTest, MB46863_NsServerWithoutSupportForIfconfig_AddressInUse) {
    SOCKET server_socket = cb::net::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_NE(INVALID_SOCKET, server_socket);
    sockaddr_in sin;
    std::memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = INADDR_ANY;
    ASSERT_EQ(0,
              cb::net::bind(server_socket,
                            reinterpret_cast<const sockaddr*>(&sin),
                            sizeof(sin)));
    ASSERT_EQ(0, cb::net::listen(server_socket, 10));
    socklen_t len = sizeof(sockaddr_in);
    ASSERT_EQ(0,
              getsockname(
                      server_socket, reinterpret_cast<sockaddr*>(&sin), &len));

    auto interfaces = nlohmann::json::array();
    interfaces.emplace_back(nlohmann::json{{"system", false},
                                           {"port", ntohs(sin.sin_port)},
                                           {"ipv4", "required"},
                                           {"ipv6", "off"},
                                           {"host", "*"}});
    auto config = memcached_cfg;
    config["interfaces"] = interfaces;
    auto rsp = reconfigure(config);

    // reload should be rejected because the port is already open
    ASSERT_FALSE(rsp.isSuccess())
            << to_string(rsp.getStatus()) << ": " << rsp.getDataString();
#ifdef WIN32
    ASSERT_NE(std::string::npos,
              rsp.getDataString().find(
                      "An attempt was made to access a socket in a way "
                      "forbidden by its access permissions"))
            << rsp.getDataString();

#else
    ASSERT_NE(std::string::npos,
              rsp.getDataString().find("Address already in use"))
            << rsp.getDataString();
#endif

    cb::net::closesocket(server_socket);
    reconfigure(memcached_cfg);
}

/// Verify that we can reload with the new configuration.
/// We do this in 3 steps
///    1. Reload with the current configuration
///    2. Reload with a new interface (should be added)
///    3. Reload where we remove the new interface (should be removed)
TEST_P(InterfacesTest, MB46863_NsServerWithoutSupportForIfconfig_ReloadOk) {
    // build up an interface array containing what we already have defined
    auto interfaces = nlohmann::json::array();
    connectionMap.iterate([&interfaces](const MemcachedConnection& c) {
        auto family = c.getFamily();
        interfaces.emplace_back(nlohmann::json{
                {"host", family == AF_INET ? "127.0.0.1" : "::1"},
                {"ipv4", family == AF_INET ? "required" : "off"},
                {"ipv6", family == AF_INET6 ? "required" : "off"},
                {"tag", c.getTag()},
                {"port", c.getPort()}});
    });

    auto config = memcached_cfg;
    config["interfaces"] = interfaces;

    auto rsp = reconfigure(config);
    ASSERT_TRUE(rsp.isSuccess())
            << to_string(rsp.getStatus()) << ": " << rsp.getDataString();
    std::remove(mcd_env->getPortnumberFile().c_str());

    // Add an ephemeral port and verify that it was created

    auto extra_interface = interfaces;
    extra_interface.emplace_back(nlohmann::json{{"system", false},
                                                {"port", 0},
                                                {"ipv4", "required"},
                                                {"ipv6", "off"},
                                                {"host", "*"},
                                                {"tag", "ephemeral"}});
    config["interfaces"] = extra_interface;
    rsp = reconfigure(config);
    ASSERT_TRUE(rsp.isSuccess())
            << to_string(rsp.getStatus()) << ": " << rsp.getDataString();

    auto portnumbers = nlohmann::json::parse(
            cb::io::loadFile(mcd_env->getPortnumberFile()));
    in_port_t actualPort = 0;
    for (const auto& p : portnumbers["ports"]) {
        if (p["tag"] == "ephemeral") {
            actualPort = p["port"].get<in_port_t>();
        }
    }

    ASSERT_NE(0, actualPort)
            << "Looks like reload didn't create an ephemeral port";
    std::remove(mcd_env->getPortnumberFile().c_str());

    // remove it from the list, reload and verify that its gone
    config["interfaces"] = interfaces;
    rsp = reconfigure(config);
    ASSERT_TRUE(rsp.isSuccess())
            << to_string(rsp.getStatus()) << ": " << rsp.getDataString();
    portnumbers = nlohmann::json::parse(
            cb::io::loadFile(mcd_env->getPortnumberFile()));
    // verify that the port is gone
    for (const auto& p : portnumbers["ports"]) {
        ASSERT_NE("ephemeral", p["tag"].get<std::string>());
    }

    // MB-47411: The prometheus interface was incorrectly deleted as part
    //           of cleaning up unused interfaces
    bool found = false;
    for (const auto& e : getInterfaces(*adminConnection)) {
        if (e["type"] == "prometheus") {
            found = true;
        }
    }
    ASSERT_TRUE(found) << "The prometheus interface should be present";
}

void InterfacesTest::test_mb47707(bool whitelist_localhost_interface) {
    memcached_cfg["whitelist_localhost_interface"] =
            whitelist_localhost_interface;
    reconfigure(memcached_cfg);

    // Define the interface to use
    nlohmann::json descr = {{"host", "127.0.0.1"},
                            {"port", 0},
                            {"family", "inet"},
                            {"system", false},
                            {"tag", "MB-47707"},
                            {"type", "mcbp"}};

    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Ifconfig, "define", descr.dump()});
    ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                 << rsp.getDataString();
    auto json = rsp.getDataJson();
    auto uuid = json["ports"][0]["uuid"].get<std::string>();
    auto new_port = json["ports"][0]["port"].get<in_port_t>();

    MemcachedConnection c("127.0.0.1", new_port, AF_INET, false);
    c.connect();
    rsp = c.execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::SaslListMechs});
    ASSERT_TRUE(rsp.isSuccess()) << "Status: " << to_string(rsp.getStatus())
                                 << " message " << rsp.getDataString();

    // Delete the interface
    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Ifconfig, "delete", uuid});
    ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                 << rsp.getDataString();

    if (whitelist_localhost_interface) {
        // The connection should not be disconnected
        rsp = c.execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::SaslListMechs});
        ASSERT_TRUE(rsp.isSuccess()) << "Status: " << to_string(rsp.getStatus())
                                     << " message " << rsp.getDataString();
    } else {
        // The connection should be disconnected
        try {
            rsp = c.execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::SaslListMechs});
            FAIL() << "Expected the connection to be disconnected.\n"
                   << "Status: " << to_string(rsp.getStatus())
                   << "\nmessage: " << rsp.getDataString();
        } catch (const std::system_error& error) {
            // we should probably have checked if the error code is
            // conn-reset, but then again that may be different on windows
            // mac and linux...
        }
    }
}

/// Verify that we don't disconnect localhost connections as part of
/// interface deletion if they're bound to localhost
TEST_P(InterfacesTest, MB_47707_LocalhostWhitelisted) {
    test_mb47707(true);
}

/// Verify that we disconnect localhost connections as part of
/// interface deletion even if they're bound to localhost
TEST_P(InterfacesTest, MB_47707_LocalhostNotWhitelisted) {
    test_mb47707(false);
}
