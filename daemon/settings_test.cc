/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "settings.h"
#include <folly/portability/GTest.h>
#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <variant>

class SettingsTest : public ::testing::Test {
public:
    /**
     * Test that all values except a string value throws an exception.
     * @param tag
     */
    void nonStringValuesShouldFail(const std::string& tag);

    /**
     * Test that all values except boolean values throws an exception.
     * @param tag
     */
    void nonBooleanValuesShouldFail(const std::string& tag);

    /**
     * Test that all values except numeric values throws an exception.
     * @param tag
     */
    void nonNumericValuesShouldFail(const std::string& tag);

    void nonArrayValuesShouldFail(const std::string& tag);

    void nonObjectValuesShouldFail(const std::string& tag);

    /**
     * Convenience method - returns a config JSON object with an "interfaces"
     * array containing single interface object with the given properties.
     */
    nlohmann::json makeInterfacesConfig(const char* protocolMode);
    template <typename T = nlohmann::json::exception>
    void expectFail(const nlohmann::json& json) {
        EXPECT_THROW(Settings settings(json), T) << json.dump();
    }
};

void SettingsTest::nonStringValuesShouldFail(const std::string& tag) {
    nlohmann::json json;
    // nlohmann::json accepts true/false when parsing numbers

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

void SettingsTest::nonBooleanValuesShouldFail(const std::string& tag) {
    // String values should not be accepted
    nlohmann::json json;
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

void SettingsTest::nonNumericValuesShouldFail(const std::string& tag) {
    nlohmann::json json;
    // nlohmann::json accepts true/false when parsing numbers

    // String values should not be accepted
    json[tag] = "foo";
    expectFail(json);

    // An array should not be accepted
    json[tag] = nlohmann::json::array();
    expectFail(json);

    // An object should not be accepted
    json[tag] = nlohmann::json::object();
    expectFail(json);
}

void SettingsTest::nonArrayValuesShouldFail(const std::string& tag) {
    // Boolean values should not be accepted
    nlohmann::json json;
    json[tag] = true;
    expectFail(json);

    json[tag] = false;
    expectFail(json);

    // Numbers should not be accepted
    json[tag] = 5;
    expectFail(json);

    json[tag] = 5.0;
    expectFail(json);

    // String values should not be accepted
    json[tag] = "foo";
    expectFail(json);

    // An object should not be accepted
    json[tag] = nlohmann::json::object();
    expectFail(json);
}

void SettingsTest::nonObjectValuesShouldFail(const std::string& tag) {
    // Boolean values should not be accepted
    nlohmann::json json;
    json[tag] = true;
    expectFail(json);

    json[tag] = false;
    expectFail(json);

    // Numbers should not be accepted
    json[tag] = 5;
    expectFail(json);

    json[tag] = 5.0;
    expectFail(json);

    // String values should not be accepted
    json[tag] = "foo";
    expectFail(json);

    // An array should not be accepted
    json[tag] = nlohmann::json::array();
    expectFail(json);
}

nlohmann::json SettingsTest::makeInterfacesConfig(const char* protocolMode) {
    nlohmann::json obj;
    obj["ipv4"] = protocolMode;
    obj["ipv6"] = protocolMode;

    nlohmann::json arr;
    arr.push_back(obj);

    nlohmann::json root;
    root["interfaces"] = arr;
    return root;
}

TEST_F(SettingsTest, AuditFile) {
    nonStringValuesShouldFail("audit_file");
    const std::string filename{"/foo/bar"};
    nlohmann::json json;
    json["audit_file"] = filename;
    try {
        Settings settings(json);
        EXPECT_EQ(filename, settings.getAuditFile());
        EXPECT_TRUE(settings.has.audit);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, RbacFile) {
    nonStringValuesShouldFail("rbac_file");
    const std::string filename{"/foo/bar"};
    nlohmann::json json;
    json["rbac_file"] = filename;
    try {
        Settings settings(json);
        EXPECT_EQ(filename, settings.getRbacFile());
        EXPECT_TRUE(settings.has.rbac_file);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, Threads) {
    nonNumericValuesShouldFail("threads");

    nlohmann::json json;
    // Explicitly make threads an unsigned int
    uint8_t threads = 10;
    json["threads"] = threads;
    try {
        Settings settings(json);
        EXPECT_EQ(10, settings.getNumWorkerThreads());
        EXPECT_TRUE(settings.has.threads);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, Prometheus) {
    nlohmann::json json;
    json["prometheus"]["port"] = 666;
    json["prometheus"]["family"] = "inet";

    try {
        Settings settings(json);
        auto [port, family] = settings.getPrometheusConfig();
        EXPECT_EQ(666, port);
        EXPECT_EQ(AF_INET, family);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // Check for inet6
    json["prometheus"]["port"] = 999;
    json["prometheus"]["family"] = "inet6";
    try {
        Settings settings(json);
        auto [port, family] = settings.getPrometheusConfig();
        EXPECT_EQ(999, port);
        EXPECT_EQ(AF_INET6, family);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // Check for invalid port
    json["prometheus"]["port"] = "asdf";
    json["prometheus"]["family"] = "inet";
    try {
        Settings settings(json);
        FAIL() << "Should detect invalid port";
    } catch (std::exception&) {
    }

    // Check for invalid family
    json["prometheus"]["port"] = 999;
    json["prometheus"]["family"] = "asdf";
    try {
        Settings settings(json);
        FAIL() << "Should detect invalid family";
    } catch (std::exception&) {
    }

    json["prometheus"] = nlohmann::json::value_t::object;
    json["prometheus"]["family"] = "inet";
    try {
        Settings settings(json);
        FAIL() << "Should detect missing port";
    } catch (std::exception&) {
    }
    json["prometheus"] = nlohmann::json::value_t::object;
    json["prometheus"]["port"] = 999;
    try {
        Settings settings(json);
        FAIL() << "Should detect missing family";
    } catch (std::exception&) {
    }
}

TEST_F(SettingsTest, Interfaces) {
    nonArrayValuesShouldFail("interfaces");

    nlohmann::json obj;
    obj["tag"] = "tls";
    obj["port"] = 0;
    obj["ipv4"] = "optional";
    obj["ipv6"] = "optional";
    obj["host"] = "*";
    obj["tls"] = true;

    nlohmann::json array;
    array.push_back(obj);

    nlohmann::json root;
    root["interfaces"] = array;

    try {
        Settings settings(root);
        EXPECT_TRUE(settings.has.interfaces);

        const auto interfaces = settings.getInterfaces();
        const auto& ifc0 = interfaces[0];

        EXPECT_EQ(1, interfaces.size());
        EXPECT_EQ(0, ifc0.port);
        EXPECT_EQ(NetworkInterface::Protocol::Optional, ifc0.ipv4);
        EXPECT_EQ(NetworkInterface::Protocol::Optional, ifc0.ipv6);
        EXPECT_EQ("*", ifc0.host);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, InterfacesEphemeralMissingTag) {
    nonArrayValuesShouldFail("interfaces");

    const auto filename = cb::io::mktemp("config_parse_test");

    nlohmann::json obj;
    obj["port"] = 0;
    obj["ipv4"] = "optional";
    obj["ipv6"] = "optional";
    obj["host"] = "*";

    nlohmann::json array;
    array.push_back(obj);

    nlohmann::json root;
    root["interfaces"] = array;

    expectFail<std::invalid_argument>(root);
    std::filesystem::remove(filename);
}

/// Test that "off" is correctly handled for ipv4 & ipv6 protocols.
TEST_F(SettingsTest, InterfacesProtocolOff) {
    auto root = makeInterfacesConfig("off");

    try {
        Settings settings(root);
        const auto interfaces = settings.getInterfaces();
        ASSERT_EQ(1, interfaces.size());
        const auto& ifc0 = interfaces[0];
        ASSERT_TRUE(settings.has.interfaces);
        EXPECT_EQ(NetworkInterface::Protocol::Off, ifc0.ipv4);
        EXPECT_EQ(NetworkInterface::Protocol::Off, ifc0.ipv6);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

/// Test that "optional" is correctly handled for ipv4 & ipv6 protocols.
TEST_F(SettingsTest, InterfacesProtocolOptional) {
    auto root = makeInterfacesConfig("optional");

    try {
        Settings settings(root);
        const auto interfaces = settings.getInterfaces();
        ASSERT_EQ(1, interfaces.size());
        const auto& ifc0 = interfaces[0];

        EXPECT_EQ(NetworkInterface::Protocol::Optional, ifc0.ipv4);
        EXPECT_EQ(NetworkInterface::Protocol::Optional, ifc0.ipv6);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

/// Test that "required" is correctly handled for ipv4 & ipv6 protocols.
TEST_F(SettingsTest, InterfacesProtocolRequired) {
    auto root = makeInterfacesConfig("required");

    try {
        Settings settings(root);
        const auto interfaces = settings.getInterfaces();
        ASSERT_EQ(1, interfaces.size());
        const auto& ifc0 = interfaces[0];

        EXPECT_EQ(NetworkInterface::Protocol::Required, ifc0.ipv4);
        EXPECT_EQ(NetworkInterface::Protocol::Required, ifc0.ipv6);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

/// Test that invalid numeric values for ipv4 & ipv6 protocols are rejected.
TEST_F(SettingsTest, InterfacesInvalidProtocolNumber) {
    // Numbers not permitted
    nlohmann::json obj;
    obj["ipv4"] = 1;
    obj["ipv6"] = 2;

    nlohmann::json array;
    array.push_back(obj);

    nlohmann::json root;
    root["interfaces"] = array;

    expectFail<nlohmann::json::exception>(root);
}

/// Test that invalid string values for ipv4 & ipv6 protocols are rejected.
TEST_F(SettingsTest, InterfacesInvalidProtocolString) {
    // Strings not in (off, optional, required) not permitted.
    auto root = makeInterfacesConfig("sometimes");

    expectFail<std::invalid_argument>(root);
}

TEST_F(SettingsTest, ParseLoggerSettings) {
    nonObjectValuesShouldFail("logger");

    nlohmann::json obj;
    obj["filename"] = "logs/n_1/memcached.log";
    obj["buffersize"] = 1024;
    obj["cyclesize"] = 10485760;
    obj["unit_test"] = true;

    nlohmann::json root;
    root["logger"] = obj;
    Settings settings(root);

    EXPECT_TRUE(settings.has.logger);

    const auto config = settings.getLoggerConfig();
    EXPECT_EQ("logs/n_1/memcached.log", config.filename);
    EXPECT_EQ(1024, config.buffersize);
    EXPECT_EQ(10485760, config.cyclesize);
    EXPECT_EQ(true, config.unit_test);
}

TEST_F(SettingsTest, StdinListener) {
    nonBooleanValuesShouldFail("stdin_listener");

    nlohmann::json obj;
    obj["stdin_listener"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isStdinListenerEnabled());
        EXPECT_TRUE(settings.has.stdin_listener);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["stdin_listener"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isStdinListenerEnabled());
        EXPECT_TRUE(settings.has.stdin_listener);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, DefaultReqsPerEvent) {
    nonNumericValuesShouldFail("default_reqs_per_event");

    nlohmann::json obj;
    // Explicitly make defaultReqsPerEvent an unsigned int
    uint8_t defaultReqsPerEvent = 10;
    obj["default_reqs_per_event"] = defaultReqsPerEvent;
    try {
        Settings settings(obj);
        EXPECT_EQ(10,
                  settings.getRequestsPerEventNotification(
                          EventPriority::Default));
        EXPECT_TRUE(settings.has.default_reqs_per_event);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, HighPriorityReqsPerEvent) {
    nonNumericValuesShouldFail("reqs_per_event_high_priority");

    nlohmann::json obj;
    // Explicitly make reqsPerEventHighPriority an unsigned int
    uint8_t reqsPerEventHighPriority = 10;
    obj["reqs_per_event_high_priority"] = reqsPerEventHighPriority;
    try {
        Settings settings(obj);
        EXPECT_EQ(
                10,
                settings.getRequestsPerEventNotification(EventPriority::High));
        EXPECT_TRUE(settings.has.reqs_per_event_high_priority);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, MediumPriorityReqsPerEvent) {
    nonNumericValuesShouldFail("reqs_per_event_med_priority");

    nlohmann::json obj;
    // Explicitly make reqsPerEventMedPriority an unsigned int
    uint8_t reqsPerEventMedPriority = 10;
    obj["reqs_per_event_med_priority"] = reqsPerEventMedPriority;
    try {
        Settings settings(obj);
        EXPECT_EQ(10,
                  settings.getRequestsPerEventNotification(
                          EventPriority::Medium));
        EXPECT_TRUE(settings.has.reqs_per_event_med_priority);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, LowPriorityReqsPerEvent) {
    nonNumericValuesShouldFail("reqs_per_event_low_priority");

    nlohmann::json obj;
    // Explicitly make reqsPerEventLowPriority an unsigned int
    uint8_t reqsPerEventLowPriority = 10;
    obj["reqs_per_event_low_priority"] = reqsPerEventLowPriority;
    try {
        Settings settings(obj);
        EXPECT_EQ(10,
                  settings.getRequestsPerEventNotification(EventPriority::Low));
        EXPECT_TRUE(settings.has.reqs_per_event_low_priority);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, Verbosity) {
    nonNumericValuesShouldFail("verbosity");

    nlohmann::json obj;
    // Explicitly make verbosity an unsigned int
    uint8_t verbosity = 1;
    obj["verbosity"] = verbosity;
    try {
        Settings settings(obj);
        EXPECT_EQ(1, settings.getVerbose());
        EXPECT_TRUE(settings.has.verbose);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, ConnectionIdleTime) {
    nonNumericValuesShouldFail("connection_idle_time");

    nlohmann::json obj;
    // Explicitly make connection_idle_time an unsigned int
    uint16_t connectionIdleTime = 500;
    obj["connection_idle_time"] = connectionIdleTime;
    try {
        Settings settings(obj);
        EXPECT_EQ(500, settings.getConnectionIdleTime());
        EXPECT_TRUE(settings.has.connection_idle_time);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, DatatypeJson) {
    nonBooleanValuesShouldFail("datatype_json");

    nlohmann::json obj;
    obj["datatype_json"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isDatatypeJsonEnabled());
        EXPECT_TRUE(settings.has.datatype_json);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["datatype_json"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isDatatypeJsonEnabled());
        EXPECT_TRUE(settings.has.datatype_json);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, DatatypeSnappy) {
    nonBooleanValuesShouldFail("datatype_snappy");

    nlohmann::json obj;
    obj["datatype_snappy"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isDatatypeSnappyEnabled());
        EXPECT_TRUE(settings.has.datatype_snappy);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["datatype_snappy"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isDatatypeSnappyEnabled());
        EXPECT_TRUE(settings.has.datatype_snappy);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, Root) {
    // Ensure that we detect non-string values for admin
    nonStringValuesShouldFail("root");

    // Ensure that we accept a string, but it must be a directory
    nlohmann::json obj;
    obj["root"] = "/";
    try {
        Settings settings(obj);
        EXPECT_EQ("/", settings.getRoot());
        EXPECT_TRUE(settings.has.root);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // But we should fail if the file don't exist
    obj["root"] = "/it/would/suck/if/this/exist";
    expectFail<std::system_error>(obj);
}

TEST_F(SettingsTest, max_packet_size) {
    nonNumericValuesShouldFail("max_packet_size");

    nlohmann::json obj;
    // the config file specifies it in MB, we're keeping it as bytes internally
    // Explicitly make maxPacketSize and unsigned int
    uint8_t maxPacketSize = 30;
    obj["max_packet_size"] = maxPacketSize;
    try {
        Settings settings(obj);
        EXPECT_EQ(30_MiB, settings.getMaxPacketSize());
        EXPECT_TRUE(settings.has.max_packet_size);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, max_connections) {
    nonNumericValuesShouldFail("max_connections");

    nlohmann::json obj;
    constexpr size_t maxconn = 100;
    obj["max_connections"] = maxconn;
    Settings settings(obj);
    EXPECT_EQ(maxconn, settings.getMaxConnections());
    EXPECT_TRUE(settings.has.max_connections);
}

TEST_F(SettingsTest, system_connections) {
    nonNumericValuesShouldFail("system_connections");

    nlohmann::json obj;
    constexpr size_t maxconn = 100;
    obj["system_connections"] = maxconn;
    Settings settings(obj);
    EXPECT_EQ(maxconn, settings.getSystemConnections());
    EXPECT_TRUE(settings.has.system_connections);
}

TEST_F(SettingsTest, max_concurrent_commands_per_connection) {
    nonNumericValuesShouldFail("max_concurrent_commands_per_connection");

    nlohmann::json obj;
    constexpr std::size_t max = 64;
    obj["max_concurrent_commands_per_connection"] = max;
    Settings settings(obj);
    EXPECT_EQ(max, settings.getMaxConcurrentCommandsPerConnection());
    EXPECT_TRUE(settings.has.max_concurrent_commands_per_connection);
}

TEST_F(SettingsTest, SaslMechanisms) {
    nonStringValuesShouldFail("sasl_mechanisms");

    // Ensure that we accept a string
    nlohmann::json obj;
    obj["sasl_mechanisms"] = "SCRAM-SHA1";
    try {
        Settings settings(obj);
        EXPECT_EQ("SCRAM-SHA1", settings.getSaslMechanisms());
        EXPECT_TRUE(settings.has.sasl_mechanisms);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // An empty string is also allowed
    obj["sasl_mechanisms"] = "";
    try {
        Settings settings(obj);
        EXPECT_EQ("", settings.getSaslMechanisms());
        EXPECT_TRUE(settings.has.sasl_mechanisms);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, DedupeNmvbMaps) {
    nonBooleanValuesShouldFail("dedupe_nmvb_maps");

    nlohmann::json obj;
    obj["dedupe_nmvb_maps"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isDedupeNmvbMaps());
        EXPECT_TRUE(settings.has.dedupe_nmvb_maps);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["dedupe_nmvb_maps"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isDedupeNmvbMaps());
        EXPECT_TRUE(settings.has.dedupe_nmvb_maps);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, XattrEnabled) {
    nonBooleanValuesShouldFail("xattr_enabled");

    nlohmann::json obj;
    obj["xattr_enabled"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isXattrEnabled());
        EXPECT_TRUE(settings.has.xattr_enabled);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["xattr_enabled"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isXattrEnabled());
        EXPECT_TRUE(settings.has.xattr_enabled);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, TracingEnabled) {
    nonBooleanValuesShouldFail("tracing_enabled");

    nlohmann::json obj;
    obj["tracing_enabled"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isTracingEnabled());
        EXPECT_TRUE(settings.has.tracing_enabled);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["tracing_enabled"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isTracingEnabled());
        EXPECT_TRUE(settings.has.tracing_enabled);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, ExternalAuthService) {
    nonBooleanValuesShouldFail("external_auth_service");

    nlohmann::json obj;
    obj["external_auth_service"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isExternalAuthServiceEnabled());
        EXPECT_TRUE(settings.has.external_auth_service);
    } catch (const std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["external_auth_service"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isExternalAuthServiceEnabled());
        EXPECT_TRUE(settings.has.external_auth_service);
    } catch (const std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, ScramshaFallbackSalt) {
    nonStringValuesShouldFail("scramsha_fallback_salt");
    nlohmann::json obj;
    obj["scramsha_fallback_salt"] = "JKouEmqRFI+Re/AA";
    try {
        Settings settings(obj);
        EXPECT_EQ("JKouEmqRFI+Re/AA", settings.getScramshaFallbackSalt());
        EXPECT_TRUE(settings.has.scramsha_fallback_salt);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST(SettingsUpdateTest, EmptySettingsShouldWork) {
    Settings updated;
    Settings settings;
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
}

TEST(SettingsUpdateTest, RootIsNotDynamic) {
    Settings settings;
    settings.setRoot("/tmp");
    // setting it to the same value should work
    Settings updated;
    updated.setRoot(settings.getRoot());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should fail
    updated.setRoot("/var");
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, PrometheusIsDynamic) {
    // setting the same value should work
    Settings settings;
    Settings updated;
    settings.setPrometheusConfig({666, AF_INET});
    updated.setPrometheusConfig({666, AF_INET});

    settings.updateSettings(updated, true);
    {
        auto [port, family] = settings.getPrometheusConfig();
        EXPECT_EQ(666, port);
        EXPECT_EQ(AF_INET, family);
    }

    updated.setPrometheusConfig({999, AF_INET6});
    settings.updateSettings(updated, true);
    {
        auto [port, family] = settings.getPrometheusConfig();
        EXPECT_EQ(999, port);
        EXPECT_EQ(AF_INET6, family);
    }
}

TEST(SettingsUpdateTest, BreakpadIsDynamic) {
    Settings updated;
    Settings settings;
    cb::breakpad::Settings breakpadSettings;
    breakpadSettings.enabled = true;
    breakpadSettings.minidump_dir = "/var/crash";

    settings.setBreakpadSettings(breakpadSettings);
    updated.setBreakpadSettings(breakpadSettings);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should also work
    breakpadSettings.enabled = false;
    updated.setBreakpadSettings(breakpadSettings);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_TRUE(settings.getBreakpadSettings().enabled);

    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_FALSE(settings.getBreakpadSettings().enabled);

    breakpadSettings.minidump_dir = "/var/crash/minidump";
    updated.setBreakpadSettings(breakpadSettings);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ("/var/crash", settings.getBreakpadSettings().minidump_dir);

    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ("/var/crash/minidump",
              settings.getBreakpadSettings().minidump_dir);
    EXPECT_FALSE(settings.getBreakpadSettings().enabled);
}

TEST(SettingsUpdateTest, AuditFileIsNotDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setAuditFile("/etc/opt/couchbase/etc/security/audit.json");
    updated.setAuditFile(settings.getAuditFile());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should fail
    updated.setAuditFile("/opt/couchbase/etc/security/audit.json");
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, ThreadsIsNotDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setNumWorkerThreads(4);
    updated.setNumWorkerThreads(settings.getNumWorkerThreads());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should fail
    updated.setNumWorkerThreads(settings.getNumWorkerThreads() - 1);
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, UpdatingLoggerSettingsShouldFail) {
    Settings settings;
    Settings updated;

    cb::logger::Config config;
    config.filename.assign("logger_test");
    config.buffersize = 1024;
    config.cyclesize = 1_MiB;

    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    updated.setLoggerConfig(config);
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, MaxConnectionsIsDynamic) {
    Settings updated;
    Settings settings;
    settings.setMaxConnections(10);
    // setting it to the same value should work
    updated.setMaxConnections(10);
    settings.updateSettings(updated, false);

    // changing it should work
    updated.setMaxConnections(1000);
    settings.updateSettings(updated, true);
    EXPECT_EQ(1000, settings.getMaxConnections());
}

TEST(SettingsUpdateTest, SystemConnectionsIsDynamic) {
    Settings updated;
    Settings settings;
    settings.setSystemConnections(10);
    // setting it to the same value should work
    updated.setSystemConnections(10);
    settings.updateSettings(updated, false);

    // changing it should work
    updated.setSystemConnections(1000);
    settings.updateSettings(updated, true);
    EXPECT_EQ(1000, settings.getSystemConnections());
}

TEST(SettingsUpdateTest, MaxConcurrentCommandsPerConnectionIsDynamic) {
    Settings updated;
    Settings settings;
    settings.setMaxConcurrentCommandsPerConnection(10);
    // setting it to the same value should work
    updated.setMaxConcurrentCommandsPerConnection(10);
    settings.updateSettings(updated, false);

    // changing it should work
    updated.setMaxConcurrentCommandsPerConnection(1000);
    settings.updateSettings(updated, true);
    EXPECT_EQ(1000, settings.getMaxConcurrentCommandsPerConnection());
}

TEST(SettingsUpdateTest, FusionMigrationRateLimitIsDynamic) {
    Settings updated;
    Settings settings;
    settings.setFusionMigrationRateLimit(10);
    // setting it to the same value should work
    updated.setFusionMigrationRateLimit(10);
    settings.updateSettings(updated, false);

    // changing it should work
    updated.setFusionMigrationRateLimit(1000);
    settings.updateSettings(updated, true);
    EXPECT_EQ(1000, settings.getFusionMigrationRateLimit());
}

TEST(SettingsUpdateTest, FusionSyncRateLimitIsDynamic) {
    Settings updated;
    Settings settings;
    settings.setFusionSyncRateLimit(10);
    // setting it to the same value should work
    updated.setFusionSyncRateLimit(10);
    settings.updateSettings(updated, false);

    // changing it should work
    updated.setFusionSyncRateLimit(1000);
    settings.updateSettings(updated, true);
    EXPECT_EQ(1000, settings.getFusionSyncRateLimit());
}

TEST(SettingsUpdateTest, DefaultReqIsDynamic) {
    Settings updated;
    Settings settings;
    settings.setRequestsPerEventNotification(10, EventPriority::Default);
    // setting it to the same value should work
    int ii = 10;
    updated.setRequestsPerEventNotification(ii, EventPriority::Default);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    ii += 1000;
    updated.setRequestsPerEventNotification(ii, EventPriority::Default);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(ii - 1000,
              settings.getRequestsPerEventNotification(EventPriority::Default));
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(ii,
              settings.getRequestsPerEventNotification(EventPriority::Default));
}

TEST(SettingsUpdateTest, HighPriReqIsDynamic) {
    Settings updated;
    Settings settings;
    settings.setRequestsPerEventNotification(10, EventPriority::High);
    // setting it to the same value should work
    int ii = 10;
    updated.setRequestsPerEventNotification(ii, EventPriority::High);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    ii += 1000;
    updated.setRequestsPerEventNotification(ii, EventPriority::High);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(ii - 1000,
              settings.getRequestsPerEventNotification(EventPriority::High));
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(ii,
              settings.getRequestsPerEventNotification(EventPriority::High));
}

TEST(SettingsUpdateTest, MedPriReqIsDynamic) {
    Settings updated;
    Settings settings;
    settings.setRequestsPerEventNotification(10, EventPriority::Medium);
    // setting it to the same value should work
    int ii = 10;
    updated.setRequestsPerEventNotification(ii, EventPriority::Medium);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    ii += 1000;
    updated.setRequestsPerEventNotification(ii, EventPriority::Medium);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(ii - 1000,
              settings.getRequestsPerEventNotification(EventPriority::Medium));
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(ii,
              settings.getRequestsPerEventNotification(EventPriority::Medium));
}

TEST(SettingsUpdateTest, LowPriReqIsDynamic) {
    Settings updated;
    Settings settings;
    settings.setRequestsPerEventNotification(10, EventPriority::Low);
    // setting it to the same value should work
    int ii = 10;
    updated.setRequestsPerEventNotification(ii, EventPriority::Low);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    ii += 1000;
    updated.setRequestsPerEventNotification(ii, EventPriority::Low);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(ii - 1000,
              settings.getRequestsPerEventNotification(EventPriority::Low));
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(ii, settings.getRequestsPerEventNotification(EventPriority::Low));
}

TEST(SettingsUpdateTest, VerbosityIsDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    auto old = settings.getVerbose();
    updated.setVerbose(old);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    updated.setVerbose(settings.getVerbose() + 1);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(old, settings.getVerbose());
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(updated.getVerbose(), settings.getVerbose());
}

TEST(SettingsUpdateTest, ConnectionIdleTimeIsDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    auto old = settings.getConnectionIdleTime();
    updated.setConnectionIdleTime(old);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    updated.setConnectionIdleTime(old + 10);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(old, settings.getConnectionIdleTime());
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(updated.getConnectionIdleTime(),
              settings.getConnectionIdleTime());
}

TEST(SettingsUpdateTest, DatatypeJsonIsNotDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setDatatypeJsonEnabled(true);
    updated.setDatatypeJsonEnabled(settings.isDatatypeJsonEnabled());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should not work
    updated.setDatatypeJsonEnabled(!settings.isDatatypeJsonEnabled());
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, DatatypeSnappyIsDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setDatatypeSnappyEnabled(true);
    updated.setDatatypeSnappyEnabled(settings.isDatatypeSnappyEnabled());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    updated.setDatatypeSnappyEnabled(!settings.isDatatypeSnappyEnabled());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
}

TEST(SettingsUpdateTest, MaxPacketSizeIsDynamic) {
    Settings settings;
    Settings updated;
    // setting it to the same value should work
    auto old = settings.getMaxPacketSize();
    updated.setMaxPacketSize(old);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    updated.setMaxPacketSize(old + 10);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(old, settings.getMaxPacketSize());
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(updated.getMaxPacketSize(), settings.getMaxPacketSize());
}

TEST(SettingsUpdateTest, SaslMechanismsIsDynamic) {
    Settings settings;
    Settings updated;
    // setting it to the same value should work
    settings.setSaslMechanisms("SCRAM-SHA1");
    updated.setSaslMechanisms(settings.getSaslMechanisms());
    settings.updateSettings(updated, false);

    // changing it should work
    updated.setSaslMechanisms("PLAIN");
    settings.updateSettings(updated);
    EXPECT_EQ("PLAIN", settings.getSaslMechanisms());
}

TEST(SettingsUpdateTest, SslSaslMechanismsIsDynamic) {
    Settings settings;
    Settings updated;
    // setting it to the same value should work
    settings.setSslSaslMechanisms("SCRAM-SHA1");
    updated.setSslSaslMechanisms(settings.getSslSaslMechanisms());
    settings.updateSettings(updated, false);

    // changing it should work
    updated.setSslSaslMechanisms("PLAIN");
    settings.updateSettings(updated);
    EXPECT_EQ("PLAIN", settings.getSslSaslMechanisms());
}

TEST(SettingsUpdateTest, DedupeNmvbMapsIsDynamic) {
    Settings settings;
    Settings updated;
    // setting it to the same value should work
    settings.setDedupeNmvbMaps(true);
    updated.setDedupeNmvbMaps(settings.isDedupeNmvbMaps());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should also work
    updated.setDedupeNmvbMaps(!settings.isDedupeNmvbMaps());
    EXPECT_TRUE(settings.isDedupeNmvbMaps());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_TRUE(settings.isDedupeNmvbMaps());
    EXPECT_NO_THROW(settings.updateSettings(updated, true));
    EXPECT_FALSE(settings.isDedupeNmvbMaps());
}

TEST(SettingsUpdateTest, OpcodeAttributesOverrideIsDynamic) {
    Settings settings;
    Settings updated;

    // setting it to the same value should work
    settings.setOpcodeAttributesOverride(R"({"version":1})");
    updated.setOpcodeAttributesOverride(settings.getOpcodeAttributesOverride());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should also work
    updated.setOpcodeAttributesOverride(R"({"version":1, "comment":"foo"})");

    // Dry-run
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_NE(updated.getOpcodeAttributesOverride(),
              settings.getOpcodeAttributesOverride());

    // with update
    EXPECT_NO_THROW(settings.updateSettings(updated, true));
    EXPECT_EQ(updated.getOpcodeAttributesOverride(),
              settings.getOpcodeAttributesOverride());
}

TEST(SettingsUpdateTest, OpcodeAttributesMustBeValidFormat) {
    Settings settings;

    // It must be json containing "version"
    EXPECT_THROW(settings.setOpcodeAttributesOverride("{}"),
                 std::invalid_argument);

    // it works if it contains a valid entry
    settings.setOpcodeAttributesOverride(
            R"({"version":1,"default": {"slow":500}})");

    // Setting to an empty value means drop the previous content
    settings.setOpcodeAttributesOverride("");
    EXPECT_EQ("", settings.getOpcodeAttributesOverride());
}

TEST_F(SettingsTest, ScramshaFallbackSaltIsDynamic) {
    Settings settings;
    Settings updated;

    // setting it to the same value should work
    settings.setScramshaFallbackSalt("Original");
    updated.setScramshaFallbackSalt("New");
    EXPECT_NO_THROW(settings.updateSettings(updated, true));

    try {
        EXPECT_EQ("New", settings.getScramshaFallbackSalt());
        EXPECT_TRUE(settings.has.scramsha_fallback_salt);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, TestSettingNumThreads) {
    std::array<std::string, 5> threadKeys = {{"threads",
                                              "num_reader_threads",
                                              "num_writer_threads",
                                              "num_auxio_threads",
                                              "num_nonio_threads"}};
    nlohmann::json config;
    Settings settings;
    auto updateConfig = [&threadKeys, &config, &settings](
                                std::variant<std::string, uint8_t> value,
                                bool shouldFail = false) {
        size_t numberOfFailures = 0;
        size_t numberOfJsonErrors = 0;
        for (const auto& key : threadKeys) {
            if (value.index() == 0) {
                if (std::get<std::string>(value) == "default") {
                    // "threads" doesn't take string values so if we're setting
                    // to "default" just set it to 0.
                    if (key == "threads") {
                        config[key] = uint8_t{0};
                    } else if (key == "num_reader_threads" ||
                               key == "num_writer_threads") {
                        // MB-63386: num_read_threads and num_writer_threads now
                        // defaults to "balanced" instead of "default"
                        config[key] = "balanced";
                    }
                } else {
                    config[key] = std::get<std::string>(value);
                }
            } else {
                config[key] = std::get<uint8_t>(value);
            }
            try {
                settings.reconfigure(config);
            } catch (const std::invalid_argument& e) {
                ++numberOfFailures;
            } catch (const nlohmann::json::type_error& e) {
                // Key "threads" will throw a json error no a
                // std::invalid_argument
                ++numberOfJsonErrors;
            }
        }
        if (shouldFail) {
            return numberOfFailures == (threadKeys.size() - 1) &&
                   numberOfJsonErrors == 1;
        }
        return numberOfFailures == 0 && numberOfJsonErrors == 0;
    };
    EXPECT_TRUE(updateConfig("default"));

    EXPECT_EQ(0, settings.getNumWorkerThreads());
    EXPECT_EQ(0, settings.getNumReaderThreads());
    EXPECT_EQ(0, settings.getNumWriterThreads());
    EXPECT_EQ(-1, settings.getNumAuxIoThreads());
    EXPECT_EQ(-1, settings.getNumNonIoThreads());

    settings.setNumWorkerThreads(1);
    settings.setNumReaderThreads(1);
    settings.setNumWriterThreads(1);
    settings.setNumAuxIoThreads(1);
    settings.setNumNonIoThreads(1);

    EXPECT_EQ(1, settings.getNumWorkerThreads());
    EXPECT_EQ(1, settings.getNumReaderThreads());
    EXPECT_EQ(1, settings.getNumWriterThreads());
    EXPECT_EQ(1, settings.getNumAuxIoThreads());
    EXPECT_EQ(1, settings.getNumNonIoThreads());

    EXPECT_TRUE(updateConfig(uint8_t{2}));

    EXPECT_EQ(2, settings.getNumWorkerThreads());
    EXPECT_EQ(2, settings.getNumReaderThreads());
    EXPECT_EQ(2, settings.getNumWriterThreads());
    EXPECT_EQ(2, settings.getNumAuxIoThreads());
    EXPECT_EQ(2, settings.getNumNonIoThreads());

    EXPECT_TRUE(updateConfig("abc", true));

    EXPECT_EQ(2, settings.getNumWorkerThreads());
    EXPECT_EQ(2, settings.getNumReaderThreads());
    EXPECT_EQ(2, settings.getNumWriterThreads());
    EXPECT_EQ(2, settings.getNumAuxIoThreads());
    EXPECT_EQ(2, settings.getNumNonIoThreads());
}

TEST_F(SettingsTest, TestQuotaSharingConcurrencyPercentage) {
    Settings settings;

    auto testValue = [](int newValue) {
        Settings s{{{"quota_sharing_pager_concurrency_percentage", newValue}}};
        EXPECT_EQ(newValue, s.getQuotaSharingPagerConcurrencyPercentage());
    };

    // Expect a reasonable default
    EXPECT_LT(0, settings.getQuotaSharingPagerConcurrencyPercentage());
    EXPECT_GE(100, settings.getQuotaSharingPagerConcurrencyPercentage());

    EXPECT_NO_THROW(testValue(1));
    EXPECT_NO_THROW(testValue(50));
    EXPECT_NO_THROW(testValue(100));

    EXPECT_THROW(testValue(-1), std::invalid_argument);
    EXPECT_THROW(testValue(0), std::invalid_argument);
    EXPECT_THROW(testValue(101), std::invalid_argument);
}

TEST_F(SettingsTest, TestQuotaSharingPagerSleepTime) {
    Settings settings;

    auto testValue = [](int newValue) {
        Settings s{{{"quota_sharing_pager_sleep_time_ms", newValue}}};
        EXPECT_EQ(std::chrono::milliseconds(newValue),
                  s.getQuotaSharingPagerSleepTime());
    };

    // Expect a reasonable default
    EXPECT_LT(0, settings.getQuotaSharingPagerSleepTime().count());

    EXPECT_NO_THROW(testValue(1));
    EXPECT_NO_THROW(testValue(5000));

    EXPECT_THROW(testValue(-1), std::invalid_argument);
    EXPECT_THROW(testValue(0), std::invalid_argument);
}

TEST_F(SettingsTest, TestPrometheusScrapeSlowDuration) {
    Settings settings;

    EXPECT_GT(settings.getSlowPrometheusScrapeDuration().count(), 0);

    auto test = [&settings](uint32_t newVal) {
        nlohmann::json config{{"slow_prometheus_scrape_duration", newVal}};
        settings.reconfigure(config);
        EXPECT_EQ(std::chrono::duration<float>(newVal),
                  settings.getSlowPrometheusScrapeDuration());
    };

    EXPECT_NO_THROW(test(1));
}

TEST_F(SettingsTest, TestDefaultExternalAuthTimeout) {
    using namespace std::chrono_literals;
    using namespace std::chrono;
    Settings settings;
    EXPECT_EQ(60s,
              duration_cast<seconds>(settings.getExternalAuthRequestTimeout()));
}

TEST_F(SettingsTest, TestDcpConsumerMaxMarkerVersion) {
    Settings settings;
    EXPECT_EQ(2.2, settings.getDcpConsumerMaxMarkerVersion());

    settings.setDcpConsumerMaxMarkerVersion(2.3);
    EXPECT_EQ(2.3, settings.getDcpConsumerMaxMarkerVersion());
}

TEST_F(SettingsTest, TestDcpSnapshotMarkerHPSEnabled) {
    Settings settings;
    EXPECT_TRUE(settings.isDcpSnapshotMarkerHPSEnabled());

    settings.setDcpSnapshotMarkerHPSEnabled(false);
    EXPECT_FALSE(settings.isDcpSnapshotMarkerHPSEnabled());
}

TEST_F(SettingsTest, TestDcpSnapshotMarkerPurgeSeqnoEnabled) {
    Settings settings;
    EXPECT_TRUE(settings.isDcpSnapshotMarkerPurgeSeqnoEnabled());

    settings.setDcpSnapshotMarkerPurgeSeqnoEnabled(false);
    EXPECT_FALSE(settings.isDcpSnapshotMarkerPurgeSeqnoEnabled());
}