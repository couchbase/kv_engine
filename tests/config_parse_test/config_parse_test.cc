/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include <system_error>
#include <platform/platform.h>
#include <gtest/gtest.h>
#include <cJSON_utils.h>
#include <daemon/settings.h>
#include <platform/dirutils.h>

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

    template <typename T = std::invalid_argument>
    void expectFail(const unique_cJSON_ptr& ptr) {
        EXPECT_THROW(Settings settings(ptr), T) << to_string(ptr, true);
    }
};

void SettingsTest::nonStringValuesShouldFail(const std::string& tag) {
    // Boolean values should not be accepted
    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddTrueToObject(obj.get(), tag.c_str());
    expectFail(obj);

    obj.reset(cJSON_CreateObject());
    cJSON_AddFalseToObject(obj.get(), tag.c_str());
    expectFail(obj);

    // Numbers should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), tag.c_str(), 5);
    expectFail(obj);

    obj.reset(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), tag.c_str(), 5.0);
    expectFail(obj);

    // Null should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddNullToObject(obj.get(), tag.c_str());
    expectFail(obj);

    // An array should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddItemToObject(obj.get(), tag.c_str(), cJSON_CreateArray());
    expectFail(obj);

    // An object should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddItemToObject(obj.get(), tag.c_str(), cJSON_CreateObject());
    expectFail(obj);
}

void SettingsTest::nonBooleanValuesShouldFail(const std::string& tag) {
    // String values should not be accepted
    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), tag.c_str(), "foo");
    expectFail(obj);

    // Numbers should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), tag.c_str(), 5);
    expectFail(obj);

    obj.reset(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), tag.c_str(), 5.0);
    expectFail(obj);

    // Null should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddNullToObject(obj.get(), tag.c_str());
    expectFail(obj);

    // An array should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddItemToObject(obj.get(), tag.c_str(), cJSON_CreateArray());
    expectFail(obj);

    // An object should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddItemToObject(obj.get(), tag.c_str(), cJSON_CreateObject());
    expectFail(obj);
}

void SettingsTest::nonNumericValuesShouldFail(const std::string& tag) {
    // Boolean values should not be accepted
    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddTrueToObject(obj.get(), tag.c_str());
    expectFail(obj);

    obj.reset(cJSON_CreateObject());
    cJSON_AddFalseToObject(obj.get(), tag.c_str());
    expectFail(obj);

    // Strings should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), tag.c_str(), "foobar");
    expectFail(obj);

    // Null should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddNullToObject(obj.get(), tag.c_str());
    expectFail(obj);

    // An array should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddItemToObject(obj.get(), tag.c_str(), cJSON_CreateArray());
    expectFail(obj);

    // An object should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddItemToObject(obj.get(), tag.c_str(), cJSON_CreateObject());
    expectFail(obj);
}

void SettingsTest::nonArrayValuesShouldFail(const std::string& tag) {
    // Boolean values should not be accepted
    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddTrueToObject(obj.get(), tag.c_str());
    expectFail(obj);

    obj.reset(cJSON_CreateObject());
    cJSON_AddFalseToObject(obj.get(), tag.c_str());
    expectFail(obj);

    // Numbers should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), tag.c_str(), 5);
    expectFail(obj);

    obj.reset(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), tag.c_str(), 5.0);
    expectFail(obj);

    // Null should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddNullToObject(obj.get(), tag.c_str());
    expectFail(obj);

    // A String should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), tag.c_str(), "foobar");
    expectFail(obj);

    // An object should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddItemToObject(obj.get(), tag.c_str(), cJSON_CreateObject());
    expectFail(obj);
}

void SettingsTest::nonObjectValuesShouldFail(const std::string& tag) {
    // Boolean values should not be accepted
    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddTrueToObject(obj.get(), tag.c_str());
    expectFail(obj);

    obj.reset(cJSON_CreateObject());
    cJSON_AddFalseToObject(obj.get(), tag.c_str());
    expectFail(obj);

    // Numbers should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), tag.c_str(), 5);
    expectFail(obj);

    obj.reset(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), tag.c_str(), 5.0);
    expectFail(obj);

    // Null should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddNullToObject(obj.get(), tag.c_str());
    expectFail(obj);

    // A String should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), tag.c_str(), "foobar");
    expectFail(obj);

    // An array should not be accepted
    obj.reset(cJSON_CreateObject());
    cJSON_AddItemToObject(obj.get(), tag.c_str(), cJSON_CreateArray());
    expectFail(obj);
}


TEST_F(SettingsTest, Admin) {
    // Ensure that we detect non-string values for admin
    nonStringValuesShouldFail("admin");

    // Ensure that we accept a string
    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), "admin", "_admin");
    try {
        Settings settings(obj);
        EXPECT_EQ("_admin", settings.getAdmin());
        EXPECT_TRUE(settings.has.admin);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // An empty string is also allowed
    obj.reset(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), "admin", "");
    try {
        Settings settings(obj);
        EXPECT_EQ("", settings.getAdmin());
        EXPECT_TRUE(settings.has.admin);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, AuditFile) {
    // Ensure that we detect non-string values for admin
    nonStringValuesShouldFail("audit_file");

    char pattern[] = {"audit_file.XXXXXX"};

    // Ensure that we accept a string, but the file must exist
    EXPECT_NE(nullptr, cb_mktemp(pattern));

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), "audit_file", pattern);
    try {
        Settings settings(obj);
        EXPECT_EQ(pattern, settings.getAuditFile());
        EXPECT_TRUE(settings.has.audit);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // But we should fail if the file don't exist
    cb::io::rmrf(pattern);
    expectFail<std::system_error>(obj);
}

TEST_F(SettingsTest, Threads) {
    nonNumericValuesShouldFail("threads");

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), "threads", 10);
    try {
        Settings settings(obj);
        EXPECT_EQ(10, settings.getNumWorkerThreads());
        EXPECT_TRUE(settings.has.threads);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, Interfaces) {
    nonArrayValuesShouldFail("interfaces");

    unique_cJSON_ptr array(cJSON_CreateArray());
    unique_cJSON_ptr obj(cJSON_CreateObject());

    char key_pattern[] = {"key.XXXXXX"};
    char cert_pattern[] = {"cert.XXXXXX"};
    EXPECT_NE(nullptr, cb_mktemp(key_pattern));
    EXPECT_NE(nullptr, cb_mktemp(cert_pattern));

    cJSON_AddNumberToObject(obj.get(), "port", 0);
    cJSON_AddTrueToObject(obj.get(), "ipv4");
    cJSON_AddTrueToObject(obj.get(), "ipv6");
    cJSON_AddNumberToObject(obj.get(), "maxconn", 10);
    cJSON_AddNumberToObject(obj.get(), "backlog", 10);
    cJSON_AddStringToObject(obj.get(), "host", "*");
    cJSON_AddStringToObject(obj.get(), "protocol", "memcached");
    cJSON_AddTrueToObject(obj.get(), "management");

    unique_cJSON_ptr ssl(cJSON_CreateObject());
    cJSON_AddStringToObject(ssl.get(), "key", key_pattern);
    cJSON_AddStringToObject(ssl.get(), "cert", cert_pattern);
    cJSON_AddItemToObject(obj.get(), "ssl", ssl.release());

    cJSON_AddItemToArray(array.get(), obj.release());

    obj.reset(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), "port", 0);
    cJSON_AddTrueToObject(obj.get(), "ipv4");
    cJSON_AddTrueToObject(obj.get(), "ipv6");
    cJSON_AddNumberToObject(obj.get(), "maxconn", 10);
    cJSON_AddNumberToObject(obj.get(), "backlog", 10);
    cJSON_AddStringToObject(obj.get(), "host", "*");
    cJSON_AddStringToObject(obj.get(), "protocol", "greenstack");
    cJSON_AddTrueToObject(obj.get(), "management");
    cJSON_AddItemToArray(array.get(), obj.release());

    unique_cJSON_ptr root(cJSON_CreateObject());
    cJSON_AddItemToObject(root.get(), "interfaces", array.release());

    try {
        Settings settings(root);
        EXPECT_EQ(2, settings.getInterfaces().size());
        EXPECT_TRUE(settings.has.interfaces);

        const auto& ifc0 = settings.getInterfaces()[0];

        EXPECT_EQ(0, ifc0.port);
        EXPECT_TRUE(ifc0.ipv4);
        EXPECT_TRUE(ifc0.ipv6);
        EXPECT_EQ(10, ifc0.maxconn);
        EXPECT_EQ(10, ifc0.backlog);
        EXPECT_EQ("*", ifc0.host);
        EXPECT_EQ(Protocol::Memcached, ifc0.protocol);
        EXPECT_TRUE(ifc0.management);

        const auto& ifc1 = settings.getInterfaces()[1];
        EXPECT_EQ(0, ifc1.port);
        EXPECT_TRUE(ifc1.ipv4);
        EXPECT_TRUE(ifc1.ipv6);
        EXPECT_EQ(10, ifc1.maxconn);
        EXPECT_EQ(10, ifc1.backlog);
        EXPECT_EQ("*", ifc1.host);
        EXPECT_EQ(Protocol::Greenstack, ifc1.protocol);
        EXPECT_TRUE(ifc1.management);


    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, InterfacesMissingSSLFiles) {
    nonArrayValuesShouldFail("interfaces");

    unique_cJSON_ptr array(cJSON_CreateArray());
    unique_cJSON_ptr obj(cJSON_CreateObject());

    char key_pattern[] = {"key.XXXXXX"};
    char cert_pattern[] = {"cert.XXXXXX"};
    EXPECT_NE(nullptr, cb_mktemp(key_pattern));
    EXPECT_NE(nullptr, cb_mktemp(cert_pattern));

    cJSON_AddNumberToObject(obj.get(), "port", 0);
    cJSON_AddTrueToObject(obj.get(), "ipv4");
    cJSON_AddTrueToObject(obj.get(), "ipv6");
    cJSON_AddNumberToObject(obj.get(), "maxconn", 10);
    cJSON_AddNumberToObject(obj.get(), "backlog", 10);
    cJSON_AddStringToObject(obj.get(), "host", "*");
    cJSON_AddStringToObject(obj.get(), "protocol", "memcached");
    cJSON_AddTrueToObject(obj.get(), "management");

    unique_cJSON_ptr ssl(cJSON_CreateObject());
    cJSON_AddStringToObject(ssl.get(), "key", key_pattern);
    cJSON_AddStringToObject(ssl.get(), "cert", cert_pattern);
    cJSON_AddItemToObject(obj.get(), "ssl", ssl.release());

    cJSON_AddItemToArray(array.get(), obj.release());
    unique_cJSON_ptr root(cJSON_CreateObject());
    cJSON_AddItemToObject(root.get(), "interfaces", array.release());

    try {
        Settings settings(root);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // We should fail if one of the files is missing
    cb::io::rmrf(key_pattern);
    expectFail<std::system_error>(root);
    fclose(fopen(key_pattern, "a"));
    cb::io::rmrf(cert_pattern);
    expectFail<std::system_error>(root);
    cb::io::rmrf(key_pattern);
}

TEST_F(SettingsTest, InterfacesInvalidSslEntry) {
    nonArrayValuesShouldFail("interfaces");

    unique_cJSON_ptr array(cJSON_CreateArray());
    unique_cJSON_ptr obj(cJSON_CreateObject());

    char pattern[] = {"ssl.XXXXXX"};
    EXPECT_NE(nullptr, cb_mktemp(pattern));

    cJSON_AddNumberToObject(obj.get(), "port", 0);
    cJSON_AddTrueToObject(obj.get(), "ipv4");
    cJSON_AddTrueToObject(obj.get(), "ipv6");
    cJSON_AddNumberToObject(obj.get(), "maxconn", 10);
    cJSON_AddNumberToObject(obj.get(), "backlog", 10);
    cJSON_AddStringToObject(obj.get(), "host", "*");
    cJSON_AddStringToObject(obj.get(), "protocol", "memcached");
    cJSON_AddTrueToObject(obj.get(), "management");

    unique_cJSON_ptr ssl(cJSON_CreateObject());
    cJSON_AddStringToObject(ssl.get(), "cert", pattern);
    cJSON_AddItemToObject(obj.get(), "ssl", ssl.release());

    cJSON_AddItemToArray(array.get(), obj.release());
    unique_cJSON_ptr root(cJSON_CreateObject());
    cJSON_AddItemToObject(root.get(), "interfaces", array.release());

    expectFail(root);

    obj.reset(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), "port", 0);
    cJSON_AddTrueToObject(obj.get(), "ipv4");
    cJSON_AddTrueToObject(obj.get(), "ipv6");
    cJSON_AddNumberToObject(obj.get(), "maxconn", 10);
    cJSON_AddNumberToObject(obj.get(), "backlog", 10);
    cJSON_AddStringToObject(obj.get(), "host", "*");
    cJSON_AddStringToObject(obj.get(), "protocol", "memcached");
    cJSON_AddTrueToObject(obj.get(), "management");

    ssl.reset(cJSON_CreateObject());
    cJSON_AddStringToObject(ssl.get(), "key", pattern);
    cJSON_AddItemToObject(obj.get(), "ssl", ssl.release());

    array.reset(cJSON_CreateArray());
    cJSON_AddItemToArray(array.get(), obj.release());
    root.reset(cJSON_CreateObject());
    cJSON_AddItemToObject(root.get(), "interfaces", array.release());
    expectFail(root);

    cb::io::rmrf(pattern);
}

TEST_F(SettingsTest, Extensions) {
    nonArrayValuesShouldFail("extensions");

    unique_cJSON_ptr array(cJSON_CreateArray());

    for (int ii = 0; ii < 10; ii++) {
        unique_cJSON_ptr obj(cJSON_CreateObject());
        std::string mod = "module-" + std::to_string(ii);
        cJSON_AddStringToObject(obj.get(), "module", mod.c_str());
        std::string cfg = "config-" + std::to_string(ii);
        cJSON_AddStringToObject(obj.get(), "config", cfg.c_str());
        cJSON_AddItemToArray(array.get(), obj.release());
    }

    unique_cJSON_ptr root(cJSON_CreateObject());
    cJSON_AddItemToObject(root.get(), "extensions", array.release());

    try {
        Settings settings(root);
        EXPECT_EQ(10, settings.getPendingExtensions().size());
        EXPECT_TRUE(settings.has.extensions);
        int ii = 0;
        for (const auto& ext : settings.getPendingExtensions()) {
            std::string mod = "module-" + std::to_string(ii);
            std::string cfg = "config-" + std::to_string(ii);
            EXPECT_EQ(mod, ext.soname);
            EXPECT_EQ(cfg, ext.config);
            ++ii;
        }
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, RequireInit) {
    nonBooleanValuesShouldFail("require_init");

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddTrueToObject(obj.get(), "require_init");
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isRequireInit());
        EXPECT_TRUE(settings.has.require_init);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj.reset(cJSON_CreateObject());
    cJSON_AddFalseToObject(obj.get(), "require_init");
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isRequireInit());
        EXPECT_TRUE(settings.has.require_init);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, RequireSasl) {
    nonBooleanValuesShouldFail("require_sasl");

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddTrueToObject(obj.get(), "require_sasl");
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isRequireSasl());
        EXPECT_TRUE(settings.has.require_sasl);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj.reset(cJSON_CreateObject());
    cJSON_AddFalseToObject(obj.get(), "require_sasl");
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isRequireSasl());
        EXPECT_TRUE(settings.has.require_sasl);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, DefaultReqsPerEvent) {
    nonNumericValuesShouldFail("default_reqs_per_event");

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), "default_reqs_per_event", 10);
    try {
        Settings settings(obj);
        EXPECT_EQ(10, settings.getRequestsPerEventNotification(
            EventPriority::Default));
        EXPECT_TRUE(settings.has.default_reqs_per_event);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, HighPriorityReqsPerEvent) {
    nonNumericValuesShouldFail("reqs_per_event_high_priority");

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), "reqs_per_event_high_priority", 10);
    try {
        Settings settings(obj);
        EXPECT_EQ(10, settings.getRequestsPerEventNotification(
            EventPriority::High));
        EXPECT_TRUE(settings.has.reqs_per_event_high_priority);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, MediumPriorityReqsPerEvent) {
    nonNumericValuesShouldFail("reqs_per_event_med_priority");

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), "reqs_per_event_med_priority", 10);
    try {
        Settings settings(obj);
        EXPECT_EQ(10, settings.getRequestsPerEventNotification(
            EventPriority::Medium));
        EXPECT_TRUE(settings.has.reqs_per_event_med_priority);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, LowPriorityReqsPerEvent) {
    nonNumericValuesShouldFail("reqs_per_event_low_priority");

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), "reqs_per_event_low_priority", 10);
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

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), "verbosity", 1);
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

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), "connection_idle_time", 500);
    try {
        Settings settings(obj);
        EXPECT_EQ(500, settings.getConnectionIdleTime());
        EXPECT_TRUE(settings.has.connection_idle_time);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, BioDrainBufferSize) {
    nonNumericValuesShouldFail("bio_drain_buffer_sz");

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddNumberToObject(obj.get(), "bio_drain_buffer_sz", 1024);
    try {
        Settings settings(obj);
        EXPECT_EQ(1024, settings.getBioDrainBufferSize());
        EXPECT_TRUE(settings.has.bio_drain_buffer_sz);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, DatatypeSupport) {
    nonBooleanValuesShouldFail("datatype_support");

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddTrueToObject(obj.get(), "datatype_support");
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isDatatypeSupport());
        EXPECT_TRUE(settings.has.datatype);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj.reset(cJSON_CreateObject());
    cJSON_AddFalseToObject(obj.get(), "datatype_support");
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isDatatypeSupport());
        EXPECT_TRUE(settings.has.datatype);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, Root) {
    // Ensure that we detect non-string values for admin
    nonStringValuesShouldFail("root");

    // Ensure that we accept a string, but it must be a directory

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), "root", "/");
    try {
        Settings settings(obj);
        EXPECT_EQ("/", settings.getRoot());
        EXPECT_TRUE(settings.has.root);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // But we should fail if the file don't exist
    obj.reset(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), "root", "/it/would/suck/if/this/exist");
    expectFail<std::system_error>(obj);
}

TEST_F(SettingsTest, SslCipherList) {
    // Ensure that we detect non-string values for ssl_cipher_list
    nonStringValuesShouldFail("ssl_cipher_list");

    // Ensure that we accept a string
    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), "ssl_cipher_list", "HIGH");
    try {
        Settings settings(obj);
        EXPECT_EQ("HIGH", settings.getSslCipherList());
        EXPECT_TRUE(settings.has.ssl_cipher_list);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // An empty string is also allowed
    obj.reset(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), "ssl_cipher_list", "");
    try {
        Settings settings(obj);
        EXPECT_EQ("", settings.getSslCipherList());
        EXPECT_TRUE(settings.has.ssl_cipher_list);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, SslMinimumProtocol) {
    nonStringValuesShouldFail("ssl_minimum_protocol");

    const std::vector<std::string> protocol = {"tlsv1", "tlsv1.1", "tlsv1_1",
                                               "tlsv1.2", "tlsv1_2"};
    for (const auto& p : protocol) {
        // Ensure that we accept a string
        unique_cJSON_ptr obj(cJSON_CreateObject());
        cJSON_AddStringToObject(obj.get(), "ssl_minimum_protocol", p.c_str());
        try {
            Settings settings(obj);
            EXPECT_EQ(p, settings.getSslMinimumProtocol());
            EXPECT_TRUE(settings.has.ssl_minimum_protocol);
        } catch (std::exception& exception) {
            FAIL() << exception.what();
        }
    }

    // An empty string is also allowed
    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), "ssl_minimum_protocol", "");
    try {
        Settings settings(obj);
        EXPECT_EQ("", settings.getSslMinimumProtocol());
        EXPECT_TRUE(settings.has.ssl_minimum_protocol);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // But random strings shouldn't be allowed
    obj.reset(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), "ssl_minimum_protocol", "foo");
    expectFail(obj);
}

TEST_F(SettingsTest, Breakpad) {
    nonObjectValuesShouldFail("breakpad");

    unique_cJSON_ptr obj(cJSON_CreateObject());

    char minidump_dir[] = {"minidump.XXXXXX"};
    EXPECT_NE(nullptr, cb_mktemp(minidump_dir));
    cb::io::rmrf(minidump_dir);
    cb::io::mkdirp(minidump_dir);

    cJSON_AddTrueToObject(obj.get(), "enabled");
    cJSON_AddStringToObject(obj.get(), "minidump_dir", minidump_dir);

    // Content is optional
    EXPECT_NO_THROW(BreakpadSettings settings(obj.get()));

    // But the minidump dir is mandatory
    cb::io::rmrf(minidump_dir);
    EXPECT_THROW(BreakpadSettings settings(obj.get()), std::system_error);
    cb::io::mkdirp(minidump_dir);

    cJSON_AddStringToObject(obj.get(), "content", "default");
    EXPECT_NO_THROW(BreakpadSettings settings(obj.get()));
    cJSON_ReplaceItemInObject(obj.get(), "content", cJSON_CreateString("foo"));
    EXPECT_THROW(BreakpadSettings settings(obj.get()), std::invalid_argument);

    cb::io::rmrf(minidump_dir);
}

TEST_F(SettingsTest, max_packet_size) {
    nonNumericValuesShouldFail("max_packet_size");

    unique_cJSON_ptr obj(cJSON_CreateObject());
    // the config file specifies it in MB, we're keeping it as bytes internally
    cJSON_AddNumberToObject(obj.get(), "max_packet_size", 30);
    try {
        Settings settings(obj);
        EXPECT_EQ(30 * 1024 * 1024, settings.getMaxPacketSize());
        EXPECT_TRUE(settings.has.max_packet_size);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, StdinListen) {
    nonBooleanValuesShouldFail("stdin_listen");

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddTrueToObject(obj.get(), "stdin_listen");
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isStdinListen());
        EXPECT_TRUE(settings.has.stdin_listen);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj.reset(cJSON_CreateObject());
    cJSON_AddFalseToObject(obj.get(), "stdin_listen");
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isStdinListen());
        EXPECT_TRUE(settings.has.stdin_listen);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, ExitOnConnectionClose) {
    nonBooleanValuesShouldFail("exit_on_connection_close");

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddTrueToObject(obj.get(), "exit_on_connection_close");
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isExitOnConnectionClose());
        EXPECT_TRUE(settings.has.exit_on_connection_close);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj.reset(cJSON_CreateObject());
    cJSON_AddFalseToObject(obj.get(), "exit_on_connection_close");
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isExitOnConnectionClose());
        EXPECT_TRUE(settings.has.exit_on_connection_close);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, SaslMechanisms) {
    nonStringValuesShouldFail("sasl_mechanisms");

    // Ensure that we accept a string
    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), "sasl_mechanisms", "SCRAM-SHA1");
    try {
        Settings settings(obj);
        EXPECT_EQ("SCRAM-SHA1", settings.getSaslMechanisms());
        EXPECT_TRUE(settings.has.sasl_mechanisms);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // An empty string is also allowed
    obj.reset(cJSON_CreateObject());
    cJSON_AddStringToObject(obj.get(), "sasl_mechanisms", "");
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

    unique_cJSON_ptr obj(cJSON_CreateObject());
    cJSON_AddTrueToObject(obj.get(), "dedupe_nmvb_maps");
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isDedupeNmvbMaps());
        EXPECT_TRUE(settings.has.dedupe_nmvb_maps);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj.reset(cJSON_CreateObject());
    cJSON_AddFalseToObject(obj.get(), "dedupe_nmvb_maps");
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isDedupeNmvbMaps());
        EXPECT_TRUE(settings.has.dedupe_nmvb_maps);
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

TEST(SettingsUpdateTest, AdminIsNotDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setAdmin("_admin");
    updated.setAdmin(settings.getAdmin());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should fail
    updated.setAdmin("bar");
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, BreakpadIsDynamic) {
    Settings updated;
    Settings settings;
    BreakpadSettings breakpadSettings;
    breakpadSettings.setEnabled(true);
    breakpadSettings.setContent(BreakpadContent::Default);
    breakpadSettings.setMinidumpDir("/var/crash");

    settings.setBreakpadSettings(breakpadSettings);
    updated.setBreakpadSettings(breakpadSettings);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should also work
    breakpadSettings.setEnabled(false);
    updated.setBreakpadSettings(breakpadSettings);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_TRUE(settings.getBreakpadSettings().isEnabled());

    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_FALSE(settings.getBreakpadSettings().isEnabled());

    breakpadSettings.setMinidumpDir("/var/crash/minidump");
    updated.setBreakpadSettings(breakpadSettings);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ("/var/crash", settings.getBreakpadSettings().getMinidumpDir());

    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ("/var/crash/minidump",
              settings.getBreakpadSettings().getMinidumpDir());
    EXPECT_FALSE(settings.getBreakpadSettings().isEnabled());
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

TEST(SettingsUpdateTest, InterfaceIdenticalArraysShouldWork) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work

    interface ifc;
    ifc.host.assign("*");
    ifc.ssl.key.assign("/etc/opt/couchbase/security/key.pem");
    ifc.ssl.cert.assign("/etc/opt/couchbase/security/cert.pem");

    updated.addInterface(ifc);
    settings.addInterface(ifc);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
}

TEST(SettingsUpdateTest, InterfaceSomeValuesMayChange) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work

    interface ifc;
    ifc.host.assign("*");
    ifc.ssl.key.assign("/etc/opt/couchbase/security/key.pem");
    ifc.ssl.cert.assign("/etc/opt/couchbase/security/cert.pem");

    settings.addInterface(ifc);

    ifc.backlog = 10;
    ifc.maxconn = 10;
    ifc.tcp_nodelay = false;
    ifc.ssl.key.assign("/opt/couchbase/security/key.pem");
    ifc.ssl.cert.assign("/opt/couchbase/security/cert.pem");

    updated.addInterface(ifc);

    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_NE(ifc.backlog, settings.getInterfaces()[0].backlog);
    EXPECT_NE(ifc.maxconn, settings.getInterfaces()[0].maxconn);
    EXPECT_NE(ifc.tcp_nodelay, settings.getInterfaces()[0].tcp_nodelay);
    EXPECT_NE(ifc.ssl.key, settings.getInterfaces()[0].ssl.key);
    EXPECT_NE(ifc.ssl.cert, settings.getInterfaces()[0].ssl.cert);

    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(ifc.backlog, settings.getInterfaces()[0].backlog);
    EXPECT_EQ(ifc.maxconn, settings.getInterfaces()[0].maxconn);
    EXPECT_EQ(ifc.tcp_nodelay, settings.getInterfaces()[0].tcp_nodelay);
    EXPECT_EQ(ifc.ssl.key, settings.getInterfaces()[0].ssl.key);
    EXPECT_EQ(ifc.ssl.cert, settings.getInterfaces()[0].ssl.cert);
}

TEST(SettingsUpdateTest, InterfaceSomeValuesMayNotChange) {
    Settings settings;
    // setting it to the same value should work

    interface ifc;

    settings.addInterface(ifc);

    {
        Settings updated;
        interface myifc;
        myifc.host.assign("localhost");
        updated.addInterface(myifc);

        EXPECT_THROW(settings.updateSettings(updated, false),
                     std::invalid_argument);
    }

    {
        Settings updated;
        interface myifc;
        myifc.port = 11200;
        updated.addInterface(myifc);

        EXPECT_THROW(settings.updateSettings(updated, false),
                     std::invalid_argument);
    }

    {
        Settings updated;
        interface myifc;
        myifc.ipv4 = false;
        updated.addInterface(myifc);

        EXPECT_THROW(settings.updateSettings(updated, false),
                     std::invalid_argument);
    }

    {
        Settings updated;
        interface myifc;
        myifc.ipv6 = false;
        updated.addInterface(myifc);

        EXPECT_THROW(settings.updateSettings(updated, false),
                     std::invalid_argument);
    }

    {
        Settings updated;
        interface myifc;
        myifc.management = true;
        updated.addInterface(myifc);

        EXPECT_THROW(settings.updateSettings(updated, false),
                     std::invalid_argument);
    }

    {
        Settings updated;
        interface myifc;
        myifc.protocol = Protocol::Greenstack;
        updated.addInterface(myifc);

        EXPECT_THROW(settings.updateSettings(updated, false),
                     std::invalid_argument);
    }
}

TEST(SettingsUpdateTest, InterfaceDifferentArraySizeShouldFail) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work

    interface ifc;
    settings.addInterface(ifc);
    updated.addInterface(ifc);

    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    updated.addInterface(ifc);
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
    settings.addInterface(ifc);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    settings.addInterface(ifc);
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, ExtensionsIdenticalArraysShouldWork) {
    Settings settings;
    extension_settings ext;
    ext.soname.assign("object.so");
    ext.config.assign("a=b");
    settings.addPendingExtension(ext);

    // setting it to the same value should work
    Settings updated;
    updated.addPendingExtension(ext);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
}

TEST(SettingsUpdateTest, ExtensionsDifferentElementsShouldFail) {
    Settings settings;
    extension_settings ext;
    ext.soname.assign("object.so");
    ext.config.assign("a=b");
    settings.addPendingExtension(ext);

    {
        Settings updated;
        ext.config.assign("a=c");
        updated.addPendingExtension(ext);
        EXPECT_THROW(settings.updateSettings(updated, false),
                     std::invalid_argument);
    }
    {
        Settings updated;
        ext.soname.assign("object.dll");
        updated.addPendingExtension(ext);
        EXPECT_THROW(settings.updateSettings(updated, false),
                     std::invalid_argument);
    }
}

TEST(SettingsUpdateTest, ExtensionsDifferentArraysSizeShouldFail) {
    Settings settings;
    extension_settings ext;
    ext.soname.assign("object.so");
    ext.config.assign("a=b");
    settings.addPendingExtension(ext);

    // setting it to the same value should work
    Settings updated;
    updated.addPendingExtension(ext);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    updated.addPendingExtension(ext);
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
    settings.addPendingExtension(ext);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    settings.addPendingExtension(ext);
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, RequireInitIsNotDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setRequireInit(true);
    updated.setRequireInit(settings.isRequireInit());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should fail
    updated.setRequireInit(!settings.isRequireInit());
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, RequireSaslIsNotDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setRequireSasl(true);
    updated.setRequireSasl(settings.isRequireSasl());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should fail
    updated.setRequireSasl(!settings.isRequireSasl());
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
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
    EXPECT_EQ(ii,
              settings.getRequestsPerEventNotification(EventPriority::Low));
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

TEST(SettingsUpdateTest, BioDrainBufferSzIsNotDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    auto old = settings.getBioDrainBufferSize();
    updated.setBioDrainBufferSize(old);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should not work
    updated.setBioDrainBufferSize(old + 10);
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, DatatypeSupportIsNotDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setDatatypeSupport(true);
    updated.setDatatypeSupport(settings.isDatatypeSupport());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should not work
    updated.setDatatypeSupport(!settings.isDatatypeSupport());
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, SslCipherListIsDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setSslCipherList("high");
    auto old = settings.getSslCipherList();
    updated.setSslCipherList(old);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    updated.setSslCipherList("low");
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(old, settings.getSslCipherList());
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ("low", settings.getSslCipherList());
}

TEST(SettingsUpdateTest, SslMinimumProtocolIsDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setSslMinimumProtocol("tlsv1.2");
    auto old = settings.getSslMinimumProtocol();
    updated.setSslMinimumProtocol(old);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    updated.setSslMinimumProtocol("tlsv1");
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(old, settings.getSslMinimumProtocol());
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ("tlsv1", settings.getSslMinimumProtocol());
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
    EXPECT_EQ(updated.getMaxPacketSize(),
              settings.getMaxPacketSize());
}

TEST(SettingsUpdateTest, StdinListenIsNotDynamic) {
    Settings settings;
    Settings updated;
    // setting it to the same value should work
    settings.setStdinListen(true);
    updated.setStdinListen(settings.isStdinListen());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should not work
    updated.setStdinListen(!settings.isStdinListen());
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, ExitOnConnectionCloseIsNotDynamic) {
    Settings settings;
    Settings updated;
    // setting it to the same value should work
    settings.setExitOnConnectionClose(true);
    updated.setExitOnConnectionClose(settings.isExitOnConnectionClose());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should not work
    updated.setExitOnConnectionClose(!settings.isExitOnConnectionClose());
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, SaslMechanismsIsNotDynamic) {
    Settings settings;
    Settings updated;
    // setting it to the same value should work
    settings.setSaslMechanisms("SCRAM-SHA1");
    updated.setSaslMechanisms(settings.getSaslMechanisms());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should not work
    updated.setSaslMechanisms("PLAIN");
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
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
