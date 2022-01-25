/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include <boost/filesystem/path.hpp>
#include <evutil.h>
#include <folly/portability/GTest.h>
#include <platform/dirutils.h>
#include <platform/process_monitor.h>
#include <string>

/**
 * This test suite tests the various settings for authenticating over SSL
 * with client certificates. It does not test all of the various paths one
 * may configure as that is tested in the unit tests for the parsing of the
 * configuration.
 */

class SslCertTest : public TestappTest {
protected:
    std::unique_ptr<MemcachedConnection> createConnection() {
        std::unique_ptr<MemcachedConnection> conn;
        connectionMap.iterate([&conn](const MemcachedConnection& c) {
            if (!conn && c.isSsl()) {
                auto family = c.getFamily();
                conn = std::make_unique<MemcachedConnection>(
                        family == AF_INET ? "127.0.0.1" : "::1",
                        c.getPort(),
                        family,
                        true);
            }
        });
        return conn;
    }
};

/**
 * Setting the control to "disabled" simply means that the server don't
 * even ask the client to provide a certificate, and if it end up providing
 * one it'll be ignored.
 *
 * Setting the control to "enabled" means that we ask the client to provide
 * a certificate, and if it does it must be:
 *   * valid
 *   * contain a user-mapping which maps to a user defined in the system
 *
 * Setting the control to "mandatory" means that the client _MUST_ provide
 * a valid certificate and it must contain a user mapping which maps to a
 * user defined in the system.
 *
 * Connections which is authenticated via certificate cannot perform SASL
 * to change their identity, and they are not automatically connected to
 * a bucket with the same name as the user.
 */

/**
 * When disabled we don't look at the certificate so it should be possible
 * to connect without one
 */
TEST_F(SslCertTest, LoginWhenDiabledWithoutCert) {
    reconfigure_client_cert_auth("disabled", "", "", "");

    auto connection = createConnection();
    ASSERT_TRUE(connection) << "Failed to locate a SSL port";
    connection->connect();
    connection->authenticate("@admin", "password", "PLAIN");
}

/**
 * When disabled we don't look at the certificate so it should be possible
 * to connect with one even if it doesn't map to a user (we've not defined
 * a user mapping).
 */
TEST_F(SslCertTest, LoginWhenDiabledWithCert) {
    reconfigure_client_cert_auth("disabled", "", "", "");

    auto connection = createConnection();
    ASSERT_TRUE(connection) << "Failed to locate a SSL port";
    setClientCertData(*connection, "john");
    connection->connect();
    connection->authenticate("@admin", "password", "PLAIN");
}

/**
 * When set to enabled we allow the user to connect even if it no certificate
 * is provided
 */
TEST_F(SslCertTest, LoginEnabledWithoutCert) {
    reconfigure_client_cert_auth("enabled", "subject.cn", "", " ");

    auto connection = createConnection();
    ASSERT_TRUE(connection) << "Failed to locate a SSL port";
    connection->connect();
    connection->authenticate("@admin", "password", "PLAIN");
}

/**
 * It should be possible to connect with a certificate when there is no
 * mapping defined on the system (only the client certificate is validated)
 */
TEST_F(SslCertTest, LoginEnabledWithCertNoMapping) {
    reconfigure_client_cert_auth("enabled", "", "", " ");

    auto connection = createConnection();
    ASSERT_TRUE(connection) << "Failed to locate a SSL port";
    setClientCertData(*connection, "trond");
    connection->connect();
    connection->authenticate("@admin", "password", "PLAIN");
}

/**
 * It should be possible to connect with a certificate which maps to a user.
 * The connection is not bound to a bucket so the client needs to explicitly
 * run select bucket to in order to perform operations.
 */
TEST_F(SslCertTest, LoginEnabledWithCert) {
    reconfigure_client_cert_auth("enabled", "subject.cn", "", " ");

    auto connection = createConnection();
    ASSERT_TRUE(connection) << "Failed to locate a SSL port";
    setClientCertData(*connection, "trond");
    connection->connect();
    connection->setXerrorSupport(true);

    // We should be authenticated from the cert, so we should not be
    // allowed to perform another authentication
    try {
        connection->authenticate("@admin", "password", "PLAIN");
        FAIL() << "SASL Auth should be disabled for cert auth'd connections";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotSupported())
                << "Received: " << to_string(error.getReason());
    }

    try {
        connection->get("foo", Vbid(0));
        FAIL() << "Should not be associated with a bucket";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied())
                << "Received: " << to_string(error.getReason());
    }

    connection->selectBucket(bucketName);
    try {
        connection->get("foo", Vbid(0));
        FAIL() << "document should not exists";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound())
                << "Received: " << to_string(error.getReason());
    }
}

/**
 * If we try to connect with a certificate which contains a "valid mapping",
 * but maps to an undefined user we should get disconnected.
 */
TEST_F(SslCertTest, LoginEnabledWithCertWithUndefinedUser) {
    reconfigure_client_cert_auth("enabled", "subject.cn", "", " ");

    auto connection = createConnection();
    ASSERT_TRUE(connection) << "Failed to locate a SSL port";
    setClientCertData(*connection, "john");

    try {
        connection->connect();
        connection->execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});
        FAIL() << "It should not be possible to connect with a cert which maps "
                  "to an undefined user";
    } catch (const std::runtime_error&) {
    }
}

/**
 * When the setting is set to mandatory a client certificate _HAS_ to be
 * provided in order to allow the connection to succeed.
 */
TEST_F(SslCertTest, LoginWhenMandatoryWithoutCert) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");

    auto connection = createConnection();
    ASSERT_TRUE(connection) << "Failed to locate a SSL port";
    try {
        connection->connect();
        connection->execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});
        FAIL() << "It should not be possible to connect without certificate";
    } catch (const std::runtime_error&) {
    }
}

/**
 * Verify that we may log into the system when we provide a certificate,
 * and that we're not automatically bound to a bucket (an explicit select
 * bucket is needed).
 */
TEST_F(SslCertTest, LoginWhenMandatoryWithCert) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");

    auto connection = createConnection();
    ASSERT_TRUE(connection) << "Failed to locate a SSL port";
    setClientCertData(*connection, "trond");
    connection->connect();
    connection->setXerrorSupport(true);

    try {
        connection->get("foo", Vbid(0));
        FAIL() << "Should not be associated with a bucket";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied())
                << "Received: " << to_string(error.getReason());
    }

    connection->selectBucket(bucketName);
    try {
        connection->get("foo", Vbid(0));
        FAIL() << "document should not exists";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound())
                << "Received: " << to_string(error.getReason());
    }
}

/**
 * The system should _only_ allow users into the system where the information
 * in the certificate map to a user defined in the system.
 */
TEST_F(SslCertTest, LoginWhenMandatoryWithCertIncorrectMapping) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    auto connection = createConnection();
    ASSERT_TRUE(connection) << "Failed to locate a SSL port";
    setClientCertData(*connection, "john");

    // The certificate will be accepted, so the connection is established
    // but the server will disconnect the client immediately
    connection->connect();

    // Try to run a hello (should NOT work)
    try {
        connection->setXerrorSupport(true);
        FAIL() << "The server should disconnect the client due to missing RBAC lookup";
    } catch (const std::exception&) {

    }
}

/**
 * A client who authenticated itself by using a certificate should not
 * be able to change it's identity by running SASL.
 */
TEST_F(SslCertTest, LoginWhenMandatoryWithCertShouldNotSupportSASL) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");

    auto connection = createConnection();
    ASSERT_TRUE(connection) << "Failed to locate a SSL port";
    setClientCertData(*connection, "trond");
    connection->connect();
    connection->setXerrorSupport(true);

    try {
        connection->authenticate("@admin", "password", "PLAIN");
        FAIL() << "SASL Auth should be disabled for cert auth'd connections";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotSupported())
                << "Received: " << to_string(error.getReason());
    }
}

#ifndef __APPLE__
/// The following test tries to use the TLS stack from golang to connect
/// to the server and establish a connection and authenticate with the
/// client certificate.
TEST_F(SslCertTest, LoginWhenMandatoryGoClient) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");

    auto connection = createConnection();
    std::vector<std::string> argv = {
            {SOURCE_ROOT "/tests/gocode/bin/tls_test"},
            {"-kv"},
            {"localhost:" + std::to_string(connection->getPort())},
            {"-clientKey"},
            {OBJECT_ROOT "/tests/cert/clients/trond.key"},
            {"-clientCert"},
            {OBJECT_ROOT "/tests/cert/clients/trond.cert"},
            {"-rootCA"},
            {OBJECT_ROOT "/tests/cert/root/ca_root.cert"},
            {"-skipCertVerify"},
            {"false"}};

    nlohmann::json json;
    std::string status;
    auto child = ProcessMonitor::create(argv, [&status, &json](auto& ec) {
        json = ec.to_json();
        status =  ec.to_string();
    });

    while (child->isRunning()) {
        std::this_thread::sleep_for(std::chrono::seconds{1});
    }

    EXPECT_EQ("Success", status) << json.dump(2);
}
#endif

/// Verify we can't connect with the client certificate if the client
/// don't have the certificate in the trusted certificate store.
TEST_F(SslCertTest, MB50564_intermediate_cert_not_in_trusted_store) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");

    auto connection = createConnection();
    ASSERT_TRUE(connection) << "Failed to locate a SSL port";
    setClientCertData(*connection, "jane");
    connection->connect();
    try {
        connection->setXerrorSupport(true);
        FAIL() << "Server should not accept client";
    } catch (const std::system_error&) {
    }
}

/// Verify that the client can connect and authenticate if it tries
/// to use the certificate chain (including the intermediate certificate
/// used to sign both the client and server certificate)
TEST_F(SslCertTest, MB50564_intermediate_cert_not_in_trusted_store_using_chain) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");

    auto connection = createConnection();
    ASSERT_TRUE(connection) << "Failed to locate a SSL port";
    const auto certfile = boost::filesystem::path(OBJECT_ROOT) / "tests" /
                        "cert" / "clients" / "jane_chain.pem";
    setClientCertData(*connection, "jane");
    connection->setSslCertFile(certfile.generic_string());
    connection->connect();
    connection->setXerrorSupport(true);
}

/// Verify that the client can connect if it put both the root ca and
/// the intermediate certificate in the trusted store.
TEST_F(SslCertTest, MB50564_intermediate_cert_in_trusted_store) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    auto connection = createConnection();
    ASSERT_TRUE(connection) << "Failed to locate a SSL port";

    // Put the intermediate certificate and the root certificate in the
    // trusted ca store
    const auto cafile = boost::filesystem::path(OBJECT_ROOT) / "tests" /
                        "cert" / "intermediate" / "client_intermediate_ca.pem";
    setClientCertData(*connection, "jane");
    connection->setCaFile(cafile.generic_string());
    connection->connect();
    connection->setXerrorSupport(true);
}
