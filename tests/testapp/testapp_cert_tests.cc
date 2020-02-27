/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <ctype.h>
#include <errno.h>
#include <evutil.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <string>

#include <folly/portability/GTest.h>
#include <platform/dirutils.h>

#include "memcached/openssl.h"
#include "testapp.h"

/**
 * This test suite tests the various settings for authenticating over SSL
 * with client certificates. It does not test all of the various paths one
 * may configure as that is tested in the unit tests for the parsing of the
 * configuration.
 */

class SslCertTest : public TestappTest {
};

/**
 * Setting the control to "disable" simply means that the server don't
 * even ask the client to provide a certificate, and if it end up providing
 * one it'll be ignored.
 *
 * Setting the control to "enable" means that we ask the client to provide
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
    reconfigure_client_cert_auth("disable", "", "", "");

    MemcachedConnection connection("127.0.0.1", ssl_port, AF_INET, true);
    connection.connect();
    connection.authenticate("@admin", "password", "PLAIN");
}

/**
 * When disabled we don't look at the certificate so it should be possible
 * to connect with one even if it doesn't map to a user (we've not defined
 * a user mapping).
 */
TEST_F(SslCertTest, LoginWhenDiabledWithCert) {
    reconfigure_client_cert_auth("disable", "", "", "");

    MemcachedConnection connection("127.0.0.1", ssl_port, AF_INET, true);
    setClientCertData(connection);
    connection.connect();
    connection.authenticate("@admin", "password", "PLAIN");
}

/**
 * When set to enabled we allow the user to connect even if it no certificate
 * is provided
 */
TEST_F(SslCertTest, LoginEnabledWithoutCert) {
    reconfigure_client_cert_auth("enable", "subject.cn", "", " ");

    MemcachedConnection connection("127.0.0.1", ssl_port, AF_INET, true);
    connection.connect();
    connection.authenticate("@admin", "password", "PLAIN");
}

/**
 * It should be possible to connect with a certificate when there is no
 * mapping defined on the system (only the client certificate is validated)
 */
TEST_F(SslCertTest, LoginEnabledWithCertNoMapping) {
    reconfigure_client_cert_auth("enable", "", "", " ");

    MemcachedConnection connection("127.0.0.1", ssl_port, AF_INET, true);
    setClientCertData(connection);
    connection.connect();
    connection.authenticate("@admin", "password", "PLAIN");
}

/**
 * It should be possible to connect with a certificate which maps to a user.
 * The connection is not bound to a bucket so the client needs to explicitly
 * run select bucket to in order to perform operations.
 */
TEST_F(SslCertTest, LoginEnabledWithCert) {
    reconfigure_client_cert_auth("enable", "subject.cn", "", " ");

    MemcachedConnection connection("127.0.0.1", ssl_port, AF_INET, true);
    setClientCertData(connection);
    connection.connect();
    connection.setXerrorSupport(true);

    try {
        connection.get("foo", Vbid(0));
        FAIL() << "Should not be associated with a bucket";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied())
                << "Received: " << to_string(error.getReason());
    }

    connection.selectBucket("default");
    try {
        connection.get("foo", Vbid(0));
        FAIL() << "document should not exists";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound())
                << "Received: " << to_string(error.getReason());
    }
}

/**
 * When the setting is set to mandatory a client certificate _HAS_ to be
 * provided in order to allow the connection to succeed.
 */
TEST_F(SslCertTest, LoginWhenMandatoryWithoutCert) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");

    MemcachedConnection connection("127.0.0.1", ssl_port, AF_INET, true);
    try {
        connection.connect();
        connection.hello("LoginWhenMandatoryWithoutCert", "1.0", "");
        FAIL() << "It should not be possible to connect without certificate";
    } catch (const std::exception&) {
    }
}

/**
 * Verify that we may log into the system when we provide a certificate,
 * and that we're not automatically bound to a bucket (an explicit select
 * bucket is needed).
 */
TEST_F(SslCertTest, LoginWhenMandatoryWithCert) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");

    MemcachedConnection connection("127.0.0.1", ssl_port, AF_INET, true);
    setClientCertData(connection);
    connection.connect();
    connection.setXerrorSupport(true);

    try {
        connection.get("foo", Vbid(0));
        FAIL() << "Should not be associated with a bucket";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied())
                << "Received: " << to_string(error.getReason());
    }

    connection.selectBucket("default");
    try {
        connection.get("foo", Vbid(0));
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
    reconfigure_client_cert_auth("mandatory", "subject.cn", "Tr", "");

    MemcachedConnection connection("127.0.0.1", ssl_port, AF_INET, true);
    setClientCertData(connection);

    // The certificate will be accepted, so the connection is established
    // but the server will disconnect the client immediately
    connection.connect();

    // Try to run a hello (should NOT work)
    try {
        connection.setXerrorSupport(true);
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

    MemcachedConnection connection("127.0.0.1", ssl_port, AF_INET, true);
    setClientCertData(connection);
    connection.connect();
    connection.setXerrorSupport(true);

    try {
        connection.authenticate("@admin", "password", "PLAIN");
        FAIL() << "SASL Auth should be disabled for cert auth'd connections";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotSupported())
                << "Received: " << to_string(error.getReason());
    }
}
