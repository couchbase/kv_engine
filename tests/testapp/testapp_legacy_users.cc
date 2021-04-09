/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "testapp_client_test.h"

#include <algorithm>
#include <platform/compress.h>

/**
 * In order to support a smooth upgrade process to Spock where we introduced
 * RBAC ns_server creates a "legacy user" to represent the old "bucket user",
 * so that the user can still create a new "normal" user with the same name
 * as a bucket. This means that the system can have two users named "trond"
 * with different passwords. Internally these users are named:
 *
 *    "trond;legacy" - which is the old bucket user
 *    "trond" - which is the new user
 *
 * The legacy users is _ONLY_ available using the PLAIN sasl mechanism
 * (this is because it is used by XDCR which only support that mechanism)
 *
 * This test bach verifies that we can log in as these two users with
 * a different password, and that the legacy user is only available
 * when using PLAIN SASL mechanism.
 */
class LegacyUsersTest : public TestappClientTest {
protected:
    const std::string username{"legacy"};
    const std::string legacy_password{"legacy"};
    const std::string new_password{"new"};
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         LegacyUsersTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

/**
 * Verify that we can authenticate to the server by using the password
 * used by the legacy user over PLAIN authentication.
 */
TEST_P(LegacyUsersTest, LoginAsLegacyUserPlain) {
    auto& conn = getConnection();
    conn.authenticate(username, legacy_password, "PLAIN");
}

/**
 * Verify that you can log in by using PLAIN auth if you specify the entire
 * legacy name "username;legacy". In theory it should work across all
 * mechanisms iff ns_server generates hashed password entries for
 * each mechanism.
 */
TEST_P(LegacyUsersTest, LoginAsLegacyUserWithFullUsername) {
    auto& conn = getConnection();
    conn.authenticate(username + ";legacy", legacy_password, "PLAIN");
}

/**
 * Verify that we can authenticate to the server by using the password
 * used by the new user over PLAIN authentication.
 */
TEST_P(LegacyUsersTest, LoginAsNewUserPlain) {
    auto& conn = getConnection();
    conn.authenticate(username, new_password, "PLAIN");
}

/**
 * Verify that we can't authenticate to the server by using the password
 * used by the legacy user over SCRAM.
 */
TEST_P(LegacyUsersTest, LoginAsLegacyUserScram) {
    auto& conn = getConnection();
    try {
        conn.authenticate(username, legacy_password, "SCRAM-SHA1");
        FAIL() << "It should not be possible to auth legacy users over SCRAM";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAuthError());
    }
}

/**
 * Verify that we can authenticate to the server by using the password
 * used by the legacy user by using SCRAM-SHA1.
 */
TEST_P(LegacyUsersTest, LoginAsNewUserScram) {
    auto& conn = getConnection();
    conn.authenticate(username, new_password, "SCRAM-SHA1");
}
