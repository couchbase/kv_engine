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

/**
 * This file contains tests related to RBAC
 */

#include "testapp.h"
#include "testapp_client_test.h"
#include <protocol/connection/client_greenstack_connection.h>
#include <protocol/connection/client_mcbp_connection.h>

#include <algorithm>
#include <platform/compress.h>

class RbacTest : public TestappClientTest {
public:
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        RbacTest,
                        ::testing::Values(TransportProtocols::McbpPlain),
                        ::testing::PrintToStringParamName());


/**
 * Memcached supports authentication through external systems (like LDAP),
 * but these users may not be defined as a user within Couchbase. Such
 * users should fail with an Access Denied error instead of Authentication
 * Success even if the username password combination is correct.
 */
TEST_P(RbacTest, DontAllowUnknownUsers) {
    auto& conn = getConnection();
    try {
        conn.authenticate("sharon", "sharonpw", "PLAIN");
        FAIL() << "Users without an RBAC profile should not be allowed access";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied()) << error.what();
    }
}

TEST_P(RbacTest, ReloadRbacData_HaveAccess) {
    auto& conn = reinterpret_cast<MemcachedBinprotConnection&>(getConnection());
    conn.authenticate("_admin", "password", "PLAIN");
    BinprotGenericCommand cmd(PROTOCOL_BINARY_CMD_RBAC_REFRESH, {}, {});
    conn.sendCommand(cmd);

    BinprotResponse resp;
    conn.recvResponse(resp);
    EXPECT_TRUE(resp.isSuccess());
}

TEST_P(RbacTest, ReloadRbacData_NoAccess) {
    auto& conn = reinterpret_cast<MemcachedBinprotConnection&>(getConnection());
    conn.reconnect();
    conn.setXerrorSupport(true);
    BinprotGenericCommand cmd(PROTOCOL_BINARY_CMD_RBAC_REFRESH, {}, {});
    conn.sendCommand(cmd);

    BinprotResponse resp;
    conn.recvResponse(resp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EACCESS, resp.getStatus());
}

TEST_P(RbacTest, ReloadSasl_HaveAccess) {
    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getAdminConnection());
    BinprotGenericCommand cmd(PROTOCOL_BINARY_CMD_ISASL_REFRESH);
    BinprotResponse resp;

    conn.sendCommand(cmd);
    conn.recvResponse(resp);
    EXPECT_TRUE(resp.isSuccess());
}

TEST_P(RbacTest, ReloadSasl_NoAccess) {
    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    BinprotGenericCommand cmd(PROTOCOL_BINARY_CMD_ISASL_REFRESH);

    conn.sendCommand(cmd);
    BinprotResponse resp;
    conn.recvResponse(resp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EACCESS, resp.getStatus());
}

TEST_P(RbacTest, ScrubNoAccess) {
    auto& c = dynamic_cast<MemcachedBinprotConnection&>(getConnection());

    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_SCRUB);
    BinprotResponse response;

    c.sendCommand(command);
    c.recvResponse(response);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EACCESS, response.getStatus());
}

TEST_P(RbacTest, Scrub) {
    auto& c = dynamic_cast<MemcachedBinprotConnection&>(getAdminConnection());

    c.selectBucket("default");
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_SCRUB);
    BinprotResponse response;

    do {
        // Retry if scrubber is already running.
        c.sendCommand(command);
        c.recvResponse(response);
    } while (response.getStatus() == PROTOCOL_BINARY_RESPONSE_EBUSY);

    EXPECT_TRUE(response.isSuccess());
}
