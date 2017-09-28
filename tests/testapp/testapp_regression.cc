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

#include <protocol/connection/client_mcbp_connection.h>
#include "testapp.h"
#include "testapp_client_test.h"

#include <platform/compress.h>
#include <algorithm>

class RegressionTest : public TestappClientTest {
protected:
    MemcachedBinprotConnection& getMcbpConnection() {
        return dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    }
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        RegressionTest,
                        ::testing::Values(TransportProtocols::McbpPlain),
                        ::testing::PrintToStringParamName());

/**
 * MB-26196: A client without xerror may still receive extended error
 *           codes instead of silently disconnect.
 */
TEST_P(RegressionTest, MB_26196) {
    auto& conn = getMcbpConnection();

    conn.authenticate("jones", "jonespassword", "PLAIN");

    BinprotGenericCommand cmd{PROTOCOL_BINARY_CMD_GET_CLUSTER_CONFIG, "", ""};
    BinprotResponse response;
    conn.executeCommand(cmd, response);
    EXPECT_FALSE(response.isSuccess());
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NO_BUCKET, response.getStatus());

    // Disable xerror
    conn.setXerrorSupport(false);

    // rerun get cluster config, and this time I should be disconnected.
    conn.sendCommand(cmd);
    try {
        conn.recvResponse(response);
        FAIL() << "Non-xerror aware clients should be disconnected";
    } catch (const std::system_error& e) {
        EXPECT_EQ(std::errc::connection_reset, std::errc(e.code().value()));
    } catch (...) {
        FAIL() << "Expected system error to be thrown";
    }
}
