/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "testapp.h"
#include "testapp_client_test.h"

// Test fixture for new MCBP miscellaneous commands
class MiscTest : public TestappClientTest {};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        MiscTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpIpv6Plain,
                                          TransportProtocols::McbpSsl,
                                          TransportProtocols::McbpIpv6Ssl),
                        ::testing::PrintToStringParamName());

TEST_P(MiscTest, GetFailoverLog) {
    TESTAPP_SKIP_IF_UNSUPPORTED(PROTOCOL_BINARY_CMD_GET_FAILOVER_LOG);

    auto& connection = getConnection();
    auto response = connection.getFailoverLog(0 /*vbid*/);
    auto header = response.getHeader().response;

    EXPECT_EQ(header.magic, PROTOCOL_BINARY_RES);
    EXPECT_EQ(header.opcode, PROTOCOL_BINARY_CMD_GET_FAILOVER_LOG);
    EXPECT_EQ(header.keylen, 0);
    EXPECT_EQ(header.extlen, 0);
    EXPECT_EQ(header.datatype, 0);
    EXPECT_EQ(header.status, 0);
    // Note: We expect 1 entry in the failover log, which is the entry created
    // at VBucket creation (8 bytes for UUID + 8 bytes for SEQNO)
    EXPECT_EQ(ntohl(header.bodylen), 0x10);
    EXPECT_EQ(header.cas, 0);

    EXPECT_EQ(response.getData().len, 0x10);
}
