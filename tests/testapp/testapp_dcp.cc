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
 * In this file you'll find unit tests related to the DCP subsystem
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <xattr/blob.h>
#include <xattr/utils.h>

class DcpTest : public TestappClientTest {

};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        DcpTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpSsl),
                        ::testing::PrintToStringParamName());

TEST_P(DcpTest, TestDcpOpenCantBeProducerAndConsumer) {
    auto& conn = getConnection();

    conn.sendCommand(BinprotDcpOpenCommand{"ewb_internal:1", 0,
                                           DCP_OPEN_PRODUCER |
                                           DCP_OPEN_NOTIFIER});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
}

TEST_P(DcpTest, TestDcpNotfierCantBeNoValue) {
    auto& conn = getConnection();

    conn.sendCommand(BinprotDcpOpenCommand{"ewb_internal:1", 0,
                                           DCP_OPEN_NO_VALUE |
                                           DCP_OPEN_NOTIFIER});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
}

TEST_P(DcpTest, TestDcpNotfierCantIncludeXattrs) {
    auto& conn = getConnection();

    conn.sendCommand(BinprotDcpOpenCommand{"ewb_internal:1", 0,
                                           DCP_OPEN_INCLUDE_XATTRS |
                                           DCP_OPEN_NOTIFIER});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
}

/**
 * Make sure that the rollback sequence number in the response isn't being
 * stripped / replaced with an error object
 */
TEST_P(DcpTest, MB24145_RollbackShouldContainSeqno) {
    auto& conn = getConnection();

    conn.sendCommand(BinprotDcpOpenCommand{"ewb_internal:1", 0,
                                           DCP_OPEN_PRODUCER});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    ASSERT_TRUE(rsp.isSuccess());

    BinprotDcpStreamRequestCommand streamReq;
    streamReq.setDcpStartSeqno(1);
    conn.sendCommand(streamReq);
    conn.recvResponse(rsp);
    ASSERT_FALSE(rsp.isSuccess());

    auto data = rsp.getData();
    ASSERT_EQ(sizeof(uint64_t), data.size());
    auto* value = reinterpret_cast<const uint64_t*>(data.data());
    EXPECT_EQ(0, *value);

}

TEST_P(DcpTest, UnorderedExecutionNotSupported) {
    // Verify that it isn't possible to run a DCP open command
    // on a connection which is set to unordered execution mode.
    // Ideally we should have verified each of the available DCP
    // packets, but our test framework does not have methods to
    // create all of them. The DCP validators does however
    // all call a common validator method to check this
    // restriction. Once the full DCP test suite is implemented
    // we should extend this test to validate all of the
    // various commands.
    auto& conn = getConnection();
    conn.setUnorderedExecutionMode(ExecutionMode::Unordered);
    conn.sendCommand(BinprotDcpOpenCommand{"ewb_internal:1", 0,
                                           DCP_OPEN_PRODUCER});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus());
}
