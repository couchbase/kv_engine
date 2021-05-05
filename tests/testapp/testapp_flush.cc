/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <array>

class FlushTest : public TestappClientTest {
protected:
    void SetUp() override {
        TestappClientTest::SetUp();
        // Store our lone document
        Document doc;
        doc.info.datatype = cb::mcbp::Datatype::Raw;
        doc.info.cas = 0;
        doc.info.flags = 0;
        doc.info.id = name;
        getConnection().mutate(doc, Vbid{0}, MutationType::Set);
    }

    cb::mcbp::Status get(MemcachedConnection& conn) {
        BinprotGetCommand cmd;
        cmd.setKey(name);
        return conn.execute(cmd).getStatus();
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         FlushTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(FlushTest, Flush) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    auto& conn = getAdminConnection();
    conn.selectBucket("default");
    ASSERT_EQ(cb::mcbp::Status::Success, get(conn));
    const auto response =
            conn.execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Flush});
    ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, get(conn));
}

TEST_P(FlushTest, FlushQ) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    auto& conn = getAdminConnection();
    conn.selectBucket("default");
    ASSERT_EQ(cb::mcbp::Status::Success, get(conn));
    conn.sendCommand(BinprotGenericCommand{cb::mcbp::ClientOpcode::Flushq});
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, get(conn));
}

TEST_P(FlushTest, FlushWithExtlen) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    auto& conn = getAdminConnection();
    conn.selectBucket("default");
    ASSERT_EQ(cb::mcbp::Status::Success, get(conn));

    BinprotGenericCommand command(cb::mcbp::ClientOpcode::Flush);
    command.setExtrasValue(uint32_t(htonl(0)));

    // Ensure it still works
    const auto response = conn.execute(command);
    ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, get(conn));
}

TEST_P(FlushTest, FlushQWithExtlen) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    auto& conn = getAdminConnection();
    conn.selectBucket("default");
    ASSERT_EQ(cb::mcbp::Status::Success, get(conn));
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::Flushq);
    command.setExtrasValue(static_cast<uint32_t>(htonl(0)));
    conn.sendCommand(command);

    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, get(conn));
}

TEST_P(FlushTest, DelayedFlushNotSupported) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    auto& conn = getAdminConnection();
    conn.selectBucket("default");

    std::array<cb::mcbp::ClientOpcode, 2> commands{
            {cb::mcbp::ClientOpcode::Flush, cb::mcbp::ClientOpcode::Flushq}};
    for (auto op : commands) {
        BinprotGenericCommand command{op};
        command.setExtrasValue(static_cast<uint32_t>(htonl(2)));
        const auto response = conn.execute(command);
        ASSERT_EQ(cb::mcbp::Status::NotSupported, response.getStatus());
    }
}
