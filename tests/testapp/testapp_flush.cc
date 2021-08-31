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
        userConnection->mutate(doc, Vbid{0}, MutationType::Set);
        adminConnection->selectBucket(bucketName);
    }

    void TearDown() override {
        adminConnection->unselectBucket();
    }

    cb::mcbp::Status getDocument() {
        BinprotGetCommand cmd;
        cmd.setKey(name);
        return userConnection->execute(cmd).getStatus();
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         FlushTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(FlushTest, Flush) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    ASSERT_EQ(cb::mcbp::Status::Success, getDocument());
    const auto response = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Flush});
    ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, getDocument());
}

TEST_P(FlushTest, FlushQ) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    ASSERT_EQ(cb::mcbp::Status::Success, getDocument());
    adminConnection->sendCommand(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Flushq});
    adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, getDocument());
}

TEST_P(FlushTest, FlushWithExtlen) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    ASSERT_EQ(cb::mcbp::Status::Success, getDocument());

    BinprotGenericCommand command(cb::mcbp::ClientOpcode::Flush);
    command.setExtrasValue(uint32_t(htonl(0)));

    // Ensure it still works
    const auto response = adminConnection->execute(command);
    ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, getDocument());
}

TEST_P(FlushTest, FlushQWithExtlen) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    ASSERT_EQ(cb::mcbp::Status::Success, getDocument());
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::Flushq);
    command.setExtrasValue(static_cast<uint32_t>(htonl(0)));
    adminConnection->sendCommand(command);
    adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});

    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, getDocument());
}

TEST_P(FlushTest, DelayedFlushNotSupported) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    std::array<cb::mcbp::ClientOpcode, 2> commands{
            {cb::mcbp::ClientOpcode::Flush, cb::mcbp::ClientOpcode::Flushq}};
    for (auto op : commands) {
        BinprotGenericCommand command{op};
        command.setExtrasValue(static_cast<uint32_t>(htonl(2)));
        const auto response = adminConnection->execute(command);
        ASSERT_EQ(cb::mcbp::Status::NotSupported, response.getStatus());
    }
}
